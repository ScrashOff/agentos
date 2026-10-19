import type {SupervisorOwnerLease} from './supervisor-owner-lease.js';
type DB={query:(sql:string,args?:any[])=>Promise<any>};
type Options={transaction:<T>(fn:(db:DB)=>Promise<T>)=>Promise<T>;execute:(runId:string)=>Promise<unknown>;onError:()=>Promise<unknown>;acquireLease:(runId:string)=>Promise<SupervisorOwnerLease|null>};
const receiptKey=(runId:string)=>'supervisor-owner:'+runId;
const liveOccupancy="(r.state IN('running','cancelling','reconciling') OR (j.state='running' AND r.state='queued'))";
/** Dispatch admitted jobs only. Session leases fence claims and finalization;
 * canonical startRun's queued CAS remains the last fence before model effects. */
export class SupervisorDispatchPool {
 private active=new Map<string,Promise<void>>();private pump:Promise<void>|undefined;private closing=false;
 constructor(private options:Options){}
 async dispatch(){if(this.closing)return;if(this.pump)return this.pump;this.pump=this.fill().catch(async()=>{await this.options.onError().catch(()=>{});}).finally(()=>{this.pump=undefined;});return this.pump;}
 private async recoverNeverStarted(){
  await this.options.transaction(async db=>{
   const rows=(await db.query(`SELECT j.run_id FROM supervisor_jobs j JOIN runs r ON r.id=j.run_id
    JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id JOIN settings s ON s.id='supervisor-owner:'||r.id
    WHERE j.state='running' AND r.state='queued' AND t.state='queued' AND g.state='active'
      AND r.attempt=t.attempt_count AND r.worker_id IS NULL AND r.started_at IS NULL AND r.context_id IS NULL
      AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='RUN_STARTED' AND e.payload->>'runId'=r.id::text)
      AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND (e.payload->>'runId'=r.id::text OR e.entity_id IN(r.id::text,t.id::text)))
      AND s.value->>'version'='1' AND s.value->>'phase' IN('claimed','dispatching')
      AND s.value->>'runId'=r.id::text AND s.value->>'runFence'=r.fence::text AND s.value->>'taskId'=t.id::text
      AND s.value->>'goalId'=t.goal_id::text AND s.value->>'attempt'=r.attempt::text
      AND s.value->>'ownerNonce' ~ '^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
      AND s.value->>'backendPid' ~ '^[1-9][0-9]*$'
    ORDER BY r.created_at,r.id FOR UPDATE OF j LIMIT 16`)).rows;
   for(const row of rows){const lease=await this.options.acquireLease(row.run_id);if(!lease)continue;
    try{if(!await lease.isAlive())continue;await db.query("UPDATE supervisor_jobs SET state='queued' WHERE run_id=$1 AND state='running'",[row.run_id]);await db.query("UPDATE settings SET value=value||'{\"phase\":\"recovered_never_started\"}'::jsonb WHERE id=$1",[receiptKey(row.run_id)]);}finally{await lease.release();}
   }
  });
 }
 private async fill(){
  await this.recoverNeverStarted();const slots=Math.max(0,2-this.active.size),attempted:string[]=[];
  for(let slot=0;slot<slots&&!this.closing;slot++){
   let acquired:SupervisorOwnerLease|null=null;let row:any;
   try{row=await this.options.transaction(async db=>{
    const count=(await db.query(`SELECT count(*)::int n FROM supervisor_jobs j JOIN runs r ON r.id=j.run_id WHERE ${liveOccupancy}`)).rows[0].n;if(count>=2)return null;
    const job=(await db.query(`SELECT j.run_id,r.fence,r.attempt,r.task_id,t.goal_id FROM supervisor_jobs j JOIN runs r ON r.id=j.run_id JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
      WHERE j.state='queued' AND r.state='queued' AND t.state='queued' AND g.state='active' AND r.attempt=t.attempt_count
      AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND (e.payload->>'runId'=r.id::text OR e.entity_id IN(r.id::text,t.id::text)))
      AND r.id<>ALL($1::uuid[]) AND NOT EXISTS(
        SELECT 1 FROM supervisor_jobs occupied JOIN runs owner ON owner.id=occupied.run_id JOIN tasks ot ON ot.id=owner.task_id
        WHERE (owner.state IN('running','cancelling','reconciling') OR (occupied.state='running' AND owner.state='queued')) AND ot.goal_id=t.goal_id)
      ORDER BY r.created_at,r.id FOR UPDATE OF j SKIP LOCKED LIMIT 1`,[attempted])).rows[0];if(!job)return null;
    acquired=await this.options.acquireLease(job.run_id);if(!acquired||!await acquired.isAlive())return null;
    await db.query("INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value",[receiptKey(job.run_id),JSON.stringify({version:1,phase:'claimed',runId:job.run_id,runFence:job.fence,attempt:job.attempt,taskId:job.task_id,goalId:job.goal_id,ownerNonce:acquired.ownerNonce,backendPid:acquired.backendPid})]);
    const claimed=await db.query("UPDATE supervisor_jobs SET state='running' WHERE run_id=$1 AND state='queued' RETURNING run_id",[job.run_id]);return claimed.rowCount===1?job:null;
   });}catch(e){if(acquired)await (acquired as SupervisorOwnerLease).release();throw e;}
   if(!row){if(acquired)await (acquired as SupervisorOwnerLease).release();break;}
   const lease=acquired! as SupervisorOwnerLease;attempted.push(row.run_id);const work=this.perform(row.run_id,lease).finally(()=>{this.active.delete(row.run_id);});this.active.set(row.run_id,work);
  }
 }
 private async perform(runId:string,lease:SupervisorOwnerLease){
  try{
   if(!await lease.isAlive())return;
   const owned=await this.options.transaction(async db=>(await db.query("UPDATE settings SET value=value||'{\"phase\":\"dispatching\"}'::jsonb WHERE id=$1 AND value->>'ownerNonce'=$2 RETURNING id",[receiptKey(runId),lease.ownerNonce])).rowCount===1);
   if(!owned||!await lease.isAlive())return;await this.options.execute(runId);if(!await lease.isAlive())return;
   await this.options.transaction(async db=>{if(!await lease.isAlive())return;
    await db.query(`UPDATE supervisor_jobs j SET state=CASE WHEN r.state='queued' THEN 'queued' ELSE 'completed' END
      FROM runs r,settings s WHERE j.run_id=$1 AND r.id=j.run_id AND j.state='running' AND s.id=$2 AND s.value->>'ownerNonce'=$3
      AND r.state IN('queued','submitted','failed','cancelled')`,[runId,receiptKey(runId),lease.ownerNonce]);
   });
  }catch{await this.options.onError().catch(()=>{});}finally{await lease.release();}
 }
 async close(){this.closing=true;await this.pump;await Promise.allSettled([...this.active.values()]);}
}
