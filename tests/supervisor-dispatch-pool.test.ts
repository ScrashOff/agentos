import {beforeAll,afterAll,beforeEach,it,expect} from 'vitest';
import pg from 'pg';
import {randomUUID} from 'node:crypto';
import {SupervisorDispatchPool} from '../server/supervisor-dispatch-pool.js';
import {supervisorLeaseAcquirer,type SupervisorOwnerLease} from '../server/supervisor-owner-lease.js';
const schema='dispatch_'+randomUUID().replaceAll('-','');
let db:pg.Pool,admin:pg.Pool;
const jobs:string[]=[];
beforeAll(async()=>{
 admin=new pg.Pool({connectionString:process.env.DATABASE_URL});await admin.query(`CREATE SCHEMA ${schema}`);
 db=new pg.Pool({connectionString:process.env.DATABASE_URL,options:`-c search_path=${schema}`});
 // Exercise production claim/finalization SQL against PostgreSQL with exact
 // relevant descriptors, isolated from the controller's canonical tables.
 await db.query('CREATE TABLE goals(id uuid PRIMARY KEY,state text NOT NULL);CREATE TABLE tasks(id uuid PRIMARY KEY,goal_id uuid NOT NULL,state text NOT NULL,attempt_count int NOT NULL);CREATE TABLE runs(id uuid PRIMARY KEY,task_id uuid NOT NULL REFERENCES tasks(id),state text NOT NULL,created_at timestamptz NOT NULL,fence uuid NOT NULL,attempt int NOT NULL,worker_id text,started_at timestamptz,context_id uuid);CREATE TABLE supervisor_jobs(run_id uuid PRIMARY KEY REFERENCES runs(id),state text NOT NULL);CREATE TABLE settings(id text PRIMARY KEY,value jsonb NOT NULL);CREATE TABLE events(kind text,payload jsonb,entity_id text)');
});
afterAll(async()=>{await db.end();await admin.query(`DROP SCHEMA ${schema} CASCADE`);await admin.end();});
beforeEach(async()=>{await db.query('TRUNCATE goals,tasks,settings,events CASCADE');jobs.length=0;});
async function enqueue(goal:string,age:number){const task=randomUUID(),run=randomUUID();await db.query("INSERT INTO goals VALUES($1,'active') ON CONFLICT DO NOTHING",[goal]);await db.query("INSERT INTO tasks VALUES($1,$2,'queued',1)",[task,goal]);await db.query("INSERT INTO runs(id,task_id,state,created_at,fence,attempt) VALUES($1,$2,'queued',to_timestamp($3),$4,1)",[run,task,age,randomUUID()]);await db.query("INSERT INTO supervisor_jobs VALUES($1,'queued')",[run]);return run;}
async function transaction<T>(fn:(client:pg.PoolClient)=>Promise<T>){const client=await db.connect();try{await client.query('BEGIN');await client.query('SELECT pg_advisory_xact_lock(4280001)');const value=await fn(client);await client.query('COMMIT');return value;}catch(e){await client.query('ROLLBACK');throw e;}finally{client.release();}}
function deferred(){let resolve!:()=>void;const promise=new Promise<void>(r=>resolve=r);return {promise,resolve};}
function dispatcher(execute:(run:string)=>Promise<unknown>,acquireLease=supervisorLeaseAcquirer(db),trans=transaction){return new SupervisorDispatchPool({acquireLease,transaction:trans,execute,onError:async()=>{}});}
const flush=()=>new Promise(r=>setTimeout(r,20));
async function abandonedClaim(run:string){
 const row=(await db.query('SELECT r.*,t.goal_id FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1',[run])).rows[0],lease=(await supervisorLeaseAcquirer(db)(run))!;
 await db.query("UPDATE supervisor_jobs SET state='running' WHERE run_id=$1",[run]);await db.query('INSERT INTO settings VALUES($1,$2)',['supervisor-owner:'+run,JSON.stringify({version:1,phase:'claimed',runId:run,runFence:row.fence,taskId:row.task_id,goalId:row.goal_id,attempt:row.attempt,ownerNonce:lease.ownerNonce,backendPid:lease.backendPid})]);
 await admin.query('SELECT pg_terminate_backend($1)',[lease.backendPid]);await lease.isAlive();await lease.release();return row;
}
it('overlapping owners atomically claim at most two, one per goal, FIFO across eligible goals',async()=>{
 const goal=randomUUID(),other=randomUUID(),third=randomUUID();const oldest=await enqueue(goal,1),same=await enqueue(goal,2),next=await enqueue(other,3);await enqueue(third,4);
 const gate=deferred();const execute=async(run:string)=>{jobs.push(run);await gate.promise;await db.query("UPDATE runs SET state='submitted' WHERE id=$1",[run]);};
 const a=dispatcher(execute),b=dispatcher(execute);await Promise.all([a.dispatch(),b.dispatch(),a.dispatch()]);await flush();
 expect(jobs).toHaveLength(2);expect(new Set(jobs)).toEqual(new Set([oldest,next]));expect(jobs).not.toContain(same);
 expect((await db.query("SELECT count(*)::int n FROM supervisor_jobs WHERE state='running'")).rows[0].n).toBe(2);
 gate.resolve();await Promise.all([a.close(),b.close()]);expect((await db.query("SELECT count(*)::int n FROM supervisor_jobs WHERE state='completed'")).rows[0].n).toBe(2);
});
it('refused start remains queued and is never falsely completed',async()=>{
 const run=await enqueue(randomUUID(),1);const d=dispatcher(async()=>{});await d.dispatch();await d.close();expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[run])).rows[0].state).toBe('queued');
});
it('ambiguous start exception keeps claimed ownership, no blind retry',async()=>{
 const run=await enqueue(randomUUID(),1);let starts=0;const d=dispatcher(async()=>{starts++;await db.query("UPDATE runs SET state='reconciling',worker_id='started' WHERE id=$1",[run]);throw Error('AMBIGUOUS_START');});await d.dispatch();await flush();await d.dispatch();await d.close();expect(starts).toBe(1);expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[run])).rows[0].state).toBe('running');
});
it('reconciling executor outcome is not completed and blocks another same-goal start',async()=>{
 const goal=randomUUID(),run=await enqueue(goal,1);await enqueue(goal,2);const d=dispatcher(async id=>{jobs.push(id);await db.query("UPDATE runs SET state='reconciling' WHERE id=$1",[id]);});await d.dispatch();await flush();await d.dispatch();await d.close();expect(jobs).toEqual([run]);expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[run])).rows[0].state).toBe('running');
});
it('shutdown waits every pending callback and does not admit queued work',async()=>{
 await enqueue(randomUUID(),1);await enqueue(randomUUID(),2);await enqueue(randomUUID(),3);const gate=deferred();const d=dispatcher(async id=>{jobs.push(id);await gate.promise;await db.query("UPDATE runs SET state='submitted' WHERE id=$1",[id]);});await d.dispatch();await flush();let closed=false;const close=d.close().then(()=>closed=true);await flush();expect(closed).toBe(false);await d.dispatch();expect(jobs).toHaveLength(2);gate.resolve();await close;expect(closed).toBe(true);expect(jobs).toHaveLength(2);
});
it('historical terminal claims do not consume live slots or goal affinity and are not rewritten',async()=>{
 const goal=randomUUID(),historical=await enqueue(goal,1);
 await db.query("UPDATE runs SET state='submitted' WHERE id=$1",[historical]);await db.query("UPDATE supervisor_jobs SET state='running' WHERE run_id=$1",[historical]);
 const a=await enqueue(goal,2),b=await enqueue(randomUUID(),3);const gate=deferred();const d=dispatcher(async id=>{jobs.push(id);await gate.promise;await db.query("UPDATE runs SET state='submitted' WHERE id=$1",[id]);});
 await d.dispatch();await flush();expect(jobs).toEqual([a,b]);gate.resolve();await d.close();expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[historical])).rows[0].state).toBe('running');
});
it('connection lost after claim before execute recovers same admitted run/attempt once',async()=>{
 const run=await enqueue(randomUUID(),1),before=(await db.query('SELECT id,attempt,fence FROM runs WHERE id=$1',[run])).rows[0];
 const acquire=supervisorLeaseAcquirer(db);let killed=false;
 const broken=async(id:string)=>{const lease=await acquire(id);if(!lease)return null;let probes=0;return {...lease,isAlive:async()=>{probes++;if(!killed&&probes===2){killed=true;await admin.query('SELECT pg_terminate_backend($1)',[lease.backendPid]);}return lease.isAlive();}};};
 const old=dispatcher(async id=>{jobs.push(id);},broken);await old.dispatch();await old.close();expect(jobs).toEqual([]);
 expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[run])).rows[0].state).toBe('running');
 const next=dispatcher(async id=>{jobs.push(id);await db.query("UPDATE runs SET state='submitted' WHERE id=$1",[id]);});await next.dispatch();await next.close();expect(jobs).toEqual([run]);expect((await db.query('SELECT id,attempt,fence FROM runs WHERE id=$1',[run])).rows[0]).toEqual(before);
});
it('held owner lease forbids reclaim before execute',async()=>{
 const run=await enqueue(randomUUID(),1),gate=deferred(),entered=deferred();const acquire=supervisorLeaseAcquirer(db);
 const paused=async(id:string)=>{const lease=await acquire(id);if(!lease)return null;let probes=0;return {...lease,isAlive:async()=>{if(++probes===2){entered.resolve();await gate.promise;}return lease.isAlive();}};};
 const old=dispatcher(async id=>{jobs.push(id);await db.query("UPDATE runs SET state='submitted' WHERE id=$1",[id]);},paused);await old.dispatch();await entered.promise;
 const next=dispatcher(async id=>{jobs.push(id);});await next.dispatch();await next.close();expect(jobs).toEqual([]);gate.resolve();await old.close();expect(jobs).toEqual([run]);
});
it('owner connection loss after canonical start never replays live/reconciling run',async()=>{
 const run=await enqueue(randomUUID(),1);let lease:SupervisorOwnerLease|null=null;const acquire=supervisorLeaseAcquirer(db);
 const old=dispatcher(async id=>{jobs.push(id);await db.query("UPDATE runs SET state='reconciling',worker_id='started',started_at=now() WHERE id=$1",[id]);await admin.query('SELECT pg_terminate_backend($1)',[lease!.backendPid]);},async id=>{lease=await acquire(id);return lease;});await old.dispatch();await old.close();
 const next=dispatcher(async id=>{jobs.push(id);});await next.dispatch();await next.close();expect(jobs).toEqual([run]);expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[run])).rows[0].state).toBe('running');
});
it('legacy queued claims lack provenance and remain untouched',async()=>{
 const run=await enqueue(randomUUID(),1);await db.query("UPDATE supervisor_jobs SET state='running' WHERE run_id=$1",[run]);const d=dispatcher(async id=>{jobs.push(id);});await d.dispatch();await d.close();expect(jobs).toEqual([]);expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[run])).rows[0].state).toBe('running');
});
it('late old callback cannot finalize receipt belonging to another nonce',async()=>{
 const run=await enqueue(randomUUID(),1);const d=dispatcher(async id=>{await db.query("UPDATE runs SET state='submitted' WHERE id=$1",[id]);await db.query("UPDATE settings SET value=jsonb_set(value,'{ownerNonce}',$2::jsonb) WHERE id=$1",['supervisor-owner:'+id,JSON.stringify(randomUUID())]);});await d.dispatch();await d.close();expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[run])).rows[0].state).toBe('running');
});
it('late start after recovery occupies global slot and goal even with job queued',async()=>{
 const goal=randomUUID(),ghost=await enqueue(goal,1),other=await enqueue(randomUUID(),2);await enqueue(goal,3);await enqueue(randomUUID(),4);
 await abandonedClaim(ghost);let first=true;
 const interleaved=async<T>(fn:(client:pg.PoolClient)=>Promise<T>)=>{const result=await transaction(fn);if(first){first=false;expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[ghost])).rows[0].state).toBe('queued');await db.query("UPDATE runs SET state='running',worker_id='late-start',started_at=now() WHERE id=$1 AND state='queued'",[ghost]);}return result;};
 const gate=deferred(),entered=deferred();
 const d=dispatcher(async id=>{jobs.push(id);entered.resolve();await gate.promise;await db.query("UPDATE runs SET state='submitted' WHERE id=$1",[id]);},supervisorLeaseAcquirer(db),interleaved);
 let startTimeout:ReturnType<typeof setTimeout>|undefined;
 try{
  await d.dispatch();
  await Promise.race([entered.promise,new Promise<never>((_,reject)=>{
   startTimeout=setTimeout(()=>reject(Error('Expected eligible supervisor callback did not start')),5000);
  })]);
  expect(jobs).toEqual([other]);
 }finally{
  if(startTimeout)clearTimeout(startTimeout);
  gate.resolve();await d.close();
 }
 expect(jobs).toEqual([other]);
});
it.each(['worker','started','context','event','foreign_fence','foreign_attempt','inactive_goal','task_running','cancel_run','cancel_task'])('never-started recovery refuses %s conflicting evidence',async kind=>{
 const run=await enqueue(randomUUID(),1),row=await abandonedClaim(run);
 if(kind==='worker')await db.query("UPDATE runs SET worker_id='prior-owner' WHERE id=$1",[run]);
 if(kind==='started')await db.query('UPDATE runs SET started_at=now() WHERE id=$1',[run]);
 if(kind==='context')await db.query('UPDATE runs SET context_id=$2 WHERE id=$1',[run,randomUUID()]);
 if(kind==='event')await db.query("INSERT INTO events(kind,payload) VALUES('RUN_STARTED',$1)",[JSON.stringify({runId:run})]);
 if(kind==='foreign_fence')await db.query("UPDATE settings SET value=jsonb_set(value,'{runFence}',$2::jsonb) WHERE id=$1",['supervisor-owner:'+run,JSON.stringify(randomUUID())]);
 if(kind==='foreign_attempt')await db.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[row.task_id]);
 if(kind==='inactive_goal')await db.query("UPDATE goals SET state='paused' WHERE id=$1",[row.goal_id]);
 if(kind==='task_running')await db.query("UPDATE tasks SET state='running' WHERE id=$1",[row.task_id]);
 if(kind==='cancel_run')await db.query("INSERT INTO events(kind,payload,entity_id) VALUES('CANCEL_REQUESTED',$1,$2)",[JSON.stringify({runId:run}),row.task_id]);
 if(kind==='cancel_task')await db.query("INSERT INTO events(kind,payload,entity_id) VALUES('CANCEL_REQUESTED','{}',$1)",[row.task_id]);
 const d=dispatcher(async id=>{jobs.push(id);});await d.dispatch();await d.close();expect(jobs).toEqual([]);expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[run])).rows[0].state).toBe('running');
});
it.each(['paused_goal','stale_attempt','task_running','cancel_run','cancel_task'])('admission refuses %s queued job before any execute',async kind=>{
 const run=await enqueue(randomUUID(),1),row=(await db.query('SELECT r.task_id,t.goal_id FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1',[run])).rows[0];
 if(kind==='paused_goal')await db.query("UPDATE goals SET state='paused' WHERE id=$1",[row.goal_id]);
 if(kind==='stale_attempt')await db.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[row.task_id]);
 if(kind==='task_running')await db.query("UPDATE tasks SET state='running' WHERE id=$1",[row.task_id]);
 if(kind==='cancel_run')await db.query("INSERT INTO events(kind,payload,entity_id) VALUES('CANCEL_REQUESTED',$1,$2)",[JSON.stringify({runId:run}),row.task_id]);
 if(kind==='cancel_task')await db.query("INSERT INTO events(kind,payload,entity_id) VALUES('CANCEL_REQUESTED','{}',$1)",[row.task_id]);
 const d=dispatcher(async id=>{jobs.push(id);});await d.dispatch();await d.close();expect(jobs).toEqual([]);expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[run])).rows[0].state).toBe('queued');
});
it('unexpected unlock on own holder fails ownership proof and does not replay started run',async()=>{
 const run=await enqueue(randomUUID(),1);let holder:pg.PoolClient|undefined,lease:SupervisorOwnerLease|null=null;
 const acquire=supervisorLeaseAcquirer({connect:async()=>{holder=await db.connect();return holder;}});
 const d=dispatcher(async id=>{jobs.push(id);expect(await lease!.isAlive()).toBe(true);await db.query("UPDATE runs SET state='reconciling',worker_id='started',started_at=now() WHERE id=$1",[id]);await holder!.query('SELECT pg_advisory_unlock_all()');expect(await lease!.isAlive()).toBe(false);},async id=>{lease=await acquire(id);return lease;});
 await d.dispatch();await d.close();const next=dispatcher(async id=>{jobs.push(id);});await next.dispatch();await next.close();expect(jobs).toEqual([run]);expect((await db.query('SELECT state FROM supervisor_jobs WHERE run_id=$1',[run])).rows[0].state).toBe('running');
});
