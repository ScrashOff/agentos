import {pool,tx,lock,event} from './db.js';
import {assertStopped,inspectStopped} from './continuation.js';
import {acceptedSnapshotProof} from './deferred-handoffs.js';

/** A superseded planning request must not leave its stopped task/case owning
 * supervisor attention forever. Only a newer applied, currently accepted plan
 * of the same goal/scope can retire it. No result, run or evidence is erased. */
export async function retireSupersededPlanningTasks(inspect=inspectStopped) {
 const candidates=(await pool.query(`SELECT p.id,t.id task_id,r.id run_id,r.runtime FROM planning_requests p
   JOIN tasks t ON t.id=p.task_id JOIN goals g ON g.id=p.goal_id
   JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
   WHERE p.state='superseded' AND g.state='active' AND t.execution->>'workKind'='planning'
   AND t.state IN('failed','submitted','recovery_blocked')
   AND NOT EXISTS(SELECT 1 FROM runs x WHERE x.task_id=t.id AND x.state IN('queued','running','cancelling','reconciling'))
   AND NOT EXISTS(SELECT 1 FROM dependencies d WHERE d.depends_on=t.id)
   AND EXISTS(SELECT 1 FROM planning_requests next JOIN tasks nt ON nt.id=next.task_id
     WHERE next.goal_id=p.goal_id AND next.scope=p.scope AND next.created_at>p.created_at
       AND next.state='applied' AND nt.state='accepted') ORDER BY p.created_at,p.id LIMIT 8`)).rows;
 const retired:string[]=[];
 for(const candidate of candidates){
  try{
   const proof=await inspect(candidate.run_id,candidate.runtime==='opencode');
   const applied=await tx(async db=>{
    await lock(db);
    const p=(await db.query('SELECT * FROM planning_requests WHERE id=$1 FOR UPDATE',[candidate.id])).rows[0];
    const task=(await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',[candidate.task_id])).rows[0];
    if(!p || p.state!=='superseded' || p.task_id!==task?.id || task.execution?.workKind!=='planning' ||
      !['failed','submitted','recovery_blocked'].includes(task.state) ||
      !(await db.query("SELECT 1 FROM goals WHERE id=$1 AND state='active'",[p.goal_id])).rowCount ||
      (await db.query("SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling')",[task.id])).rowCount ||
      (await db.query('SELECT 1 FROM dependencies WHERE depends_on=$1',[task.id])).rowCount)return false;
    const run=(await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[task.id])).rows[0];
    if(run?.id!==candidate.run_id || run.attempt!==task.attempt_count ||
      (await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",[run.id])).rowCount)return false;
    assertStopped(proof,run,task);
    const replacement=(await db.query(`SELECT next.id,next.task_id FROM planning_requests next JOIN tasks nt ON nt.id=next.task_id
      WHERE next.goal_id=$1 AND next.scope=$2 AND next.created_at>$3 AND next.state='applied' AND nt.state='accepted'
      ORDER BY next.created_at DESC LIMIT 1 FOR UPDATE OF next,nt`,[p.goal_id,p.scope,p.created_at])).rows[0];
    if(!replacement)return false;
    const source=(await db.query('SELECT * FROM tasks WHERE id=$1',[replacement.task_id])).rows[0];
    const accepted=await acceptedSnapshotProof(db,source);
    if(!(await db.query("SELECT 1 FROM artifacts WHERE run_id=$1 AND name='scope-plan.json'",[accepted.runId])).rowCount)return false;
    const cases=(await db.query(`UPDATE supervision_cases SET state='superseded',updated_at=now()
      WHERE task_id=$1 AND state IN('open','decided','delegated','waiting_evidence') RETURNING id`,[task.id])).rows.map(c=>c.id);
    await db.query("UPDATE tasks SET state='superseded',reason='Planejamento substituído por um plano mais recente aceito e aplicado.',updated_at=now() WHERE id=$1",[task.id]);
    await event(db,'SCOPED_PLANNING_RETIRED',p.goal_id,{requestId:p.id,taskId:task.id,sourceRunId:run.id,caseIds:cases,
      replacementRequestId:replacement.id,replacementProof:accepted,stopCheckedAt:proof.checkedAt});
    return true;
   });
   if(applied)retired.push(candidate.task_id);
  }catch(error:any){
   const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(error.code??error.message??'') ? error.code??error.message : 'RETIREMENT_INSPECTION_FAILED';
   if(!(await pool.query("SELECT 1 FROM events WHERE kind='SCOPED_PLANNING_RETIREMENT_BLOCKED' AND payload->>'sourceRunId'=$1 AND payload->>'code'=$2",[candidate.run_id,code])).rowCount)
    await event(pool,'SCOPED_PLANNING_RETIREMENT_BLOCKED',candidate.task_id,{requestId:candidate.id,sourceRunId:candidate.run_id,code});
  }
 }
 return retired;
}
