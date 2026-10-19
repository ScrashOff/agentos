import {randomUUID} from 'node:crypto';
import {pool,tx,event,type DB} from './db.js';
import {assertStopped,inspectStopped,queueVerificationRecheckInTransaction,queueInterruptedVerificationRecheckInTransaction} from './continuation.js';
import {verificationInterruptionCodes,interruptedVerificationReason,interruptedVerification} from './verification-interruption.js';
import {currentSemanticDecision} from './semantic-retry.js';
import {setSupervisionInstruction} from './task-instructions.js';
import {supervisionGenerationReady} from './supervision-generations.js';
import {deliveryDigest} from './delivery-supervision.js';

/** Applied QA revisions cannot use writable-runtime continuation. Observe the
 * exact current owner; this query neither launches nor accepts an execution. */
async function candidates(db:DB,taskId:string|null=null){
 return (await db.query(`WITH eligible AS MATERIALIZED (
  SELECT t.id,t.goal_id,t.stage,t.state,t.attempt_count,t.instruction_revision_id,
   m.control execution,r.id run_id,r.fence,r.attempt,r.state run_state,r.error_code FROM tasks t JOIN goals g ON g.id=t.goal_id
   JOIN settings enabled ON enabled.id='supervision:'||g.id
   LEFT JOIN settings autonomy ON autonomy.id='autonomy:'||g.project
   JOIN task_execution_metadata m ON m.task_id=t.id AND m.goal_id=t.goal_id AND m.version=1 AND m.execution_type='object'
   JOIN LATERAL(SELECT id,fence,attempt,state,error_code FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
   WHERE ($1::uuid IS NULL OR t.id=$1) AND g.state='active' AND enabled.value->>'enabled'='true'
    AND COALESCE(autonomy.value->>'enabled','true')='true'
    AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR autonomy.value->>'enabled'='true')
    AND t.state IN('submitted','recovery_blocked','failed') AND t.stage IN('verify','review')
    AND m.control->'readOnlySource'='true'::jsonb AND m.control->>'kind' IN('harness','opencode')
    AND (m.control->'verificationRecheck' IS NULL OR m.control->'verificationRecheck'='null'::jsonb OR (
     jsonb_typeof(m.control->'verificationRecheck')='object'
     AND m.control->'verificationRecheck'->>'sourceTaskId'=t.id::text
     AND m.control->'verificationRecheck'->>'sourceRunId'<>r.id::text
     AND m.control->'verificationRecheck'->>'workspace'=m.workspace
     AND EXISTS(SELECT 1 FROM runs previous WHERE previous.task_id=t.id AND (previous.state='submitted' OR
      (previous.state='failed' AND previous.error_code=ANY($2::text[]) AND
       m.control->'verificationRecheck'->>'reason'='APPLIED_INTERRUPTED_REVISION_RECONCILED'))
      AND previous.attempt<r.attempt AND previous.attempt::text=m.control->'verificationRecheck'->>'sourceAttempt'
      AND previous.id::text=m.control->'verificationRecheck'->>'sourceRunId')
     AND EXISTS(SELECT 1 FROM events prepared WHERE prepared.entity_id=t.id::text
      AND prepared.kind='VERIFICATION_RECHECK_QUEUED'
      AND prepared.payload->>'sourceRunId'=m.control->'verificationRecheck'->>'sourceRunId'
      AND prepared.payload=m.control->'verificationRecheck')))
    AND r.attempt=t.attempt_count
    AND ((r.state='submitted' AND t.state IN('submitted','recovery_blocked') AND
      (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)=false)
     OR (r.state='failed' AND t.state IN('failed','recovery_blocked') AND r.error_code=ANY($2::text[]) AND
      (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1) IS DISTINCT FROM true))
    AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id AND live.state IN('queued','running','cancelling','reconciling'))
    AND NOT EXISTS(SELECT 1 FROM leases l JOIN runs leased ON leased.id=l.run_id WHERE leased.task_id=t.id)
    AND NOT EXISTS(SELECT 1 FROM events e WHERE e.entity_id=t.id::text AND e.kind='CANCEL_REQUESTED' AND e.payload->>'runId'=r.id::text)
    AND NOT EXISTS(SELECT 1 FROM supervision_cases pending WHERE pending.task_id=t.id AND pending.state IN('open','decided','delegated','waiting_evidence'))
  ) SELECT e.*,e.execution||jsonb_build_object('supervision',directive.supervision,
    'supervisionHandoff',directive."supervisionHandoff",'sourceWorkspace',directive."sourceWorkspace") execution,
    c.id case_id,c.decision_run_id,c.decision,c.source_run_id,c.applied_attempt
   FROM eligible e JOIN tasks t ON t.id=e.id
   CROSS JOIN LATERAL jsonb_to_record(t.execution) AS directive(supervision jsonb,"supervisionHandoff" jsonb,"sourceWorkspace" text)
   JOIN supervision_cases c ON c.id::text=directive.supervision->>'caseId' AND c.task_id=e.id AND c.goal_id=e.goal_id
   WHERE c.state='applied' AND c.applied_attempt=e.attempt AND c.source_run_id=e.run_id
    AND directive.supervision->>'sourceRunId'=e.run_id::text
   ORDER BY c.updated_at,c.id LIMIT 4`,[taskId,verificationInterruptionCodes])).rows;
}
async function review(db:DB,row:any,cause:string){
 if(!await supervisionGenerationReady(db))return null;
 const key='verification-revision:'+row.case_id+':'+cause;
 if((await db.query('SELECT 1 FROM supervision_cases WHERE task_id=$1 AND source_run_id=$2 AND request_key=$3',[row.id,row.run_id,key])).rowCount)return null;
 const id=randomUUID(),reason='QA revised without executable continuation; supervisor must delegate implementation repair or a bound verification recheck.';
 await db.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,request_key,parent_case_id,reason,evidence)
  VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,[id,row.goal_id,row.id,row.run_id,key,row.case_id,reason,
  JSON.stringify([{kind:'VERIFICATION_REVISION_RECOVERY_REQUIRED',code:cause,runId:row.run_id,previousCaseId:row.case_id,
   decisionRunId:row.decision_run_id,authority:'review_only',sameCandidateRetryAuthorized:false,acceptance:false,
   nextAction:'Delegate repair to the existing writable implementation using its accepted source. Keep QA read-only; rerun independent QA after the candidate changes. A prepared recheck still requires canonical candidate and source binding.'}])]);
 await db.query("UPDATE tasks SET state='recovery_blocked',reason=$2,updated_at=clock_timestamp() WHERE id=$1",[row.id,reason]);
 await event(db,'SUPERVISION_REQUESTED',row.id,{caseId:id,goalId:row.goal_id,sourceRunId:row.run_id,code:cause,parentCaseId:row.case_id,requestKey:key});
 return id;
}
async function decisionPins(db:DB,reviewId:string){
 return (await db.query(`SELECT r.id,r.fence,r.state,t.goal_id,t.stage,
  (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1) passed,
  a.id artifact_id,a.name,a.digest,a.size FROM runs r JOIN tasks t ON t.id=r.task_id
  JOIN artifacts a ON a.run_id=r.id AND a.name='coordination.json' WHERE r.id=$1 ORDER BY a.id`,[reviewId])).rows;
}

/** A bounded reconciliation converts a byte-verified applied revision into the
 * existing immutable-candidate QA recheck. Fresh stop inspection is outside the
 * admission lock. Duplicate scans, stale ownership, pauses, cancels and live
 * actors cannot create another execution or change an acceptance. */
export async function reconcileVerificationRevisions(inspect=inspectStopped){
 const schema=(await pool.query("SELECT to_regclass('public.supervision_cases') cases,to_regclass('public.task_execution_metadata') metadata")).rows[0];
 if(!schema.cases||!schema.metadata)return [];
 const rows=await candidates(pool),results:any[]=[];
 for(const observed of rows){
  const actions=observed.decision?.actions??[observed.decision];
  if(!Array.isArray(actions)||actions.length!==1||actions[0]?.kind!=='revise'||actions[0]?.sourceTaskId)continue;
  const task={...observed,execution:observed.execution},run={id:observed.run_id,fence:observed.fence,attempt:observed.attempt,state:observed.run_state,error_code:observed.error_code};
  const c={id:observed.case_id,task_id:observed.id,goal_id:observed.goal_id,state:'applied',applied_attempt:observed.applied_attempt,
   source_run_id:observed.source_run_id,decision_run_id:observed.decision_run_id,decision:observed.decision};
  const pins=await decisionPins(pool,c.decision_run_id);
  if(!pins.length||!await currentSemanticDecision(pool,task,run,c)||deliveryDigest(pins)!==deliveryDigest(await decisionPins(pool,c.decision_run_id)))continue;
  let proof:any;try{proof=await inspect(run.id,task.execution.kind==='opencode');assertStopped(proof,run,task);}catch{continue;}
  let waitingForAdmission=false;
  let outcome:any;
  try{outcome=await tx(async db=>{
   // Join the admission lock's wait queue. A nonblocking try can starve forever
   // while the other controllers continuously have queued work. The bounded
   // wait neither interrupts an owner nor relaxes any source/stop check.
   await db.query("SET LOCAL statement_timeout='7s'");
   const previousLockTimeout=(await db.query("SELECT current_setting('lock_timeout') value")).rows[0].value;
   await db.query("SET LOCAL lock_timeout='5s'");
   waitingForAdmission=true;
   await db.query('SELECT pg_advisory_xact_lock(4280001)');
   waitingForAdmission=false;
   await db.query("SELECT set_config('lock_timeout',$1,true)",[previousLockTimeout]);
   await db.query("SET LOCAL statement_timeout='5s'");
   const fresh=(await candidates(db,observed.id))[0];
   if(!fresh||deliveryDigest(fresh)!==deliveryDigest(observed)||deliveryDigest(pins)!==deliveryDigest(await decisionPins(db,c.decision_run_id)))return {taskId:observed.id,status:'owner_changed'};
   assertStopped(proof,run,task);
   const target=(await db.query(`SELECT t.id,r.id run_id FROM dependencies d JOIN tasks t ON t.id=d.depends_on
    JOIN task_execution_metadata parent_metadata ON parent_metadata.task_id=t.id AND parent_metadata.goal_id=t.goal_id
     AND parent_metadata.version=1 AND parent_metadata.execution_type='object'
    JOIN LATERAL(SELECT id,state FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    WHERE d.task_id=$1 AND t.goal_id=$2 AND parent_metadata.workspace=$3 AND t.state='accepted'
     AND COALESCE(parent_metadata.control->>'readOnlySource','false')='false' AND r.state='submitted'
     AND (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)=true
     AND NOT EXISTS(SELECT 1 FROM runs active WHERE active.task_id=t.id AND active.state IN('queued','running','cancelling','reconciling'))
     AND NOT EXISTS(SELECT 1 FROM leases l JOIN runs held ON held.id=l.run_id WHERE held.task_id=t.id)`,
    [task.id,task.goal_id,task.execution.sourceWorkspace])).rows;
   if(target.length!==1)return {taskId:task.id,status:'review',caseId:await review(db,fresh,'VERIFICATION_IMPLEMENTATION_UNAVAILABLE')};
   const parent=target[0];
   const repeated=(await db.query(`SELECT 1 FROM events WHERE entity_id=$1 AND kind='SUPERVISION_QA_RECHECK_READY'
     AND payload->>'reason'='APPLIED_REVISION_RECONCILED' AND payload->>'implementationRunId'=$2 LIMIT 1`,[task.id,parent.run_id])).rowCount;
   const interrupted=interruptedVerification(run);
   if(repeated&&!interrupted)return {taskId:task.id,status:'review',caseId:await review(db,fresh,'VERIFICATION_SAME_CANDIDATE_REPEATED')};
   await db.query('SAVEPOINT verification_revision_queue');
   try{
    const reason=interrupted?interruptedVerificationReason:'APPLIED_REVISION_RECONCILED';
    const request={version:1,reason,sourceTaskId:task.id,
     sourceRunId:run.id,sourceAttempt:run.attempt,targetTaskId:parent.id,targetRunId:parent.run_id};
    if(interrupted)await queueInterruptedVerificationRecheckInTransaction(db,request,{caseId:c.id,proof});
    else await queueVerificationRecheckInTransaction(db,request);
    const prepared=(await db.query('SELECT execution FROM tasks WHERE id=$1',[task.id])).rows[0].execution.verificationRecheck;
    await setSupervisionInstruction(db,task.id,c.id,'delivery',
     '\nQA_RECHECK_V1\nExecute the revised verification in its isolated workspace. Keep the implementation read-only and every original acceptance criterion. This is not acceptance.\n'+JSON.stringify(prepared),run.id);
    await event(db,'SUPERVISION_QA_RECHECK_READY',task.id,{caseId:c.id,sourceRunId:run.id,decisionRunId:c.decision_run_id,
     reason,implementationTaskId:parent.id,implementationRunId:parent.run_id,stopConfirmed:true,acceptance:false});
    await db.query('RELEASE SAVEPOINT verification_revision_queue');
    return {taskId:task.id,status:'recheck_queued'};
   }catch(error:any){
    if(['40001','40P01','57014'].includes(error?.code))throw error;
    await db.query('ROLLBACK TO SAVEPOINT verification_revision_queue');await db.query('RELEASE SAVEPOINT verification_revision_queue');
    const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(error?.message??'')?error.message:'VERIFICATION_RECHECK_BINDING_FAILED';
    return {taskId:task.id,status:'review',caseId:await review(db,fresh,code)};
   }
  });}catch(error:any){
   if(!waitingForAdmission||!['55P03','57014'].includes(error?.code))throw error;
   outcome={taskId:observed.id,status:'admission_busy'};
  }
  results.push(outcome);
 }
 return results;
}
