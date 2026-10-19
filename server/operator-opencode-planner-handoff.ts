import {pool,tx,lock,event,type DB} from './db.js';
import {operatorRecoveryDigest} from './operator-recovery-contract.js';
import {hasOperatorOpenCodeRecoveryIntent} from './operator-opencode-recovery-contract.js';
import {requestStrategyPlanning,scopeSourceRevision,planningSourceFence} from './planning-demand.js';
import {inspectStopped,assertStopped} from './continuation.js';
import {trustedSupervision,assertSupervisedResume} from './supervision-handoff.js';
import {acceptedSupervisionActions} from './supervision-actions.js';
import {nativeSupervisorLockNamespace,nativeSupervisorLockKey,type NativeUnitObservation} from './native-supervision.js';
import {observeNativeSupervisor} from '../scripts/native-supervisor-unit.js';
import {operatorOpenCodeRecoveryBatch} from './operator-opencode-recovery-scan.js';
import {appliedOperatorOpenCodeReviewIsFresh} from './operator-opencode-review.js';

/** Use an already accepted AND applied revision. This helper does not decide
 * what to do, mark a decision applied, accept a plan or reuse old input bytes. */
async function binding(db:DB,runId:string){
 const run=(await db.query('SELECT * FROM runs WHERE id=$1',[runId])).rows[0];
 if(!run||run.runtime!=='opencode'||run.state!=='failed'||!await hasOperatorOpenCodeRecoveryIntent(db,run))return null;
 const receipt=run.checkpoint.operatorRecoveryStop;
 const task=(await db.query('SELECT * FROM tasks WHERE id=$1',[run.task_id])).rows[0];
 if(!task||task.attempt_count!==run.attempt||!['failed','recovery_blocked'].includes(task.state)||
  task.execution?.kind!=='opencode'||task.execution.workKind!=='planning'||
  task.execution.planningRequestId!==receipt.request.planningRequestId||run.context_id!==receipt.request.contextVersion||
  run.checkpoint.nativeStopReceipt?.runFence!==run.fence||!run.checkpoint.nativeStopReceipt?.bridgeReceiptDigest)return null;
 const request=(await db.query(`SELECT * FROM planning_requests WHERE id=$1 AND task_id=$2 AND goal_id=$3
  AND state='awaiting_supervision'`,[receipt.request.planningRequestId,task.id,task.goal_id])).rows[0];
 if(!request||request.source_revision!==receipt.sourceRevision||request.receipt?.operatorRecoverySuccessor)return null;
 const c=await trustedSupervision(db,task);
 const action=c&&acceptedSupervisionActions(c.decision?.actions??[c.decision]).find(a=>a.caseId===c.id&&a.kind==='revise');
 if(!c||c.source_run_id!==run.id||c.applied_attempt!==run.attempt||!c.decision_run_id||!action||
  action.route||action.lane||action.workKind||action.sourceTaskId||action.deliveryResolution||action.initialAdmission||action.validationActionId)return null;
 if(!await appliedOperatorOpenCodeReviewIsFresh(db,c))return null;
 if(!(await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id AND t.goal_id=$2 AND t.stage='coordinate'
  JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON a.passed
  WHERE r.id=$1 AND r.state='submitted'`,[c.decision_run_id,task.goal_id])).rowCount)return null;
 if(!(await db.query(`SELECT 1 FROM events WHERE kind='SUPERVISION_APPLIED' AND entity_id=$1
  AND payload->>'caseId'=$2 AND payload->>'sourceRunId'=$3 AND payload->>'decisionRunId'=$4
  AND payload->>'stopConfirmed'='true'`,[task.id,c.id,run.id,c.decision_run_id])).rowCount)return null;
 if((await db.query(`SELECT 1 FROM leases WHERE run_id=$1
  UNION ALL SELECT 1 FROM dependencies WHERE depends_on=$2
  UNION ALL SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1::text
  UNION ALL SELECT 1 FROM runs WHERE task_id=$2 AND state IN('queued','running','cancelling','reconciling')
  UNION ALL SELECT 1 FROM supervision_cases WHERE task_id=$2 AND state IN('open','decided','delegated','waiting_evidence') LIMIT 1`,[run.id,task.id])).rowCount)return null;
 const reservation=(await db.query('SELECT supervision FROM opencode_jobs WHERE run_id=$1',[run.id])).rows[0]?.supervision;
 if(!reservation||reservation.runId!==run.id||reservation.runFence!==run.fence||reservation.launchToken!==receipt.launchToken||
  reservation.candidate!==receipt.candidate||reservation.digest!==receipt.candidateDigest)return null;
 return {run,task,request,c,action,reservation,receipt,key:operatorRecoveryDigest([run.id,run.fence,run.context_id,
  task.id,task.attempt_count,task.instruction_revision_id,task.state,request.id,request.source_revision,request.receipt,
  c.id,c.decision_run_id,c.applied_attempt,c.decision,reservation])};
}
function ownerStopped(proof:NativeUnitObservation,unit:string){
 const age=Date.now()-Date.parse(proof.checkedAt);
 return proof.unit===unit&&proof.mainPid===0&&proof.cgroupEmpty===true&&proof.pendingJob===false&&
  ['inactive','failed'].includes(proof.activeState)&&['loaded','not-found'].includes(proof.loadState)&&
  Number.isFinite(age)&&age>=-5000&&age<=30000;
}
export async function handoffOperatorOpenCodePlanner(runId:string,inspect=inspectStopped,owner=observeNativeSupervisor){
 const observed=await binding(pool,runId);if(!observed)return null;
 const source=await tx(async db=>{
  await db.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
  return {revision:await scopeSourceRevision(db,observed.task.goal_id,observed.request.scope),
   fence:await planningSourceFence(db,observed.task.goal_id,observed.request.scope)};
 });
 const proof=await inspect(runId,true),ownerProof=await owner(observed.reservation);
 assertStopped(proof,observed.run,observed.task);
 if(!ownerStopped(ownerProof,observed.reservation.unit))return null;
 return tx(async db=>{
  await lock(db);const current=await binding(db,runId);if(!current||current.key!==observed.key)return null;
  assertStopped(proof,current.run,current.task);
  if(!ownerStopped(ownerProof,current.reservation.unit)||
   !(await db.query('SELECT pg_try_advisory_xact_lock($1::int,$2::int) owned',[
    nativeSupervisorLockNamespace,nativeSupervisorLockKey(runId)])).rows[0].owned)return null;
  if((await db.query("SELECT 1 FROM settings WHERE id='maintenance' AND value->>'draining'='true'")).rowCount)return null;
  // Check the same progress/stop gates, but this handoff is not an execution
  // admission. Preserve no EXECUTION_RETRY_ADMITTED accounting side effects.
  await db.query('SAVEPOINT opencode_planner_handoff_guard');
  await assertSupervisedResume(db,current.task,current.run,{...proof,supervisionResumeCaseId:current.c.id});
  await db.query('ROLLBACK TO SAVEPOINT opencode_planner_handoff_guard');
  await db.query('RELEASE SAVEPOINT opencode_planner_handoff_guard');
  if(await planningSourceFence(db,current.task.goal_id,current.request.scope)!==source.fence||
   await scopeSourceRevision(db,current.task.goal_id,current.request.scope)!==source.revision)return null;
  const strategyKey='opencode_recovery_'+operatorRecoveryDigest([current.request.id,current.c.id,current.c.decision_run_id])
   .replace(/[a-f0-9]/g,c=>String.fromCharCode(97+parseInt(c,16)));
  const next=await requestStrategyPlanning(db,current.task.goal_id,current.c.decision_run_id,{
   strategyKey,scope:current.request.scope,sourceRevision:source.revision,
   cause:current.action.diagnosis,hypothesis:current.action.instruction,
   expectedArtifact:'scope-plan.json em uma nova request, catálogo e helper atuais; preservar evidências e não repetir o scanner antigo.',
  });
  if(next.code!=='STRATEGY_PLANNING_REQUESTED'||!next.requestId)return null;
  const handoff={version:1,requestId:next.requestId,sourceRunId:runId,sourceFence:current.run.fence,
   contextVersion:current.run.context_id,caseId:current.c.id,decisionRunId:current.c.decision_run_id,
   sourceRevision:source.revision,strategyKey,stopCheckedAt:proof.checkedAt,ownerCheckedAt:ownerProof.checkedAt};
  await db.query(`UPDATE planning_requests SET state='superseded',receipt=receipt||jsonb_build_object('operatorRecoverySuccessor',$2::jsonb),
   updated_at=clock_timestamp() WHERE id=$1`,[current.request.id,JSON.stringify(handoff)]);
  await db.query(`UPDATE planning_requests SET receipt=receipt||jsonb_build_object('operatorRecoveryPredecessor',$2::jsonb)
   WHERE id=$1`,[next.requestId,JSON.stringify({...handoff,requestId:current.request.id})]);
  await db.query("UPDATE tasks SET state='superseded',reason='Planejamento transferido à sucessora com contexto atual após revisão aplicada.',updated_at=clock_timestamp() WHERE id=$1",[current.task.id]);
  await event(db,'OPENCODE_PLANNER_RECOVERY_HANDOFF',current.task.id,{...handoff,previousRequestId:current.request.id,
   goalId:current.task.goal_id,scope:current.request.scope,acceptance:false,newAttemptQueued:false});
  return handoff;
 });
}
export async function reconcileOperatorOpenCodePlannerHandoffs(inspect=inspectStopped,owner=observeNativeSupervisor){
 const rows=await operatorOpenCodeRecoveryBatch('handoff',async(db,cursor)=>(await db.query(`SELECT r.id FROM planning_requests p JOIN tasks t ON t.id=p.task_id
  JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
  JOIN goals g ON g.id=t.goal_id AND g.state='active'
  JOIN supervision_cases c ON c.task_id=t.id AND c.source_run_id=r.id AND c.state='applied' AND c.applied_attempt=r.attempt
   AND c.id::text IN(t.execution#>>'{supervision,caseId}',t.execution#>>'{supervisionHandoff,caseId}')
  WHERE p.state='awaiting_supervision'
  AND r.runtime='opencode' AND r.state='failed' AND r.checkpoint#>>'{operatorRecoveryStop,request,runtime}'='opencode'
  AND r.checkpoint#>>'{operatorRecoveryStop,request,planningRequestId}'=p.id::text
  AND r.checkpoint#>>'{nativeStopReceipt,runFence}'=r.fence::text
  AND t.state IN('failed','recovery_blocked') AND t.execution->>'workKind'='planning'
  AND EXISTS(SELECT 1 FROM jsonb_array_elements(COALESCE(c.decision->'actions',jsonb_build_array(c.decision)))a
    WHERE a->>'kind'='revise' AND a->>'caseId'=c.id::text)
  AND NOT EXISTS(SELECT 1 FROM leases WHERE run_id=r.id)
  AND NOT EXISTS(SELECT 1 FROM dependencies WHERE depends_on=t.id)
  AND NOT EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=r.id::text)
  AND NOT EXISTS(SELECT 1 FROM supervision_cases pending WHERE pending.task_id=t.id
    AND pending.state IN('open','decided','delegated','waiting_evidence'))
  ORDER BY (r.id>$1::uuid) DESC,r.id LIMIT 2`,[cursor])).rows);
 for(const row of rows)await handoffOperatorOpenCodePlanner(row.id,inspect,owner).catch(()=>{});
}
