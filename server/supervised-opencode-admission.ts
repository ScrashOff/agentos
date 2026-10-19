import { createHash, randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import type { DB } from './db.js';
import { event } from './db.js';
import { trustedSupervision, assertSupervisedResume, assertDependenciesAccepted } from './supervision-handoff.js';
import { pendingProviderBackoff } from '../bridge/opencode-provider-error.mjs';

const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
const digest=/^[a-f0-9]{64}$/;
const waiting=['retry_scheduled','ready','waiting_quota','waiting_resource','waiting_configuration','blocked_dependencies','blocked_failed'];
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'?
  Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const sha=(v:any)=>createHash('sha256').update(JSON.stringify(canonical(v))).digest('hex');
const failure=(code:string)=>Object.assign(Error(code),{code:'SUPERVISED_NATIVE_'+code});
export interface PreparedNativeAdmission {
  version:1; id:string; sourceRunId:string; executionRunId:string; sourceFence:string;
  attempt:number; caseId:string; decisionRunId:string|null; caseDigest:string;
  taskDigest:string; checkpointDigest:string; workspace:string;
  route:{accountId:string;model:string;agentVersion:number}; signature:string|null; preparedAt:string;
}
export function nativeAdmissionReceipt(value:any):PreparedNativeAdmission|null {
  if(value?.version!==1 || !['id','sourceRunId','executionRunId','sourceFence','caseId'].every(k=>uuid.test(value[k]??'')) ||
    (value.decisionRunId!==null&&!uuid.test(value.decisionRunId??'')) ||
    !['caseDigest','taskDigest','checkpointDigest'].every(k=>digest.test(value[k]??'')) ||
    !Number.isSafeInteger(value.attempt)||value.attempt<1||typeof value.workspace!=='string'||!value.workspace||
    typeof value.route?.accountId!=='string'||!value.route.accountId||typeof value.route?.model!=='string'||!value.route.model||
    !Number.isSafeInteger(value.route.agentVersion)||value.route.agentVersion<1) return null;
  return value;
}
function contractDigest(task:any) {
  const execution={...task.execution};
  delete execution.supervisedAdmission; delete execution.continuation; delete execution.recoveryContext;
  return sha({id:task.id,goalId:task.goal_id,agentId:task.agent_id,stage:task.stage,description:task.description,
    criteria:task.criteria,resources:task.resources,requiredMemories:task.required_memories,recovery:task.recovery,
    instructionRevision:task.instruction_revision_id,accountId:task.account_id,allowedAccounts:task.allowed_accounts,
    model:task.model,execution});
}
const caseDigest=(c:any)=>sha({id:c.id,goalId:c.goal_id,taskId:c.task_id,sourceRunId:c.source_run_id,
  state:c.state,appliedAttempt:c.applied_attempt,decisionRunId:c.decision_run_id,decision:c.decision});

/** Preparation runs all canonical semantic checks, but it is not a retry.
 * Roll back their retry-accounting events until a slot is actually admitted. */
export async function checkNativePreparation(db:DB,task:any,run:any,proof:any) {
  await db.query('SAVEPOINT native_preparation_guard');
  try { await assertSupervisedResume(db,task,run,proof); }
  finally { await db.query('ROLLBACK TO SAVEPOINT native_preparation_guard'); await db.query('RELEASE SAVEPOINT native_preparation_guard'); }
}
export async function createNativeAdmissionReceipt(db:DB,task:any,run:any,execution:any,proof:any,checkpoint:any) {
  const c=await trustedSupervision(db,task);
  if(!c||c.id!==proof.supervisionResumeCaseId)throw failure('BINDING_CHANGED');
  const receipt:PreparedNativeAdmission={version:1,id:randomUUID(),sourceRunId:run.id,
    executionRunId:execution.id,sourceFence:run.fence,attempt:run.attempt,caseId:c.id,
    decisionRunId:c.decision_run_id??null,caseDigest:caseDigest(c),taskDigest:contractDigest(task),
    checkpointDigest:sha(checkpoint),workspace:task.execution.workspace,
    route:{accountId:execution.account_id,model:execution.model,agentVersion:execution.agent_version},
    signature:proof.continuationSignature??null,preparedAt:new Date().toISOString()};
  if(!nativeAdmissionReceipt(receipt))throw failure('BINDING_INVALID');
  return receipt;
}

/** No external I/O. The caller holds admission's mutex; equality is checked
 * again after inspection and immediately before queue insertion. */
export async function readPreparedNativeAdmission(db:DB,taskId:string,expectedId?:string) {
  const task=(await db.query('SELECT * FROM tasks WHERE id=$1',[taskId])).rows[0];
  if(!task?.execution?.supervisedAdmission)return null;
  const receipt=nativeAdmissionReceipt(task.execution.supervisedAdmission);
  if(!receipt||expectedId&&receipt.id!==expectedId)throw failure('BINDING_CHANGED');
  if(!(await db.query(`SELECT 1 FROM events WHERE kind='SUPERVISION_RESUME_PREPARED' AND entity_id=$1
    AND payload->>'sourceRunId'=$2 AND payload->>'admissionReceiptId'=$3 LIMIT 1`,
    [taskId,receipt.sourceRunId,receipt.id])).rowCount)throw failure('BINDING_CHANGED');
  const run=(await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[taskId])).rows[0];
  const c=await trustedSupervision(db,task);
  if(!run||run.id!==receipt.sourceRunId||run.fence!==receipt.sourceFence||run.attempt!==receipt.attempt||
    task.attempt_count!==receipt.attempt||run.runtime!=='opencode'||!['failed','submitted'].includes(run.state)||
    !waiting.includes(task.state)||task.execution.kind!=='opencode'||task.execution.workspace!==receipt.workspace||
    task.account_id!==receipt.route.accountId||task.model!==receipt.route.model||
    !c||c.id!==receipt.caseId||caseDigest(c)!==receipt.caseDigest||contractDigest(task)!==receipt.taskDigest||
    sha(task.execution.recoveryContext)!==receipt.checkpointDigest||
    /DIGEST|CORRUPT|INVALID_WRITE_SCOPE|INVALID_WORKSPACE/.test(run.error_code??''))throw failure('BINDING_CHANGED');
  const executionId=run.checkpoint?.kind==='artifact_recovery'?run.checkpoint.sourceRunId:run.id;
  const execution=executionId===run.id?run:(await db.query('SELECT * FROM runs WHERE id=$1',[executionId])).rows[0];
  if(executionId!==receipt.executionRunId||!execution||execution.task_id!==task.id||execution.runtime!=='opencode'||
    execution.account_id!==receipt.route.accountId||execution.model!==receipt.route.model||execution.agent_version!==receipt.route.agentVersion||
    executionId!==run.id&&(!['failed','submitted'].includes(execution.state)||execution.attempt>=run.attempt||
      execution.instruction_snapshot?.task?.execution?.workspace!==receipt.workspace))throw failure('BINDING_CHANGED');
  return {task,run,receipt,checkpoint:task.execution.recoveryContext};
}

export function assertNativeAdmissionStop(proof:any,receipt:PreparedNativeAdmission) {
  const age=Date.now()-Date.parse(proof?.checkedAt),p=proof?.process;
  if(proof?.runId!==receipt.sourceRunId||proof.runtime!=='opencode'||proof.workspace!==receipt.workspace||
    (receipt.executionRunId!==receipt.sourceRunId&&proof.executionRunId!==receipt.executionRunId)||
    p?.unit!==`agentos-run-${receipt.executionRunId}`||p.stopped!==true||p.mainPid!==0||p.populated!==false||
    !['inactive','failed'].includes(p.activeState)||!Number.isFinite(age)||age < -5000||age>60000)
    throw failure('STOP_UNCONFIRMED');
}
export async function validateNativeAdmission(db:DB,binding:NonNullable<Awaited<ReturnType<typeof readPreparedNativeAdmission>>>,proof:any) {
  const {task,run,receipt}=binding;
  assertNativeAdmissionStop(proof,receipt);
  if(proof.notStarted&&(run.state!=='failed'||proof.state?.state!=='not_started'||!(await db.query(
    `SELECT 1 FROM events WHERE kind='OPENCODE_NOT_STARTED' AND entity_id=$1 AND payload->>'runId'=$2
      AND payload->>'confirmed'='true'`,[task.id,run.id])).rowCount))throw failure('STOP_UNCONFIRMED');
  const wait=pendingProviderBackoff(proof.state,{runId:receipt.executionRunId,workspace:receipt.workspace,model:receipt.route.model});
  if(wait)throw Object.assign(failure('PROVIDER_RETRY_PENDING'),{notBefore:wait.notBefore});
  await assertSupervisedResume(db,task,run,{...proof,supervisionResumeCaseId:receipt.caseId,continuationSignature:receipt.signature});
  await assertDependenciesAccepted(db,task.id);
}

/** Canonical inspector with a real process timeout. No SSH command is copied,
 * no actor is stopped, and no promise leaves an unbounded child behind. */
export async function inspectPreparedNativeRun(runId:string) {
  const {inspectStopped}=await import('./continuation.js');
  const execute=promisify(execFile);
  return inspectStopped(runId,true,(file,args,options)=>execute(file,args,{...options,timeout:5000}));
}

/** Consume the transient queue marker BEFORE INSERT freezes instructions.
 * A failed insertion rolls this change back with the admission savepoint. */
export async function consumeNativeAdmission(db:DB,binding:NonNullable<Awaited<ReturnType<typeof readPreparedNativeAdmission>>>,newRunId:string) {
  const {task,run,receipt}=binding;
  const consumed=await db.query(`UPDATE tasks SET execution=jsonb_set(execution-'supervisedAdmission','{continuation}',$2::jsonb)
    WHERE id=$1 AND execution#>>'{supervisedAdmission,id}'=$3`,[task.id,JSON.stringify({state:'queued',owner:'scheduler',runId:newRunId,
    sourceRunId:run.id,caseId:receipt.caseId,updatedAt:new Date().toISOString()}),receipt.id]);
  if(consumed.rowCount!==1)throw failure('BINDING_CHANGED');
}
export async function finishNativeAdmission(db:DB,binding:NonNullable<Awaited<ReturnType<typeof readPreparedNativeAdmission>>>,newRunId:string) {
  const {task,run,receipt}=binding;
  await event(db,'CONTINUATION_QUEUED',task.id,{runId:run.id,nextRunId:newRunId,signature:receipt.signature,caseId:receipt.caseId});
  await event(db,'SUPERVISION_RESUMED',task.id,{caseId:receipt.caseId,sourceRunId:run.id,nextRunId:newRunId,
    stopConfirmed:true,admissionReceiptId:receipt.id});
}
