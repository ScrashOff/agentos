import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';
import {pool,tx,lock,event,type DB} from './db.js';
import {config} from './config.js';
import {DomainError} from './contracts.js';
import {bridgeScript} from './bridge-path.js';
import {independentNativeSupervisorSql} from './native-supervision.js';
import {inspectStopped} from './continuation.js';
import {requestRecoveryReview} from './recovery-supervision.js';
import {operatorOpenCodeRecoveryBatch} from './operator-opencode-recovery-scan.js';
import {operatorRecoveryCode,operatorRecoveryDigest,type OperatorAuth} from './operator-recovery-contract.js';
import {operatorOpenCodeRecoveryRequestSchema,operatorOpenCodeRecoveryIdentitySchema,checkedOperatorOpenCodeIdentity,hasOperatorOpenCodeRecoveryIntent,
 type OperatorOpenCodeRecoveryRequest} from './operator-opencode-recovery-contract.js';

const exec=promisify(execFile),controllerCandidate=fileURLToPath(new URL('../',import.meta.url)).replace(/[\\/]$/,'');
const refused=(code:string)=>new DomainError(code,'A identidade ou a autoridade da execução OpenCode mudou; nenhuma nova execução foi autorizada.',409);
async function observe(db:DB,taskId:string,request:OperatorOpenCodeRecoveryRequest,controller:string,state='running'){
 // Materialize the frozen object once. Repeated JSON paths otherwise detoast a
 // large embedded planning catalog separately. The full object remains in the
 // exact existing digest and every identity guard below is preserved.
 return (await db.query(`WITH frozen_snapshot AS MATERIALIZED (
  SELECT id,instruction_snapshot || '{}'::jsonb instruction_snapshot FROM runs WHERE id=$2::uuid
 ) SELECT r.id,r.fence,r.task_id,r.attempt,r.context_id,t.goal_id,t.instruction_revision_id,
  m.workspace,m.work_kind,t.execution->>'planningRequestId' planning_request_id,
  j.supervision->>'launchToken' launch_token,j.supervision->>'candidate' candidate,
  j.supervision->>'digest' digest,j.supervision->'owner' owner,p.source_revision,p.scope,
  encode(sha256(convert_to(jsonb_build_array(t.description,t.criteria,t.resources,t.allowed_accounts,
    t.required_memories,t.recovery,m.control,t.execution->'planningRequestId',m.input_digests,frozen_snapshot.instruction_snapshot)::text,'UTF8')),'hex') contract_digest
 FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
 JOIN frozen_snapshot ON frozen_snapshot.id=r.id
 JOIN task_execution_metadata m ON m.task_id=t.id AND m.goal_id=t.goal_id AND m.version=1 AND m.execution_type='object'
 JOIN opencode_jobs j ON j.run_id=r.id JOIN settings s ON s.id='supervision:'||g.id
 LEFT JOIN settings a ON a.id='autonomy:'||g.project
 LEFT JOIN planning_requests p ON p.id=$8::uuid AND p.task_id=t.id AND p.goal_id=t.goal_id
 WHERE t.id=$1 AND r.id=$2 AND r.fence=$3 AND r.attempt=$4 AND t.attempt_count=r.attempt
  AND r.state=$9 AND t.state=$9 AND r.runtime='opencode' AND m.control->>'kind'='opencode'
  AND r.context_id=$7 AND frozen_snapshot.instruction_snapshot->>'schema'='1'
  AND EXISTS(SELECT 1 FROM contexts cx WHERE cx.id=r.context_id AND cx.run_id=r.id)
  AND frozen_snapshot.instruction_snapshot#>>'{task,id}'=t.id::text
  AND frozen_snapshot.instruction_snapshot#>>'{task,goal_id}'=t.goal_id::text
  AND frozen_snapshot.instruction_snapshot#>>'{task,execution,kind}'='opencode'
  AND frozen_snapshot.instruction_snapshot#>>'{task,execution,workspace}'=m.workspace
  AND ((m.work_kind='planning' AND p.id IS NOT NULL AND t.execution->>'planningRequestId'=p.id::text
    AND frozen_snapshot.instruction_snapshot#>>'{task,execution,planningRequestId}'=p.id::text
    AND p.state IN('assigned','running','awaiting_supervision') AND NOT EXISTS(SELECT 1 FROM planning_requests newer
      WHERE newer.goal_id=p.goal_id AND newer.scope=p.scope AND (newer.created_at,newer.id)>(p.created_at,p.id)))
    OR (m.work_kind IS DISTINCT FROM 'planning' AND $8::uuid IS NULL))
  AND g.state='active' AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'
  AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
  AND j.state='running' AND ${independentNativeSupervisorSql()}
  AND NOT EXISTS(SELECT 1 FROM runs other WHERE other.task_id=t.id AND other.id<>r.id
    AND other.state IN('queued','running','cancelling','reconciling'))
  AND NOT EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=r.id::text)
  AND NOT EXISTS(SELECT 1 FROM supervision_cases c WHERE c.task_id=t.id
    AND c.state IN('open','decided','delegated','waiting_evidence'))
  AND (SELECT count(*)=4 AND count(DISTINCT value->>'digest')=1 AND bool_and(value->>'digest' ~ '^[a-f0-9]{64}$')
    FROM settings WHERE id=ANY($6::text[]) AND value->>'candidate'=$5)
  AND (SELECT value->>'draining' FROM settings WHERE id='maintenance')='false'`,[
   taskId,request.runId,request.runFence,request.expectedAttempt,controller,
   ['agentos-workers','agentos-backlog','agentos-cycles','agentos-opencode'].map(s=>'worker-readiness:'+s),
   request.contextVersion,request.planningRequestId,state])).rows[0];
}
export async function inspectOperatorOpenCodeRecoveryIdentity(row:any){
 const {stdout}=await exec('ssh',['-T','-o','BatchMode=yes','-o','ConnectTimeout=8',config.bridgeHost,
  '/opt/deepseek-harness/node/bin/node',bridgeScript('inspect-operator-opencode-recovery.mjs'),
  row.id,row.workspace,row.fence,row.context_id,row.candidate],{timeout:20000,maxBuffer:32768,windowsHide:true});
 return JSON.parse(stdout);
}
export async function stopOperatorOpenCodeRecovery(payload:{requestId:string;identity:any}){
 // No shell text or provider data is accepted. The trusted helper rechecks the
 // exact physical owner before signalling its one sandbox; it never stops owner.
 return new Promise<unknown>((resolve,reject)=>{
  const child=execFile('ssh',['-T','-o','BatchMode=yes','-o','ConnectTimeout=8',config.bridgeHost,
   '/opt/deepseek-harness/node/bin/node',bridgeScript('stop-operator-opencode-recovery.mjs')],
   {timeout:30000,maxBuffer:32768,windowsHide:true},(error,stdout)=>{
    if(error){reject(Error('OPENCODE_RECOVERY_STOP_UNCERTAIN'));return;}
    try{resolve(JSON.parse(stdout));}catch{reject(Error('OPENCODE_RECOVERY_STOP_UNCERTAIN'));}
   });
  child.stdin?.on('error',()=>{});child.stdin?.end(JSON.stringify(payload));
 });
}
function matches(proof:ReturnType<typeof checkedOperatorOpenCodeIdentity>,row:any){
 return proof.runId===row.id&&proof.runFence===row.fence&&proof.contextVersion===row.context_id&&
  proof.workspace===row.workspace&&proof.owner.candidate===row.candidate&&
  proof.owner.mainPid===Number(row.owner.pid)&&proof.owner.controlGroup===row.owner.cgroup;
}
function response(row:any,duplicate:boolean){
 const receipt=row.checkpoint.operatorRecoveryStop,dispatch=row.checkpoint.operatorOpenCodeRecovery;
 const stop=row.checkpoint.nativeStopReceipt;
 const stopConfirmed=stop?.runFence===row.fence&&/^[a-f0-9]{64}$/.test(stop.bridgeReceiptDigest??'')&&
  stop.process?.unit===`agentos-run-${row.id}`&&stop.process.stopped===true&&stop.process.mainPid===0&&
  stop.process.populated===false&&['inactive','failed'].includes(stop.process.activeState)&&
  ['loaded','not-found'].includes(stop.process.loadState)&&
  stop.supervisor?.unit===`agentos-native-supervisor-${row.id}`&&stop.supervisor.mainPid===0&&
  stop.supervisor.pendingJob===false&&stop.supervisor.cgroupEmpty===true&&
  ['inactive','failed'].includes(stop.supervisor.activeState)&&['loaded','not-found'].includes(stop.supervisor.loadState);
 return {requestId:receipt.request.requestId,runId:row.id,state:row.state,duplicate,
  phase:stopConfirmed?'awaiting_supervision':dispatch?.phase??'requested',stopConfirmed,
  recoveryPending:!['submitted','cancelled'].includes(row.state),leasesRetained:row.state==='reconciling',
  newAttemptQueued:false,code:operatorRecoveryCode,runtime:'opencode'};
}
async function previous(db:DB,taskId:string,request:OperatorOpenCodeRecoveryRequest){
 const row=(await db.query('SELECT id,task_id,fence,state,checkpoint FROM runs WHERE id=$1 AND task_id=$2',[request.runId,taskId])).rows[0];
 const receipt=row?.checkpoint?.operatorRecoveryStop;if(!receipt)return null;
 if(row.fence!==request.runFence||receipt.requestDigest!==operatorRecoveryDigest(request)||
  !await hasOperatorOpenCodeRecoveryIntent(db,row))throw refused('OPERATOR_RECOVERY_REQUEST_CONFLICT');
 return response(row,true); // Historical receipt only; no renewed signal authority.
}
function checkedRequestIdentity(raw:unknown){
 // Keep the canonical ten-second authority window. Classifying its rejection
 // must not refresh a timestamp, relax the owner allowlist or inspect twice.
 const parsed=operatorOpenCodeRecoveryIdentitySchema.safeParse(raw);
 if(!parsed.success)throw new DomainError('OPENCODE_RECOVERY_IDENTITY_INVALID',
  'A observação do executor não comprovou uma identidade compatível; nenhuma interrupção foi autorizada.',409);
 try{return checkedOperatorOpenCodeIdentity(parsed.data);}
 catch{
  if(Date.now()-Date.parse(parsed.data.checkedAt)>10000)
   throw new DomainError('OPENCODE_RECOVERY_OBSERVATION_EXPIRED',
    'A observação do executor expirou antes da persistência. Consulte o pedido canônico antes de qualquer nova solicitação.',409);
  throw new DomainError('OPENCODE_RECOVERY_IDENTITY_INVALID',
   'A observação do executor não comprovou uma identidade compatível; nenhuma interrupção foi autorizada.',409);
 }
}
export async function requestOperatorOpenCodeRecovery(taskId:string,raw:unknown,auth:OperatorAuth,
 inspect=inspectOperatorOpenCodeRecoveryIdentity,expectedController=controllerCandidate){
 const request=operatorOpenCodeRecoveryRequestSchema.parse(raw);
 if(!['api-token','operator-session'].includes(auth?.channel))throw new DomainError('OPERATOR_REQUIRED','Operador autenticado obrigatório.',403);
 const started=performance.now();let phase='lookup_receipt',phaseStarted=started,writeAttempted=false;
 const begin=(name:string)=>{phase=name;phaseStarted=performance.now();};
 const report=(code:string)=>{
  // Explicit allowlist of fields: never include exceptions, SQL, stderr, shell
  // arguments, task content, physical-owner metadata or authentication data.
  try{console.warn(JSON.stringify({event:'OPENCODE_RECOVERY_PREFLIGHT',version:1,phase,
   elapsedMs:Math.max(0,Math.round(performance.now()-started)),
   phaseElapsedMs:Math.max(0,Math.round(performance.now()-phaseStarted)),
   code,requestId:request.requestId,runId:request.runId}));}catch{/* Observation grants no authority. */}
 };
 const step=async<T>(name:string,read:()=>Promise<T>)=>{begin(name);const result=await read();report('OK');return result;};
 try{
 const recorded=await step('lookup_receipt',()=>previous(pool,taskId,request));if(recorded)return recorded;
 if(!config.bridgeHost||!config.remoteExecution||!/^\/opt\/agentos\/releases\/managed-[a-f0-9-]{36}$/.test(expectedController))
  throw refused('OPENCODE_RECOVERY_UNSUPPORTED');
 const observed=await step('observe_contract',()=>observe(pool,taskId,request,expectedController));
 if(!observed){const replay=await step('lookup_replay',()=>previous(pool,taskId,request));if(replay)return replay;throw refused('OPENCODE_RECOVERY_STATE_CHANGED');}
 const rawProof=await step('inspect_owner',async()=>{
  try{return await inspect(observed);}
  catch{throw new DomainError('OPENCODE_RECOVERY_INSPECTION_UNAVAILABLE',
   'Não foi possível inspecionar o executor. Nenhuma interrupção foi autorizada por esta chamada.',503);}
 });
 begin('validate_identity');const proof=checkedRequestIdentity(rawProof);report('OK');
 begin('open_transaction');
 const result=await tx(async db=>{
  await step('wait_admission_lock',()=>lock(db));
  const replay=await step('locked_lookup_receipt',()=>previous(db,taskId,request));if(replay)return replay;
  const current=await step('revalidate_contract',()=>observe(db,taskId,request,expectedController));
  if(!current||operatorRecoveryDigest(current)!==operatorRecoveryDigest(observed)||!matches(proof,current))
   throw refused('OPENCODE_RECOVERY_STATE_CHANGED');
  begin('revalidate_identity');checkedRequestIdentity(proof);report('OK');
  const receipt={version:1,taskId,goalId:current.goal_id,request,requestDigest:operatorRecoveryDigest(request),
   authChannel:auth.channel,requestedAt:new Date().toISOString(),launchToken:current.launch_token,
   candidate:current.candidate,candidateDigest:current.digest,contractDigest:current.contract_digest,
   sourceRevision:current.source_revision??null,identity:proof,identityDigest:operatorRecoveryDigest(proof),
   externalEvidence:{operatorSupplied:true,contextOnly:true,verifiedByServer:false},administrative:true,gameFailureClaimed:false};
  const dispatch={version:1,requestDigest:receipt.requestDigest,phase:'requested'};
  begin('persist_intent');writeAttempted=true;
  const changed=await db.query(`UPDATE runs SET state='reconciling',error_code=$3,
   checkpoint=COALESCE(checkpoint,'{}'::jsonb)||jsonb_build_object('operatorRecoveryStop',$4::jsonb,'operatorOpenCodeRecovery',$5::jsonb)
   WHERE id=$1 AND fence=$2 AND state='running' RETURNING id,task_id,fence,state,checkpoint`,[
    current.id,current.fence,operatorRecoveryCode,JSON.stringify(receipt),JSON.stringify(dispatch)]);
  if(!changed.rowCount)throw refused('OPENCODE_RECOVERY_STATE_CHANGED');
  await db.query("UPDATE tasks SET state='reconciling',reason=$2,updated_at=clock_timestamp() WHERE id=$1",[
   taskId,'Interrupção administrativa OpenCode solicitada; aguardando encerramento confirmado e supervisão.']);
  if(request.planningRequestId)await db.query(`UPDATE planning_requests SET state='awaiting_supervision',
   receipt=receipt||jsonb_build_object('operatorRecovery',$2::jsonb),updated_at=clock_timestamp() WHERE id=$1`,[
    request.planningRequestId,JSON.stringify({runId:current.id,runFence:current.fence,requestId:request.requestId,code:operatorRecoveryCode})]);
  await event(db,operatorRecoveryCode,taskId,{runId:current.id,runFence:current.fence,requestId:request.requestId,
   requestDigest:receipt.requestDigest,identityDigest:receipt.identityDigest,receiptDigest:operatorRecoveryDigest(receipt),
   authChannel:auth.channel,runtime:'opencode',reasonCode:request.reasonCode,evidenceRefs:request.evidenceRefs,
   externalEvidence:receipt.externalEvidence,administrative:true,gameFailureClaimed:false,leasesRetained:true,newAttemptQueued:false});
  report('OK');begin('commit_intent');
  return response(changed.rows[0],false);
 });
 report('OK');return result;
 }catch(error){
  // A transport failure during COMMIT cannot prove that the intent is absent.
  // Keep that uncertainty explicit; no stop/retry is issued by this API path.
  const safe=error instanceof DomainError?error:writeAttempted?
   new DomainError('OPENCODE_RECOVERY_COMMIT_UNCONFIRMED',
    'Não foi possível confirmar a persistência do pedido. Consulte seu estado canônico antes de qualquer nova solicitação.',503):
   new DomainError('OPENCODE_RECOVERY_DATABASE_UNAVAILABLE',
    'Não foi possível validar o estado persistido. Nenhuma intenção nova foi gravada por esta chamada.',503);
  report(safe.code);throw safe;
 }
}

type ConsumerIO={inspect?:typeof inspectOperatorOpenCodeRecoveryIdentity;stop?:typeof stopOperatorOpenCodeRecovery;expectedController?:string};
/** One durable dispatch; uncertainty never causes another signal. The existing
 * detached reconciler continues read-only observations until both owners stop. */
export async function consumeOperatorOpenCodeRecovery(runId:string,io:ConsumerIO={}){
 const initial=(await pool.query('SELECT id,task_id,fence,state,checkpoint FROM runs WHERE id=$1',[runId])).rows[0];
 if(!initial||initial.state!=='reconciling'||initial.checkpoint?.operatorOpenCodeRecovery?.phase!=='requested'||
  !await hasOperatorOpenCodeRecoveryIntent(pool,initial))return false;
 const receipt=initial.checkpoint.operatorRecoveryStop,request=operatorOpenCodeRecoveryRequestSchema.parse(receipt.request);
 const controller=io.expectedController??controllerCandidate;
 const observed=await observe(pool,initial.task_id,request,controller,'reconciling');if(!observed)return false;
 let proof:ReturnType<typeof checkedOperatorOpenCodeIdentity>;
 try{proof=checkedOperatorOpenCodeIdentity(await(io.inspect??inspectOperatorOpenCodeRecoveryIdentity)(observed));}
 catch{return false;} // Before dispatch: no physical action was attempted.
 const admitted=await tx(async db=>{
  await lock(db);
  const row=(await db.query('SELECT id,task_id,fence,state,checkpoint FROM runs WHERE id=$1 FOR UPDATE',[runId])).rows[0];
  if(!row||row.state!=='reconciling'||row.checkpoint?.operatorOpenCodeRecovery?.phase!=='requested'||
   !await hasOperatorOpenCodeRecoveryIntent(db,row)||row.checkpoint.operatorRecoveryStop.requestDigest!==receipt.requestDigest)return false;
  const current=await observe(db,row.task_id,request,controller,'reconciling');
  if(!current||operatorRecoveryDigest(current)!==operatorRecoveryDigest(observed)||!matches(proof,current)||
   current.contract_digest!==receipt.contractDigest||current.launch_token!==receipt.launchToken||
   current.candidate!==receipt.candidate||current.digest!==receipt.candidateDigest||
   operatorRecoveryDigest({process:proof.process,owner:proof.owner})!==
    operatorRecoveryDigest({process:receipt.identity.process,owner:receipt.identity.owner})||
   (current.source_revision??null)!==receipt.sourceRevision)return false;
  checkedOperatorOpenCodeIdentity(proof);
  await db.query(`UPDATE runs SET checkpoint=jsonb_set(checkpoint,'{operatorOpenCodeRecovery}',$3::jsonb)
   WHERE id=$1 AND fence=$2`,[runId,row.fence,JSON.stringify({version:1,requestDigest:receipt.requestDigest,
    phase:'stopping',dispatchedAt:new Date().toISOString(),physicalIdentityDigest:operatorRecoveryDigest(proof)})]);
  await event(db,'OPENCODE_RECOVERY_STOP_DISPATCHED',row.task_id,{runId,runFence:row.fence,
   requestId:request.requestId,identityDigest:operatorRecoveryDigest(proof),newAttemptQueued:false});
  return true;
 });
 if(!admitted)return false;
 let observation:unknown=null;
 try{observation=await(io.stop??stopOperatorOpenCodeRecovery)({requestId:request.requestId,identity:proof});}
 catch{/* A failed transport does not prove that the stop was not delivered. */}
 await tx(async db=>{
  await lock(db);
  const changed=await db.query(`UPDATE runs SET checkpoint=jsonb_set(checkpoint,'{operatorOpenCodeRecovery}',
    checkpoint->'operatorOpenCodeRecovery'||$3::jsonb) WHERE id=$1 AND fence=$2 AND state='reconciling'
    AND checkpoint#>>'{operatorOpenCodeRecovery,phase}'='stopping' RETURNING task_id`,[
     runId,request.runFence,JSON.stringify({phase:'uncertain',observationReceived:observation!==null,
      observationDigest:observation===null?null:operatorRecoveryDigest(observation),observedAt:new Date().toISOString()})]);
  if(changed.rowCount)await event(db,'OPENCODE_RECOVERY_STOP_AWAITING_CONFIRMATION',changed.rows[0].task_id,
   {runId,runFence:request.runFence,requestId:request.requestId,observationalOnly:true,newAttemptQueued:false});
 });
 return true;
}
export async function reconcileOperatorOpenCodeRecoveries(io:ConsumerIO&{reviewInspect?:typeof inspectStopped}={}){
 const rows=await operatorOpenCodeRecoveryBatch('dispatch',async(db,cursor)=>(await db.query(`SELECT r.id FROM runs r
  JOIN tasks t ON t.id=r.task_id AND t.attempt_count=r.attempt JOIN goals g ON g.id=t.goal_id AND g.state='active'
  WHERE r.state='reconciling' AND r.runtime='opencode' AND t.state='reconciling'
  AND r.checkpoint#>>'{operatorOpenCodeRecovery,phase}'='requested'
  ORDER BY (r.id>$1::uuid) DESC,r.id LIMIT 2`,[cursor])).rows);
 for(const row of rows)await consumeOperatorOpenCodeRecovery(row.id,io).catch(()=>{});
 const stopped=await operatorOpenCodeRecoveryBatch('review',async(db,cursor)=>(await db.query(`SELECT r.id FROM runs r JOIN tasks t ON t.id=r.task_id AND t.attempt_count=r.attempt
  WHERE r.state='failed' AND r.runtime='opencode' AND r.error_code=$1
  AND t.state IN('failed','recovery_blocked') AND r.checkpoint#>>'{operatorRecoveryStop,request,runtime}'='opencode'
  AND r.checkpoint#>>'{nativeStopReceipt,runFence}'=r.fence::text
  AND NOT EXISTS(SELECT 1 FROM supervision_cases c WHERE c.task_id=t.id AND c.source_run_id=r.id
    AND (c.request_key='recovery:'||encode(sha256(convert_to($1,'UTF8')),'hex')||':'||(r.checkpoint#>>'{operatorRecoveryStop,request,requestId}') OR EXISTS(
      SELECT 1 FROM jsonb_array_elements(c.evidence)e WHERE e->>'kind'=$1
       AND e->>'requestId'=r.checkpoint#>>'{operatorRecoveryStop,request,requestId}')))
  ORDER BY (r.id>$2::uuid) DESC,r.id LIMIT 2`,[operatorRecoveryCode,cursor])).rows);
 for(const row of stopped)await reviewStoppedOperatorOpenCodeRecovery(row.id,io.reviewInspect??inspectStopped).catch(()=>{});
}
/** Planning is deliberately excluded from generic continuation. The exact
 * administrative receipt therefore has its own bounded review consumer. It
 * reuses canonical review guards and never accepts output or queues a retry. */
export async function reviewStoppedOperatorOpenCodeRecovery(runId:string,inspect=inspectStopped){
 const run=(await pool.query(`SELECT r.* FROM runs r JOIN tasks t ON t.id=r.task_id AND t.attempt_count=r.attempt
  WHERE r.id=$1 AND r.runtime='opencode' AND r.state='failed' AND r.error_code=$2
   AND t.state IN('failed','recovery_blocked')`,[runId,operatorRecoveryCode])).rows[0];
 if(!run||!await hasOperatorOpenCodeRecoveryIntent(pool,run)||
  run.checkpoint.nativeStopReceipt?.runFence!==run.fence||
  !/^[a-f0-9]{64}$/.test(run.checkpoint.nativeStopReceipt?.bridgeReceiptDigest??''))return null;
 const proof=await inspect(runId,true);
 return requestRecoveryReview(run.task_id,runId,operatorRecoveryCode,
  'Interrupção administrativa OpenCode confirmada; decidir recuperação com contexto atual, preservando o histórico e os critérios.',
  {stopProof:proof});
}
async function plannerIntent(db:DB,requestId:string,runId?:string){
 const row=(await db.query(`SELECT r.id,r.task_id,r.fence,r.checkpoint FROM planning_requests p
  JOIN tasks t ON t.id=p.task_id JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
  WHERE p.id=$1 AND ($2::uuid IS NULL OR r.id=$2) AND r.runtime='opencode'
  AND r.state NOT IN('submitted','cancelled') AND p.state NOT IN('applied','superseded','blocked_external')
  AND r.checkpoint#>>'{operatorRecoveryStop,request,planningRequestId}'=p.id::text`,[requestId,runId??null])).rows[0];
 return row&&await hasOperatorOpenCodeRecoveryIntent(db,row)?row:null;
}
/** Prevent only this administrative attempt from becoming generic planner retry. */
export async function keepOperatorOpenCodePlannerInSupervision(requestId:string,runId:string){
 return tx(async db=>{
  await lock(db);const row=await plannerIntent(db,requestId,runId);if(!row)return false;
  await db.query(`UPDATE planning_requests SET state='awaiting_supervision',updated_at=clock_timestamp()
   WHERE id=$1 AND state<>'awaiting_supervision'`,[requestId]);return true;
 });
}
export async function operatorOpenCodeRecoveryOwnsScope(db:DB,goalId:string,scope:string){
 const requests=(await db.query(`SELECT p.id FROM planning_requests p JOIN tasks t ON t.id=p.task_id
  JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count WHERE p.goal_id=$1 AND p.scope=$2
  AND p.state='awaiting_supervision' AND r.runtime='opencode' AND r.state NOT IN('submitted','cancelled')
  AND r.checkpoint#>>'{operatorRecoveryStop,request,runtime}'='opencode'
  AND r.checkpoint#>>'{operatorRecoveryStop,request,planningRequestId}'=p.id::text
  ORDER BY p.created_at DESC,p.id DESC`,[goalId,scope])).rows;
 for(const request of requests)if(await plannerIntent(db,request.id))return true;
 return false;
}
