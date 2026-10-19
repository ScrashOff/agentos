import {randomUUID,createHash} from 'node:crypto';
import {pool,tx,lock,event,type DB} from './db.js';
import {readArtifact} from './service.js';
import {acceptedSupervisionActions} from './supervision-actions.js';
import {reviseInstruction} from './task-instructions.js';
import {executionInputManifest} from './execution-inputs.js';
import {acceptedSnapshotProof,sameSnapshotProof} from './deferred-handoffs.js';
import {deliveryDigest} from './delivery-supervision.js';
import {recordManagementTransitions} from './management.js';

const sha=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const phaseKind='FIRST_ADMISSION_CONTRACT_READY';
const nextAction='Read the current FIRST_ADMISSION_CONTRACT_READY evidence and the original verification contract. Submit one revise with initialAdmission:{phaseEventId,contractDigest} copied exactly from that phase. Preserve read-only execution, all criteria, inputs and dependencies. When contract.handoffs specifies preserve_existing_consumer, preserve that integrator as owner, including any replanning/conflict; do not reattach its diagnostic to QA or claim the reuse completed. This only authorizes the first instruction revision; scheduler admission still requires accepted dependencies. It does not consume or approve any handoff.';

/** No previous run is fabricated: initial instruction authority is a fresh
 * accepted review of an explicit, server-pinned, zero-attempt contract. */
async function readState(db:DB,caseId:string,reviewId?:string){
 const c=(await db.query('SELECT * FROM supervision_cases WHERE id=$1',[caseId])).rows[0];
 if(!c)return null;
 const task=(await db.query('SELECT * FROM tasks WHERE id=$1',[c.task_id])).rows[0];
 const authority=(await db.query(`SELECT g.state goal_state,s.value supervision,a.value autonomy
  FROM goals g LEFT JOIN settings s ON s.id='supervision:'||g.id
  LEFT JOIN settings a ON a.id='autonomy:'||g.project WHERE g.id=$1`,[c.goal_id])).rows[0];
 const review=(await db.query(`SELECT r.id,r.fence,r.state,r.created_at,t.goal_id,t.stage,
  r.created_at>(SELECT created_at FROM events WHERE entity_id=$2 AND kind='FIRST_ADMISSION_CONTRACT_READY'
    AND payload->>'caseId'=$3 ORDER BY id DESC LIMIT 1) after_phase,
  (SELECT passed FROM acceptances WHERE run_id=r.id AND task_id=t.id ORDER BY created_at DESC,id DESC LIMIT 1) passed
  FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1`,[reviewId??c.decision_run_id,c.task_id,c.id])).rows[0];
 const refs=(await db.query(`SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 AND name='coordination.json' ORDER BY id`,[reviewId??c.decision_run_id])).rows;
 const dependencies=(await db.query(`SELECT p.id,p.goal_id,p.stage,p.state,p.execution->>'workspace' workspace,
  p.execution->>'baseWorkspace' scope,p.execution->'readOnlySource' read_only
  FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=$1 ORDER BY p.id`,[c.task_id])).rows;
 const exclusion=(await db.query(`SELECT EXISTS(SELECT 1 FROM runs WHERE task_id=$1::uuid) any_run,
  EXISTS(SELECT 1 FROM leases l JOIN runs r ON r.id=l.run_id WHERE r.task_id=$1::uuid) leased,
  EXISTS(SELECT 1 FROM opencode_jobs j JOIN runs r ON r.id=j.run_id WHERE r.task_id=$1::uuid) any_job,
  EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND (entity_id=$1::text OR payload->>'taskId'=$1::text)) cancelled,
  EXISTS(SELECT 1 FROM supervision_cases WHERE task_id=$1::uuid AND id<>$2::uuid AND
    (state IN('open','decided','delegated','waiting_evidence') OR created_at>(SELECT created_at FROM supervision_cases WHERE id=$2::uuid))) other_case,
  EXISTS(SELECT 1 FROM task_instruction_origins WHERE task_id=$1::uuid) prior_origin,
  EXISTS(SELECT 1 FROM task_instruction_revisions WHERE task_id=$1::uuid) prior_revision`,[c.task_id,c.id])).rows[0];
 const phase=(await db.query(`SELECT id::text,created_at,payload FROM events WHERE entity_id=$1 AND kind=$2
  AND payload->>'caseId'=$3 ORDER BY id DESC LIMIT 1`,[c.task_id,phaseKind,c.id])).rows[0]??null;
 const sources=[];
 for(const e of (Array.isArray(c.evidence)?c.evidence:[]).filter((e:any)=>e.kind==='SCOPED_DELIVERY_ACTION')){
  if(!uuid.test(e?.action?.sourceTaskId??''))continue;
  const source=(await db.query('SELECT * FROM tasks WHERE id=$1',[e.action.sourceTaskId])).rows[0];
  let proof;try{proof=source&&await acceptedSnapshotProof(db,source);}catch{}
  const artifacts=proof?(await db.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[proof.runId])).rows:[];
  const handoff=uuid.test(e?.handoffId??'')?(await db.query(`SELECT h.id,h.goal_id,h.producer_task_id,h.consumer_task_id,
   h.source_task_id,h.source_proof,h.consumer_proof,h.handoff,h.state,
   jsonb_build_object('code',h.receipt->>'code','producerRunId',h.receipt->>'producerRunId','consumerRunId',h.receipt->>'consumerRunId') receipt,
   target.goal_id consumer_goal,target.stage consumer_stage,target.execution->>'workspace' consumer_workspace,
   target.execution->>'baseWorkspace' consumer_scope,target.execution->'readOnlySource' consumer_read_only,
   EXISTS(SELECT 1 FROM dependencies d WHERE d.task_id=h.consumer_task_id AND d.depends_on=h.producer_task_id) attached,
   EXISTS(WITH RECURSIVE ancestors(id) AS(SELECT depends_on FROM dependencies WHERE task_id=h.producer_task_id
    UNION SELECT d.depends_on FROM dependencies d JOIN ancestors a ON d.task_id=a.id)
    SELECT 1 FROM ancestors WHERE id=ANY(ARRAY[h.consumer_task_id,$2::uuid])) cycle
   FROM deferred_handoffs h JOIN tasks target ON target.id=h.consumer_task_id WHERE h.id=$1`,[e.handoffId,c.task_id])).rows[0]??null:null;
  sources.push({source,proof:proof??null,artifacts,handoff});
 }
 return {c,task,authority,review,refs,dependencies,exclusion,phase,sources};
}
type State=NonNullable<Awaited<ReturnType<typeof readState>>>;
const scopedEvidence=(s:State)=>s.c.evidence.filter((e:any)=>e.kind==='SCOPED_DELIVERY_ACTION');
function eligible(s:State|null):s is State{
 if(!s)return false;const {c,task:t,authority:a,dependencies,exclusion:x,sources}=s;
 if(!t||!['open','decided'].includes(c.state)||c.source_run_id!==null||c.applied_attempt!==null||
  t.goal_id!==c.goal_id||t.attempt_count!==0||t.instruction_revision_id||t.stage!=='verify'||
  !['recovery_blocked','blocked_dependencies','ready','waiting_resource'].includes(t.state)||
  t.execution?.kind!=='harness'||t.execution?.readOnlySource!==true||!t.execution.baseWorkspace||
  !t.execution.workspace||!t.execution.sourceWorkspace||t.execution.workspace===t.execution.sourceWorkspace||
  a?.goal_state!=='active'||a.supervision?.enabled!==true||a.autonomy?.enabled===false||
  (t.recovery?.mode==='manual'&&a.autonomy?.enabled!==true)||Object.values(x).some(Boolean)||
  !Array.isArray(c.evidence)||c.evidence.some((e:any)=>!['SCOPED_DELIVERY_ACTION',phaseKind,'FIRST_ADMISSION_DECISION_REJECTED'].includes(e.kind))||
  !Array.isArray(t.criteria)||!t.criteria.length)return false;
 const candidates=dependencies.filter(p=>p.stage==='integrate'&&p.goal_id===c.goal_id&&p.scope===t.execution.baseWorkspace&&
  p.workspace===t.execution.sourceWorkspace&&p.read_only!==true);
 if(candidates.length!==1)return false;
 const evidence=scopedEvidence(s);if(!evidence.length||evidence.length>8)return false;
 for(const e of evidence){
  const action=e?.action,item=sources.find(p=>p.source?.id===action?.sourceTaskId);
  if(!['request_verification','reuse_existing'].includes(action?.kind)||action.taskId!==t.id||
   action.expectedAttempt!==0||e.consumerAttempt!==0||e.consumerRunId!==null||!item||
   item.source.goal_id!==c.goal_id||item.source.execution?.baseWorkspace!==t.execution.baseWorkspace||
   !sameSnapshotProof(item.proof,e.sourceProof)||
   item.proof?.runId!==action.expectedSourceRunId||
   (action.kind==='reuse_existing'&&item.source.execution?.workKind!=='diagnostic')||
   !Array.isArray(action.artifactIds)||!action.artifactIds.length||
   action.artifactIds.some((id:string)=>!item.artifacts.some(ref=>ref.id===id)))return false;
  if(!dependencies.some(p=>p.id===item.source.id)){
   const h=item.handoff;
   if(action.kind!=='reuse_existing'||e.conflict!=='EXISTING_HANDOFF'||!h||h.id!==e.handoffId||
    h.goal_id!==c.goal_id||h.producer_task_id!==item.source.id||h.consumer_task_id!==candidates[0].id||
    h.consumer_goal!==c.goal_id||h.consumer_stage!=='integrate'||h.consumer_scope!==t.execution.baseWorkspace||
    h.consumer_workspace!==t.execution.sourceWorkspace||h.consumer_read_only===true||h.cycle||
    h.handoff?.workKind!=='diagnostic'||h.handoff?.taskId!==item.source.id||
    !['applied','replanning_required','staged','waiting_result'].includes(h.state)||
    (h.state==='applied'&&(!h.attached||h.receipt?.producerRunId!==item.proof.runId)))return false;
  }
 }
 try{executionInputManifest(t.execution.inputs??[]);}catch{return false;}
 return true;
}
function reviewAccepted(s:State){const r=s.review;return r&&r.goal_id===s.c.goal_id&&r.stage==='coordinate'&&
 r.state==='submitted'&&r.passed===true&&s.refs.length===1&&s.refs[0].size<=1024*1024;}
function contract(s:State){
 const t=s.task,{continuation,...execution}=t.execution;
 const value={taskId:t.id,goalId:t.goal_id,stage:t.stage,title:t.title,description:t.description,criteria:t.criteria,
  execution,agentId:t.agent_id,resources:t.resources,requiredMemories:t.required_memories,allowedAccounts:t.allowed_accounts,
  accountId:t.account_id,model:t.model,covers:t.covers,recovery:t.recovery,maxAttempts:t.max_attempts,
  dependencies:s.dependencies.map(({id,goal_id,stage,workspace,scope,read_only})=>({id,goal_id,stage,workspace,scope,read_only})),
  sources:s.sources.map(item=>{const {continuation,...sourceExecution}=item.source.execution;
   return {proof:item.proof,artifacts:item.artifacts,handoff:item.handoff,contractDigest:deliveryDigest({description:item.source.description,
    criteria:item.source.criteria,execution:sourceExecution,revisionId:item.source.instruction_revision_id})};})};
 return {digest:deliveryDigest(value),summary:{taskId:t.id,goalId:t.goal_id,attempt:0,workspace:execution.workspace,
  sourceWorkspace:execution.sourceWorkspace,scope:execution.baseWorkspace,readOnlySource:true,
  descriptionDigest:sha(t.description),criteriaDigest:deliveryDigest(t.criteria),executionDigest:deliveryDigest(execution),
  inputManifest:executionInputManifest(execution.inputs??[]),dependencyIds:s.dependencies.map(p=>p.id),
  sourceProofs:s.sources.map(item=>item.proof),handoffs:s.sources.filter(item=>item.handoff).map(item=>({
   id:item.handoff.id,producerTaskId:item.handoff.producer_task_id,consumerTaskId:item.handoff.consumer_task_id,
   state:item.handoff.state,code:item.handoff.receipt?.code??null,bindingDigest:deliveryDigest(item.handoff),
   disposition:'preserve_existing_consumer'})),handoffsConsumed:false,acceptance:false}};
}
async function actionsFromArtifact(s:State){
 if(!reviewAccepted(s))return null;
 const ref=s.refs[0],artifact=await readArtifact(ref.id);
 if(artifact.digest!==ref.digest||artifact.bytes.length!==ref.size||sha(artifact.bytes)!==ref.digest)return null;
 try{const value=JSON.parse(artifact.bytes.toString('utf8'));
  return acceptedSupervisionActions(Array.isArray(value.supervision?.actions)?value.supervision.actions:[]).filter(a=>a.caseId===s.c.id);
 }catch{return null;}
}
const validAction=(actions:any[],caseId:string)=>actions.length===1&&actions[0].caseId===caseId&&actions[0].kind==='revise'&&
 !actions[0].sourceTaskId&&!actions[0].deliveryResolution&&!actions[0].lane;
function bound(s:State,actions:any[]){return validAction(actions,s.c.id)&&s.phase&&reviewAccepted(s)&&
 s.review.after_phase===true&&
 actions[0].initialAdmission?.phaseEventId===s.phase.id&&actions[0].initialAdmission?.contractDigest===contract(s).digest&&
 s.phase.payload.contractDigest===contract(s).digest;}
async function preparePhase(db:DB,s:State,code='FIRST_ADMISSION_REVIEW_REQUIRED'){
 const current=contract(s);
 const inserted=(await db.query(`INSERT INTO events(kind,entity_id,payload,created_at) VALUES($1,$2,$3,clock_timestamp()) RETURNING id::text,created_at`,
  [phaseKind,s.task.id,JSON.stringify({caseId:s.c.id,contractDigest:current.digest,contract:current.summary,
   archivedDecision:s.c.decision,decisionRunId:s.c.decision_run_id,code,nextAction})])).rows[0];
 const evidence={kind:phaseKind,phaseEventId:inserted.id,contractDigest:current.digest,at:new Date(inserted.created_at).toISOString(),
  contract:current.summary,requiredAction:{kind:'revise',initialAdmission:{phaseEventId:inserted.id,contractDigest:current.digest}},nextAction};
 await db.query(`UPDATE supervision_cases SET state='open',decision=NULL,decision_run_id=NULL,
  evidence=evidence||$2::jsonb,reason='First verification requires a fresh contract-bound decision.',updated_at=clock_timestamp()
  WHERE id=$1 AND state=ANY($3::text[])`,[s.c.id,JSON.stringify([evidence]),['open','decided']]);
}
async function reject(db:DB,s:State,reviewId:string,code:string){
 if((await db.query(`SELECT 1 FROM events WHERE entity_id=$1 AND kind='SUPERVISION_PLAN_REJECTED'
  AND payload->>'caseId'=$2 AND payload->>'runId'=$3 AND payload->>'code'=$4`,[s.task.id,s.c.id,reviewId,code])).rowCount)return;
 const evidence={kind:'FIRST_ADMISSION_DECISION_REJECTED',caseId:s.c.id,decisionRunId:reviewId,code,
  phaseEventId:s.phase?.id??null,contractDigest:s.phase?.payload.contractDigest??null,nextAction};
 await event(db,'SUPERVISION_PLAN_REJECTED',s.task.id,{...evidence,runId:reviewId});
 await db.query("UPDATE supervision_cases SET evidence=evidence||$2::jsonb,updated_at=clock_timestamp() WHERE id=$1 AND state='open'",[s.c.id,JSON.stringify([evidence])]);
}
async function observe(caseId:string,reviewId?:string){const db=await pool.connect();
 try{await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');const s=await readState(db,caseId,reviewId);await db.query('COMMIT');return s;}
 catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}}

/** Read-only operational probe. Exposes eligibility flags and hashes only. The
 * caller must use a read-only snapshot; this is never a mutation authorization. */
export async function inspectInitialVerificationEligibility(db:DB,caseId:string){
 const s=await readState(db,caseId);if(!s)return{exists:false,eligible:false};
 const t=s.task,evidence=Array.isArray(s.c.evidence)?scopedEvidence(s):[];
 return{exists:true,eligible:eligible(s),caseId:s.c.id,caseState:s.c.state,taskId:t?.id,
  taskState:t?.state,attempt:t?.attempt_count,sourceRunId:s.c.source_run_id,phaseEventId:s.phase?.id??null,
  authority:{goalState:s.authority?.goal_state,supervision:s.authority?.supervision?.enabled,
   autonomy:s.authority?.autonomy?.enabled??null,manual:t?.recovery?.mode==='manual'},
  execution:{kind:t?.execution?.kind,readOnly:t?.execution?.readOnlySource,distinctWorkspace:t?.execution?.workspace!==t?.execution?.sourceWorkspace,
   candidateDependencies:s.dependencies.filter(p=>p.stage==='integrate'&&p.goal_id===s.c.goal_id&&p.scope===t?.execution?.baseWorkspace&&p.workspace===t?.execution?.sourceWorkspace&&p.read_only!==true).length},
  exclusions:s.exclusion,reviewAccepted:reviewAccepted(s),evidenceKinds:[...new Set((s.c.evidence??[]).map((e:any)=>e.kind))],
  dependencyCount:s.dependencies.length,pendingDependencies:s.dependencies.filter(p=>p.state!=='accepted').length,
  sources:evidence.map((e:any)=>{const item=s.sources.find(p=>p.source?.id===e.action?.sourceTaskId);
   return{taskId:e.action?.sourceTaskId,action:e.action?.kind,kind:item?.source?.execution?.workKind,
    proofMatches:sameSnapshotProof(item?.proof,e.sourceProof),expectedRunMatches:item?.proof?.runId===e.action?.expectedSourceRunId,
    inDependencies:s.dependencies.some(p=>p.id===item?.source?.id),scopeMatches:item?.source?.execution?.baseWorkspace===t?.execution?.baseWorkspace,
    artifactsMatch:Array.isArray(e.action?.artifactIds)&&e.action.artifactIds.every((id:string)=>item?.artifacts.some(a=>a.id===id)),
    handoff:item?.handoff?{id:item.handoff.id,state:item.handoff.state,consumerTaskId:item.handoff.consumer_task_id,
     disposition:'preserve_existing_consumer',cycle:item.handoff.cycle}:null};}),
  ...(eligible(s)?{contractDigest:contract(s).digest}:{}),authorization:false};
}

/** Returns true when this phase owns recording, including a typed rejection. */
export async function recordInitialVerificationDecision(caseId:string,reviewId:string,proposed:any[]){
 const observed=await observe(caseId,reviewId);if(!observed?.phase)return false;
 const published=await actionsFromArtifact(observed);
 if(!published||deliveryDigest(published)!==deliveryDigest(proposed))return true;
 return tx(async db=>{await lock(db);const s=await readState(db,caseId,reviewId);
  if(!eligible(s)||!s.phase||s.c.state!=='open'||!reviewAccepted(s)||deliveryDigest(s)!==deliveryDigest(observed))return true;
  if(s.phase.payload.contractDigest!==contract(s).digest){await preparePhase(db,s,'FIRST_ADMISSION_CONTRACT_CHANGED');return true;}
  if(!bound(s,published)){await reject(db,s,reviewId,'FIRST_ADMISSION_BINDING_REQUIRED');return true;}
  await db.query(`UPDATE supervision_cases SET state='decided',decision=$2,decision_run_id=$3,updated_at=clock_timestamp()
   WHERE id=$1 AND state='open' AND decision_run_id IS NULL`,[caseId,JSON.stringify({actions:published}),reviewId]);
  return true;
 });
}

export async function applyInitialVerificationDecision(caseId:string){
 if(!uuid.test(caseId))return false;
 const observed=await observe(caseId);if(!eligible(observed)||observed.c.state!=='decided')return false;
 const actions=await actionsFromArtifact(observed);
 if(!actions||!validAction(actions,caseId)||deliveryDigest(actions)!==deliveryDigest(observed.c.decision?.actions))return false;
 // Metadata proofs and bytes are checked before the global lock; no handoff is
 // consumed here, and even a historical accepted diagnostic does not grant QA.
 for(const e of scopedEvidence(observed))for(const id of e.action.artifactIds){
  const expected=observed.sources.flatMap(p=>p.artifacts).find(a=>a.id===id),artifact=await readArtifact(id);
  if(!expected||artifact.digest!==expected.digest||artifact.bytes.length!==expected.size)return false;
 }
 const stamp=deliveryDigest(observed);
 // Preserve the normal supervision ordering: bind a genuine accepted manager
 // transition before applying its effect. A legacy phase refresh is no effect.
 if(bound(observed,actions))await recordManagementTransitions(observed.c.decision_run_id);
 return tx(async db=>{await lock(db);const s=await readState(db,caseId);
  if(!eligible(s)||s.c.state!=='decided'||deliveryDigest(s)!==stamp)return false;
  if(!s.phase||s.phase.payload.contractDigest!==contract(s).digest){await preparePhase(db,s);return false;}
  if(!bound(s,actions)){
   await db.query("UPDATE supervision_cases SET state='open',decision=NULL,decision_run_id=NULL WHERE id=$1",[caseId]);
   await reject(db,s,s.review.id,'FIRST_ADMISSION_BINDING_REQUIRED');return false;
  }
  const {c,task:t,dependencies}=s,original=t.description,instruction=reviseInstruction(actions[0]);
  const rendered=original+'\n\nINITIAL SUPERVISION DIRECTION (original contract preserved):\n'+instruction;
  const originId=randomUUID(),revisionId=randomUUID(),ref=s.refs[0];
  const provenance={caseId:c.id,kind:'revise',sourceRunId:null,sourceAttempt:0,firstAdmission:true,
   initialAdmission:actions[0].initialAdmission,decisionRunId:c.decision_run_id,decisionArtifactId:ref.id,
   decisionArtifactDigest:ref.digest,decisionSnapshot:c.decision,originalContractDigest:contract(s).digest,handoffsConsumed:false};
  await db.query("UPDATE supervision_cases SET state='applied',applied_attempt=0,updated_at=clock_timestamp() WHERE id=$1 AND state='decided'",[c.id]);
  await db.query(`INSERT INTO task_instruction_origins(id,task_id,goal_id,baseline,baseline_hash,original,original_hash,proof)
   VALUES($1,$2,$3,$4,$5,$4,$5,$6)`,[originId,t.id,t.goal_id,original,sha(original),JSON.stringify({kind:'first_admission',sourceAttempt:0,sourceRunId:null,initialAdmission:actions[0].initialAdmission})]);
  await db.query(`INSERT INTO task_instruction_revisions(id,task_id,goal_id,origin_id,number,predecessor,operation_key,
   instruction,instruction_hash,provenance,rendered,rendered_hash) VALUES($1,$2,$3,$4,1,NULL,$5,$6,$7,$8,$9,$10)`,
   [revisionId,t.id,t.goal_id,originId,c.id+':'+c.decision_run_id+':first-admission',instruction,sha(instruction),JSON.stringify(provenance),rendered,sha(rendered)]);
  const pending=dependencies.some(p=>p.state!=='accepted');
  await db.query(`UPDATE tasks SET description=$2,instruction_revision_id=$3,execution=execution-'continuation',
   state=$4,reason=$5,updated_at=clock_timestamp() WHERE id=$1`,[t.id,rendered,revisionId,pending?'blocked_dependencies':'ready',
   pending?'Initial verification authorized; waiting for accepted dependencies.':'Initial verification authorized; waiting for scheduler admission.']);
  await event(db,'TASK_INSTRUCTIONS_VERSIONED',t.id,{revisionId,number:1,originId,baselineHash:sha(original),originalHash:sha(original),renderedHash:sha(rendered),provenance});
  await event(db,'SUPERVISION_INITIAL_ADMISSION_APPLIED',t.id,{caseId:c.id,decisionRunId:c.decision_run_id,sourceRunId:null,sourceAttempt:0,
   revisionId,initialAdmission:actions[0].initialAdmission,dependencyCount:dependencies.length,waitingDependencies:pending,
   priorExecution:false,handoffsConsumed:false,acceptance:false,newRunCreated:false});
  return true;
 });
}
