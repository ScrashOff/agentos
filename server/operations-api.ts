import type {FastifyInstance} from 'fastify';
import {z} from 'zod';
import {pool,type DB} from './db.js';
import {DomainError} from './contracts.js';
import {readArtifact} from './service.js';
import {readCanonicalRepairRejection,readCanonicalRepairRejections} from './canonical-repair-rejection.js';
import {assertOperationsScope,decodeOperationsCursor,encodeOperationsCursor,operationDigest,operationHash,
 operationalCode,operationalLabel,operationsPrefix,operationsPrincipal,redactOperationsText,
 type OperationsPrincipal} from './operations-contract.js';

const activeStates=['queued','running','cancelling','reconciling'];
// Mirrors the canonical work-frontier predicate in recordManagementReviews.
// This read projection does not reset counters or decide whether to replan.
const deliveryStagnationThreshold=3;
const integer=(v:any)=>Number.isSafeInteger(Number(v))?Number(v):0;
const observedInteger=(v:any)=>Number.isSafeInteger(v)&&v>=0?v:null;
const smallCodes=(v:any,max=12)=>Array.isArray(v)?v.slice(0,max).map(r=>({code:operationalCode(r?.code),
 caseId:z.uuid().safeParse(r?.caseId).success?r.caseId:null})):[];
const uuid=(v:any)=>z.uuid().safeParse(v).success?v:null;
const instant=(v:any)=>v instanceof Date?v.toISOString():typeof v==='string'&&Number.isFinite(Date.parse(v))?new Date(v).toISOString():null;
const table=async(db:DB,name:string)=>!!(await db.query('SELECT to_regclass($1) present',[name])).rows[0]?.present;
const missing=()=>new DomainError('NOT_FOUND','Registro não encontrado neste objetivo.',404);
const reference=(goalId:string,kind:string,id:string)=>`${operationsPrefix}/goals/${goalId}/items/${kind}/${id}`;

/** This API never acquires scheduler locks or invokes bridge/process commands. */
export async function operationsRead<T>(read:(db:DB)=>Promise<T>):Promise<T> {
 const db=await pool.connect();
 try{await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
  await db.query("SET LOCAL statement_timeout='2s'");await db.query("SET LOCAL lock_timeout='500ms'");
  const value=await read(db);await db.query('COMMIT');return value;
 }catch(error:any){await db.query('ROLLBACK').catch(()=>{});
  if(error?.code==='57014'||error?.code==='55P03')throw new DomainError('OPERATIONS_OBSERVATION_UNAVAILABLE','A observação excedeu o prazo; nenhum estado foi inferido ou alterado.',503);
  throw error;
 }finally{db.release();}
}
async function authorizedGoal(db:DB,goalId:string,principal:OperationsPrincipal,scope:'observe'|'evidence'='observe') {
 const row=(await db.query('SELECT id,project,title,state,created_at FROM goals WHERE id=$1',[goalId])).rows[0];
 if(!row)throw missing();assertOperationsScope(principal,row,scope);return row;
}
function focus(value:any){return value?{kind:operationalCode(value.kind),caseId:uuid(value.caseId),taskId:uuid(value.taskId),
 runId:uuid(value.runId),expectedAttempt:Number.isSafeInteger(value.expectedAttempt)?value.expectedAttempt:null}:null;}
function runView(row:any){return {id:row.id,taskId:row.task_id,attempt:row.attempt,state:operationalCode(row.state),
 runtime:operationalCode(row.runtime),model:operationalLabel(row.model,100),startedAt:instant(row.started_at),
 finishedAt:instant(row.finished_at),heartbeatAt:instant(row.heartbeat_at),errorCode:operationalCode(row.error_code)};}
function rejectionView(value:any,goalId:string,taskId:string,runId:string){
 if(!value||value.goalId!==goalId||value.taskId!==taskId||value.runId!==runId)return null;
 const artifact=(v:any)=>v&&uuid(v.id)&&/^[a-f0-9]{64}$/.test(v.digest??'')?{id:v.id,name:operationalLabel(v.name),digest:v.digest,bytes:integer(v.size)}:null;
 return {authority:'review_only',acceptance:false,acceptanceId:uuid(value.acceptanceId),ownerCaseId:uuid(value.ownerCaseId),
  ownerState:operationalCode(value.ownerState),disposition:operationalCode(value.disposition),routed:value.routed===true,
  observedAt:instant(value.observedAt),code:operationalCode(value.rejection?.code),kind:operationalCode(value.rejection?.kind),
  criterionId:operationalLabel(value.rejection?.failedCriterion?.id,80),
  report:artifact(value.rejection?.report),patch:artifact(value.rejection?.patch)};
}

/** A bounded projection of current state, NOT a replay of every historical event. */
export async function readOperationsSnapshot(db:DB,goalId:string,principal:OperationsPrincipal) {
 const goal=await authorizedGoal(db,goalId,principal),at=new Date().toISOString();
 const taskStates=(await db.query(`SELECT state,count(*)::int total FROM tasks WHERE goal_id=$1 AND state<>'superseded'
  GROUP BY state ORDER BY state`,[goalId])).rows.map(r=>({state:operationalCode(r.state),count:r.total}));
 const taskFingerprint=(await db.query(`SELECT count(*)::int total,max(updated_at) updated_at,
  md5(COALESCE(string_agg(concat_ws(':',id,state,attempt_count,updated_at::text),',' ORDER BY id),'')) fingerprint
  FROM tasks WHERE goal_id=$1 AND state<>'superseded'`,[goalId])).rows[0];
 const runSummary=(await db.query(`SELECT r.state,r.runtime,count(*)::int total FROM runs r JOIN tasks t ON t.id=r.task_id
  WHERE t.goal_id=$1 AND r.state=ANY($2::text[]) GROUP BY r.state,r.runtime ORDER BY r.state,r.runtime`,[goalId,activeStates])).rows
  .map(r=>({state:operationalCode(r.state),runtime:operationalCode(r.runtime),count:r.total}));
 const runs=(await db.query(`SELECT r.id,r.task_id,r.attempt,r.state,r.runtime,r.model,r.started_at,r.finished_at,r.heartbeat_at,r.error_code
  FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 AND r.state=ANY($2::text[])
  ORDER BY r.started_at NULLS FIRST,r.id LIMIT 10`,[goalId,activeStates])).rows;
 const latest=(await db.query(`SELECT r.id,r.task_id,r.attempt,r.state,r.runtime,r.model,r.started_at,r.finished_at,r.heartbeat_at,r.error_code,
  a.passed accepted,a.created_at acceptance_at
  FROM tasks t JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
  LEFT JOIN LATERAL(SELECT passed,created_at FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
  WHERE t.goal_id=$1 AND t.state<>'superseded' ORDER BY r.created_at DESC,r.id DESC LIMIT 5`,[goalId])).rows;
 // Aggregate metadata only. A later artifact/acceptance remains visible even if it
 // commits after an event with a larger sequence ID; no event watermark is used.
 const evidence=(await db.query(`SELECT count(*)::int count,max(a.created_at) latest_at,
  md5(COALESCE(string_agg(a.id::text||':'||a.digest,',' ORDER BY a.id),'')) fingerprint
  FROM tasks t JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count JOIN artifacts a ON a.run_id=r.id
  WHERE t.goal_id=$1 AND t.state<>'superseded'`,[goalId])).rows[0];
 const acceptanceFingerprint=(await db.query(`SELECT md5(COALESCE(string_agg(a.id::text||':'||a.passed::text,',' ORDER BY a.id),'')) fingerprint
  FROM tasks t JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
  JOIN LATERAL(SELECT id,passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
  WHERE t.goal_id=$1 AND t.state<>'superseded'`,[goalId])).rows[0]?.fingerprint;
 const values=(await db.query(`SELECT id,jsonb_build_object(
  'observedAt',value->'observedAt','reviewedAt',value->'reviewedAt','runId',value->'runId',
  'reviewRunId',value->'reviewRunId','lastReviewRunId',value->'lastReviewRunId',
  'delivery',value->'delivery','stagnation',value->'stagnation','lastDeliveryAt',value->'lastDeliveryAt',
  'complete',value->'complete','unchangedReviews',value->'unchangedReviews','inputFingerprint',value->'inputFingerprint',
  'focus',value->'contextRequestFocus','receipts',CASE WHEN id=$2 THEN value->'receipts' END,
  'requests',COALESCE(jsonb_array_length(value->'contextRequests'),0),
  'nextTransition',value->'nextTransition','enabled',value->'enabled','glmTarget',value->'glmTarget',
  'museTarget',value->'museTarget') value FROM settings WHERE id=ANY($1::text[])`,
  [['operation-progress:'+goalId,'management-state:'+goalId,'workforce:'+goalId,'work-frontier:'+goalId],
   'management-state:'+goalId])).rows;
 const setting=(key:string)=>values.find(v=>v.id===key+':'+goalId)?.value;
 const progress=setting('operation-progress'),manager=setting('management-state'),workforce=setting('workforce'),frontier=setting('work-frontier');
 const deliveryReviews=observedInteger(frontier?.unchangedReviews);
 const deliveryObserved=typeof frontier?.complete==='boolean'&&deliveryReviews!==null;
 const inputReviews=observedInteger(progress?.stagnation?.unchangedReviews),inputThreshold=observedInteger(progress?.stagnation?.threshold);
 const inputObserved=typeof progress?.stagnation?.detected==='boolean'&&inputReviews!==null&&inputThreshold!==null&&inputThreshold>0;
 const deliveryStagnationObservation={source:frontier?'persisted-work-frontier':'unavailable',available:deliveryObserved,
  observedAt:instant(frontier?.observedAt)??instant(frontier?.reviewedAt),reviewedAt:instant(frontier?.reviewedAt),
  reviewRunId:uuid(frontier?.reviewRunId),complete:typeof frontier?.complete==='boolean'?frontier.complete:null,
  threshold:deliveryObserved?deliveryStagnationThreshold:null,meaning:'reviews_without_new_final_criterion_receipt'};
 const casesAvailable=await table(db,'supervision_cases');
 const caseCounts=casesAvailable?(await db.query(`SELECT state,count(*)::int total FROM supervision_cases WHERE goal_id=$1
  AND state IN('open','decided','delegated','waiting_evidence') GROUP BY state ORDER BY state`,[goalId])).rows
  .map(r=>({state:operationalCode(r.state),count:r.total})):[];
 const caseRows=casesAvailable?(await db.query(`SELECT id,task_id,source_run_id,state,decision_run_id,updated_at FROM supervision_cases WHERE goal_id=$1
  AND state IN('open','decided','delegated','waiting_evidence') ORDER BY created_at,id LIMIT 10`,[goalId])).rows:[];
 const caseFingerprint=casesAvailable?(await db.query(`SELECT md5(COALESCE(string_agg(concat_ws(':',id,state,decision_run_id,updated_at::text),',' ORDER BY id),'')) fingerprint
  FROM supervision_cases WHERE goal_id=$1`,[goalId])).rows[0]?.fingerprint:null;
 const pendingRejections=casesAvailable?(await readCanonicalRepairRejections(db,goalId)).map(value=>({taskId:value.taskId,runId:value.runId,
  href:reference(goalId,'run',value.runId),observation:rejectionView(value,goalId,value.taskId,value.runId)})):[];
 const summary={goal:{id:goal.id,title:operationalLabel(goal.title),project:operationalLabel(goal.project),state:operationalCode(goal.state)},
  tasks:{total:taskFingerprint.total,states:taskStates},executions:{states:runSummary,live:runs.map(runView),
   omitted:Math.max(0,runSummary.reduce((n,r)=>n+r.count,0)-runs.length),latest:latest.map(r=>({...runView(r),accepted:r.accepted??null,acceptanceAt:instant(r.acceptance_at)}))},
  delivery:{source:progress?'persisted-operation-progress':'unavailable',observedAt:instant(progress?.observedAt)??instant(progress?.reviewedAt),
   integrated:observedInteger(progress?.delivery?.integrated),verified:observedInteger(progress?.delivery?.verified),total:observedInteger(progress?.delivery?.total),
   lastDeliveryAt:instant(progress?.lastDeliveryAt),stagnation:deliveryObserved?!frontier.complete&&deliveryReviews! >= deliveryStagnationThreshold:null,
   unchangedReviews:deliveryObserved?deliveryReviews:null,stagnationObservation:deliveryStagnationObservation},
  inputChange:{source:progress?'persisted-operation-progress':'unavailable',available:inputObserved,
   observedAt:instant(progress?.observedAt)??instant(progress?.reviewedAt),reviewedAt:instant(progress?.reviewedAt),
   lastReviewRunId:uuid(progress?.lastReviewRunId),inputFingerprint:typeof progress?.inputFingerprint==='string'&&/^[a-f0-9]{64}$/.test(progress.inputFingerprint)?progress.inputFingerprint:null,
   unchangedReviews:inputObserved?inputReviews:null,threshold:inputObserved?inputThreshold:null,
   stagnation:inputObserved?progress.stagnation.detected:null,meaning:'reviews_without_new_delivery_or_input'},
  supervisor:{sourceAvailable:casesAvailable,states:caseCounts,pendingRejections,pendingRejectionsMayHaveMore:pendingRejections.length===8,cases:caseRows.map(c=>({id:c.id,taskId:c.task_id,
   sourceRunId:c.source_run_id,state:operationalCode(c.state),decisionRunId:c.decision_run_id,updatedAt:instant(c.updated_at),href:reference(goalId,'case',c.id)})),
   omitted:Math.max(0,caseCounts.reduce((n,r)=>n+r.count,0)-caseRows.length),lastReview:manager?{runId:uuid(manager.runId),reviewedAt:instant(manager.reviewedAt),
    focus:focus(manager.focus),contextRequestCount:integer(manager.requests),receipts:smallCodes(manager.receipts),
    omittedReceipts:Math.max(0,(Array.isArray(manager.receipts)?manager.receipts.length:0)-12)}:null},
  capacity:{requested:{glm:observedInteger(workforce?.glmTarget),muse:observedInteger(workforce?.museTarget)},providerRequests:'not_observed'},
  evidence:{currentAttemptArtifacts:evidence.count,lastArtifactAt:instant(evidence.latest_at)},
  coverage:{runtimeStageObservation:['harness'],runtimeGuidance:['harness'],openCodeStageObservation:'unsupported_by_this_observer'},
  needsHuman:{classification:'not_inferred_from_failure',details:'inspect_case'},eta:{status:'unknown'}};
 // Heartbeats/time-of-read never count as material deltas. All goal task metadata
 // participates even when its detail is outside the ten displayed rows.
 const sections={goal:operationDigest(summary.goal),tasks:operationDigest([taskStates,taskFingerprint.fingerprint]),
  executions:operationDigest([runSummary,runs.map(r=>({id:r.id,state:r.state,attempt:r.attempt}))]),delivery:operationDigest(summary.delivery),
  inputChange:operationDigest(summary.inputChange),
  supervisor:operationDigest([caseFingerprint,summary.supervisor.lastReview,pendingRejections]),
  evidence:operationDigest([evidence.fingerprint,acceptanceFingerprint]),capacity:operationDigest(summary.capacity)};
 const baseline={revision:operationDigest(sections),sections};
 return {schemaVersion:1,observedAt:at,summary,revision:baseline.revision,
  freshness:{database:'current_snapshot',taskUpdatedAt:instant(taskFingerprint.updated_at),deliveryObservedAt:summary.delivery.observedAt,
   deliveryStagnationObservedAt:summary.delivery.stagnationObservation.observedAt,inputChangeObservedAt:summary.inputChange.observedAt,
   heartbeatIsProgress:false},semantics:'current_state_reconciliation',eventHistoryComplete:false,
  cursor:encodeOperationsCursor(goalId,principal,baseline),baseline};
}

export async function operationsSnapshot(goalId:string,principal:OperationsPrincipal){return operationsRead(async db=>{
 const {baseline,...result}=await readOperationsSnapshot(db,goalId,principal);return result;});}
export async function operationsChanges(goalId:string,principal:OperationsPrincipal,cursor:string){
 const prior=decodeOperationsCursor(cursor,goalId,principal);
 return operationsRead(async db=>{
  const next=await readOperationsSnapshot(db,goalId,principal);
  const changes=Object.entries(next.baseline.sections).filter(([key,digest])=>prior.sections[key]!==digest)
   .map(([section,digest])=>({section,digest,kind:'state_changed',current:(next.summary as any)[section],
    ...(section==='tasks'||section==='evidence'?{recentRuns:next.summary.executions.latest.map(r=>({id:r.id,taskId:r.taskId,href:reference(goalId,'run',r.id)}))}:{})}));
  return {schemaVersion:1,observedAt:next.observedAt,goalId,revision:next.revision,previousRevision:prior.revision,
   changed:next.revision!==prior.revision,changes,nextCursor:next.cursor,hasMore:false,resetRequired:false,
   semantics:'current_state_reconciliation',eventHistoryComplete:false,
   snapshotHref:`${operationsPrefix}/goals/${goalId}/snapshot`,
   historyNote:'Transient intermediate states are not an event replay. Read exact attempts/cases when historical proof is required.'};
 });
}

export async function operationsInspect(goalId:string,principal:OperationsPrincipal,kind:'task'|'run'|'case',id:string) {
 return operationsRead(async db=>{
  await authorizedGoal(db,goalId,principal);
  let item:any;
  if(kind==='task'){
   const t=(await db.query(`SELECT id,goal_id,title,state,stage,attempt_count,updated_at,criteria FROM tasks WHERE id=$1 AND goal_id=$2`,[id,goalId])).rows[0];
   if(!t)throw missing();
   const runs=(await db.query(`SELECT id,task_id,attempt,state,runtime,model,started_at,finished_at,heartbeat_at,error_code FROM runs
    WHERE task_id=$1 ORDER BY attempt DESC LIMIT 5`,[id])).rows;
   item={id:t.id,title:operationalLabel(t.title),state:operationalCode(t.state),stage:operationalCode(t.stage),attempt:t.attempt_count,
    updatedAt:instant(t.updated_at),criteria:Array.isArray(t.criteria)?t.criteria.slice(0,30).map((c:any)=>({id:operationalLabel(c.id,80),type:operationalCode(c.type),label:operationalLabel(c.label)})):[],
    omittedCriteria:Math.max(0,(Array.isArray(t.criteria)?t.criteria.length:0)-30),runs:runs.map(r=>({...runView(r),href:reference(goalId,'run',r.id)})),
    moreAttemptsHref:`/api/panel/tasks/${id}`};
  }else if(kind==='run'){
   const r=(await db.query(`SELECT r.id,r.task_id,r.attempt,r.state,r.fence,r.context_id,r.runtime,r.model,r.started_at,r.finished_at,
    r.heartbeat_at,r.error_code,encode(sha256(convert_to(COALESCE(r.instruction_snapshot#>>'{task,description}',''),'UTF8')),'hex') instruction_digest,
    length(r.instruction_snapshot#>>'{task,description}') instruction_characters FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1 AND t.goal_id=$2`,[id,goalId])).rows[0];
   if(!r)throw missing();
   const acceptance=(await db.query('SELECT id,passed,created_at,checks FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1',[id])).rows[0];
   const artifacts=(await db.query('SELECT id,name,mime,digest,size FROM artifacts WHERE run_id=$1 ORDER BY created_at,id LIMIT 51',[id])).rows;
   const rejection=await readCanonicalRepairRejection(db,id);
   item={...runView(r),binding:{goalId,taskId:r.task_id,runId:r.id,runFence:r.fence,contextVersion:r.context_id,expectedAttempt:r.attempt},
    source:{instructionDigest:r.instruction_characters===null?null:r.instruction_digest,instructionCharacters:r.instruction_characters,
     runtimeRelease:'not_observed'},stopAuthority:'not_granted_by_this_read',pendingRejection:rejectionView(rejection,goalId,r.task_id,r.id),
    acceptance:acceptance?{id:acceptance.id,passed:acceptance.passed,at:instant(acceptance.created_at),checks:Array.isArray(acceptance.checks)?acceptance.checks.slice(0,50).map((c:any)=>({id:operationalLabel(c.id,80),type:operationalCode(c.type),passed:typeof c.passed==='boolean'?c.passed:null})):[],
     omittedChecks:Math.max(0,(Array.isArray(acceptance.checks)?acceptance.checks.length:0)-50)}:null,
    artifacts:artifacts.slice(0,50).map(a=>({id:a.id,name:operationalLabel(a.name),mime:operationalLabel(a.mime,80),digest:a.digest,bytes:a.size,
     read:{goalId,reference:{kind:'artifact',artifactId:a.id,expectedDigest:a.digest},offset:0,limit:4000}})),artifactsTruncated:artifacts.length>50};
  }else{
   if(!await table(db,'supervision_cases'))throw missing();
   const c=(await db.query(`SELECT id,task_id,source_run_id,state,decision_run_id,support_task_ids,applied_attempt,created_at,updated_at,
    jsonb_array_length(evidence) evidence_count FROM supervision_cases WHERE id=$1 AND goal_id=$2`,[id,goalId])).rows[0];
   if(!c)throw missing();
   const rejection=c.source_run_id?await readCanonicalRepairRejection(db,c.source_run_id):null;
   item={id:c.id,taskId:c.task_id,sourceRunId:c.source_run_id,state:operationalCode(c.state),decisionRunId:c.decision_run_id,
    supportTaskIds:Array.isArray(c.support_task_ids)?c.support_task_ids.filter((v:any)=>uuid(v)).slice(0,20):[],appliedAttempt:c.applied_attempt,
    createdAt:instant(c.created_at),updatedAt:instant(c.updated_at),evidenceCount:c.evidence_count,
    sourceRunHref:c.source_run_id?reference(goalId,'run',c.source_run_id):null,
    pendingRejection:rejectionView(rejection,goalId,c.task_id,c.source_run_id)};
  }
  return {schemaVersion:1,observedAt:new Date().toISOString(),goalId,kind,item,revision:operationDigest(item),authority:'read_only'};
 });
}

export const operationsEvidenceSchema=z.object({goalId:z.uuid(),reference:z.object({kind:z.literal('artifact'),artifactId:z.uuid(),
 expectedDigest:z.string().regex(/^[a-f0-9]{64}$/)}).strict(),offset:z.number().int().min(0).max(2000000).default(0),limit:z.number().int().min(1).max(8000).default(4000)}).strict();
export async function operationsEvidence(principal:OperationsPrincipal,raw:unknown,reader=readArtifact){
 const request=operationsEvidenceSchema.parse(raw);
 const meta=await operationsRead(async db=>{
  await authorizedGoal(db,request.goalId,principal,'evidence');
  const a=(await db.query(`SELECT a.id,a.run_id,a.name,a.digest,a.size,a.mime,t.id task_id FROM artifacts a JOIN runs r ON r.id=a.run_id
   JOIN tasks t ON t.id=r.task_id WHERE a.id=$1 AND t.goal_id=$2`,[request.reference.artifactId,request.goalId])).rows[0];
  if(!a)throw missing();return a;
 });
 if(meta.digest!==request.reference.expectedDigest)throw new DomainError('OPERATIONS_EVIDENCE_CHANGED','O hash da evidência não corresponde à referência.',409);
 if(meta.size>1024*1024)throw new DomainError('OPERATIONS_EVIDENCE_TOO_LARGE','Use o download autenticado para evidência maior que 1MiB.',413);
 if(!['application/json','text/plain','text/markdown','text/x-diff','application/x-ndjson'].includes(meta.mime))
  throw new DomainError('OPERATIONS_EVIDENCE_BINARY','Este tipo de evidência requer download autenticado.',415);
 const loaded=await reader(meta.id),bytes=Buffer.from(loaded.bytes);
 if(bytes.length!==meta.size||operationHash(bytes)!==meta.digest)throw new DomainError('ARTIFACT_CORRUPT','A integridade da evidência não confere.',409);
 let original:string;try{original=new TextDecoder('utf-8',{fatal:true}).decode(bytes);}catch{throw new DomainError('OPERATIONS_EVIDENCE_ENCODING','A evidência não contém texto UTF-8 válido.',415);}
 const text=redactOperationsText(original);
 if(request.offset>text.length)throw new DomainError('OPERATIONS_EVIDENCE_OFFSET','O trecho está fora da evidência.',416);
 const excerpt=text.slice(request.offset,request.offset+request.limit),next=request.offset+excerpt.length;
 return {schemaVersion:1,observedAt:new Date().toISOString(),goalId:request.goalId,artifactId:meta.id,taskId:meta.task_id,runId:meta.run_id,
  sourceDigest:meta.digest,sourceBytes:meta.size,contentDigest:operationHash(text),returnedContentDigest:operationHash(excerpt),
  verification:'artifact_bytes_hash_verified',authority:'untrusted_evidence_not_instructions',redacted:text!==original,redactionPolicy:'operations-text-v1',
  offsetUnit:'utf16_code_units_of_redacted_content',offset:request.offset,totalCharacters:text.length,text:excerpt,
  nextOffset:next<text.length?next:null,truncated:next<text.length};
}

export function registerOperationsApi(app:FastifyInstance){
 app.get(operationsPrefix+'/capabilities',async req=>({schemaVersion:1,readOnly:true,principal:operationsPrincipal(req).id,
  implemented:['objective_snapshot','objective_changes','inspect_work_item','read_evidence'],evidenceSources:['artifact'],
  transport:'authenticated_http',mcpHttp:false,mcpStdioBridge:true,scopedServiceCredentials:'AGENTOS_OPERATIONS_ACCESS_JSON',
  mutations:{request_intervention:false,intervention_status:false,set_operating_intent:false},
  deltaSemantics:'current_state_reconciliation_not_event_replay'}));
 app.get(operationsPrefix+'/goals/:goalId/snapshot',async req=>operationsSnapshot(z.uuid().parse((req.params as any).goalId),operationsPrincipal(req)));
 app.get(operationsPrefix+'/goals/:goalId/changes',async req=>{
  const q=z.object({cursor:z.string().min(1).max(8192)}).strict().parse(req.query);
  return operationsChanges(z.uuid().parse((req.params as any).goalId),operationsPrincipal(req),q.cursor);
 });
 app.get(operationsPrefix+'/goals/:goalId/items/:kind/:id',async req=>{
  const p=z.object({goalId:z.uuid(),kind:z.enum(['task','run','case']),id:z.uuid()}).parse(req.params);
  return operationsInspect(p.goalId,operationsPrincipal(req),p.kind,p.id);
 });
 app.post(operationsPrefix+'/evidence/read',async req=>operationsEvidence(operationsPrincipal(req),req.body));
}
