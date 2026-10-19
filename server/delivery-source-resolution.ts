import {event,lock,tx,type DB} from './db.js';
import {readArtifact} from './service.js';
import {runtimeCandidateDigest} from './operation-progress.js';
import {deliveryDigest} from './delivery-supervision.js';
import {deliveryRepairProofCodes,deliveryRepairProofNextAction,verifyDeliveryRepairArtifacts,deliveryRepairReceiptProof} from './delivery-proof-feedback.js';
import {assertStopped} from './continuation.js';
import {supervisionReviewAccepted} from './supervision-recertification.js';

const fail=(code:string):never=>{throw Error(code);};
const live=['queued','running','cancelling','reconciling'];
async function stoppedCandidateProof(db:DB,run:any,task:any,stopProof:any){
 const observed=stopProof?.candidate?.candidateDigest;
 const refs=(await db.query(`SELECT id,run_id,name,digest,size FROM artifacts
   WHERE run_id=$1 AND name='candidate.json' ORDER BY id LIMIT 2`,[run.id])).rows;
 // Legacy source decisions may concern a stopped attempt that never emitted
 // candidate.json. Never use mutable delivery-consumption receipt fields.
 if(!refs.length&&(observed===undefined||observed===null))return null;
 if(refs.length!==1||!/^[a-f0-9]{64}$/.test(observed??'')||stopProof?.runId!==run.id||
   stopProof?.workspace!==task.execution?.workspace)fail('DELIVERY_RESOLUTION_CANDIDATE_PROOF_CHANGED');
 const ref=refs[0],artifact=await readArtifact(ref.id);
 if(ref.run_id!==run.id||ref.name!=='candidate.json'||artifact.bytes.length!==ref.size||
   artifact.digest!==ref.digest||runtimeCandidateDigest(artifact.bytes,ref.digest)!==observed)
  fail('DELIVERY_RESOLUTION_CANDIDATE_PROOF_CHANGED');
 return {consumerTaskId:task.id,consumerRunId:run.id,workspace:task.execution.workspace,
   candidateArtifactId:ref.id,candidateArtifactDigest:ref.digest,candidateDigest:observed};
}
async function replacementProof(db:DB,source:any){
 if(source?.work_kind!=='repair')return {version:1,verified:false,code:'DELIVERY_RESOLUTION_REPAIR_REQUIRED',liveAuthorization:false,
   nextAction:deliveryRepairProofNextAction};
 const refs=(await db.query(`SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1
   AND name IN('implementation.patch','repair-report.json') ORDER BY name,id LIMIT 3`,[source.run_id])).rows;
 const acceptance=(await db.query(`SELECT id,run_id,task_id,passed,checks FROM acceptances
   WHERE run_id=$1 AND task_id=$2 ORDER BY created_at DESC,id DESC LIMIT 1`,[source.run_id,source.id])).rows[0];
 return deliveryRepairReceiptProof(source,refs,acceptance);
}

/** Current semantic identity; returns digests/booleans, never input or prompt
 * bytes. The legacy snapshot absence is different from a proven field change. */
export async function readDeliverySourceIdentity(db:DB,taskId:string){
 return (await db.query(`WITH selected AS MATERIALIZED(SELECT t.id,t.goal_id,t.state,t.attempt_count,
  t.execution,t.description,t.criteria,t.instruction_revision_id,t.agent_id,t.resources,t.required_memories,t.allowed_accounts,
  r.id run_id,r.fence,r.state run_state,r.attempt,r.instruction_snapshot->>'schema' snapshot_schema,
  r.instruction_snapshot->'task' frozen FROM tasks t JOIN LATERAL(
   SELECT id,fence,state,attempt,instruction_snapshot FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true WHERE t.id=$1)
 SELECT id,goal_id,state,attempt_count,run_id,fence,run_state,attempt,snapshot_schema,
  execution->>'baseWorkspace' scope,execution->>'workspace' workspace,execution->>'workKind' work_kind,
  encode(sha256(convert_to(jsonb_build_object('description',description,'criteria',criteria,'revision',instruction_revision_id,
    'agent',agent_id,'resources',resources,'memories',required_memories,'accounts',allowed_accounts,'execution',execution-'continuation')::text,'utf8')),'hex') contract_digest,
  (frozen->>'description'=description AND frozen->'criteria'=criteria AND
    frozen->>'instruction_revision_id' IS NOT DISTINCT FROM instruction_revision_id::text AND
    frozen->'agent_id'=to_jsonb(agent_id) AND frozen->'resources' IS NOT DISTINCT FROM resources AND
    frozen->'required_memories' IS NOT DISTINCT FROM required_memories AND
    frozen->'allowed_accounts' IS NOT DISTINCT FROM to_jsonb(allowed_accounts)) other_fields_equal,
  ((frozen->'execution')-'continuation' IS NOT DISTINCT FROM execution-'continuation') execution_equal,
  ((frozen->'execution')-'continuation'-'recoveryContext' IS NOT DISTINCT FROM execution-'continuation'-'recoveryContext') execution_without_recovery_equal,
  (SELECT passed FROM acceptances WHERE run_id=selected.run_id ORDER BY created_at DESC,id DESC LIMIT 1) passed,
  EXISTS(SELECT 1 FROM runs x WHERE x.task_id=selected.id AND x.state=ANY($2::text[])) live,
  EXISTS(SELECT 1 FROM leases l JOIN runs x ON x.id=l.run_id WHERE x.task_id=selected.id) leased,
  EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=selected.run_id::text) cancelled
 FROM selected`,[taskId,live])).rows[0]??null;
}
export function sourceIdentityAccepted(value:any){return value?.state==='accepted' && value.run_state==='submitted' &&
 value.attempt===value.attempt_count && value.passed===true && !value.live && !value.leased && !value.cancelled;}
export function sourceIdentityFrozen(value:any){return value?.snapshot_schema==='1' && value.other_fields_equal===true && value.execution_equal===true;}

/** A completed recertification is new evidence for a successor decision, not an
 * implicit source replacement. At most four supports from the latest owner. */
export async function deliverySourceFollowthrough(db:DB,row:any,generation:string){
 const c=(await db.query(`SELECT id,support_task_ids FROM supervision_cases WHERE task_id=$1 AND state='applied'
   AND EXISTS(SELECT 1 FROM jsonb_array_elements(evidence) e WHERE e->>'obligationKey'=$2 AND e#>>'{sourceConflict,generation}'=$3)
   ORDER BY created_at DESC,id DESC LIMIT 1`,[row.consumer_task_id,'delivery-consumption:'+row.id,generation])).rows[0];
 const result:any[]=[];
 for(const id of (Array.isArray(c?.support_task_ids)?c.support_task_ids:[]).slice(0,4)){
   const source=await readDeliverySourceIdentity(db,id);
   if(sourceIdentityAccepted(source)&&sourceIdentityFrozen(source)&&source.goal_id===row.goal_id&&source.scope===row.scope&&['repair','diagnostic'].includes(source.work_kind)){
     const proof=await replacementProof(db,source);
     result.push({caseId:c.id,taskId:source.id,runId:source.run_id,contractDigest:source.contract_digest,
       workKind:source.work_kind,eligibleReplacement:proof.verified,replacementProof:proof});
   }
 }
 const other=c?(await db.query(`SELECT id FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED'
   AND payload->>'caseId'=$1 AND payload->>'obligationId'<>$2 ORDER BY id DESC LIMIT 1`,[c.id,row.id])).rows[0]:null;
 return {recertificationSources:result.sort((a,b)=>a.taskId.localeCompare(b.taskId)),
   fulfilledOtherObligation:other?{caseId:c.id,eventId:String(other.id)}:null};
}

/** Recertification finishes the delegated support, not source selection. Keep
 * pending ownership continuously until a new accepted decision selects the
 * exact support run; there is no applied→unowned admission window. */
export async function reopenRecertifiedSourceReview(db:DB,c:any){
 // Keep the completed delegation's identity until the phase transition commits.
 // The detector holds the global lock; the row CAS also protects direct callers.
 const current=(await db.query('SELECT * FROM supervision_cases WHERE id=$1 FOR UPDATE',[c.id])).rows[0];
 if(!current||current.state!=='delegated'||current.decision_run_id!==c.decision_run_id||
  current.source_run_id!==c.source_run_id||deliveryDigest(current.support_task_ids)!==deliveryDigest(c.support_task_ids))return false;
 c=current;
 if(!(await supervisionReviewAccepted(db,c.decision_run_id,c.goal_id)))return false;
 const reviews=(Array.isArray(c.evidence)?c.evidence:[]).filter((e:any)=>e.kind==='DELIVERY_SOURCE_REVIEW_REQUIRED');
 if(!reviews.length)return false;
 const unresolved=(await db.query(`SELECT id FROM delivery_consumptions WHERE consumer_task_id=$1
  AND id=ANY($2::uuid[]) AND state NOT IN('completed','superseded')`,[c.task_id,reviews.map((e:any)=>e.obligationId)])).rows;
 if(!unresolved.length)return false;
 const scope=(await db.query("SELECT execution->>'baseWorkspace' scope FROM tasks WHERE id=$1 AND goal_id=$2",[c.task_id,c.goal_id])).rows[0]?.scope;
 const sources=[] as any[];
 for(const id of (c.support_task_ids?.length?c.support_task_ids:[c.support_task_id]).filter(Boolean).slice(0,4)){
   const source=await readDeliverySourceIdentity(db,id);
   const currentAccepted=sourceIdentityAccepted(source),currentFrozen=sourceIdentityFrozen(source);
   const proof=currentAccepted&&currentFrozen&&source.goal_id===c.goal_id&&source.scope===scope?
     await replacementProof(db,source):null;
   sources.push({taskId:id,runId:source?.run_id??null,currentAccepted,currentFrozen,
     contractDigest:source?.contract_digest??null,workKind:source?.work_kind??null,
     eligibleReplacement:proof?.verified===true,...(proof?{replacementProof:proof}:{})});
 }
 const evidence={kind:'DELIVERY_RECERTIFICATION_READY',caseId:c.id,decisionRunId:c.decision_run_id,
  obligationIds:unresolved.map(r=>r.id),sources,nextAction:'Select an accepted frozen repair with revise and explicit deliveryResolution; diagnostic evidence alone does not replace source proof. If the current DELIVERY_RECERTIFICATION_READY sources are only accepted frozen diagnostics, do not substitute a diagnostic or repeat supervision.delegate. Re-read the same-scope source, writable integrator, DAG and alreadyPlanned. Propose the missing correction through existing coordination.backlog.tasks with workKind:"repair", lane:"glm", sourceTaskId of that diagnostic, deliverToTaskId of the original writable integrator, baseWorkspace of that scope, delivery:"deferred" and one stable coverageKey describing the missing repair. Reuse equivalent existing work; never rename coverage to evade deduplication. This requests isolated repair work, not source replacement or acceptance. After a repair is actually accepted with patch/report/source proof, submit a fresh revise with its exact sourceTaskId and deliveryResolution; composition and independent QA remain required.'};
 const changed=await db.query(`UPDATE supervision_cases SET state='open',decision=NULL,decision_run_id=NULL,support_task_id=NULL,
  support_task_ids='[]',evidence=evidence||$2::jsonb,reason='Recertificação concluída; selecionar fonte exata antes de retomar.',updated_at=now()
  WHERE id=$1 AND state='delegated' AND decision_run_id IS NOT DISTINCT FROM $3::uuid RETURNING id`,[c.id,JSON.stringify([evidence]),c.decision_run_id]);
 if(!changed.rowCount)return false;
 // The phase starts here, not at the beginning of a potentially long detector
 // transaction. A review prepared earlier in that transaction is still stale.
 await db.query(`INSERT INTO events(kind,entity_id,payload,created_at)
  VALUES('DELIVERY_RECERTIFICATION_READY',$1,$2,clock_timestamp())`,[c.task_id,JSON.stringify({...evidence,decision:c.decision})]);
 await db.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1",[c.task_id]);
 return true;
}

export function deliverySourceConflict(row:any,identity:any,manifest:any[]){
 const manifestDigest=deliveryDigest(manifest);
 let code:string|undefined;
 if(!identity || !sourceIdentityAccepted(identity))code='SOURCE_ACCEPTANCE_UNAVAILABLE';
 else if(identity.run_id!==row.producer_run_id)code='SOURCE_RUN_CHANGED';
 else if(manifestDigest!==row.producer_manifest_digest)code='SOURCE_MANIFEST_CHANGED';
 else if(identity.snapshot_schema!=='1')code='SOURCE_SNAPSHOT_MISSING';
 else if(!sourceIdentityFrozen(identity))code=identity.other_fields_equal===true && identity.execution_without_recovery_equal===true?
  'SOURCE_RECOVERY_CONTEXT_CHANGED':'SOURCE_CONTRACT_CHANGED';
 if(!code)return null;
 const proof={version:1,code,original:{taskId:row.producer_task_id,runId:row.producer_run_id,manifestDigest:row.producer_manifest_digest},
  observed:identity?{taskId:identity.id,runId:identity.run_id,contractDigest:identity.contract_digest,manifestDigest,
    snapshotSchema:identity.snapshot_schema,currentAccepted:sourceIdentityAccepted(identity),currentFrozen:sourceIdentityFrozen(identity)}:null};
 return {...proof,generation:deliveryDigest(proof),resolution:{kind:'revise',
   requiredFields:['sourceTaskId','deliveryResolution.obligationId','deliveryResolution.generation','deliveryResolution.expectedSourceRunId'],
   nextAction:code==='SOURCE_SNAPSHOT_MISSING'?'Delegate a bounded repair/recertification from retained evidence; only a newly accepted frozen source can replace this unproven contract.':
    'Choose a compatible current accepted frozen repair, or delegate bounded recertification. Explicitly bind its task/run to this obligation; never rewrite the original proof.'}};
}

/** Append-only substitution, after an exact accepted supervisor decision has
 * actually attached this source. It authorizes a new source binding, not QA. */
export async function recordDeliverySourceResolution(db:DB,caseId:string,action:any,repairDelivery:any,stopProof:any){
 if(!action.deliveryResolution)return null;
 await lock(db);
 const request=action.deliveryResolution;
 if(action.kind!=='revise'||!action.sourceTaskId||repairDelivery?.taskId!==action.sourceTaskId||
   repairDelivery.runId!==request.expectedSourceRunId)fail('DELIVERY_RESOLUTION_ACTION_INVALID');
 const row=(await db.query('SELECT * FROM delivery_consumptions WHERE id=$1 FOR UPDATE',[request.obligationId])).rows[0];
 const c=(await db.query('SELECT * FROM supervision_cases WHERE id=$1 FOR UPDATE',[caseId])).rows[0];
 if(!row||!c||c.task_id!==row.consumer_task_id||c.goal_id!==row.goal_id||c.state!=='applied'||
   !c.decision_run_id||
   !c.evidence.some((e:any)=>e.obligationKey==='delivery-consumption:'+row.id && e.sourceConflict?.generation===request.generation)||
   c.decision?.actions?.length!==1||deliveryDigest(c.decision.actions[0])!==deliveryDigest(action))fail('DELIVERY_RESOLUTION_DECISION_CHANGED');
 const key=deliveryDigest({obligationId:row.id,generation:request.generation,decisionRunId:c.decision_run_id});
 const previous=(await db.query("SELECT id,payload FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1 AND payload->>'key'=$2",[row.id,key])).rows[0];
 if(previous)return previous.payload;
 if(row.receipt?.sourceConflict?.generation!==request.generation)fail('DELIVERY_RESOLUTION_GENERATION_CHANGED');
 const priorResolution=await readDeliverySourceResolution(db,row),effective=effectiveDeliverySource(row,priorResolution);
 const originalIdentity=await readDeliverySourceIdentity(db,effective.producer_task_id);
 const originalRefs=originalIdentity?(await db.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[originalIdentity.run_id])).rows:[];
 if(deliverySourceConflict(effective,originalIdentity,originalRefs)?.generation!==request.generation)
  fail('DELIVERY_RESOLUTION_GENERATION_CHANGED');
 const authority=(await db.query(`SELECT g.state,s.value->>'enabled' supervision,a.value->>'enabled' autonomy,
  t.state task_state,t.attempt_count,t.execution->>'baseWorkspace' scope,t.execution->>'workspace' workspace,
  t.recovery->>'mode' recovery_mode,r.id run_id,r.state run_state,
  EXISTS(SELECT 1 FROM runs x WHERE x.task_id=t.id AND x.state=ANY($3::text[])) live,
  EXISTS(SELECT 1 FROM leases l JOIN runs x ON x.id=l.run_id WHERE x.task_id=t.id) leased
  FROM goals g JOIN tasks t ON t.goal_id=g.id LEFT JOIN settings s ON s.id='supervision:'||g.id
  LEFT JOIN settings a ON a.id='autonomy:'||g.project LEFT JOIN LATERAL(SELECT id,state FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
  WHERE g.id=$1 AND t.id=$2`,[row.goal_id,row.consumer_task_id,live])).rows[0];
 if(!authority||authority.state!=='active'||authority.supervision!=='true'||authority.autonomy==='false'||
  (authority.recovery_mode==='manual'&&authority.autonomy!=='true')||authority.live||authority.leased||
  ['accepted','cancelled','superseded','cancelling','paused'].includes(authority.task_state)||authority.run_state==='cancelled'||
 authority.run_id!==c.source_run_id||authority.attempt_count!==c.applied_attempt)fail('DELIVERY_RESOLUTION_AUTHORITY_CHANGED');
 if((await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",[authority.run_id])).rowCount)
  fail('DELIVERY_RESOLUTION_AUTHORITY_CHANGED');
 const target=(await db.query('SELECT id,goal_id,criteria,covers,stage,execution FROM tasks WHERE id=$1',[row.consumer_task_id])).rows[0];
 const targetRun=(await db.query('SELECT id,task_id,fence,attempt,state,instruction_snapshot,checkpoint FROM runs WHERE id=$1',[authority.run_id])).rows[0];
 if(deliveryDigest({criteria:target.criteria,covers:target.covers,stage:target.stage,
   baseWorkspace:target.execution?.baseWorkspace??null,sourceWorkspace:target.execution?.sourceWorkspace??null,
   readOnlySource:target.execution?.readOnlySource??false,writePaths:target.execution?.writePaths??null})!==row.consumer_contract_digest)
  fail('DELIVERY_RESOLUTION_CONSUMER_CHANGED');
 assertStopped(stopProof,targetRun,target);
 const targetCandidate=await stoppedCandidateProof(db,targetRun,target,stopProof);
 if(!(await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1 AND t.goal_id=$2
  AND t.stage='coordinate' AND r.state='submitted' AND (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)=true`,[c.decision_run_id,row.goal_id])).rowCount)
  fail('DELIVERY_RESOLUTION_REVIEW_UNACCEPTED');
 const source=await readDeliverySourceIdentity(db,action.sourceTaskId);
 if(!sourceIdentityAccepted(source)||!sourceIdentityFrozen(source)||source.run_id!==request.expectedSourceRunId||
   source.goal_id!==row.goal_id||source.scope!==authority.scope||source.work_kind!=='repair')fail('DELIVERY_RESOLUTION_SOURCE_UNPROVEN');
 const refs=(await db.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[source.run_id])).rows;
 const patch=refs.filter(x=>x.name==='implementation.patch'),report=refs.filter(x=>x.name==='repair-report.json');
 if(patch.length!==1||report.length!==1)fail('DELIVERY_RESOLUTION_ARTIFACTS_INVALID');
 await verifyDeliveryRepairArtifacts([patch[0],report[0]],readArtifact,source.run_id);
 if(!(await db.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2',[row.consumer_task_id,source.id])).rowCount)
  fail('DELIVERY_RESOLUTION_DEPENDENCY_MISSING');
 const value={version:1,key,obligationId:row.id,generation:request.generation,caseId,decisionRunId:c.decision_run_id,
  previousResolutionEventId:priorResolution?.eventId??null,
  resolvedSource:{taskId:effective.producer_task_id,runId:effective.producer_run_id,manifestDigest:effective.producer_manifest_digest},
  original:{producerTaskId:row.producer_task_id,producerRunId:row.producer_run_id,producerManifestDigest:row.producer_manifest_digest},
  replacement:{producerTaskId:source.id,producerRunId:source.run_id,producerManifestDigest:deliveryDigest(refs),contractDigest:source.contract_digest,
   producerArtifacts:refs,repairArtifacts:[patch[0],report[0]]},consumerTaskId:row.consumer_task_id,consumerRunId:authority.run_id,
  ...(targetCandidate?{targetCandidate}:{}),
  acceptance:false,compositionRequired:true,independentQARequired:true};
 await event(db,'DELIVERY_SOURCE_RESOLVED',row.id,value);return value;
}

export async function readDeliverySourceResolution(db:DB,row:any){
 const resolution=(await db.query("SELECT id,payload FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1 ORDER BY id DESC LIMIT 1",[row.id])).rows[0];
 if(!resolution)return null;
 const p=resolution.payload;
 if(p.version!==1||p.obligationId!==row.id||p.consumerTaskId!==row.consumer_task_id||
  p.original?.producerTaskId!==row.producer_task_id||p.original?.producerRunId!==row.producer_run_id||
  p.original?.producerManifestDigest!==row.producer_manifest_digest)fail('DELIVERY_RESOLUTION_BINDING_CHANGED');
 if(p.targetCandidate && (p.targetCandidate.consumerTaskId!==row.consumer_task_id||
   p.targetCandidate.consumerRunId!==p.consumerRunId||p.targetCandidate.workspace!==row.binding?.consumerWorkspace||
   typeof p.targetCandidate.candidateArtifactId!=='string'||
   !/^[a-f0-9]{64}$/.test(p.targetCandidate.candidateArtifactDigest??'')||
   !/^[a-f0-9]{64}$/.test(p.targetCandidate.candidateDigest??'')))fail('DELIVERY_RESOLUTION_CANDIDATE_PROOF_CHANGED');
 return {...p,eventId:String(resolution.id)};
}
export function effectiveDeliverySource(row:any,resolution:any){
 const replacement=resolution?.replacement;
 return replacement?{...row,producer_task_id:replacement.producerTaskId,producer_run_id:replacement.producerRunId,
  producer_manifest_digest:replacement.producerManifestDigest,binding:{...row.binding,producerArtifacts:replacement.producerArtifacts,repairArtifacts:replacement.repairArtifacts}}:row;
}

const recoverable=new Set([...deliveryRepairProofCodes,'DELIVERY_RESOLUTION_ACTION_INVALID','DELIVERY_RESOLUTION_GENERATION_CHANGED',
 'DELIVERY_RESOLUTION_SOURCE_UNPROVEN','DELIVERY_RESOLUTION_ARTIFACTS_INVALID','DELIVERY_RESOLUTION_ARTIFACT_CHANGED',
 'DELIVERY_RESOLUTION_CONSUMER_CHANGED','DELIVERY_RESOLUTION_CANDIDATE_PROOF_CHANGED','SUPERVISION_SOURCE_INVALID','SUPERVISION_SOURCE_TOO_LARGE',
 'SUPERVISION_SOURCE_DEPENDS_ON_TARGET','SUPERVISION_REQUIRES_IMPLEMENTATION_TARGET']);
/** A refused, unapplied decision can be replaced, but never replayed. CAS
 * preserves a newer decision that arrived while artifact/stop I/O ran. */
export async function rejectDeliverySourceDecision(caseId:string,decisionRunId:string,code:string){
 if(!recoverable.has(code))return false;
 return tx(async db=>{
  await lock(db);
  const c=(await db.query(`SELECT c.* FROM supervision_cases c JOIN tasks t ON t.id=c.task_id JOIN goals g ON g.id=c.goal_id
   JOIN LATERAL(SELECT id,attempt FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)latest ON true
   JOIN settings s ON s.id='supervision:'||g.id LEFT JOIN settings a ON a.id='autonomy:'||g.project
   WHERE c.id=$1 AND c.state='decided' AND c.decision_run_id=$2 AND g.state='active'
   AND c.source_run_id=latest.id AND latest.attempt=t.attempt_count AND s.value->>'enabled'='true'
   AND COALESCE(a.value->>'enabled','true')='true' AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
   AND t.state NOT IN('accepted','cancelled','superseded','cancelling','paused')
   AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.task_id=t.id AND r.state=ANY($3::text[]))
   AND NOT EXISTS(SELECT 1 FROM leases l JOIN runs r ON r.id=l.run_id WHERE r.task_id=t.id)
   AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.payload->>'runId'=c.source_run_id::text)
   FOR UPDATE OF c`,[caseId,decisionRunId,live])).rows[0];
  const action=c?.decision?.actions?.find((a:any)=>a.deliveryResolution);
  if(!c||!action)return false;
  const rejection={caseId,decisionRunId,code,decision:c.decision,obligationId:action.deliveryResolution.obligationId,
   generation:action.deliveryResolution.generation,expectedSourceRunId:action.deliveryResolution.expectedSourceRunId,
   ...(deliveryRepairProofCodes.includes(code)?{replacementEligibleAtRejection:false,sourceTaskId:action.sourceTaskId,
     proofFailure:{version:1,code,sourceTaskId:action.sourceTaskId,sourceRunId:action.deliveryResolution.expectedSourceRunId}}:{}),
   nextAction:deliveryRepairProofCodes.includes(code)?deliveryRepairProofNextAction:
     'Reconsultar a fonte e a obrigação atuais, escolher reparo comprovado ou delegar recertificação; não reenviar a decisão rejeitada.'};
  await event(db,'DELIVERY_SOURCE_DECISION_REJECTED',c.task_id,rejection);
  await db.query(`UPDATE supervision_cases SET state='open',decision=NULL,decision_run_id=NULL,
   evidence=evidence||$2::jsonb,reason=$3,updated_at=now() WHERE id=$1`,[caseId,
    JSON.stringify([{kind:'DELIVERY_SOURCE_DECISION_REJECTED',code,decisionRunId,obligationId:rejection.obligationId,
      ...(rejection.proofFailure?{proofFailure:rejection.proofFailure,replacementEligibleAtRejection:false,nextAction:rejection.nextAction}:{})}]),
    'Revisar fonte alterada antes da aplicação: '+code]);
  return true;
 });
}
