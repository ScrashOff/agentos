import {z} from 'zod';
import type {DB} from './db.js';
import {deliveryDigest} from './delivery-supervision.js';
import {expandSharedManagerContext} from './manager-context-sharing.js';
import {acceptedSupervisionActions} from './supervision-actions.js';
import {managementContextRequests} from './management-retrieval.js';
import {managerMaterialConsumptionReview} from './manager-focus.js';

const sha=z.string().regex(/^[a-f0-9]{64}$/);
const ref=z.object({id:z.uuid(),name:z.string(),digest:sha,size:z.number().int().nonnegative()}).passthrough();
const bindingSchema=z.object({goalId:z.uuid(),caseId:z.uuid(),consumerTaskId:z.uuid(),consumerRunId:z.uuid(),
  consumerAttempt:z.number().int().nonnegative(),obligations:z.array(z.object({obligationId:z.uuid(),generation:sha,
    handoffId:z.uuid(),producerTaskId:z.uuid(),producerRunId:z.uuid(),producerManifestDigest:sha,
    repairArtifacts:z.array(ref).length(2),sourceResolutionEventId:z.string().optional()}).strict()).min(1).max(128)}).strict();
type Binding=z.infer<typeof bindingSchema>;
const live=['queued','running','cancelling','reconciling'];

/** Only a concrete missing read is model-authored here. Actions remain in the
 * existing supervision contract; a wait must be observed from its real owner. */
export const materialContextGapSchema=z.object({caseId:z.uuid(),consumerRunId:z.uuid(),identityDigest:sha,
  reason:z.string().min(20).max(1200),requestIndices:z.array(z.number().int().min(0).max(3)).min(1).max(4)}).strict();

function bindingOf(value:any):{binding:Binding;identityDigest:string}|null {
  if(value?.version!==1 || value.authority!=='review_only' || value.actionValidationRequired!==true)return null;
  const parsed=bindingSchema.safeParse({goalId:value.goalId,caseId:value.caseId,consumerTaskId:value.consumerTaskId,
    consumerRunId:value.consumerRunId,consumerAttempt:value.consumerAttempt,obligations:value.obligations});
  if(!parsed.success || deliveryDigest(parsed.data)!==value.identityDigest)return null;
  return {binding:parsed.data,identityDigest:value.identityDigest};
}

export function frozenMaterialDecision(description:string) {
  const start=description.lastIndexOf('\n{"cases":');
  if(start<0)return null;
  try {
    const snapshot=expandSharedManagerContext(JSON.parse(description.slice(start+1)));
    const focus=snapshot.operation?.focus;
    const current=snapshot.cases?.find((entry:any)=>entry.id===focus?.caseId && entry.taskId===focus?.taskId);
    const hint=bindingOf(current?.materialConsumptionReview);
    // A prior requirement may be carried forward only for the same explicit
    // focused case. It never selects a source or confers action authority.
    const prior=snapshot.operation?.requiredDecision;
    const retained=prior && focus && prior.caseId===focus.caseId && prior.taskId===focus.taskId ? bindingOf(prior.materialReview):null;
    const selected=hint??retained;
    if(selected)return {invalid:false as const,...selected,snapshot};
    return current?.materialConsumptionReview || prior?.materialReview ? {invalid:true as const}:null;
  } catch { return {invalid:true as const}; }
}

function requirement(binding:Binding,identityDigest:string) {
  return {version:1,caseId:binding.caseId,taskId:binding.consumerTaskId,consumerRunId:binding.consumerRunId,
    consumerAttempt:binding.consumerAttempt,identityDigest,
    materialReview:{version:1,authority:'review_only',...binding,identityDigest,actionValidationRequired:true},
    contextGapContract:{address:'management.materialContextGap',
      template:{caseId:binding.caseId,consumerRunId:binding.consumerRunId,identityDigest,reason:'',requestIndices:[]},
      modelFields:['reason','requestIndices'],reasonCharacters:{minimum:20,maximum:1200},
      requestIndices:{minimum:1,maximum:4,range:[0,3]},
      guide:'Copy the three fixed identities exactly; fill only reason and requestIndices. Indices refer to management.contextRequests in this response, not evidence-directory positions. Every selected request must address this bound case, consumer, one of its material obligations/producers, or a planning request explicitly referenced in this case. Discovery elsewhere does not extend this binding. Empty placeholders are invalid; a valid gap keeps the decision open.'},
    actionAddress:'supervision.actions',
    actionContract:{caseId:binding.caseId,kinds:['revise','delegate','wait_for_evidence'],
      required:['caseId','kind','diagnosis','instruction'],
      revise:'Choose an explicitly verified compatible accepted repair with sourceTaskId. Supply deliveryResolution only when the current source-conflict contract requires it. Existing source, stop, scope, DAG and acceptance validators remain mandatory.',
      alternative:'If the available repair cannot be used, submit a validated focal alternative or a concrete materialContextGap linked to the exact requested read. Do not substitute prioritize, tracking, no_work or repeat a refused source.'},
    closure:'Only an observed canonical action owner, live consumer or legitimately closed episode discharges this decision. It never accepts integration or same-candidate QA.'};
}

/** Supply the same decision contract before a case's first review. The caller
 * passes the raw selected cases already enriched by readManagerCases in this
 * observation; no new read, source selection or action authority is introduced.
 * Any prior requirement, including needs_revalidation, remains authoritative. */
export function bootstrapRequiredMaterialDecision(goalId:string,focus:{caseId?:string;taskId?:string}|null|undefined,
  selectedCases:readonly any[],prior:any=null) {
  if(prior!==null && prior!==undefined)return prior;
  if(!z.uuid().safeParse(goalId).success || !z.uuid().safeParse(focus?.caseId).success ||
    !z.uuid().safeParse(focus?.taskId).success || !Array.isArray(selectedCases))return null;
  const matches=selectedCases.filter(entry=>entry?.id===focus!.caseId && entry?.task_id===focus!.taskId);
  if(matches.length!==1)return null;
  const current=matches[0],found=bindingOf(managerMaterialConsumptionReview(current));
  if(!found || current.goal_id!==goalId || found.binding.goalId!==goalId ||
    found.binding.caseId!==focus!.caseId || found.binding.consumerTaskId!==focus!.taskId)return null;
  return requirement(found.binding,found.identityDigest);
}

function normalizedRequest(value:any) {
  const parsed=managementContextRequests.safeParse([value]);
  if(!parsed.success)return null;
  const request=parsed.data[0];
  return request.kind==='task_metadata'?{...request,part:request.part??'decision'}:request;
}
function sameRead(request:any,entry:any,wholeSource=false) {
  const left=normalizedRequest(request),right=normalizedRequest(entry?.request);
  if(!left||!right)return false;
  const {snapshotDigest:expected,offset:leftOffset,...leftIdentity}=left as any;
  const {snapshotDigest:_prior,offset:rightOffset,...rightIdentity}=right as any;
  if(deliveryDigest(leftIdentity)!==deliveryDigest(rightIdentity) || (!wholeSource&&leftOffset!==rightOffset))return false;
  return !expected || expected===(entry.snapshotDigest??entry.digest);
}
function completeRead(request:any,snapshot:any) {
  if((snapshot.operation?.recoveredContext?.results??[]).some((entry:any)=>sameRead(request,entry) &&
    entry.status==='OK' && entry.truncated===false && entry.nextOffset===null))return true;
  return (snapshot.operation?.retainedEvidence?.entries??[]).some((entry:any)=>{
    if(entry.complete!==true || !sameRead(request,entry,true) || !Number.isSafeInteger(entry.totalCharacters))return false;
    const pages=Array.isArray(entry.pages)?entry.pages:[];
    return pages.length===1 && pages[0]?.offset===0 && typeof pages[0].text==='string' && pages[0].text.length===entry.totalCharacters;
  });
}

function deliveredPage(request:any,snapshot:any):{nextOffset:number|null}|null {
  const normalized=normalizedRequest(request);
  if(!normalized)return null;
  const offset=(normalized as any).offset??0;
  for(const entry of snapshot.operation?.recoveredContext?.results??[]){
    if(sameRead(request,entry) && entry.status==='OK' &&
      (typeof entry.text==='string'&&entry.text.length>0 || Array.isArray(entry.rows)&&entry.rows.length>0))
      return {nextOffset:entry.nextOffset??null};
  }
  for(const entry of snapshot.operation?.retainedEvidence?.entries??[]){
    if(!sameRead(request,entry,true))continue;
    const page=(entry.pages??[]).find((page:any)=>typeof page.text==='string' && Number.isSafeInteger(page.offset) &&
      page.offset<=offset && page.offset+page.text.length>offset);
    if(page){const end=page.offset+page.text.length;return {nextOffset:end<entry.totalCharacters?end:null};}
  }
  return null;
}

/** Planning requests are related only through explicit references in this
 * focal case, not by their mere presence in the goal-wide catalog. */
function focalPlanningReferences(snapshot:any,caseId:string) {
  const result=new Set<string>();let visited=0;
  const visit=(value:any,depth=0)=>{
    if(++visited>20000 || depth>30)return;
    if(Array.isArray(value)){for(const item of value)visit(item,depth+1);return;}
    if(!value || typeof value!=='object')return;
    for(const [key,item] of Object.entries(value)){
      if(['requestId','planningRequestId'].includes(key) && z.uuid().safeParse(item).success)result.add(item as string);
      else if(item && typeof item==='object')visit(item,depth+1);
    }
  };
  visit(snapshot.cases?.find((entry:any)=>entry.id===caseId)?.evidence);
  return result;
}

async function currentEpisode(db:DB,goalId:string,binding:Binding) {
  return (await db.query(`SELECT c.id,c.task_id,c.source_run_id,c.state case_state,c.decision_run_id,c.decision,
    t.state task_state,t.attempt_count,r.id run_id,r.attempt,r.state run_state,
    EXISTS(SELECT 1 FROM runs active WHERE active.task_id=t.id AND active.state=ANY($3::text[])) live,
    EXISTS(SELECT 1 FROM runs decision JOIN tasks manager ON manager.id=decision.task_id
      WHERE decision.id=c.decision_run_id AND decision.state='submitted' AND manager.goal_id=c.goal_id
      AND manager.stage='coordinate' AND (SELECT passed FROM acceptances WHERE run_id=decision.id
        AND task_id=manager.id ORDER BY created_at DESC,id DESC LIMIT 1)=true) accepted_decision
    FROM supervision_cases c JOIN tasks t ON t.id=c.task_id AND t.goal_id=c.goal_id
    LEFT JOIN LATERAL(SELECT id,attempt,state FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    WHERE c.id=$1 AND c.goal_id=$2 AND t.id=$4`,[binding.caseId,goalId,live,binding.consumerTaskId])).rows[0];
}

/** Revalidate retained control identities, not repair applicability. Immutable
 * receipt refs and current accepted source runs are checked without artifact IO. */
async function retainedSourcesCurrent(db:DB,goalId:string,binding:Binding) {
  const rows=(await db.query(`SELECT d.id,d.handoff_id,d.producer_task_id,d.producer_run_id,d.producer_manifest_digest,d.binding,
    d.receipt,d.state,h.state handoff_state,sr.id::text resolution_event_id,sr.payload resolution
    FROM delivery_consumptions d JOIN deferred_handoffs h ON h.id=d.handoff_id
    LEFT JOIN LATERAL(SELECT id,payload FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=d.id::text ORDER BY id DESC LIMIT 1)sr ON true
    WHERE d.goal_id=$1 AND d.consumer_task_id=$2 AND d.id=ANY($3::uuid[])`,
    [goalId,binding.consumerTaskId,binding.obligations.map(o=>o.obligationId)])).rows;
  const sources=(await db.query(`SELECT t.id,t.state,t.attempt_count,r.id run_id,r.attempt,r.state run_state,
    (SELECT passed FROM acceptances WHERE task_id=t.id AND run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1) passed
    FROM tasks t JOIN LATERAL(SELECT id,attempt,state FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    WHERE t.goal_id=$1 AND t.id=ANY($2::uuid[])`,[goalId,[...new Set(binding.obligations.map(o=>o.producerTaskId))]])).rows;
  return binding.obligations.every(expected=>{
    const row=rows.find(row=>row.id===expected.obligationId),source=sources.find(row=>row.id===expected.producerTaskId);
    if(!row || row.handoff_id!==expected.handoffId || row.state!=='waiting_supervision' || row.handoff_state!=='applied' || row.receipt?.code!=='SUPERVISION_REQUESTED' ||
      row.receipt?.sourceConflict!=null || row.receipt?.caseId!==binding.caseId || row.receipt?.generation!==expected.generation ||
      !source || source.state!=='accepted' || source.run_state!=='submitted' || source.passed!==true ||
      source.attempt!==source.attempt_count || source.run_id!==expected.producerRunId)return false;
    const resolution=row.resolution;
    if(row.resolution_event_id){
      if(row.resolution_event_id!==expected.sourceResolutionEventId || resolution?.version!==1 ||
        resolution.obligationId!==row.id || resolution.consumerTaskId!==binding.consumerTaskId ||
        resolution.original?.producerTaskId!==row.producer_task_id || resolution.original?.producerRunId!==row.producer_run_id ||
        resolution.original?.producerManifestDigest!==row.producer_manifest_digest)return false;
    }else if(expected.sourceResolutionEventId)return false;
    const current=row.resolution_event_id?resolution.replacement:{producerTaskId:row.producer_task_id,
      producerRunId:row.producer_run_id,producerManifestDigest:row.producer_manifest_digest,repairArtifacts:row.binding?.repairArtifacts};
    return current?.producerTaskId===expected.producerTaskId && current.producerRunId===expected.producerRunId &&
      current.producerManifestDigest===expected.producerManifestDigest && deliveryDigest(current.repairArtifacts??null)===deliveryDigest(expected.repairArtifacts);
  });
}

/** A disposition of a frozen material decision, not a second action applier.
 * No writes, artifact IO, source selection, acceptance or runtime signalling. */
export async function readMaterialDecisionOutcome(db:DB,goalId:string,reviewRunId:string,description:string,coordination:any) {
  const frozen=frozenMaterialDecision(description);
  if(!frozen)return null;
  if(frozen.invalid || frozen.binding.goalId!==goalId)return {code:'MANAGEMENT_MATERIAL_BINDING_INVALID',
    satisfied:false,owner:'supervisor',nextAction:'The frozen focal material binding cannot be verified. Retrieve a fresh canonical case; do not infer action authority from malformed or cross-goal context.'};
  const {binding,identityDigest,snapshot}=frozen;
  const requiredDecision=requirement(binding,identityDigest);
  const result=(code:string,satisfied:boolean,owner:string,nextAction:string,extra:Record<string,unknown>={})=>({
    code,satisfied,owner,taskId:binding.consumerTaskId,caseId:binding.caseId,
    obligationIds:binding.obligations.map(o=>o.obligationId),nextAction,...extra,
    ...(!satisfied?{requiredDecision}:{})});
  const current=await currentEpisode(db,goalId,binding);
  if(!current)return result('MANAGEMENT_MATERIAL_EPISODE_CHANGED',false,'supervisor','Retrieve the current focal case; its former consumer binding no longer exists.');
  if(['accepted','cancelled','superseded'].includes(current.task_state)||['resolved','superseded','cancelled'].includes(current.case_state))
    return result('MANAGEMENT_MATERIAL_EPISODE_CLOSED',true,'controller','The focal episode is already closed. Preserve that outcome; do not reopen it or infer overall objective acceptance.');
  const bound=current.source_run_id===binding.consumerRunId;
  const sameRun=current.run_id===binding.consumerRunId && current.attempt===binding.consumerAttempt && current.attempt_count===binding.consumerAttempt;
  const owned=bound && current.accepted_decision===true &&
    ['decided','applied','delegated','waiting_evidence'].includes(current.case_state) &&
    acceptedSupervisionActions(Array.isArray(current.decision?.actions)?current.decision.actions:[current.decision])
      .some(action=>action.caseId===binding.caseId);
  if(owned){
    const effect=(await db.query(`SELECT id,kind FROM events WHERE entity_id=$1 AND payload->>'caseId'=$2
      AND payload->>'sourceRunId'=$3 AND payload->>'decisionRunId'=$4
      AND kind IN('SUPERVISION_APPLIED','SUPERVISION_DELEGATED','SUPERVISION_WAITING_EVIDENCE')
      ORDER BY id DESC LIMIT 1`,[binding.consumerTaskId,binding.caseId,binding.consumerRunId,current.decision_run_id])).rows[0];
    return result(effect?'MANAGEMENT_MATERIAL_ACTION_APPLIED':'MANAGEMENT_MATERIAL_ACTION_OWNED',true,
      current.live?'consumer':'supervisor','Follow this exact existing decision through admission, bound runtime inputs, integration and independent QA. Do not dispatch a duplicate.',
      {decisionRunId:current.decision_run_id,state:current.case_state,...(effect?{effectEventId:String(effect.id),effectKind:effect.kind}:{})});
  }
  if(!bound||!sameRun)return result('MANAGEMENT_MATERIAL_EPISODE_CHANGED',false,'supervisor','Retrieve the current case/run binding before deciding; the frozen episode has changed.');
  if(current.live)return result('MANAGEMENT_MATERIAL_WAIT_ACTOR',true,'consumer','The same bound consumer has a live actor. Preserve it and await its actual delivery; this is not acceptance.',{state:current.run_state});
  const gap=materialContextGapSchema.safeParse(coordination?.management?.materialContextGap);
  const requests=managementContextRequests.safeParse(coordination?.management?.contextRequests);
  let contextGapFeedback:Record<string,unknown>|undefined;
  if(gap.success && requests.success){
    const bindingMatches=gap.data.caseId===binding.caseId && gap.data.consumerRunId===binding.consumerRunId &&
      gap.data.identityDigest===identityDigest;
    const linked=new Set([binding.caseId,binding.consumerTaskId,...binding.obligations.flatMap(o=>[o.obligationId,o.producerTaskId])]);
    const indices=[...new Set(gap.data.requestIndices)];
    const selected=indices.map(index=>requests.data[index]);
    const planning=focalPlanningReferences(snapshot,binding.caseId);
    const addresses=(request:any)=>[request?.caseId,request?.taskId,request?.obligationId,request?.consumerTaskId].filter(Boolean);
    const isRelated=(request:any)=>Boolean(request && (addresses(request).some(id=>linked.has(id)) ||
      (request.kind==='planning_request'&&planning.has(request.requestId))));
    const relatedRequestIndices=indices.filter((_,i)=>isRelated(selected[i]));
    const unrelatedRequestIndices=indices.filter((_,i)=>!isRelated(selected[i]));
    const related=unrelatedRequestIndices.length===0;
    if(!bindingMatches || !related)contextGapFeedback={bindingMatches,relatedRequestIndices,unrelatedRequestIndices,
      expectedSource:{caseId:binding.caseId,consumerRunId:binding.consumerRunId,identityDigest,
        consumerTaskId:binding.consumerTaskId,
        obligationIdsPath:'operation.requiredDecision.materialReview.obligations[].obligationId',
        producerTaskIdsPath:'operation.requiredDecision.materialReview.obligations[].producerTaskId',
        planningRequestIds:[...planning].slice(0,16),planningRequestIdsTruncated:planning.size>16,
        planningRequestSource:{caseId:binding.caseId,path:'cases[].evidence',fields:['requestId','planningRequestId']}}};
    const deliveredIndices=related?indices.filter((_,i)=>completeRead(selected[i],snapshot)||deliveredPage(selected[i],snapshot)):[];
    const readFeedback=deliveredIndices.map(index=>({requestIndex:index,code:'READ_ALREADY_DELIVERED',
      nextOffset:deliveredPage(requests.data[index],snapshot)?.nextOffset??null,
      nextAction:'Use the delivered interval; continue at nextOffset if present. Re-read only after the source identity/digest changes.'}));
    const pendingIndices=indices.filter(index=>!deliveredIndices.includes(index));
    if(bindingMatches && related && pendingIndices.length)return result('MANAGEMENT_MATERIAL_CONTEXT_GAP',false,'supervisor',
      'Resolve only these identified reads, then submit the existing supervision action. A context gap keeps the material decision open.',
      {contextGap:{...gap.data,requestIndices:pendingIndices,alreadyDeliveredRequestIndices:deliveredIndices},readFeedback});
    if(bindingMatches && related)return result('MANAGEMENT_MATERIAL_CONTEXT_ALREADY_DELIVERED',false,'supervisor',
      'These requested intervals were already delivered in the frozen context. Use them and continue at readFeedback.nextOffset when present, or identify a different concrete missing read; a delivered interval does not prove the whole source is complete.',
      {alreadyDeliveredRequestIndices:deliveredIndices,readFeedback});
  }
  const proposed=acceptedSupervisionActions(Array.isArray(coordination?.supervision?.actions)?coordination.supervision.actions:[])
    .find(action=>action.caseId===binding.caseId);
  return result(proposed?'MANAGEMENT_MATERIAL_ACTION_UNAPPLIED':'MANAGEMENT_MATERIAL_DECISION_REQUIRED',false,'supervisor',
    proposed?'The proposed supervision action has no canonical owner or applied effect. Inspect its exact rejection and correct it; JSON presence is not execution.':
      'Submit a valid supervision action for this bound case or a concrete materialContextGap. Prioritize, tracking, no_work and unrelated work do not discharge this material decision.',
    contextGapFeedback?{contextGapFeedback}:{});
}

/** Retain the last unresolved decision for this focus, across other scopes'
 * reviews. A fresh same-goal episode check prevents reopening closed work. */
export async function readRequiredMaterialDecision(db:DB,goalId:string,focus?:{caseId?:string;taskId?:string}) {
  if(!z.uuid().safeParse(focus?.caseId).success || !z.uuid().safeParse(focus?.taskId).success)return null;
  const row=(await db.query(`SELECT receipt FROM management_reviews m JOIN runs review ON review.id=m.run_id
    JOIN tasks manager ON manager.id=review.task_id AND manager.goal_id=m.goal_id AND manager.stage='coordinate'
    CROSS JOIN LATERAL jsonb_array_elements(m.receipts)receipt
    WHERE m.goal_id=$1 AND receipt->>'caseId'=$2 AND receipt->>'code' LIKE 'MANAGEMENT_MATERIAL_%'
      AND review.state='submitted' AND (SELECT passed FROM acceptances WHERE run_id=review.id
        AND task_id=manager.id ORDER BY created_at DESC,id DESC LIMIT 1)=true
    ORDER BY review.created_at DESC,review.id DESC LIMIT 1`,[goalId,focus!.caseId])).rows[0]?.receipt;
  const found=bindingOf(row?.requiredDecision?.materialReview);
  if(!found || found.binding.goalId!==goalId || found.binding.consumerTaskId!==focus!.taskId)return null;
  const current=await currentEpisode(db,goalId,found.binding);
  if(!current || current.case_state!=='open' || current.source_run_id!==found.binding.consumerRunId ||
    current.run_id!==found.binding.consumerRunId || current.attempt_count!==found.binding.consumerAttempt ||
    current.live || ['accepted','cancelled','superseded'].includes(current.task_state))return null;
  if(!await retainedSourcesCurrent(db,goalId,found.binding))return {version:1,caseId:found.binding.caseId,
    taskId:found.binding.consumerTaskId,consumerRunId:found.binding.consumerRunId,consumerAttempt:found.binding.consumerAttempt,
    bindingState:'needs_revalidation',previousIdentityDigest:found.identityDigest,actionAddress:'management.contextRequests',
    nextAction:'The retained repair/obligation control binding changed. Read the current obligation and case before choosing a source; the old source options are intentionally not retained.',
    requests:found.binding.obligations.map(o=>({kind:'delivery_consumption',obligationId:o.obligationId,offset:0})).slice(0,4)};
  return {...requirement(found.binding,found.identityDigest),lastDisposition:row.code,
    bindingState:'control_metadata_revalidated',sourceApplicability:'Requires the existing authoritative applier; this is not proof of repair compatibility.'};
}
