import {createHash} from 'node:crypto';
import {administrativeRecoveryEpisodeDigest} from './manager-administrative-recovery.js';
import {managerReviewOwnerReleased as reviewOwnerReleased} from './manager-review-owner.js';
/** A receipt only requests a semantic review. It never grants retry, source
 * replacement or acceptance. The reader has checked current control bindings;
 * the unchanged action applier still verifies full source/stop authority. */
export function managerMaterialConsumptionReview(entry:any) {
  const value=entry?.materialConsumptionReview;
  return value?.version===1 && value.authority==='review_only' && value.actionValidationRequired===true &&
    reviewOwnerReleased(entry) &&
    ['failed','submitted','recovery_blocked'].includes(entry.current_task_state) &&
    ['failed','submitted'].includes(entry.current_run_state) && entry.source_run_id===entry.current_run_id &&
    value.goalId===entry.goal_id && value.caseId===entry.id && value.consumerTaskId===entry.task_id &&
    value.consumerRunId===entry.current_run_id && value.consumerAttempt===entry.current_attempt &&
    Number.isSafeInteger(value.consumerAttempt) && value.consumerAttempt>0 &&
    /^[a-f0-9]{64}$/.test(value.identityDigest??'') && Array.isArray(value.obligations) && value.obligations.length>0
      ?value:null;
}
/** Keep a decision stable within its priority class. A stopped delivery with
 * available dependencies and new handoffs takes precedence over auxiliary
 * planning; allocating more workers cannot consume that delivery for it. */
export function managerCasePriority(entry:any) {
  if(managerMaterialConsumptionReview(entry))return 4;
  const canonical=!['diagnostic','repair','planning'].includes(entry.work_kind);
  const available=entry.pending_dependencies===0;
  if(canonical && available && entry.fresh_handoffs>0)return 4;
  if(canonical && available)return 3;
  if(entry.work_kind==='planning')return 1;
  return 2;
}
export const managerFocusReviewQuantum = 3;
// One bounded lower-priority review after a full high-priority quantum. Age is
// measured from case creation, never reset by feedback, artifacts or heartbeats.
export const managerFocusAgingMilliseconds = managerFocusReviewQuantum * 120000;
// Provider aliases describe the same interrupted delivery. Selection grants a
// semantic review only; retries still require the existing source/stop guards.
const interruptedDeliveryCodes = new Set(['INCOMPLETE_TURN','MODEL_OUTPUT_LIMIT','MODEL_CONTEXT_LIMIT','CONTEXT_TOO_LARGE']);
/** Retain completed review order beyond the bounded quantum query. This ledger
 * stays in controller state, not in the model context; closed cases are pruned.
 * Event IDs preserve PostgreSQL order without timestamp precision loss. */
export function managerFocusServiceHistory(cases:any[], previous:any, reviews:any[]) {
  const ids=new Set(cases.filter(entry=>entry.state==='open').map(entry=>entry.id));
  const lastReviewed:Record<string,string>={};
  const eventId=(value:any):value is string=>typeof value==='string' && /^[1-9][0-9]{0,29}$/.test(value);
  if(previous?.version===1 && previous.lastReviewed && typeof previous.lastReviewed==='object')
    for(const [id,value] of Object.entries(previous.lastReviewed))
      if(ids.has(id) && eventId(value))lastReviewed[id]=value;
  for(const review of reviews)if(review.valid!==false && ids.has(review.caseId) && eventId(review.eventId) &&
    /^[a-f0-9]{64}$/.test(review.sourceFingerprint??'') &&
    (!lastReviewed[review.caseId] || BigInt(review.eventId)>BigInt(lastReviewed[review.caseId])))
      lastReviewed[review.caseId]=review.eventId;
  return {version:1,lastReviewed};
}
function canBorrowReview(entry:any, now:number) {
  const created=Date.parse(entry.created_at);
  return reviewOwnerReleased(entry) &&
    Number.isFinite(created) && Number.isFinite(now) && now-created>=managerFocusAgingMilliseconds;
}
/** Only a canonical, stopped administrative episode may borrow its first
 * semantic review without the normal age/quantum. This hint is not authority
 * to retry, refresh planning inputs, apply a decision or accept a delivery. */
export function managerAdministrativeRecoveryReview(entry:any) {
  const value=entry?.administrativeRecoveryReview;
  return value?.version===1 && value.authority==='review_only' && value.actionValidationRequired===true &&
    reviewOwnerReleased(entry) && entry.current_run_state==='failed' &&
    ['failed','recovery_blocked'].includes(entry.current_task_state) &&
    entry.current_error_code==='OPERATOR_RECOVERY_REQUESTED' && entry.source_run_id===entry.current_run_id &&
    value.goalId===entry.goal_id && value.caseId===entry.id && value.taskId===entry.task_id &&
    value.runId===entry.current_run_id && value.runFence===entry.current_run_fence &&
    value.contextVersion===entry.current_context_version && value.attempt===entry.current_attempt &&
    value.attempt===entry.task_attempt_count && Number.isSafeInteger(value.attempt) && value.attempt>0 &&
    ['requestDigest','sourceDigest','bridgeReceiptDigest','episodeDigest'].every(key=>/^[a-f0-9]{64}$/.test(value[key]??'')) &&
    /^[1-9][0-9]{0,29}$/.test(value.readinessEventId??'') && Number.isFinite(Date.parse(value.readyAt)) &&
    typeof value.priorityConsumed==='boolean' && typeof value.lastCompletedReviewWasAdministrative==='boolean' &&
    value.episodeDigest===administrativeRecoveryEpisodeDigest(value) ? value : null;
}
export function managerFocusSourceFingerprint(entry:any) {
  const material=managerMaterialConsumptionReview(entry);
  const administrative=managerAdministrativeRecoveryReview(entry);
  return createHash('sha256').update(JSON.stringify([
    entry.id,entry.current_run_id??null,entry.current_attempt??null,
    entry.current_run_state??null,entry.current_error_code??null,
    entry.pending_dependencies??null,entry.fresh_handoffs??null,
    (entry.current_artifacts??[]).map((artifact:any)=>[artifact.id,artifact.digest]),
    ...(material?[material.identityDigest]:[]),
    ...(administrative?[administrative.episodeDigest]:[]),
  ])).digest('hex');
}
/** Accepted reviews form the decision quantum; worker/tool chatter does not.
 * Move to an equally urgent, less recently reviewed scope after three decisions
 * on unchanged evidence. The old case remains open with all evidence intact. */
export function selectManagerFocus(cases: any[], previous: any, tasks?: any[], reviews:any[] = [], now=Date.now(), serviceHistory?:any) {
  reviews=reviews.filter(review=>review.valid!==false);
  const history=managerFocusServiceHistory(cases,serviceHistory,reviews).lastReviewed;
  const reviewOrder=(a:any,b:any)=>{
    const aId=history[a.id],bId=history[b.id];
    if(aId || bId)return !aId?-1:!bId?1:BigInt(aId)<BigInt(bId)?-1:BigInt(aId)>BigInt(bId)?1:0;
    // Legacy callers without event IDs still use the bounded history. The
    // operational wake always supplies IDs and persists the complete ledger.
    const ai=reviews.findIndex(review=>review.caseId===a.id),bi=reviews.findIndex(review=>review.caseId===b.id);
    return (bi<0?Number.MAX_SAFE_INTEGER:bi)-(ai<0?Number.MAX_SAFE_INTEGER:ai);
  };
  const ranked=[...cases].sort((a,b)=>managerCasePriority(b)-managerCasePriority(a));
  const prior=cases.find(c=>c.id===previous?.caseId);
  const highest=ranked.length?managerCasePriority(ranked[0]):0;
  // Returning from a borrowed review must not restart the first high case and
  // starve its equally urgent peers. Retain the normal uninterrupted quantum.
  const preferred=prior && managerCasePriority(prior)<highest ?
    ranked.filter(candidate=>managerCasePriority(candidate)===highest).sort(reviewOrder)[0] : ranked[0];
  let entry=prior && managerCasePriority(prior)>=managerCasePriority(ranked[0]??{}) ? prior : preferred;
  if (!entry) return null;
  let selectionReason=entry===prior?'stable_decision':'priority';
  const completed=reviews.slice(0,managerFocusReviewQuantum);
  // Review history is written only for accepted manager results. New evidence
  // keeps its full authority, but cannot make an old waiting case disappear.
  // A loan selects a review only; it never retries or consumes a handoff.
  const quantum=entry===prior && completed.length===managerFocusReviewQuantum &&
    completed.every(review=>review.caseId===entry.id && /^[a-f0-9]{64}$/.test(review.sourceFingerprint??''));
  if(quantum){
    const lower=ranked.filter(candidate=>managerCasePriority(candidate)<managerCasePriority(entry) && canBorrowReview(candidate,now));
    const next=lower.sort((a,b)=>reviewOrder(a,b) || Date.parse(a.created_at)-Date.parse(b.created_at) ||
      managerCasePriority(b)-managerCasePriority(a) || String(a.id).localeCompare(String(b.id)))[0];
    if(next){entry=next;selectionReason='aged_review_quantum';}
  }
  // The material episode is part of the full authority fingerprint, but
  // auxiliary chatter must not reset this case's completed service quantum.
  // A changed source still needs fresh action validation after focus returns.
  const material=managerMaterialConsumptionReview(entry);
  if(selectionReason!=='aged_review_quantum' && entry===prior && (material || previous?.sourceFingerprint===managerFocusSourceFingerprint(entry))) {
    const completed=reviews.slice(0,managerFocusReviewQuantum);
    const exhausted=completed.slice(0,managerFocusReviewQuantum).length===managerFocusReviewQuantum &&
      completed.every(review=>review.caseId===entry.id && /^[a-f0-9]{64}$/.test(review.sourceFingerprint??'') &&
        (material || review.sourceFingerprint===previous.sourceFingerprint));
    if(exhausted){
      const eligible=ranked.filter(candidate=>candidate.id!==entry.id &&
        managerCasePriority(candidate)===managerCasePriority(entry));
      const scope=(candidate:any)=>tasks?.find(task=>task.id===(candidate.taskId??candidate.task_id))?.baseWorkspace ??
        candidate.base_workspace??null;
      const otherScopes=eligible.filter(candidate=>scope(candidate)!==scope(entry));
      const next=(otherScopes.length?otherScopes:eligible).sort(reviewOrder)[0];
      if(next){entry=next;selectionReason='review_quantum';}
    }
  }
  // Additional-case visibility did not service interrupted deliveries: three
  // actual accepted reviews can still ignore them. Give an unseen, aged,
  // stopped and current delivery its own bounded semantic review. This does
  // not grant execution, physical stop, source replacement or acceptance.
  const interrupted=ranked.filter(candidate=>canBorrowReview(candidate,now) &&
    candidate.current_run_state==='failed' &&
    ['failed','recovery_blocked'].includes(candidate.current_task_state) &&
    interruptedDeliveryCodes.has(candidate.current_error_code) &&
    typeof candidate.current_run_id==='string' && candidate.source_run_id===candidate.current_run_id &&
    Number.isSafeInteger(candidate.current_attempt) && candidate.current_attempt>0);
  const interruptedIds=new Set(interrupted.map(candidate=>candidate.id));
  const preceding=reviews.slice(0,managerFocusReviewQuantum);
  if(preceding.length===managerFocusReviewQuantum &&
    preceding.every(review=>!interruptedIds.has(review.caseId) && /^[a-f0-9]{64}$/.test(review.sourceFingerprint??''))){
    const unseen=interrupted.filter(candidate=>!history[candidate.id])
      .sort((a,b)=>Date.parse(a.created_at)-Date.parse(b.created_at) || String(a.id).localeCompare(String(b.id)))[0];
    if(unseen){entry=unseen;selectionReason='unseen_interrupted_delivery';}
  }
  // One first review for each verified administrative episode. A valid frozen
  // MANAGEMENT_REVIEWED consumes it even if the model proposed no action.
  // History and artifact churn cannot rearm it. Between two such loans serve
  // an ordinary case when one exists; the same manager/admission path owns both.
  const administrative=ranked.filter(candidate=>{
    const hint=managerAdministrativeRecoveryReview(candidate);
    return hint && !hint.priorityConsumed;
  }).sort((a,b)=>{
    const left=BigInt(a.administrativeRecoveryReview.readinessEventId),right=BigInt(b.administrativeRecoveryReview.readinessEventId);
    return left<right?-1:left>right?1:String(a.id).localeCompare(String(b.id));
  });
  const ordinary=ranked.filter(candidate=>!managerAdministrativeRecoveryReview(candidate) && reviewOwnerReleased(candidate));
  const lastAdministrative=administrative.some(candidate=>candidate.administrativeRecoveryReview.lastCompletedReviewWasAdministrative);
  if(administrative.length && (!lastAdministrative || !ordinary.length)){
    entry=administrative[0];selectionReason='administrative_first_review';
  }else if(administrative.length && lastAdministrative && ordinary.length && !ordinary.includes(entry)){
    entry=ordinary.sort((a,b)=>managerCasePriority(b)-managerCasePriority(a) || reviewOrder(a,b))[0];
    selectionReason='administrative_review_fairness';
  }
  const task = tasks?.find(t => t.id === (entry.taskId ?? entry.task_id));
  return {caseId:entry.id,taskId:entry.taskId ?? entry.task_id,
    scope:task?.baseWorkspace ?? task?.execution?.baseWorkspace ?? entry.base_workspace ?? null,
    mode:'resolve_blocker',contextMode:'delivery_graph',remainingCases:Math.max(0,cases.length-1),
    sourceFingerprint:managerFocusSourceFingerprint(entry),selectionReason,
    ...(selectionReason==='administrative_first_review'?{
      administrativeRecoveryEpisodeDigest:entry.administrativeRecoveryReview.episodeDigest}:{}),
    maxConsecutiveReviews:managerFocusReviewQuantum};
}

/** Keep one semantic delivery focus, but let that same manager review several
 * stopped transport failures in one turn. Selection does not authorize retry:
 * every case still needs a fresh decision, stop proof and normal admission. */
export function selectManagerCaseWindow(cases:any[],focus:any) {
  if(!focus)return cases.slice(0,6);
  const focal=cases.find(entry=>entry.id===focus.caseId);
  if(!focal)return [];
  const transient=new Set(['MODEL_TIMEOUT','MODEL_IDLE_TIMEOUT','PROVIDER_TIMEOUT','MODEL_REQUEST_FAILED']);
  const currentStopped=(entry:any)=>entry.id!==focal.id && entry.state!=='decided' && entry.decision_run_id==null &&
    entry.has_live_run!==true && entry.current_run_state==='failed' &&
    (entry.source_run_id==null || entry.source_run_id===entry.current_run_id);
  // One delivery-failure review slot, never retry/acceptance or physical stop.
  const delivery=cases.filter(entry=>currentStopped(entry) &&
    interruptedDeliveryCodes.has(entry.current_error_code))
    .sort((a,b)=>Date.parse(a.created_at)-Date.parse(b.created_at) || String(a.id).localeCompare(String(b.id))).slice(0,1);
  const operational=[...delivery,...cases.filter(entry=>currentStopped(entry) && transient.has(entry.current_error_code))
    .sort((a,b)=>managerCasePriority(b)-managerCasePriority(a) ||
      (a.current_artifacts?.length??0)-(b.current_artifacts?.length??0) ||
      JSON.stringify(a.evidence??[]).length-JSON.stringify(b.evidence??[]).length).slice(0,5-delivery.length)]
    .map(entry=>({...entry,operationalRecovery:true}));
  return [focal,...operational];
}

/** A global presence directory remains visible. Exact instructions, criteria,
 * routes and evidence are delivered for the scope being decided this review. */
export function focusManagerTasks(tasks: any[], focus: any) {
  if (!focus?.scope) return tasks;
  let decisionIds:Set<string>|undefined;
  if(focus.contextMode==='delivery_graph'){
    const byId=new Map(tasks.map(t=>[t.id,t]));
    // A decision needs the focal producer chain and its immediate consumers.
    // Other canonical deliveries are visible in the directory/frontier, but
    // seeding all of their dependency trees recreates the entire objective.
    const pending=[focus.taskId];decisionIds=new Set();
    while(pending.length){const id=pending.pop();if(!id||decisionIds.has(id))continue;
      decisionIds.add(id);pending.push(...(byId.get(id)?.dependencies??[]));}
    for(const task of tasks)if(task.baseWorkspace===focus.scope &&
      !['accepted','cancelled','superseded'].includes(task.state) &&
      (task.dependencies??[]).includes(focus.taskId))decisionIds.add(task.id);
  }
  return tasks.map(task => (task.baseWorkspace===focus.scope && (!decisionIds || decisionIds.has(task.id))) || task.id===focus.taskId ? task : {
    id:task.id,title:task.title,state:task.state,stage:task.stage,
    baseWorkspace:task.baseWorkspace,expectedAttempt:task.expectedAttempt,
    live:task.live,workKind:task.workKind,
    dependencySummary:{count:(task.dependencies??[]).length,
      sha256:createHash('sha256').update(JSON.stringify(task.dependencies??[])).digest('hex')},
    selection:{detailOmitted:true,omittedFields:decisionIds?['dependencies','criteria','intent','routeOptions']:['dependencies'],fullSourcesPreserved:true,
      recovery:{kind:decisionIds?'task_metadata':'task',taskId:task.id,expectedAttempt:task.expectedAttempt,
        graphApi:'GET /api/goals/:goalId',graphTable:'dependencies',taskColumn:'task_id'}},
  });
}
