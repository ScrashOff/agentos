import {createHash} from 'node:crypto';
import type {DB} from './db.js';
import {runtimeStageObservationSchema,stageObservationStatement,type RuntimeStageObservation} from './runtime-stage-contract.js';
const uuid=/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
const sha=/^[a-f0-9]{64}$/;
const eventId=/^[1-9][0-9]{0,17}$/;
export type LiveFailureFocus={kind:'live_failure';version:1;authority:'review_only';nonfatal:true;
  goalId:string;taskId:string;runId:string;runFence:string;contextVersion:string;expectedAttempt:number;
  scope:string;eventId:string;failureKey:string;semanticCauseDigest:string;category:'missing_resource'|'tool_error'|'unproductive_wait'|'stage_failure';
  toolName:string;callId:string;evidenceRef:string;statement:string;remainingCases:number;
  stageEvidence?:{verification:'unverified_observation'}&Pick<RuntimeStageObservation,'category'|'evidenceDigest'|'reports'|'wait'>;};
export function liveFailureKey(row:any){
  return createHash('sha256').update(JSON.stringify([row.goal_id,row.task_id,row.run_id,row.run_fence,
    row.context_id,row.semantic_cause_digest])).digest('hex');
}
/** Reader-projected current identity is authoritative; telemetry supplies only
 * a bounded nonfatal fact. A failure never opens a stopped recovery case. */
export function currentLiveFailure(row:any):LiveFailureFocus|null{
  const stage=row?.event_kind==='RUNTIME_STAGE_OBSERVATION';
  const observation=stage?runtimeStageObservationSchema.safeParse(row.stage_observation):null;
  if(stage&&(!observation?.success||row.observation_authority!=='review_only'||
    row.observation_current!==true||observation.data.category!==row.category))return null;
  if(!row||!['goal_id','task_id','run_id','run_fence','context_id'].every(k=>uuid.test(row[k]??''))||
    !eventId.test(row.event_id??'')||!sha.test(row.semantic_cause_digest??'')||
    row.task_state!=='running'||row.run_state!=='running'||row.runtime!=='harness'||row.execution_kind!=='harness'||
    row.stage==='coordinate'||row.attempt!==row.attempt_count||!Number.isSafeInteger(row.attempt)||row.attempt<=0||
    row.cancel_requested!==false||row.guidance_available!==true||row.persisted_context_id!==row.context_id||
    row.payload_goal_id!==row.goal_id||row.payload_task_id!==row.task_id||row.payload_run_id!==row.run_id||
    row.payload_run_fence!==row.run_fence||row.payload_context_id!==row.context_id||
    !(stage?['unproductive_wait','stage_failure']:['missing_resource','tool_error']).includes(row.category)||typeof row.scope!=='string'||!row.scope||row.scope.length>240||
    typeof row.tool_name!=='string'||!/^[a-zA-Z0-9_.:-]{1,80}$/.test(row.tool_name)||
    typeof row.call_id!=='string'||!/^[a-zA-Z0-9_.:-]{1,200}$/.test(row.call_id))return null;
  return {kind:'live_failure',version:1,authority:'review_only',nonfatal:true,
    goalId:row.goal_id,taskId:row.task_id,runId:row.run_id,runFence:row.run_fence,contextVersion:row.context_id,
    expectedAttempt:row.attempt,scope:row.scope,eventId:row.event_id,failureKey:liveFailureKey(row),
    semanticCauseDigest:row.semantic_cause_digest,category:row.category,toolName:row.tool_name,callId:row.call_id,
    evidenceRef:'event:'+row.event_id,statement:stage&&observation?.success
      ?stageObservationStatement(observation.data,row.observation_generation>0)
      :`Tool ${row.tool_name} reported ${row.category}. This may be an intentional negative test; inspect the current task before choosing guidance.`,remainingCases:0,
    ...(stage&&observation?.success?{stageEvidence:{verification:'unverified_observation' as const,
      category:observation.data.category,evidenceDigest:observation.data.evidenceDigest,
      reports:observation.data.reports,...(observation.data.wait?{wait:observation.data.wait}:{})}}:{})};
}
export function selectLiveFailureFocus(rows:any[],reviewedKeys:ReadonlySet<string>,remainingCases=0){
  const byKey=new Map<string,LiveFailureFocus>();
  for(const row of rows){const focus=currentLiveFailure(row);
    if(!focus||reviewedKeys.has(focus.failureKey))continue;
    const prior=byKey.get(focus.failureKey);
    if(!prior||BigInt(focus.eventId)<BigInt(prior.eventId))byKey.set(focus.failureKey,focus);
  }
  const focus=[...byKey.values()].sort((a,b)=>BigInt(a.eventId)<BigInt(b.eventId)?-1:1)[0];
  return focus?{...focus,remainingCases}:null;
}
/** A semantic review is not a resolved failure. Only an exactly bound owned
 * guidance action or controller-validated bounded no-action can debounce it. */
export function liveFailureReviewDisposition(focus:LiveFailureFocus,receipt:any,action?:any){
  if(receipt?.code==='LIVE_FAILURE_NO_ACTION'&&receipt.failureKey===focus.failureKey&&
    receipt.eventId===focus.eventId&&receipt.goalId===focus.goalId&&receipt.taskId===focus.taskId&&
    receipt.runId===focus.runId&&receipt.runFence===focus.runFence&&receipt.contextVersion===focus.contextVersion&&
    receipt.decision==='continue_observing'&&typeof receipt.reason==='string'&&receipt.reason.trim().length>=40&&receipt.reason.length<=1000)
    return {kind:'bounded_no_action' as const,accepted:false,resolved:false};
  if(receipt?.kind!=='guide'||receipt.code!=='PROPOSED'||!uuid.test(receipt.actionId??'')||
    action?.actionId!==receipt.actionId||action.goalId!==focus.goalId||action.taskId!==focus.taskId||
    action.runId!==focus.runId||action.runFence!==focus.runFence||action.contextVersion!==focus.contextVersion||
    action.expectedAttempt!==focus.expectedAttempt||
    !Array.isArray(action.facts)||!action.facts.some((fact:any)=>fact.evidenceRef===focus.evidenceRef)||
    !['proposed','dispatching','queued_next_step','consumed','uncertain','effectverified'].includes(action.state))return null;
  return {kind:'owned_guidance' as const,phase:action.state,accepted:false,resolved:false,
    consumed:['consumed','effectverified'].includes(action.state),requiresReconciliation:['dispatching','uncertain'].includes(action.state)};
}
/** One accepted semantic review per current cause. Failed/invalid calls and
 * mere wake proposals do not consume this ledger. No new tables or writes. */
export async function readLiveFailureReview(db:DB,goalId:string){
  const rows=(await db.query(`SELECT e.id::text event_id,t.goal_id,t.id task_id,t.state task_state,t.stage,
    t.attempt_count,t.execution->>'kind' execution_kind,t.execution->>'baseWorkspace' scope,
    r.id run_id,r.fence run_fence,r.context_id,r.attempt,r.state run_state,r.runtime,c.id persisted_context_id,
    e.payload->>'goalId' payload_goal_id,e.payload->>'taskId' payload_task_id,e.payload->>'runId' payload_run_id,
    e.payload->>'runFence' payload_run_fence,e.payload->>'contextVersion' payload_context_id,
    e.payload->>'semanticCauseDigest' semantic_cause_digest,e.payload->>'category' category,
    e.payload->>'toolName' tool_name,e.payload->>'callId' call_id,e.kind event_kind,
    e.payload->>'authority' observation_authority,e.payload->'observation' stage_observation,
    e.payload->>'generation' observation_generation,
    EXISTS(SELECT 1 FROM settings observed WHERE observed.id='runtime-stage:'||r.id
      AND observed.value->>'runFence'=r.fence::text AND observed.value->>'contextVersion'=r.context_id::text
      AND observed.value->'activeEventIds' ? e.id::text) observation_current,
    EXISTS(SELECT 1 FROM events cancelled WHERE cancelled.kind='CANCEL_REQUESTED' AND
      (cancelled.payload->>'runId'=r.id::text OR cancelled.entity_id IN(r.id::text,t.id::text))) cancel_requested,
    EXISTS(SELECT 1 FROM events available WHERE available.kind='RUNTIME_GUIDANCE_AVAILABLE'
      AND available.entity_id=t.id::text AND available.payload->>'runId'=r.id::text
      AND available.payload->>'runFence'=r.fence::text AND available.payload->>'contextVersion'=r.context_id::text) guidance_available
    FROM tasks t JOIN goals g ON g.id=t.goal_id AND g.state='active'
    JOIN settings management ON management.id='management:'||g.id AND management.value->>'enabled'='true'
    JOIN settings supervision ON supervision.id='supervision:'||g.id AND supervision.value->>'enabled'='true'
    JOIN settings workforce ON workforce.id='workforce:'||g.id AND workforce.value->>'enabled'='true'
    LEFT JOIN settings autonomy ON autonomy.id='autonomy:'||g.project
    JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count AND r.state='running'
    JOIN contexts c ON c.id=r.context_id AND c.run_id=r.id
    JOIN LATERAL(SELECT DISTINCT ON(ev.payload->>'semanticCauseDigest') ev.id,ev.kind,ev.payload
      FROM events ev WHERE ev.entity_id=t.id::text AND ev.kind IN('HARNESS_TOOL_FAILURE','RUNTIME_STAGE_OBSERVATION')
      AND ev.payload->>'runId'=r.id::text
      AND ev.payload->>'goalId'=t.goal_id::text AND ev.payload->>'taskId'=t.id::text
      AND ev.payload->>'runFence'=r.fence::text AND ev.payload->>'contextVersion'=r.context_id::text
      AND ev.payload->>'semanticCauseDigest' ~ '^[a-f0-9]{64}$'
      AND (ev.kind='HARNESS_TOOL_FAILURE' OR EXISTS(SELECT 1 FROM settings observed
        WHERE observed.id='runtime-stage:'||r.id AND observed.value->>'runFence'=r.fence::text
        AND observed.value->>'contextVersion'=r.context_id::text AND observed.value->'activeEventIds' ? ev.id::text))
      AND (ev.kind='HARNESS_TOOL_FAILURE' OR NOT EXISTS(SELECT 1 FROM settings owned
        WHERE owned.id LIKE 'runtime-guidance:%' AND owned.value->>'runId'=r.id::text
        AND owned.value->>'runFence'=r.fence::text AND owned.value->>'contextVersion'=r.context_id::text
        AND owned.value->>'taskId'=t.id::text AND owned.value->>'goalId'=t.goal_id::text
        AND owned.value->>'state' IN('proposed','dispatching','queued_next_step','uncertain')))
      ORDER BY ev.payload->>'semanticCauseDigest',ev.id ASC)e ON true
    WHERE t.goal_id=$1 AND t.state='running' AND t.stage IS DISTINCT FROM 'coordinate'
      AND COALESCE(autonomy.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR autonomy.value->>'enabled'='true')
    ORDER BY e.id ASC LIMIT 128`,[goalId])).rows;
  const keys=[...new Set(rows.map(currentLiveFailure).filter((x):x is LiveFailureFocus=>x!==null).map(x=>x.failureKey))];
  const reviewed=keys.length?(await db.query(`SELECT DISTINCT e.payload->'contextRequestFocus'->>'failureKey' failure_key
    FROM events e JOIN runs mr ON mr.id::text=e.payload->>'runId' JOIN tasks mt ON mt.id=mr.task_id
    WHERE e.kind='MANAGEMENT_REVIEWED' AND e.entity_id=$1 AND mt.goal_id=$1::uuid AND mt.stage='coordinate'
      AND mr.state='submitted' AND (SELECT passed FROM acceptances a WHERE a.run_id=mr.id AND a.task_id=mt.id ORDER BY created_at DESC,id DESC LIMIT 1)=true
      AND e.payload->'contextRequestFocus'->>'kind'='live_failure'
      AND e.payload->'contextRequestFocus'->>'failureKey'=ANY($2::text[])
      AND jsonb_typeof(e.payload->'receipts')='array'
      AND NOT e.payload->'receipts' @> '[{"code":"MANAGEMENT_FORMAT"}]'::jsonb
      AND (EXISTS(SELECT 1 FROM management_actions ma JOIN settings owned ON owned.id='runtime-guidance:'||(ma.receipt->>'actionId')
        WHERE ma.run_id=mr.id AND ma.receipt->>'kind'='guide' AND ma.receipt->>'code'='PROPOSED'
          AND owned.value->>'state' IN('proposed','dispatching','queued_next_step','consumed','uncertain','effectverified')
          AND owned.value->>'goalId'=$1 AND owned.value->>'taskId'=e.payload->'contextRequestFocus'->>'taskId'
          AND owned.value->>'runId'=e.payload->'contextRequestFocus'->>'runId'
          AND owned.value->>'runFence'=e.payload->'contextRequestFocus'->>'runFence'
          AND owned.value->>'contextVersion'=e.payload->'contextRequestFocus'->>'contextVersion'
          AND owned.value->>'expectedAttempt'=e.payload->'contextRequestFocus'->>'expectedAttempt'
          AND owned.value->'facts' @> jsonb_build_array(jsonb_build_object('evidenceRef','event:'||(e.payload->'contextRequestFocus'->>'eventId'))))
        OR EXISTS(SELECT 1 FROM management_actions ma WHERE ma.run_id=mr.id
          AND ma.receipt->>'code'='LIVE_FAILURE_NO_ACTION'
          AND ma.receipt->>'failureKey'=e.payload->'contextRequestFocus'->>'failureKey'
          AND ma.receipt->>'eventId'=e.payload->'contextRequestFocus'->>'eventId'
          AND ma.receipt->>'goalId'=$1 AND ma.receipt->>'taskId'=e.payload->'contextRequestFocus'->>'taskId'
          AND ma.receipt->>'runId'=e.payload->'contextRequestFocus'->>'runId'
          AND ma.receipt->>'runFence'=e.payload->'contextRequestFocus'->>'runFence'
          AND ma.receipt->>'contextVersion'=e.payload->'contextRequestFocus'->>'contextVersion'
          AND ma.receipt->>'decision'='continue_observing' AND length(trim(ma.receipt->>'reason')) BETWEEN 40 AND 1000))`,[goalId,keys])).rows:[];
  const reviewedKeys=new Set<string>(reviewed.map(row=>row.failure_key));
  const focus=selectLiveFailureFocus(rows,reviewedKeys);
  const pendingKeys=keys.filter(key=>!reviewedKeys.has(key)).sort();
  const pendingFocuses=pendingKeys.map(key=>selectLiveFailureFocus(rows,new Set(keys.filter(k=>k!==key))))
    .filter((x):x is LiveFailureFocus=>x!==null).sort((a,b)=>BigInt(a.eventId)<BigInt(b.eventId)?-1:1);
  return {focus,reviewedKeys,selectedEventCount:rows.length,truncated:rows.length===128,
    pendingKeys,pendingFocuses,materiality:pendingKeys.length?createHash('sha256').update(JSON.stringify(pendingKeys)).digest('hex'):null};
}
/** Existing management_actions is the disposition ledger; this helper never
 * admits runtime work or rewrites a worker's immutable contract. */
export async function recordLiveFailureNoAction(db:DB,goalId:string,managerRunId:string,index:number,raw:any,focus:any){
  const prior=(await db.query('SELECT receipt FROM management_actions WHERE run_id=$1 AND action_index=$2',[managerRunId,index])).rows[0];
  if(prior)return prior.receipt;
  const current=(await readLiveFailureReview(db,goalId)).pendingFocuses.find(f=>f.failureKey===focus?.failureKey&&f.eventId===focus?.eventId);
  const authorized=(await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id
    WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate' AND r.state='submitted'
      AND (SELECT passed FROM acceptances a WHERE a.run_id=r.id AND a.task_id=t.id ORDER BY created_at DESC,id DESC LIMIT 1)=true`,[managerRunId,goalId])).rowCount;
  const receipt={code:'LIVE_FAILURE_NO_ACTION',failureKey:raw?.failureKey,eventId:raw?.eventId,
    goalId,taskId:focus?.taskId,runId:focus?.runId,runFence:focus?.runFence,contextVersion:focus?.contextVersion,
    decision:raw?.decision,reason:raw?.reason};
  if(!authorized||!current||focus?.kind!=='live_failure'||current.failureKey!==focus.failureKey||
    current.eventId!==focus.eventId||!liveFailureReviewDisposition(current,receipt))return {code:'LIVE_FAILURE_DISPOSITION_STALE'};
  await db.query('INSERT INTO management_actions(run_id,action_index,receipt) VALUES($1,$2,$3)',[managerRunId,index,JSON.stringify(receipt)]);
  return receipt;
}
