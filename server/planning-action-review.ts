import {createHash} from 'node:crypto';
import type {DB} from './db.js';
import {scopedPlanActionSchema,scopedPlanSchema} from './scoped-plan-contract.js';
import {taskExecutionMetadataVersion} from './task-execution-metadata.js';

const digest=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const bytesDigest=(value:Buffer)=>createHash('sha256').update(value).digest('hex');
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
const sha=/^[a-f0-9]{64}$/;
const id=(value:any)=>typeof value==='string'&&uuid.test(value)?value:null;
const code=(value:any)=>typeof value==='string'&&/^[A-Z][A-Z0-9_]{0,95}$/.test(value)?value:'UNKNOWN';
const maximumActions=12;
const semantics='receiptApplied records an action request or supervision routing only; it does not prove material consumption, task dispatch, game acceptance or permission to rebind/retry.';
type ArtifactReader=(id:string)=>Promise<{bytes:Buffer}>;

/** Cheap directory hint from the request alone. Mutable event/artifact/handoff
 * authority is deliberately checked only by the explicit detail reader. */
export function reportedPlanningActions(requestId:string,raw:any,total:number){
  const values=Array.isArray(raw)?raw.slice(0,maximumActions):[];
  return {version:1,authority:'review_only',status:'REPORTED_REQUIRES_DETAIL',totalReceipts:total,
    reported:{receiptApplied:values.filter(r=>r?.applied===true).length,
      refused:values.filter(r=>r?.applied===false&&r?.preparing===false).length,
      preparing:values.filter(r=>r?.preparing===true).length},
    reportedCodes:[...new Set(values.map(r=>code(r?.code)))].slice(0,3),
    truncated:total>maximumActions||new Set(values.map(r=>code(r?.code))).size>3,
    detailRequest:{kind:'planning_request',requestId,offset:0},semantics};
}

/** Read scalar authority and twelve receipts, never the embedded planner catalog.
 * xmin and the accepted artifact/run identity also fence the second read. */
async function authority(db:DB,goalId:string,requestIds:string[]){
  return (await db.query(`SELECT p.id,p.goal_id,p.scope,p.state,p.source_revision,p.task_id,
    p.updated_at::text request_updated_at,p.xmin::text request_version,
    p.receipt->>'code' receipt_code,p.receipt->>'evaluatedRunId' evaluated_run_id,
    CASE WHEN jsonb_typeof(p.receipt->'deliveryActionReceipts')='array'
      THEN jsonb_array_length(p.receipt->'deliveryActionReceipts') ELSE 0 END receipt_count,
    CASE WHEN jsonb_typeof(p.receipt->'deliveryActionReceipts')='array' THEN
      (SELECT COALESCE(jsonb_agg(entry.value ORDER BY entry.ordinality),'[]'::jsonb)
       FROM jsonb_array_elements(p.receipt->'deliveryActionReceipts') WITH ORDINALITY entry(value,ordinality)
       WHERE entry.ordinality<=12) ELSE '[]'::jsonb END action_receipts,
    t.state task_state,t.attempt_count,metadata.work_kind,
    metadata.task_id metadata_task_id,metadata.goal_id metadata_goal_id,metadata.version metadata_version,metadata.execution_type,
    t.execution->>'planningRequestId' task_request_id,metadata.base_workspace task_scope,
    r.id run_id,r.state run_state,r.fence,r.context_id,r.created_at::text run_created_at,
    a.id artifact_id,a.digest artifact_digest,a.size artifact_size,
    accepted.id acceptance_id,accepted.passed,accepted.created_at::text accepted_at,
    EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id AND live.state IN('queued','running','cancelling','reconciling')) live
    FROM planning_requests p LEFT JOIN tasks t ON t.id=p.task_id AND t.goal_id=p.goal_id
    LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    LEFT JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    LEFT JOIN artifacts a ON a.run_id=r.id AND a.name='scope-plan.json'
    LEFT JOIN LATERAL(SELECT id,passed,created_at FROM acceptances WHERE run_id=r.id AND task_id=t.id
      ORDER BY created_at DESC,id DESC LIMIT 1)accepted ON true
    WHERE p.goal_id=$1 AND p.id=ANY($2::uuid[]) ORDER BY p.id`,[goalId,requestIds])).rows;
}
function authorized(row:any){
  return row.state==='awaiting_supervision'&&row.task_state==='accepted'&&row.run_state==='submitted'&&
    row.evaluated_run_id===row.run_id&&row.work_kind==='planning'&&row.task_request_id===row.id&&
    row.metadata_task_id===row.task_id&&row.metadata_goal_id===row.goal_id&&row.metadata_version===taskExecutionMetadataVersion&&row.execution_type==='object'&&
    row.task_scope===row.scope&&row.passed===true&&!row.live&&id(row.artifact_id)&&sha.test(row.artifact_digest??'');
}
function receipt(value:any){
  if(!value||typeof value.applied!=='boolean'||typeof value.preparing!=='boolean'||
    Object.keys(value).some(key=>!['applied','preparing','code','actionKey','handoffId','caseId','existing'].includes(key))||
    value.existing!==undefined&&typeof value.existing!=='boolean'||
    typeof value.actionKey!=='string'||!/^scoped-action:[a-f0-9]{64}$/.test(value.actionKey)||code(value.code)!==value.code||
    value.handoffId!==undefined&&!id(value.handoffId)||value.caseId!==undefined&&!id(value.caseId))return null;
  // `existing` is an observational replay marker, not part of the authority.
  return {applied:value.applied,preparing:value.preparing,code:value.code,actionKey:value.actionKey,
    ...(value.handoffId!==undefined?{handoffId:value.handoffId}:{}),...(value.caseId!==undefined?{caseId:value.caseId}:{})};
}
function pending(row:any,status:string){
  return {version:1,authority:'review_only',status,requestId:row.id,sourceRevision:row.source_revision,
    requestUpdatedAt:row.request_updated_at,totalReceipts:row.receipt_count,
    detailRequest:{kind:'planning_request',requestId:row.id,offset:0},semantics};
}

/** Batch discovery is bounded by the current directory page. Event scans use
 * the goal index and each accepted planner's persisted start, never global JSON
 * scans. Unknown evidence remains visible; this function never applies work. */
export async function readPlanningActionReviews(db:DB,goalId:string,requestIds:string[]){
  if(!requestIds.length)return new Map<string,any>();
  if(requestIds.length>15)throw Error('PLANNING_ACTION_REVIEW_PAGE_LIMIT');
  const rows=await authority(db,goalId,requestIds),eligible=rows.filter(row=>authorized(row)&&row.receipt_count>0&&row.receipt_count<=maximumActions);
  const keys=eligible.flatMap(row=>row.action_receipts.map(receipt).filter(Boolean).map((r:any)=>r.actionKey));
  const events=eligible.length&&keys.length?(await db.query(`WITH wanted AS (
      SELECT * FROM unnest($2::text[],$3::text[],$4::text[],$5::text[],$6::timestamptz[])
      AS w(request_id,run_id,artifact_id,artifact_digest,started_at))
    SELECT w.request_id,e.id::text event_id,e.created_at::text event_at,e.payload
    FROM wanted w CROSS JOIN LATERAL(SELECT id,created_at,payload FROM events
      WHERE entity_id=$1::text AND kind='SCOPED_DELIVERY_ACTION_RECORDED' AND created_at>=w.started_at
        AND payload->>'requestId'=w.request_id AND payload->>'planRunId'=w.run_id
        AND payload->>'planArtifactId'=w.artifact_id AND payload->>'planDigest'=w.artifact_digest
        AND payload->>'actionKey'=ANY($7::text[]) ORDER BY id DESC LIMIT 13)e`,
    [goalId,eligible.map(r=>r.id),eligible.map(r=>r.run_id),eligible.map(r=>r.artifact_id),
      eligible.map(r=>r.artifact_digest),eligible.map(r=>r.run_created_at),keys])).rows:[];
  const handoffIds=[...new Set(eligible.flatMap(row=>row.action_receipts.map((r:any)=>id(r?.handoffId)).filter(Boolean)))];
  const handoffs=handoffIds.length?(await db.query(`SELECT h.id,h.goal_id,h.producer_task_id,h.consumer_task_id,h.state,
      h.created_at::text created_at,h.applied_at::text applied_at,
      producer.goal_id producer_goal,consumer.goal_id consumer_goal
    FROM deferred_handoffs h LEFT JOIN tasks producer ON producer.id=h.producer_task_id
    LEFT JOIN tasks consumer ON consumer.id=h.consumer_task_id WHERE h.id=ANY($1::uuid[]) AND h.goal_id=$2`,[handoffIds,goalId])).rows:[];
  const after=new Map((await authority(db,goalId,requestIds)).map(row=>[row.id,row]));
  const result=new Map<string,any>();
  for(const row of rows){
    if(!row.receipt_count&&row.receipt_code!=='DELIVERY_ACTION_REVIEW_REQUIRED')continue;
    let status=!authorized(row)?'SOURCE_UNVERIFIED':!row.receipt_count?'RECEIPT_UNAVAILABLE':row.receipt_count>maximumActions?'RECEIPT_LIMIT_EXCEEDED':'BOUND_EVENT_METADATA';
    if(!after.has(row.id)||digest(row)!==digest(after.get(row.id)))status='STALE_SOURCE';
    if(status!=='BOUND_EVENT_METADATA'){
      result.set(row.id,{basis:row,value:pending(row,status)});continue;
    }
    const actions=row.action_receipts.map((raw:any,index:number)=>{
      const normalized=receipt(raw),matches=normalized?events.filter(e=>e.request_id===row.id&&e.payload.actionKey===normalized.actionKey):[];
      const found=matches.length===1?matches[0]:undefined,parsed=scopedPlanActionSchema.safeParse(found?.payload?.action);
      const bound=normalized&&found&&parsed.success&&
        normalized.actionKey==='scoped-action:'+digest([row.id,row.run_id,parsed.data])&&
        digest(normalized)===digest(receipt(found.payload.receipt));
      if(!bound||!parsed.success||!normalized||!found)return {index,status:'EVENT_BINDING_UNVERIFIED',code:code(raw?.code)};
      const action=parsed.data;
      const stored=normalized.handoffId?handoffs.find(h=>h.id===normalized.handoffId&&h.producer_task_id===action.sourceTaskId&&
        h.producer_goal===goalId&&h.consumer_goal===goalId):undefined;
      const reservation=normalized.handoffId?stored?{status:'OBSERVED',handoffId:stored.id,producerTaskId:stored.producer_task_id,
        reservedConsumerTaskId:stored.consumer_task_id,state:code(stored.state?.toUpperCase()),createdAt:stored.created_at,appliedAt:stored.applied_at,
        conflictsWithRequestedConsumer:stored.consumer_task_id!==action.taskId}:{status:'BINDING_UNVERIFIED'}:{status:'NOT_REFERENCED'};
      return {index,status:'BOUND_EVENT_METADATA',kind:action.kind,producerTaskId:action.sourceTaskId,
        requestedConsumerTaskId:action.taskId,expectedSourceRunId:action.expectedSourceRunId,
        expectedConsumerAttempt:action.expectedAttempt,actionKey:normalized.actionKey,code:normalized.code,
        receiptApplied:normalized.applied,preparing:normalized.preparing,...(normalized.caseId?{caseId:normalized.caseId}:{}),
        eventId:found.event_id,eventAt:found.event_at,reservation};
    });
    const unknown=actions.filter((a:any)=>a.status!=='BOUND_EVENT_METADATA').length;
    const value={...pending(row,unknown?'INCOMPLETE_EVENT_BINDING':'BOUND_EVENT_METADATA'),
      planner:{taskId:row.task_id,runId:row.run_id,attempt:row.attempt_count,runFence:row.fence,contextVersion:row.context_id,
        artifactId:row.artifact_id,artifactDigest:row.artifact_digest,acceptedAt:row.accepted_at},
      counts:{total:row.receipt_count,receiptApplied:actions.filter((a:any)=>a.receiptApplied===true).length,
        refused:actions.filter((a:any)=>a.receiptApplied===false&&!a.preparing).length,
        preparing:actions.filter((a:any)=>a.preparing===true).length,unknown},actions};
    result.set(row.id,{basis:row,value:{...value,bindingDigest:digest(value)}});
  }
  return result;
}

/** The detailed reader verifies the accepted bytes without re-running the new
 * validator on a historical plan. Its old acceptance remains historical. */
export async function readPlanningActionReviewDetail(db:DB,goalId:string,requestId:string,read:ArtifactReader){
  const review=(await readPlanningActionReviews(db,goalId,[requestId])).get(requestId);
  if(!review)return null;
  const row=review.basis;
  if(!['BOUND_EVENT_METADATA','INCOMPLETE_EVENT_BINDING'].includes(review.value.status))return review;
  let verified=false;
  try{
    if(row.artifact_size>8*1024*1024)throw Error('TOO_LARGE');
    const {bytes}=await read(row.artifact_id);
    if(bytes.length!==Number(row.artifact_size)||bytesDigest(bytes)!==row.artifact_digest)throw Error('INTEGRITY_FAILED');
    const plan=scopedPlanSchema.parse(JSON.parse(bytes.toString('utf8')));
    if(plan.requestId!==row.id||plan.sourceRevision!==row.source_revision)throw Error('BINDING_CHANGED');
    verified=plan.actions.length===row.receipt_count&&new Set(review.value.actions.map((a:any)=>a.actionKey)).size===plan.actions.length&&
      review.value.actions.every((a:any)=>a.status==='BOUND_EVENT_METADATA'&&
      plan.actions.some(action=>'scoped-action:'+digest([row.id,row.run_id,action])===a.actionKey));
  }catch{/* A missing/corrupt source is evidence unavailable, not authorization. */}
  const current=(await authority(db,goalId,[requestId]))[0];
  if(!current||digest(current)!==digest(row))return {basis:row,value:pending(row,'STALE_SOURCE')};
  if(!verified)return {basis:row,value:{...pending(row,'ARTIFACT_BINDING_UNVERIFIED'),bindingDigest:review.value.bindingDigest}};
  const value={...review.value,status:'VERIFIED_ACCEPTED_ARTIFACT'};
  return {basis:row,value:{...value,bindingDigest:digest(value)}};
}

export async function samePlanningActionReviewSource(db:DB,goalId:string,review:any){
  const row=(await authority(db,goalId,[review.basis.id]))[0];
  return !!row&&digest(row)===digest(review.basis);
}
