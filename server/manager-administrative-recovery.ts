import type {DB} from './db.js';
import {operatorRecoveryCode,operatorRecoveryDigest} from './operator-recovery-contract.js';
import {hasOperatorOpenCodeRecoveryIntent,operatorOpenCodeRecoveryNextAction} from './operator-opencode-recovery-contract.js';

const hash=/^[a-f0-9]{64}$/;
/** The episode is independent of log/heartbeat/artifact churn. It identifies
 * one existing administrative cause, not permission to resume its execution. */
export function administrativeRecoveryEpisodeDigest(value:any) {
 return operatorRecoveryDigest(['administrative_recovery_review_v1',value.goalId,value.caseId,value.taskId,
  value.runId,value.runFence,value.contextVersion,value.attempt,value.planningRequestId,value.requestId,
  value.requestDigest,value.sourceDigest,value.bridgeReceiptDigest,value.readinessEventId]);
}

function confirmedStop(row:any) {
 const stop=row.checkpoint?.nativeStopReceipt;
 return stop?.runFence===row.fence&&stop.workspace===row.workspace&&hash.test(stop.bridgeReceiptDigest??'')&&
  stop.process?.unit===`agentos-run-${row.id}`&&stop.process.stopped===true&&stop.process.mainPid===0&&
  stop.process.populated===false&&['inactive','failed'].includes(stop.process.activeState)&&
  ['loaded','not-found'].includes(stop.process.loadState)&&
  stop.supervisor?.unit===`agentos-native-supervisor-${row.id}`&&stop.supervisor.mainPid===0&&
  stop.supervisor.pendingJob===false&&stop.supervisor.cgroupEmpty===true&&
  ['inactive','failed'].includes(stop.supervisor.activeState)&&['loaded','not-found'].includes(stop.supervisor.loadState)&&
  ['failed','cancelled'].includes(stop.actorState);
}

/** Metadata-only read. The intent, closure and readiness events are canonical;
 * an error string, operator-supplied evidence or a lone checkpoint is not.
 * This does not acquire a lease, signal a process, close a case or grant retry. */
export async function attachAdministrativeRecoveryReviews(db:DB,goalId:string,cases:any[]) {
 const eligible=cases.filter(c=>c.goal_id===goalId&&c.state==='open'&&c.has_live_run===false&&
  ['failed','recovery_blocked'].includes(c.current_task_state)&&c.current_run_state==='failed'&&
  c.current_error_code===operatorRecoveryCode&&c.source_run_id===c.current_run_id&&
  Number.isSafeInteger(c.current_attempt)&&c.current_attempt>0&&c.current_attempt===c.task_attempt_count&&
  (Array.isArray(c.evidence)?c.evidence:[]).some((e:any)=>e?.kind===operatorRecoveryCode&&e.runtime==='opencode'));
 if(!eligible.length)return cases;
 const byCase=new Map<string,any>(),entries=new Map(eligible.map(c=>[c.id,c]));
 // Batches bound each SQL request without dropping an older valid episode.
 for(let offset=0;offset<eligible.length;offset+=32){
  const rows=(await db.query(`SELECT c.id case_id,c.evidence,r.id,r.task_id,r.fence,r.attempt,r.context_id,r.checkpoint,
   t.execution->>'workspace' workspace,t.execution->>'planningRequestId' planning_request_id,
   t.execution->>'workKind' work_kind,p.source_revision,p.state request_state,p.receipt request_receipt,
   ready.id::text readiness_event_id,ready.created_at::text readiness_at,
   encode(sha256(convert_to(jsonb_build_array(t.description,t.criteria,t.resources,t.allowed_accounts,
    t.required_memories,t.recovery,m.control,t.execution->'planningRequestId',m.input_digests,r.instruction_snapshot)::text,'UTF8')),'hex') contract_digest
   FROM supervision_cases c JOIN tasks t ON t.id=c.task_id AND t.goal_id=c.goal_id
   JOIN goals g ON g.id=t.goal_id AND g.state='active'
   JOIN runs r ON r.id=c.source_run_id AND r.task_id=t.id AND r.attempt=t.attempt_count
   JOIN contexts ctx ON ctx.id=r.context_id AND ctx.run_id=r.id
   JOIN task_execution_metadata m ON m.task_id=t.id
   LEFT JOIN planning_requests p ON p.id=CASE WHEN t.execution->>'planningRequestId' ~
     '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$' THEN (t.execution->>'planningRequestId')::uuid END
    AND p.task_id=t.id AND p.goal_id=t.goal_id
   JOIN LATERAL(SELECT id,created_at FROM events e WHERE e.entity_id=t.id::text
    AND e.kind='OPENCODE_RECOVERY_REVIEW_READY' AND e.payload->>'caseId'=c.id::text
    AND e.payload->>'sourceRunId'=r.id::text
    AND e.payload->>'requestId'=r.checkpoint#>>'{operatorRecoveryStop,request,requestId}'
    AND e.payload->>'requestDigest'=r.checkpoint#>>'{operatorRecoveryStop,requestDigest}'
    ORDER BY e.id LIMIT 1) ready ON true
   WHERE c.goal_id=$1 AND c.id=ANY($2::uuid[]) AND c.state='open'
    AND r.runtime='opencode' AND r.state='failed' AND r.error_code=$3
    AND t.state IN('failed','recovery_blocked') AND t.execution->>'kind'='opencode'
    AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id AND live.state IN('queued','running','cancelling','reconciling'))
    AND NOT EXISTS(SELECT 1 FROM leases l JOIN runs leased ON leased.id=l.run_id WHERE leased.task_id=t.id)
    AND NOT EXISTS(SELECT 1 FROM events e WHERE e.entity_id=t.id::text AND e.kind='CANCEL_REQUESTED' AND e.payload->>'runId'=r.id::text)
    AND EXISTS(SELECT 1 FROM events e WHERE e.entity_id=t.id::text AND e.kind='DETACHED_RUN_STOP_RECONCILED'
     AND e.payload->>'runId'=r.id::text AND e.payload->>'attempt'=r.attempt::text
     AND e.payload->>'cancelled'='false' AND e.payload->'receipt'=r.checkpoint->'nativeStopReceipt')`,
   [goalId,eligible.slice(offset,offset+32).map(c=>c.id),operatorRecoveryCode])).rows;
  for(const row of rows){
   const entry=entries.get(row.case_id),intent=row.checkpoint?.operatorRecoveryStop,request=intent?.request;
   if(!entry||entry.current_run_id!==row.id||entry.current_attempt!==row.attempt||
    entry.current_run_fence!==row.fence||entry.current_context_version!==row.context_id||!confirmedStop(row)||
    !await hasOperatorOpenCodeRecoveryIntent(db,row)||intent.goalId!==goalId||request.contextVersion!==row.context_id||
    request.expectedAttempt!==row.attempt||intent.contractDigest!==row.contract_digest||
    (request.planningRequestId??null)!==(row.planning_request_id??null))continue;
   if(row.work_kind==='planning'&&(!request.planningRequestId||row.request_state!=='awaiting_supervision'||
    row.source_revision!==intent.sourceRevision||row.request_receipt?.operatorRecoverySuccessor))continue;
   const evidence=(Array.isArray(row.evidence)?row.evidence:[]).filter((e:any)=>e?.kind===operatorRecoveryCode&&e.runtime==='opencode');
   if(!evidence.length||evidence.some((e:any)=>e.runId!==row.id||e.requestId!==request.requestId||
    e.administrative!==true||e.stopConfirmed!==true||e.gameFailureClaimed!==false))continue;
   const identity={goalId,caseId:entry.id,taskId:entry.task_id,runId:row.id,runFence:row.fence,
    contextVersion:row.context_id,attempt:row.attempt,planningRequestId:request.planningRequestId,
    requestId:request.requestId,requestDigest:intent.requestDigest,sourceDigest:row.contract_digest,
    bridgeReceiptDigest:row.checkpoint.nativeStopReceipt.bridgeReceiptDigest,readinessEventId:row.readiness_event_id};
   byCase.set(entry.id,{version:1,authority:'review_only',...identity,
    episodeDigest:administrativeRecoveryEpisodeDigest(identity),readyAt:row.readiness_at,
    priorityConsumed:false,lastCompletedReviewWasAdministrative:false,actionValidationRequired:true,
    nextAction:operatorOpenCodeRecoveryNextAction});
  }
 }
 if(!byCase.size)return cases;
 // The pinned writer extracts the entire focus from the accepted run's frozen
 // description. Join its canonical review row and acceptance at that event; a model
 // output or failed/foreign/older review cannot consume this one-turn priority.
 const latest=(await db.query(`SELECT e.id::text event_id,e.payload->'contextRequestFocus' focus
  FROM events e JOIN management_reviews m ON m.run_id=CASE WHEN e.payload->>'runId' ~
   '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$' THEN (e.payload->>'runId')::uuid END AND m.goal_id=$1::uuid
  JOIN runs r ON r.id=m.run_id AND r.state='submitted'
  JOIN tasks t ON t.id=r.task_id AND t.goal_id=$1::uuid AND t.stage='coordinate'
  JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id AND created_at<=e.created_at
   ORDER BY created_at DESC,id DESC LIMIT 1)a ON a.passed
  WHERE e.entity_id=$1::text AND e.kind='MANAGEMENT_REVIEWED'
   AND jsonb_typeof(e.payload->'receipts')='array' AND NOT(e.payload->'receipts' @> '[{"code":"MANAGEMENT_FORMAT"}]'::jsonb)
   AND jsonb_typeof(m.receipts)='array' AND NOT(m.receipts @> '[{"code":"MANAGEMENT_FORMAT"}]'::jsonb)
  ORDER BY e.id DESC LIMIT 1`,[goalId])).rows[0];
 const lastAdministrative=latest?.focus?.selectionReason==='administrative_first_review'&&
  hash.test(latest.focus.administrativeRecoveryEpisodeDigest??'');
 const identities=[...byCase.values()];
 for(let offset=0;offset<identities.length;offset+=32){
  const served=(await db.query(`WITH episodes AS MATERIALIZED(SELECT * FROM jsonb_to_recordset($2::jsonb)
    AS x("caseId" text,"episodeDigest" text,"readyAt" timestamptz)),
   review_events AS MATERIALIZED(SELECT id,payload,created_at FROM events
    WHERE entity_id=$1::text AND kind='MANAGEMENT_REVIEWED' AND created_at>=(SELECT min("readyAt") FROM episodes))
   SELECT ep."caseId" case_id,served.id::text event_id FROM episodes ep JOIN LATERAL(
    SELECT e.id FROM review_events e JOIN management_reviews m ON m.run_id=CASE WHEN e.payload->>'runId' ~
      '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$' THEN (e.payload->>'runId')::uuid END AND m.goal_id=$1::uuid
    JOIN runs r ON r.id=m.run_id AND r.state='submitted' AND r.created_at>ep."readyAt"
    JOIN tasks t ON t.id=r.task_id AND t.goal_id=$1::uuid AND t.stage='coordinate'
    JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id AND created_at<=e.created_at
     ORDER BY created_at DESC,id DESC LIMIT 1)a ON a.passed
    WHERE e.payload#>>'{contextRequestFocus,caseId}'=ep."caseId"
     AND e.payload#>>'{contextRequestFocus,administrativeRecoveryEpisodeDigest}'=ep."episodeDigest"
     AND e.payload#>>'{contextRequestFocus,selectionReason}'='administrative_first_review'
     AND jsonb_typeof(e.payload->'receipts')='array' AND NOT(e.payload->'receipts' @> '[{"code":"MANAGEMENT_FORMAT"}]'::jsonb)
     AND jsonb_typeof(m.receipts)='array' AND NOT(m.receipts @> '[{"code":"MANAGEMENT_FORMAT"}]'::jsonb)
    ORDER BY e.id LIMIT 1)served ON true`,[goalId,JSON.stringify(identities.slice(offset,offset+32)
      .map(({caseId,episodeDigest,readyAt})=>({caseId,episodeDigest,readyAt})))])).rows;
  for(const row of served){const value=byCase.get(row.case_id);value.priorityConsumed=true;value.reviewedEventId=row.event_id;}
 }
 return cases.map(c=>byCase.has(c.id)?{...c,administrativeRecoveryReview:{...byCase.get(c.id),
  lastCompletedReviewWasAdministrative:lastAdministrative}}:c);
}
