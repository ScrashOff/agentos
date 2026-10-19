import {deliveryRepairProofRemedies,deliveryRepairProofNextAction} from './delivery-proof-feedback.js';
import type {DB} from './db.js';
import {deliveryResolutionGenerationGuide} from './delivery-consumption-catalog.js';

// These descriptions are application-owned, never event.reason/model text.
const remedies:Record<string,{cause:string;nextAction:string}>={
  ...deliveryRepairProofRemedies,
  DELIVERY_RECERTIFICATION_PHASE_CHANGED:{cause:'The decision was prepared before the current recertification evidence became available.',
    nextAction:'Retrieve this case again and inspect its latest DELIVERY_RECERTIFICATION_READY evidence, accepted support sources and current obligations. Submit a new explicit source-resolution decision against that phase; do not replay the earlier delegation.'},
  DELIVERY_RECERTIFICATION_SOURCE_SELECTION_REQUIRED:{cause:'Recertification already produced accepted support; another delegation in the same case would repeat that work.',
    nextAction:'Inspect the accepted support sources in DELIVERY_RECERTIFICATION_READY and the current obligation. Select a verified frozen repair with revise, sourceTaskId and deliveryResolution {obligationId,generation,expectedSourceRunId}. A materially different defect requires its own evidence and normal planning path, not a renamed duplicate delegation. Integration and independent QA remain mandatory. If the current DELIVERY_RECERTIFICATION_READY sources are only accepted frozen diagnostics, do not substitute a diagnostic or repeat supervision.delegate. Re-read the same-scope source, writable integrator, DAG and alreadyPlanned. Propose the missing correction through existing coordination.backlog.tasks with workKind:"repair", lane:"glm", sourceTaskId of that diagnostic, deliverToTaskId of the original writable integrator, baseWorkspace of that scope, delivery:"deferred" and one stable coverageKey describing the missing repair. Reuse equivalent existing work; never rename coverage to evade deduplication. This requests isolated repair work, not source replacement or acceptance. After a repair is actually accepted with patch/report/source proof, submit a fresh revise with its exact sourceTaskId and deliveryResolution; composition and independent QA remain required.'},
  SUPERVISION_DELEGATION_IDENTITY_CONFLICT:{cause:'The requested support identity is already occupied and cannot be silently reused or recreated.',
    nextAction:'Retrieve the current case and existing support provenance. Reuse evidence only through explicit verified source resolution, or plan distinct work justified by a new material defect. Do not retry the same delegation or change its title to evade the conflict.'},
  DELIVERY_RESOLUTION_REQUIRED:{cause:'The revision omitted an explicit source binding or used an obligation/generation that does not match this case source conflict.',
    nextAction:'Select an accepted frozen repair via revise/sourceTaskId and deliveryResolution. If unavailable, obtain focal repair through the current phase. Preserve composition and independent QA. '+deliveryResolutionGenerationGuide},
  NEW_EVIDENCE_REQUIRED:{cause:'The proposed revision repeats the same candidate without new discriminating evidence.',
    nextAction:'Use a verified new source or delegate a distinct focal test that discriminates the unresolved hypothesis; renaming the instruction does not release the retry guard.'},
  DELIVERY_RESOLUTION_ACTION_INVALID:{cause:'The source-resolution action and its expected repair run did not agree.',
    nextAction:'Re-read the current obligation and accepted repair; bind the exact sourceTaskId, obligationId, generation and expectedSourceRunId in one authorized revise action.'},
  DELIVERY_RESOLUTION_GENERATION_CHANGED:{cause:'The obligation generation changed before the decision could be applied.',
    nextAction:'Retrieve the current obligation and source evidence before proposing a new decision; do not reuse the rejected generation.'},
  DELIVERY_RESOLUTION_SOURCE_UNPROVEN:{cause:'The replacement source was not an accepted frozen repair in the authorized scope.',
    nextAction:'Choose an accepted repair with current frozen proof in this scope, or obtain focal recertification; do not reinterpret historical acceptance as current proof. If the current DELIVERY_RECERTIFICATION_READY sources are only accepted frozen diagnostics, do not substitute a diagnostic or repeat supervision.delegate. Re-read the same-scope source, writable integrator, DAG and alreadyPlanned. Propose the missing correction through existing coordination.backlog.tasks with workKind:"repair", lane:"glm", sourceTaskId of that diagnostic, deliverToTaskId of the original writable integrator, baseWorkspace of that scope, delivery:"deferred" and one stable coverageKey describing the missing repair. Reuse equivalent existing work; never rename coverage to evade deduplication. This requests isolated repair work, not source replacement or acceptance. After a repair is actually accepted with patch/report/source proof, submit a fresh revise with its exact sourceTaskId and deliveryResolution; composition and independent QA remain required.'},
  DELIVERY_RESOLUTION_ARTIFACTS_INVALID:{cause:'The required replacement artifacts did not prove the proposed repair.',
    nextAction:'This historical generic receipt does not identify which proof check failed. Inspect the exact retained artifacts under the current strict verifier; do not infer a specific missing field. '+deliveryRepairProofNextAction},
  DELIVERY_RESOLUTION_ARTIFACT_CHANGED:{cause:'Replacement artifact bytes no longer matched their registered proof.',
    nextAction:'Reconcile the exact artifact identity and digest through the existing repair workflow; never substitute unverified bytes or reuse the rejected proof.'},
  DELIVERY_RESOLUTION_CONSUMER_CHANGED:{cause:'The consumer contract changed before the source resolution could be applied.',
    nextAction:'Re-read the current consumer contract and obligation, then propose a decision against those exact inputs and criteria.'},
  SUPERVISION_SOURCE_INVALID:{cause:'The proposed source failed the supervision source guard.',
    nextAction:'Inspect the current accepted source, scope, frozen contract and artifacts, then select a verified compatible repair or delegate one.'},
  SUPERVISION_SOURCE_TOO_LARGE:{cause:'The source exceeded the existing supervision input contract.',
    nextAction:'Use the existing artifact contract to obtain a focused accepted repair of admissible size; do not raise limits or truncate proof to bypass the guard.'},
  SUPERVISION_SOURCE_DEPENDS_ON_TARGET:{cause:'The proposed source depends on the target it is meant to unblock.',
    nextAction:'Choose an independent accepted source or delegate an independent focal repair without introducing a dependency cycle.'},
  SUPERVISION_REQUIRES_IMPLEMENTATION_TARGET:{cause:'The proposed repair was not bound to an authorized implementation target.',
    nextAction:'Resolve the existing implementation dependency and its case; keep verification read-only and preserve its acceptance criteria.'},
};
const pageSize=6;
export type ManagementDecisionFeedback={
  eventId:string;eventKind:string;rejectedAt:string;caseId:string;taskId:string;sourceRunId:string;
  sourceAttempt:number;decisionRunId:string;decisionAttempt:number;code:string;cause:string;nextAction:string;
  rejectedBinding?:{obligationId:string;generation:string;expectedSourceRunId:string};
  currentResolutionBindings?:Array<{obligationId:string;generation:string}>;
  currentResolutionBindingsTruncated?:boolean;
};
const uuid=/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;

/** A backlog refusal is not a supervision event. Keep its exact destination
 * visible after the next unrelated review, without reading proposal bodies.
 * The bounded history is explanatory: it never retargets closed work. */
export async function readBacklogDecisionRejections(db:DB,goalId:string) {
  const rows=(await db.query(`WITH recent AS MATERIALIZED (
    SELECT p.run_id,p.receipts,p.created_at FROM workforce_backlog_plans p
    JOIN runs r ON r.id=p.run_id AND r.state='submitted'
    JOIN tasks manager ON manager.id=r.task_id AND manager.goal_id=p.goal_id AND manager.stage='coordinate'
    WHERE p.goal_id=$1 AND (SELECT passed FROM acceptances WHERE run_id=r.id AND task_id=manager.id
      ORDER BY created_at DESC,id DESC LIMIT 1)=true ORDER BY r.created_at DESC,r.id DESC LIMIT 120
  ), rejected AS (
    SELECT p.run_id,p.created_at,receipt->>'sourceTaskId' source_task_id,t.id consumer_task_id,t.state consumer_state,
      t.attempt_count consumer_attempt,receipt->>'index' item_index,
      count(*) OVER(PARTITION BY t.id,receipt->>'sourceTaskId')::int occurrences,
      row_number() OVER(PARTITION BY t.id,receipt->>'sourceTaskId' ORDER BY p.created_at DESC,p.run_id DESC) position
    FROM recent p CROSS JOIN LATERAL jsonb_array_elements(p.receipts)receipt
    JOIN tasks t ON t.id=CASE WHEN receipt->>'consumerTaskId' ~* '^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
      THEN (receipt->>'consumerTaskId')::uuid END AND t.goal_id=$1
    WHERE receipt->>'code'='CONSUMER_CLOSED' AND receipt->>'status'='rejected'
      AND t.state IN('accepted','cancelled','superseded')
  ) SELECT * FROM rejected WHERE position=1 ORDER BY created_at DESC,run_id DESC LIMIT 7`,[goalId])).rows;
  return {entries:rows.slice(0,6).map(row=>({code:'CONSUMER_CLOSED',reviewRunId:row.run_id,
    rejectedAt:new Date(row.created_at).toISOString(),
    ...(uuid.test(row.source_task_id??'')?{sourceTaskId:row.source_task_id}:{}),
    consumerTaskId:row.consumer_task_id,currentConsumerState:row.consumer_state,currentConsumerAttempt:row.consumer_attempt,
    ...(/^\d{1,3}$/.test(row.item_index??'')?{itemIndex:Number(row.item_index)}:{}),
    occurrencesInHistoryWindow:row.occurrences,
    nextAction:'This destination remains closed. Preserve its accepted/cancelled/superseded outcome; do not repeat the same source/consumer pair. Identify an existing authorized pending consumer using current criteria and dependencies, or explicitly plan the missing consumer through the existing planner. No replacement is chosen automatically.'})),
    truncated:rows.length>6,historyPlanLimit:120,
    guide:'Actual rejected backlog receipts from accepted coordination runs. Counts cover only the bounded history window. A closed destination is not reopened by changing a title, coverage key or tracking another task; select and validate the intended pending consumer explicitly.'};
}

/** Explanatory feedback for an unresolved episode, not an authorization. Read
 * only metadata: no event.reason/decision, frozen prompt, or artifact bytes.
 * Prefer the newest rejected review, then its latest event: recording an old
 * review later must not hide current feedback. An unknown review uses receipt
 * time and fails closed until a genuinely newer review supersedes it.
 * Any later applied effect supersedes the receipt, including reopened cases. */
export async function readManagementDecisionFeedback(db:DB,goalId:string,
  selection:{caseId?:string;focusCaseId?:string}={}) {
  const rows=(await db.query(`SELECT c.id case_id,c.task_id,c.source_run_id,s.attempt source_attempt,
    e.id event_id,e.kind event_kind,e.created_at rejected_at,e.code,e.decision_run_id,
    e.obligation_id,e.generation,e.expected_source_run_id,r.attempt decision_attempt,
    (SELECT jsonb_agg(v.binding ORDER BY v.obligation_id,v.generation) FROM (
      SELECT DISTINCT x->>'obligationId' obligation_id,x#>>'{sourceConflict,generation}' generation,
       jsonb_build_object('obligationId',x->>'obligationId','generation',x#>>'{sourceConflict,generation}') binding
      FROM jsonb_array_elements(c.evidence)x JOIN delivery_consumptions d ON d.id=CASE
       WHEN x->>'obligationId' ~ '^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$' THEN (x->>'obligationId')::uuid END
       AND d.goal_id=c.goal_id AND d.consumer_task_id=c.task_id AND d.state NOT IN('completed','superseded')
       AND d.receipt#>>'{sourceConflict,generation}'=x#>>'{sourceConflict,generation}'
      WHERE x->>'kind'='DELIVERY_SOURCE_REVIEW_REQUIRED'
      AND x->>'obligationId' ~ '^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
      AND x#>>'{sourceConflict,generation}' ~ '^[a-f0-9]{64}$'
      ORDER BY obligation_id,generation LIMIT 17)v) current_resolution_bindings,
    count(*) OVER()::int total
    FROM supervision_cases c JOIN tasks t ON t.id=c.task_id AND t.goal_id=c.goal_id
    JOIN runs s ON s.id=c.source_run_id AND s.task_id=t.id AND s.attempt=t.attempt_count
    CROSS JOIN LATERAL(SELECT id FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1) latest
    CROSS JOIN LATERAL(SELECT x.id,x.kind,x.created_at,x.payload->>'code' code,
      CASE WHEN x.kind='SUPERVISION_PLAN_REJECTED' THEN x.payload->>'runId' ELSE x.payload->>'decisionRunId' END decision_run_id,
      x.payload->>'obligationId' obligation_id,x.payload->>'generation' generation,
      x.payload->>'expectedSourceRunId' expected_source_run_id
      FROM events x LEFT JOIN runs rejected_review ON rejected_review.id=CASE
        WHEN (CASE WHEN x.kind='SUPERVISION_PLAN_REJECTED' THEN x.payload->>'runId'
          ELSE x.payload->>'decisionRunId' END) ~* '^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
        THEN (CASE WHEN x.kind='SUPERVISION_PLAN_REJECTED' THEN x.payload->>'runId'
          ELSE x.payload->>'decisionRunId' END)::uuid END
      WHERE x.entity_id=c.task_id::text AND x.payload->>'caseId'=c.id::text
      AND x.kind IN('SUPERVISION_PLAN_REJECTED','DELIVERY_SOURCE_DECISION_REJECTED')
      AND x.created_at>=c.created_at
      ORDER BY COALESCE(rejected_review.created_at,x.created_at) DESC,x.id DESC LIMIT 1)e
    JOIN runs r ON r.id=CASE WHEN e.decision_run_id ~* '^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
      THEN e.decision_run_id::uuid END AND r.state='submitted'
    JOIN tasks manager ON manager.id=r.task_id AND manager.goal_id=c.goal_id AND manager.stage='coordinate'
    CROSS JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id AND task_id=manager.id
      ORDER BY created_at DESC,id DESC LIMIT 1) accepted
    WHERE c.goal_id=$1 AND c.state='open' AND c.decision_run_id IS NULL
      AND ($2::uuid IS NULL OR c.id=$2) AND latest.id=s.id
      AND t.state NOT IN('accepted','cancelled','superseded') AND accepted.passed
      AND e.code=ANY($4::text[])
      AND NOT EXISTS(SELECT 1 FROM events effect WHERE effect.entity_id=c.task_id::text
        AND effect.payload->>'caseId'=c.id::text AND effect.id>e.id
        AND effect.kind IN('SUPERVISION_APPLIED','SUPERVISION_DELEGATED','SUPERVISION_WAITING_EVIDENCE',
          'SUPERVISION_RESUMED','DELIVERY_RECERTIFICATION_READY'))
    ORDER BY (c.id=$3::uuid) DESC NULLS LAST,e.id DESC,c.id LIMIT $5`,
    [goalId,selection.caseId??null,selection.focusCaseId??null,Object.keys(remedies),pageSize])).rows;
  const entries:ManagementDecisionFeedback[]=rows.map(row=>({
    eventId:String(row.event_id),eventKind:row.event_kind,rejectedAt:new Date(row.rejected_at).toISOString(),
    caseId:row.case_id,taskId:row.task_id,sourceRunId:row.source_run_id,sourceAttempt:row.source_attempt,
    decisionRunId:row.decision_run_id,decisionAttempt:row.decision_attempt,code:row.code,...remedies[row.code],
    ...(row.code.startsWith('DELIVERY_RESOLUTION_')?{currentResolutionBindings:(row.current_resolution_bindings??[]).slice(0,16),
      currentResolutionBindingsTruncated:(row.current_resolution_bindings??[]).length>16}:{}),
    ...(uuid.test(row.obligation_id??'') && /^[a-f0-9]{64}$/i.test(row.generation??'') && uuid.test(row.expected_source_run_id??'')
      ?{rejectedBinding:{obligationId:row.obligation_id,generation:row.generation,expectedSourceRunId:row.expected_source_run_id}}:{}),
  }));
  return {version:1,entries,total:rows[0]?.total??0,truncated:(rows[0]?.total??0)>entries.length,
    backlogRejections:await readBacklogDecisionRejections(db,goalId),
    guide:'These receipts describe the identified rejected decision at rejectedAt, not a new permission or a claim that its source generation is still current. Re-read the current case, obligation and source before acting. A rejected coordination action is not remedied by merely tracking the same task. Only current open episodes are shown; other cases and complete historical receipts remain persisted.'};
}
