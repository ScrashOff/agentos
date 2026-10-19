import type {DB} from './db.js';
import {readManagementDecisionFeedback,type ManagementDecisionFeedback} from './management-decision-feedback.js';
import {readStrategyPlannerOwner} from './planner-strategy-intent.js';
import {checkBacklogCapacityIntent} from './backlog-capacity-intent.js';

const live=['queued','running','cancelling','reconciling'];
const waits=['waiting_quota','waiting_resource','waiting_configuration','retry_scheduled'];
export type ManagementFollowThrough={code:string;satisfied:boolean;owner:string;
  taskId?:string;requestId?:string;caseId?:string;state?:string;scope?:string;obligationIds?:string[];
  criterionIds?:string[];blockedTaskIds?:string[];decisionFeedback?:ManagementDecisionFeedback;nextAction:string};

/** A published scoped plan keeps ownership of its consuming effects. This is
 * an observation only: neither a completed auxiliary nor an attached patch is
 * promoted to final acceptance by the manager. */
async function appliedPlanFollowThrough(db:DB,goalId:string,request:any):Promise<ManagementFollowThrough> {
  const obligations=(await db.query(`SELECT c.id,c.consumer_task_id,c.state,c.receipt->>'code' code,
    t.state consumer_state,EXISTS(SELECT 1 FROM runs r WHERE r.task_id=t.id AND r.state=ANY($3::text[])) live,
    EXISTS(SELECT 1 FROM supervision_cases s WHERE s.task_id=t.id AND s.state='open') open_case,
    EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=t.id
      AND p.state IN('failed','recovery_blocked','blocked_failed','cancelled','superseded')) failed_dependency
    FROM delivery_consumptions c JOIN tasks t ON t.id=c.consumer_task_id
    WHERE c.goal_id=$1 AND c.scope=$2 AND c.state NOT IN('completed','superseded') ORDER BY c.id`,
    [goalId,request.scope,live])).rows;
  const extra={requestId:request.id,state:request.state,scope:request.scope,
    obligationIds:obligations.map(o=>o.id),blockedTaskIds:[...new Set<string>(obligations.map(o=>o.consumer_task_id))]};
  if(obligations.length) {
    const actionable=obligations.some(o=>o.open_case || o.failed_dependency || ['blocked','waiting_supervision'].includes(o.state) ||
      (!o.live && ![...live,...waits,'ready','blocked_dependencies'].includes(o.consumer_state)));
    return {code:actionable?'MANAGEMENT_SCOPE_CONSUMPTION_REQUIRED':'MANAGEMENT_SCOPE_WAIT',
      satisfied:!actionable,owner:actionable?'supervisor':'consumer',...extra,
      nextAction:actionable?'Resolve the existing obligation and its exact source/consumer; keep this scoped plan owner until composition and same-candidate QA are verified.':
        'The identified consumers own the next execution. Keep these obligations attached; a published plan and an active consumer are not delivery acceptance.'};
  }
  const ids=Array.isArray(request.receipt?.backlog?.task_ids)?request.receipt.backlog.task_ids.filter((id:any)=>typeof id==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(id)):[];
  const outputs=ids.length?(await db.query(`SELECT t.id,t.state,
    EXISTS(SELECT 1 FROM runs r WHERE r.task_id=t.id AND r.state=ANY($3::text[])) live,
    EXISTS(SELECT 1 FROM supervision_cases s WHERE s.task_id=t.id AND s.state='open') open_case,
    EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=t.id
      AND p.state IN('failed','recovery_blocked','blocked_failed','cancelled','superseded')) failed_dependency
    FROM tasks t WHERE t.goal_id=$1 AND t.id=ANY($2::uuid[]) ORDER BY t.id`,[goalId,ids,live])).rows:[];
  const pending=outputs.filter(t=>t.state!=='accepted');
  const owned=outputs.length===ids.length && pending.length>0 && pending.every(t=>!t.open_case && !t.failed_dependency &&
    (t.live || [...live,...waits,'ready','blocked_dependencies'].includes(t.state)));
  return {code:owned?'MANAGEMENT_SCOPE_WAIT':'MANAGEMENT_PLAN_EFFECT_REQUIRED',satisfied:owned,
    owner:owned?'consumer':'supervisor',...extra,blockedTaskIds:pending.map(t=>t.id),
    nextAction:owned?'Consume the exact tasks produced by this scoped plan, then bind their consumer and final criteria; publication does not release scope ownership.':
      'Read the plan action receipts and bind a real consumer or final verification. Accepted auxiliaries alone do not complete the scope.'};
}

/** A proposal is not an effect. This dated read-only observation is explanatory;
 * existing transition, admission and supervision owners keep all authority. */
export async function readManagementFollowThrough(db:DB,goalId:string,reviewRunId:string):Promise<ManagementFollowThrough> {
  const backlog=(await db.query('SELECT receipts FROM workforce_backlog_plans WHERE goal_id=$1 AND run_id=$2',[goalId,reviewRunId])).rows[0];
  const transition=(await db.query(`SELECT task_id,expected_attempt,scope,state,proposal,receipt,planning_request_id
    FROM delivery_transitions WHERE goal_id=$1 AND review_run_id=$2`,[goalId,reviewRunId])).rows[0];
  const unresolved=(code:string,nextAction:string,extra:Partial<ManagementFollowThrough>={})=>
    ({code,satisfied:false,owner:'supervisor',nextAction,...extra});
  const pending=(backlog?.receipts??[]).filter((r:any)=>['waiting_capacity','preparing'].includes(r.capacityIntent?.state));
  const needsAttention=(backlog?.receipts??[]).find((r:any)=>r.capacityIntent?.state==='needs_revalidation'||
    (r.status==='rejected'&&r.code!=='PENDING_LIMIT'));
  const closedConsumer=(backlog?.receipts??[]).find((r:any)=>r.status==='rejected'&&r.code==='CONSUMER_CLOSED');
  if(closedConsumer)return unresolved('MANAGEMENT_BACKLOG_CONSUMER_CLOSED',
    'The accepted backlog proposal targeted a closed consumer and created no work for that item. Read operation.decisionFeedback.backlogRejections, preserve the closed task and bind an existing authorized pending consumer or explicitly plan the missing consumer. Do not repeat the same source/consumer pair or count an unrelated tracked task as its remedy.',
    {taskId:closedConsumer.consumerTaskId,state:'needs_revalidation'});
  if(needsAttention&&(pending.length||needsAttention.capacityIntent))return unresolved('MANAGEMENT_BACKLOG_REVALIDATION_REQUIRED',
    'Some items have a durable capacity owner, but a sibling item still requires correction. Read every original receipt and capacityIntent.code; the legitimate wait does not satisfy the rejected work.',
    {taskId:needsAttention.consumerTaskId,state:'needs_revalidation'});
  const sameTrackedConsumer=transition?.state==='tracking'&&pending.every((r:any)=>r.consumerTaskId===transition.task_id)&&
    !!(await db.query(`SELECT 1 FROM tasks t WHERE t.id=$1 AND t.goal_id=$2 AND t.attempt_count=$3
      AND EXISTS(SELECT 1 FROM jsonb_array_elements(t.criteria) criterion WHERE criterion->>'name'=$4)`,
      [transition.task_id,goalId,transition.expected_attempt,transition.proposal?.expectedArtifact])).rowCount;
  if(pending.length&&(!transition||sameTrackedConsumer)){
    for(const receipt of pending){
      if(!(await checkBacklogCapacityIntent(db,goalId,receipt.capacityIntent)).ok)return unresolved('MANAGEMENT_BACKLOG_REVALIDATION_REQUIRED',
        'A persisted wait no longer has a current binding. Inspect the source, consumer and authority before issuing a new decision.',
        {taskId:receipt.consumerTaskId,state:'needs_revalidation'});
    }
    const receipt=pending[0];
    return {code:'MANAGEMENT_BACKLOG_CAPACITY_WAIT',satisfied:true,owner:'scheduler',state:receipt.capacityIntent.state,
      taskId:receipt.consumerTaskId,scope:receipt.capacityIntent.binding.consumer.scope,
      nextAction:'The exact accepted backlog items are durably waiting for shared task capacity. They have not created an execution or accepted a delivery; the scheduler revalidates their sources and consumers before admission.'};
  }
  if(!transition){
    const invalid=backlog?.receipts?.find((r:any)=>r.capacityIntent?.state==='needs_revalidation');
    if(invalid)return unresolved('MANAGEMENT_BACKLOG_REVALIDATION_REQUIRED',
      'Read the original backlog item and its capacityIntent.code, then bind the current source and consumer in a new accepted review. The historical refusal is preserved and reserves no capacity.',{taskId:invalid.consumerTaskId,state:'needs_revalidation'});
    return unresolved('MANAGEMENT_EFFECT_REQUIRED','Bind an authorized task/attempt or a valid scoped plan to the pending criterion.');
  }
  if(transition.state==='strategy_duplicate')return unresolved('MANAGEMENT_STRATEGY_ALREADY_OWNED',
    'The same causal strategy already has a durable owner. Follow canonicalTransitionId from the transition receipt; do not create a renamed duplicate.',{scope:transition.scope});
  if(transition.state==='strategy_revalidation')return unresolved('MANAGEMENT_STRATEGY_REVALIDATION_REQUIRED',
    'Read the exact strategy receipt and current episode, then submit a newly validated decision. The stored intent was not dispatched.',{scope:transition.scope});
  if(transition.state==='awaiting_planner'){
    const owner=await readStrategyPlannerOwner(db,goalId,transition.scope);
    return {code:owner?'MANAGEMENT_STRATEGY_DEFERRED':'MANAGEMENT_STRATEGY_PENDING',satisfied:true,owner:'scheduler',
      scope:transition.scope,...(owner?{requestId:owner.request_id,state:owner.state}:{}),
      nextAction:'The new strategy is durably pending, not consumed or delivered. The scheduler revalidates it after the existing planner and its effects release this scope.'};
  }
  if(transition.planning_request_id) {
    const request=(await db.query('SELECT id,state,task_id,scope,receipt FROM planning_requests WHERE id=$1 AND goal_id=$2',
      [transition.planning_request_id,goalId])).rows[0];
    if(!request)return unresolved('MANAGEMENT_PLAN_MISSING','Reconcile the missing planning request without inventing work.');
    if(['pending','assigned','running','preparing'].includes(request.state))
      return {code:'MANAGEMENT_PLAN_OWNED',satisfied:true,owner:'planner',requestId:request.id,
        state:request.state,nextAction:'Consume this exact plan and its validated actions; the request is not a delivery acceptance.'};
    if(request.receipt?.remaining?.version===1 && request.receipt.remaining.items?.some((item:any)=>item.status==='pending'))
      return unresolved('PLANNING_REMAINDER_REQUIRED','Read the exact refused items in operation.planningDemands and submit only an explicitly bound backlog replacement. Keep the already-created tasks and their consumers unchanged.',
        {requestId:request.id,state:request.state,scope:request.scope});
    if(request.state==='applied')return appliedPlanFollowThrough(db,goalId,request);
    return unresolved('MANAGEMENT_PLAN_EFFECT_REQUIRED','Read the plan receipt and bind its consumer or correct the rejected proposal.',
      {requestId:request.id,state:request.state});
  }
  // A refused strategy may truthfully report a planner already in flight. Read
  // the live owner, not the text of the proposed capacity explanation.
  if(transition.receipt?.code==='PLANNER_ALREADY_ACTIVE' && transition.scope) {
    const request=(await db.query(`SELECT p.id,p.state,p.task_id FROM planning_requests p
      WHERE p.goal_id=$1 AND p.scope=$2 AND p.state IN('pending','assigned','running','preparing')
      ORDER BY p.created_at,p.id LIMIT 1`,[goalId,transition.scope])).rows[0];
    if(request)return {code:'MANAGEMENT_WAIT_PLANNER',satisfied:true,owner:'planner',requestId:request.id,
      state:request.state,nextAction:'Wait for the already-owned scoped plan; do not create another strategy only to replace its identity.'};
  }
  if(transition.state==='rejected' || !transition.task_id)
    return unresolved('MANAGEMENT_TRANSITION_REJECTED','Correct the exact binding/schema reported by the transition receipt; no executable work was created.');
  const task=(await db.query(`SELECT t.id,t.state,t.attempt_count,t.criteria,t.stage,
    EXISTS(SELECT 1 FROM runs r WHERE r.task_id=t.id AND r.state=ANY($3::text[])) live,
    c.id case_id,c.state case_state,c.decision_run_id
    FROM tasks t LEFT JOIN LATERAL(SELECT id,state,decision_run_id FROM supervision_cases
      WHERE task_id=t.id AND state IN('open','decided','delegated','waiting_evidence')
      ORDER BY created_at,id LIMIT 1)c ON true WHERE t.id=$1 AND t.goal_id=$2`,
    [transition.task_id,goalId,live])).rows[0];
  if(!task || ['cancelled','superseded'].includes(task.state))
    return unresolved('MANAGEMENT_TARGET_UNAVAILABLE','Select a current authorized consumer; do not revive cancelled or superseded work.');
  const extra={taskId:task.id,state:task.state,criterionIds:(task.criteria??[]).filter((c:any)=>c.name===transition.proposal.expectedArtifact).map((c:any)=>c.id),
    ...(task.case_id?{caseId:task.case_id}:{})};
  if(!task.criteria?.some((criterion:any)=>criterion.name===transition.proposal.expectedArtifact))
    return unresolved('MANAGEMENT_EXPECTED_ARTIFACT_UNBOUND','Select an exact artifact required by the existing task criteria; tracking an arbitrary report cannot prove this obligation.',extra);
  if(task.live || live.includes(task.state))return {code:'MANAGEMENT_WAIT_ACTOR',satisfied:true,owner:'consumer',...extra,
    nextAction:'Await this actor and its required artifact; keep independent eligible work running and never cancel it for age.'};
  if(task.case_id) {
    if(task.case_state!=='open')return {code:'MANAGEMENT_SUPERVISION_OWNED',satisfied:true,owner:'supervisor',...extra,
      nextAction:'Consume the current case decision/evidence through its existing owner; do not create a duplicate diagnostic.'};
    const feedback=(await readManagementDecisionFeedback(db,goalId,{caseId:task.case_id})).entries[0];
    return unresolved('MANAGEMENT_CASE_ACTION_REQUIRED',feedback?.nextAction??'Resolve this exact open case with a verified source, focal correction or evidence request; tracking it alone does not dispatch work.',
      {...extra,...(feedback?{decisionFeedback:feedback}:{})});
  }
  if(waits.includes(task.state))return {code:'MANAGEMENT_WAIT_CAPACITY',satisfied:true,owner:'scheduler',...extra,
    nextAction:'The existing scheduler owns this qualified wait. A quota or resource wait is not a new functional failure.'};
  if(task.state==='ready')return {code:'MANAGEMENT_DISPATCH_OWNED',satisfied:true,owner:'scheduler',...extra,
    nextAction:'The ready task and exact expected artifact are bound; admission remains deterministic.'};
  if(task.state==='accepted')return unresolved('MANAGEMENT_CONSUMER_REQUIRED',
    'Reuse this accepted artifact in its pending consumer or verification; tracking already-accepted work does not move the delivery.',extra);
  if(task.state==='blocked_dependencies') {
    const blockers=(await db.query(`SELECT p.id,p.state FROM dependencies d JOIN tasks p ON p.id=d.depends_on
      WHERE d.task_id=$1 AND p.state<>'accepted' ORDER BY p.id`,[task.id])).rows;
    const actionable=blockers.some(p=>!['ready','blocked_dependencies',...live,...waits].includes(p.state));
    if(!blockers.length || actionable)return unresolved('MANAGEMENT_DEPENDENCY_ACTION_REQUIRED',
      'Resolve the exact blocked prerequisites or reconcile the stale wait; do not replace them with equivalent diagnostics.',
      {...extra,blockedTaskIds:blockers.map(p=>p.id)});
    return {code:'MANAGEMENT_WAIT_DEPENDENCIES',satisfied:true,owner:'scheduler',...extra,blockedTaskIds:blockers.map(p=>p.id),
      nextAction:'The recorded prerequisites own the next work. Keep this consumer and its criteria intact.'};
  }
  return unresolved('MANAGEMENT_RECOVERY_ACTION_REQUIRED','Bind the current recovery case or an authorized consumer action; a pending transition is not a recovery decision.',extra);
}
