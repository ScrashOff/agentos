import {createHash} from 'node:crypto';
import type {DB} from './db.js';
import {scopedPlanSchema,type ScopedPlan} from './scoped-plan-contract.js';
import {taskExecutionMetadataVersion} from './task-execution-metadata.js';

type Blocker=ScopedPlan['blockers'][number];
const digest=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const live=['queued','running','cancelling','reconciling'];

/** Observation only. An accepted prerequisite resolves its completion wait,
 * never the consumer's criterion, patch consumption or permission to retry. */
export async function reconcileNoWorkBlockers(db:DB,goalId:string,scope:string,blockers:Blocker[]){
  const ids=[...new Set(blockers.flatMap(b=>[b.taskId,...b.blockedByTaskIds]))].sort();
  const rows=(await db.query(`SELECT t.id,t.state,t.stage,t.attempt_count,t.criteria,
    m.base_workspace,r.id run_id,r.state run_state,r.attempt run_attempt,
    a.passed acceptance_passed,
    EXISTS(SELECT 1 FROM runs x WHERE x.task_id=t.id AND x.state=ANY($3::text[])) has_live_run,
    COALESCE((SELECT jsonb_agg(jsonb_build_object('taskId',p.id,'state',p.state) ORDER BY p.id)
      FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=t.id),'[]') dependencies
    FROM tasks t JOIN task_execution_metadata m ON m.task_id=t.id AND m.goal_id=t.goal_id
      AND m.version=${taskExecutionMetadataVersion}
    LEFT JOIN LATERAL(SELECT id,state,attempt FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    LEFT JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
    WHERE t.goal_id=$1 AND t.id=ANY($2::uuid[]) ORDER BY t.id`,[goalId,ids,live])).rows;
  const byId=new Map(rows.map(row=>[row.id,row]));
  const entries=blockers.map(blocker=>{
    const consumer=byId.get(blocker.taskId);
    const criterionPresent=consumer?.base_workspace===scope &&
      consumer.criteria?.some((criterion:any)=>criterion.id===blocker.criterionId)===true;
    const prerequisites=blocker.blockedByTaskIds.map(taskId=>{
      const task=byId.get(taskId),bound=task?.base_workspace===scope;
      const accepted=bound && task.state==='accepted' && task.acceptance_passed===true &&
        task.run_state==='submitted' && task.run_attempt===task.attempt_count && !task.has_live_run;
      return {taskId,state:bound?task.state:'unavailable',completion:accepted?'accepted':
        !bound||['accepted','cancelled','superseded'].includes(task.state)?'requires_review':'pending',
        ...(bound?{runId:task.run_id,attempt:task.attempt_count}:{}),
        evidenceContentRead:false};
    });
    const acceptedTaskIds=prerequisites.filter(p=>p.completion==='accepted').map(p=>p.taskId);
    const pendingTaskIds=prerequisites.filter(p=>p.completion==='pending').map(p=>p.taskId);
    const reviewTaskIds=prerequisites.filter(p=>p.completion==='requires_review').map(p=>p.taskId);
    const code=!criterionPresent?'BLOCKER_BINDING_CHANGED':reviewTaskIds.length?'BLOCKER_SOURCE_REVIEW_REQUIRED':
      pendingTaskIds.length?'BLOCKER_PREREQUISITES_PENDING':acceptedTaskIds.length?
        'BLOCKER_COMPLETION_OBSERVED_CONSUMPTION_UNVERIFIED':'BLOCKER_EVIDENCE_REVIEW_REQUIRED';
    return {taskId:blocker.taskId,criterionId:blocker.criterionId,code,prerequisites,
      acceptedTaskIds,pendingTaskIds,reviewTaskIds,artifactIds:[...blocker.artifactIds],
      consumerState:consumer?.state??'unavailable',
      currentDependencyTaskIds:(consumer?.dependencies??[]).map((d:any)=>d.taskId),
      pendingDependencyTaskIds:(consumer?.dependencies??[]).filter((d:any)=>d.state!=='accepted').map((d:any)=>d.taskId),
      consumerReadyClaimed:false,criterionAccepted:false,
      nextAction:acceptedTaskIds.length?'Read the accepted evidence and reconcile its existing consumer; completion does not prove consumption.':
        'Resolve the exact current prerequisites or inspect the referenced evidence; preserve the original criterion.'};
  });
  return {version:1,authority:'observation_only',materiality:digest(entries),entries,
    consumerReadyClaimed:false,criterionAccepted:false};
}

/** This key only suppresses non-semantic handoff bookkeeping after no_work.
 * It never replaces source_revision or its admission fence. New accepted
 * evidence, task/criterion/workspace/dependency changes remain material. */
export function planningDecisionMateriality(snapshot:any,signalKey:string|null=null){
  const tasks=(snapshot.tasks??[]).filter((t:any)=>t.execution?.workKind!=='planning');
  const accepted=new Set(tasks.filter((t:any)=>t.state==='accepted').map((t:any)=>t.id));
  const taskFacts=tasks.map((t:any)=>[t.id,['accepted','cancelled','superseded'].includes(t.state)?t.state:'open',
    t.stage,t.execution?.workKind??null,t.execution?.workspace,t.intent??null,t.criteria,t.covers,
    [...(t.dependencies??[])].sort(),t.state==='accepted'?
      [t.attempt_count,t.last_run_id,t.acceptance_passed??null]:null]).sort((a:any,b:any)=>String(a[0]).localeCompare(String(b[0])));
  const artifacts=(snapshot.artifacts??[]).filter((a:any)=>accepted.has(a.task_id))
    .map((a:any)=>[a.task_id,a.name,a.digest]).sort();
  // waiting_result -> staged carries an already-bound result to its consumer;
  // receipt/conflict changes and applied consumption remain material. A new
  // producer/consumer/source relationship is also never coalesced.
  const handoffs=(snapshot.deferredDeliveries??[]).map((h:any)=>[
    h.producer_task_id,h.consumer_task_id,h.source_task_id??null,
    ['waiting_result','staged'].includes(h.state)?'pending':h.state,h.receipt??null,
    h.consumer_proof??null]).sort();
  return digest([1,taskFacts,artifacts,handoffs,signalKey]);
}

export function noWorkDecisionUnchanged(prior:any,snapshot:any,signalKey:string|null){
  if(!evidencedNoWorkDecision(prior)||!Array.isArray(prior.snapshot?.tasks))return false;
  return planningDecisionMateriality(prior.snapshot,prior.snapshot.demandSignal?.key??null)===
    planningDecisionMateriality(snapshot,signalKey);
}

export function evidencedNoWorkDecision(prior:any){
  return prior?.accepted_plan===true && prior.state==='blocked_external' &&
    prior.receipt?.code==='EVIDENCED_NO_WORK' &&
    scopedPlanSchema.shape.blockers.safeParse(prior.receipt?.blockers).success;
}

export function observedNoWorkRevision(prior:any,sourceRevision:string,signalKey:string|null){
  const observed=prior?.receipt?.noWorkDecision;
  return evidencedNoWorkDecision(prior) && observed?.version===1 &&
    observed.sourceRevision===sourceRevision && observed.signalKey===signalKey &&
    /^[a-f0-9]{64}$/.test(observed.decisionDemandKey??'');
}

/** Only the most recent, currently accepted planner can park its decision.
 * Legacy/incomplete receipts do not acquire authority by looking like no_work. */
export async function readLatestPlanningDecision(db:DB,goalId:string,scope:string,options:{catalog?:boolean}={}){
  return (await db.query(`SELECT p.id,p.xmin::text row_version,p.state,${options.catalog===false?'NULL':'p.snapshot'} snapshot,p.receipt,
    (t.state='accepted' AND r.state='submitted' AND p.receipt->>'evaluatedRunId'=r.id::text
      AND m.work_kind='planning' AND m.base_workspace=p.scope AND m.version=${taskExecutionMetadataVersion}
      AND EXISTS(SELECT 1 FROM artifacts a WHERE a.run_id=r.id AND a.name='scope-plan.json')
      AND (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)=true
      AND NOT EXISTS(SELECT 1 FROM runs x WHERE x.task_id=t.id AND x.state=ANY($3::text[]))) accepted_plan
    FROM planning_requests p LEFT JOIN tasks t ON t.id=p.task_id AND t.goal_id=p.goal_id
    LEFT JOIN task_execution_metadata m ON m.task_id=t.id AND m.goal_id=t.goal_id
    LEFT JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    WHERE p.goal_id=$1 AND p.scope=$2 ORDER BY p.created_at DESC,p.id DESC LIMIT 1`,[goalId,scope,live])).rows[0];
}
