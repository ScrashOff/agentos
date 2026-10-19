import {createHash} from 'node:crypto';
import type {DB} from './db.js';
import {expandSharedManagerContext} from './manager-context-sharing.js';
import {readManagementDecisionFeedback} from './management-decision-feedback.js';
import {z} from 'zod';
import {taskExecutionMetadataVersion} from './task-execution-metadata.js';

export const deliveryTransitionSchema=z.object({
  cause:z.string().min(10).max(2000),hypothesis:z.string().min(10).max(2000),
  expectedArtifact:z.string().min(5).max(1200),taskId:z.uuid().optional(),scope:z.string().max(240).optional(),
  mode:z.enum(['task','plan']).optional(),expectedAttempt:z.number().int().min(0).optional(),
  sourceRevision:z.string().regex(/^[a-f0-9]{64}$/).optional(),
  strategyKey:z.string().regex(/^[a-z][a-z_-]{9,99}$/).optional(),
});

const uuid=/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
const hash=/^[a-f0-9]{64}$/;
const sha=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const live=['queued','running','cancelling','reconciling'];
const reserved=['pending','assigned','running','preparing','replanning_required'];
export type StrategyIntent={version:1;proposalDigest:string;bodyDigest:string;basisDigest:string;
  binding:{caseId:string;taskId:string;sourceRunId:string;expectedAttempt:number;scope:string;
    generations:Array<{obligationId:string;generation:string}>;feedback:unknown};
  blockedByRequestId?:string;blockedByRunId?:string;originalReceipt?:unknown};

/** This digest records bytes/identity, not a claim of semantic equivalence. */
export function strategyProposalDigests(p:any){
  const body=[p.mode,p.scope,p.sourceRevision,p.cause,p.hypothesis,p.expectedArtifact,
    p.taskId??null,p.expectedAttempt??null];
  return {proposalDigest:sha([...body,p.strategyKey]),bodyDigest:sha(body)};
}
function feedbackBasis(entry:any){
  return entry?{caseId:entry.caseId,taskId:entry.taskId,sourceRunId:entry.sourceRunId,
    sourceAttempt:entry.sourceAttempt,code:entry.code,rejectedBinding:entry.rejectedBinding??null,
    currentResolutionBindings:[...(entry.currentResolutionBindings??[])].sort((a,b)=>a.obligationId.localeCompare(b.obligationId))}:null;
}
/** One owner includes uncertain actors and preparation/retry, not heartbeat age. */
export async function readStrategyPlannerOwner(db:DB,goalId:string,scope:string){
  return (await db.query(`SELECT p.id request_id,p.state,p.task_id,r.id run_id,r.state run_state
    FROM planning_requests p LEFT JOIN LATERAL(SELECT id,state FROM runs WHERE task_id=p.task_id
      AND state=ANY($3::text[]) ORDER BY attempt DESC LIMIT 1)r ON true
    WHERE p.goal_id=$1 AND p.scope=$2 AND (p.state=ANY($4::text[]) OR r.id IS NOT NULL)
    ORDER BY (r.id IS NOT NULL) DESC,p.created_at,p.id LIMIT 1`,[goalId,scope,live,reserved])).rows[0]??null;
}

/** A deferred semantic instruction must be tied to an accepted frozen episode.
 * New review IDs, repeated rejection events and clock ticks are not new causes.
 * This reader never changes source authority or substitutes current context. */
export async function captureStrategyIntent(db:DB,goalId:string,reviewRunId:string,p:any):Promise<
  {ok:true;intent:StrategyIntent}|{ok:false;code:string}> {
  const row=(await db.query(`SELECT CASE WHEN octet_length(r.instruction_snapshot#>>'{task,description}')<=300000
      THEN r.instruction_snapshot#>>'{task,description}' END description
    FROM runs r JOIN tasks t ON t.id=r.task_id AND t.goal_id=$2 AND t.stage='coordinate'
    JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id AND task_id=t.id
      ORDER BY created_at DESC,id DESC LIMIT 1)a ON a.passed
    WHERE r.id=$1 AND r.state='submitted'`,[reviewRunId,goalId])).rows[0];
  if(!row)return {ok:false,code:'STRATEGY_AUTHORITY_CHANGED'};
  let snapshot:any;
  try{
    const offset=typeof row.description==='string'?row.description.lastIndexOf('\n{"cases":'):-1;
    if(offset<0)return {ok:false,code:'STRATEGY_CONTEXT_REQUIRED'};
    snapshot=expandSharedManagerContext(JSON.parse(row.description.slice(offset+1)));
  }catch{return {ok:false,code:'STRATEGY_CONTEXT_INVALID'};}
  const focus=snapshot.operation?.focus;
  if(!uuid.test(focus?.caseId??'')||!uuid.test(focus?.taskId??'')||focus.scope!==p.scope)
    return {ok:false,code:'STRATEGY_FOCUS_REQUIRED'};
  const entry=snapshot.cases?.find((c:any)=>c.id===focus.caseId&&c.taskId===focus.taskId&&c.scope===p.scope);
  if(!entry||!uuid.test(entry.currentRun?.id??'')||!Number.isSafeInteger(entry.currentRun?.attempt))
    return {ok:false,code:'STRATEGY_EPISODE_REQUIRED'};
  const current=(await db.query(`SELECT c.id,c.task_id,c.source_run_id,c.state,c.decision_run_id,
      t.attempt_count,t.state task_state,m.base_workspace,r.id current_run_id
    FROM supervision_cases c JOIN tasks t ON t.id=c.task_id AND t.goal_id=c.goal_id
    JOIN task_execution_metadata m ON m.task_id=t.id AND m.goal_id=t.goal_id AND m.version=$3
    JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    WHERE c.id=$1 AND c.goal_id=$2`,[focus.caseId,goalId,taskExecutionMetadataVersion])).rows[0];
  if(!current||current.state!=='open'||current.decision_run_id||current.task_id!==focus.taskId||
    current.base_workspace!==p.scope||current.source_run_id!==entry.currentRun.id||
    current.current_run_id!==entry.currentRun.id||current.attempt_count!==entry.currentRun.attempt||
    ['accepted','cancelled','superseded'].includes(current.task_state))return {ok:false,code:'STRATEGY_EPISODE_CHANGED'};
  const generations:Array<{obligationId:string;generation:string}>=[];
  if(!Array.isArray(entry.evidence))return {ok:false,code:'STRATEGY_EVIDENCE_REQUIRED'};
  for(const evidence of entry.evidence){
    if(evidence?.kind!=='DELIVERY_SOURCE_REVIEW_REQUIRED')continue;
    if(!uuid.test(evidence.obligationId??'')||!hash.test(evidence.sourceConflict?.generation??''))
      return {ok:false,code:'STRATEGY_GENERATION_REQUIRED'};
    const binding={obligationId:evidence.obligationId,generation:evidence.sourceConflict.generation};
    if(!generations.some(x=>x.obligationId===binding.obligationId&&x.generation===binding.generation))generations.push(binding);
  }
  if(generations.length>16)return {ok:false,code:'STRATEGY_GENERATION_BOUND'};
  generations.sort((a,b)=>a.obligationId.localeCompare(b.obligationId)||a.generation.localeCompare(b.generation));
  for(const binding of generations){
    const present=(await db.query(`SELECT 1 FROM delivery_consumptions WHERE id=$1 AND goal_id=$2
      AND consumer_task_id=$3 AND state NOT IN('completed','superseded')
      AND receipt#>>'{sourceConflict,generation}'=$4`,[binding.obligationId,goalId,focus.taskId,binding.generation])).rowCount;
    if(!present)return {ok:false,code:'STRATEGY_GENERATION_CHANGED'};
  }
  const currentGenerations=(await db.query(`SELECT DISTINCT x->>'obligationId' "obligationId",
      x#>>'{sourceConflict,generation}' generation FROM supervision_cases c,
      LATERAL jsonb_array_elements(c.evidence)x WHERE c.id=$1 AND c.goal_id=$2
      AND x->>'kind'='DELIVERY_SOURCE_REVIEW_REQUIRED' ORDER BY 1,2 LIMIT 17`,[focus.caseId,goalId])).rows;
  if(sha(currentGenerations)!==sha(generations))return {ok:false,code:'STRATEGY_GENERATION_CHANGED'};
  const oldFeedback=snapshot.operation?.decisionFeedback?.entries?.find((f:any)=>f.caseId===focus.caseId);
  if(oldFeedback?.currentResolutionBindingsTruncated)return {ok:false,code:'STRATEGY_GENERATION_BOUND'};
  const newFeedback=(await readManagementDecisionFeedback(db,goalId,{caseId:focus.caseId})).entries[0];
  const feedback=feedbackBasis(oldFeedback);
  if(sha(feedback)!==sha(feedbackBasis(newFeedback)))return {ok:false,code:'STRATEGY_FEEDBACK_CHANGED'};
  const binding={caseId:focus.caseId,taskId:focus.taskId,sourceRunId:entry.currentRun.id,
    expectedAttempt:entry.currentRun.attempt,scope:p.scope,generations,feedback};
  return {ok:true,intent:{version:1,...strategyProposalDigests(p),basisDigest:sha([goalId,p.sourceRevision,binding]),binding}};
}

/** Alias reviews keep their own immutable proposal but cannot acquire a second
 * successor for the same episode. Unknown context is a supervisor concern. */
export async function prepareDeferredStrategy(db:DB,goalId:string,reviewRunId:string,p:any,
  options:{excludeTransitionId?:string;originalReceipt?:unknown}={}){
  const captured=await captureStrategyIntent(db,goalId,reviewRunId,p);
  if(!captured.ok)return {state:'strategy_revalidation',receipt:{code:captured.code,
    ...(options.originalReceipt?{originalReceipt:options.originalReceipt}:{}),nextAction:'Revalidate this strategy against the current exact episode; no new planner was created.'}};
  const intent=captured.intent;
  const duplicate=(await db.query(`SELECT id,planning_request_id,receipt,state,strategy_key FROM delivery_transitions
    WHERE goal_id=$1 AND scope=$2 AND source_revision=$3 AND id IS DISTINCT FROM $4::uuid
      AND receipt#>>'{intent,version}'='1' AND receipt#>>'{intent,basisDigest}'=$5
      AND (strategy_key=$6 OR receipt#>>'{intent,bodyDigest}'=$7)
    ORDER BY created_at,id LIMIT 1`,[goalId,p.scope,p.sourceRevision,options.excludeTransitionId??null,intent.basisDigest,
      p.strategyKey,intent.bodyDigest])).rows[0];
  if(duplicate)return {state:duplicate.strategy_key===p.strategyKey&&duplicate.receipt.intent.bodyDigest!==intent.bodyDigest
      ?'strategy_revalidation':'strategy_duplicate',receipt:{code:duplicate.strategy_key===p.strategyKey&&duplicate.receipt.intent.bodyDigest!==intent.bodyDigest
        ?'STRATEGY_IDENTITY_CONFLICT':'STRATEGY_INTENT_ALREADY_RECORDED',canonicalTransitionId:duplicate.id,
    planningRequestId:duplicate.planning_request_id??null,intent,originalReceipt:options.originalReceipt??null,
    nextAction:'Follow the exact canonical intent or resolve an identity conflict; changing only the key does not create another planner.'}};
  const owner=await readStrategyPlannerOwner(db,goalId,p.scope);
  return {state:'awaiting_planner',receipt:{code:'STRATEGY_DEFERRED',intent:{...intent,
    ...(owner?{blockedByRequestId:owner.request_id,...(owner.run_id?{blockedByRunId:owner.run_id}:{})}:{}),
    ...(options.originalReceipt?{originalReceipt:options.originalReceipt}:{})},
    nextAction:'The new strategy is persisted but not consumed. Preserve the active planner; revalidate this exact intent after its result and effects are recorded.'}};
}
