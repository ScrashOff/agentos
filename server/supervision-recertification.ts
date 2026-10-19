import {event,type DB} from './db.js';
import {staleCanonicalRepairReview} from './canonical-repair-rejection.js';
import {staleOperatorOpenCodeReview} from './operator-opencode-review.js';
import {operatorOpenCodeRecoveryNextAction} from './operator-opencode-recovery-contract.js';

export const recertificationRejectionCodes = new Set([
  'DELIVERY_RECERTIFICATION_PHASE_CHANGED',
  'DELIVERY_RECERTIFICATION_SOURCE_SELECTION_REQUIRED',
  'SUPERVISION_DELEGATION_IDENTITY_CONFLICT',
  'CANONICAL_REPAIR_REVIEW_STALE',
  'OPENCODE_RECOVERY_REVIEW_STALE',
]);
export type RecertificationPhase = {
  eventId:string|null; createdAt:string|null; reviewIsFresh:boolean;
  sources:any[]; obligationIds:string[];
  canonicalRepair?:true;
  operatorOpenCode?:true;
};

export async function supervisionReviewAccepted(db:DB,decisionRunId:string|null,goalId:string){
  if(!decisionRunId)return false;
  return !!(await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id
    WHERE r.id=$1 AND r.state='submitted' AND t.goal_id=$2 AND t.stage='coordinate'
      AND (SELECT v.passed FROM acceptances v WHERE v.run_id=r.id AND v.task_id=t.id
        ORDER BY v.created_at DESC,v.id DESC LIMIT 1)=true`,[decisionRunId,goalId])).rowCount;
}

/** The event is the phase fence. Compare timestamps in PostgreSQL so two
 * transitions within one JavaScript millisecond cannot admit an old review.
 * Legacy evidence without its immutable event fails closed. */
export async function readRecertificationPhase(db:DB,c:any,decisionRunId:string):Promise<RecertificationPhase|null>{
  const administrative=await staleOperatorOpenCodeReview(db,c,decisionRunId);
  if(administrative)return {...administrative,sources:[],obligationIds:[],operatorOpenCode:true};
  const canonical=await staleCanonicalRepairReview(db,c,decisionRunId);
  if(canonical)return {...canonical,sources:[],obligationIds:[],canonicalRepair:true};
  if(!(Array.isArray(c.evidence)&&c.evidence.some((e:any)=>e.kind==='DELIVERY_RECERTIFICATION_READY')))return null;
  const row=(await db.query(`SELECT e.id::text event_id,e.created_at::text created_at,
    COALESCE(r.created_at>e.created_at,false) review_is_fresh,
    e.payload->'sources' sources,e.payload->'obligationIds' obligation_ids
    FROM events e LEFT JOIN runs r ON r.id=$3
    WHERE e.entity_id=$1 AND e.kind='DELIVERY_RECERTIFICATION_READY'
      AND e.payload->>'caseId'=$2 ORDER BY e.id DESC LIMIT 1`,[c.task_id,c.id,decisionRunId])).rows[0];
  return {eventId:row?.event_id??null,createdAt:row?.created_at??null,reviewIsFresh:row?.review_is_fresh===true,
    sources:Array.isArray(row?.sources)?row.sources:[],obligationIds:Array.isArray(row?.obligation_ids)?row.obligation_ids:[]};
}
export function recertificationDecisionCode(phase:RecertificationPhase|null,actions:{kind:string}[]):string|null{
  if(!phase)return null;
  if(phase.operatorOpenCode)return 'OPENCODE_RECOVERY_REVIEW_STALE';
  if(phase.canonicalRepair)return 'CANONICAL_REPAIR_REVIEW_STALE';
  if(!phase.reviewIsFresh)return 'DELIVERY_RECERTIFICATION_PHASE_CHANGED';
  // A completed support is evidence for source selection, not permission to
  // create another support with a new title or another manager run. A genuinely
  // different material need belongs to an explicit successor plan/case.
  if(actions.some(a=>a.kind==='delegate'))return 'DELIVERY_RECERTIFICATION_SOURCE_SELECTION_REQUIRED';
  return null;
}

/** Caller holds the global lock and the case row lock. For a decided case it
 * must first recheck current run/attempt, authority, leases and physical stop.
 * No task, acceptance, artifact, criterion or prior decision event is rewritten. */
export async function recordRecertificationRejection(db:DB,c:any,decisionRunId:string,code:string,
  phase:RecertificationPhase|null,actions:any[],reopen:boolean){
  if(!recertificationRejectionCodes.has(code))throw Error('RECERTIFICATION_REJECTION_CODE_INVALID');
  if(!(await supervisionReviewAccepted(db,decisionRunId,c.goal_id)))return false;
  const kind=reopen?'DELIVERY_SOURCE_DECISION_REJECTED':'SUPERVISION_PLAN_REJECTED';
  if(reopen){
    const changed=await db.query(`UPDATE supervision_cases SET state='open',decision=NULL,decision_run_id=NULL,
      reason=$5,updated_at=now() WHERE id=$1 AND state='decided' AND decision_run_id=$2
      AND source_run_id IS NOT DISTINCT FROM $3::uuid AND decision=$4::jsonb RETURNING id`,
      [c.id,decisionRunId,c.source_run_id,JSON.stringify(c.decision),code==='OPENCODE_RECOVERY_REVIEW_STALE'
        ?'Supervisor deve revisar a recuperação administrativa anexada após a instrução congelada: '+code
        :code==='CANONICAL_REPAIR_REVIEW_STALE'
        ?'Supervisor deve revisar a rejeição canônica anexada após a instrução congelada: '+code
        :'Supervisor deve selecionar a fonte recertificada: '+code]);
    if(!changed.rowCount)return false;
  }else if(c.state!=='open')return false;
  const prior=(await db.query(`SELECT 1 FROM events WHERE entity_id=$1 AND kind=$2
    AND payload->>'caseId'=$3 AND COALESCE(payload->>'decisionRunId',payload->>'runId')=$4
    AND payload->>'code'=$5 AND payload->>'phaseEventId' IS NOT DISTINCT FROM $6::text`,
    [c.task_id,kind,c.id,decisionRunId,code,phase?.eventId??null])).rowCount;
  if(!prior){
    const payload={caseId:c.id,runId:decisionRunId,decisionRunId,sourceRunId:c.source_run_id,code,
      phaseEventId:phase?.eventId??null,phaseCreatedAt:phase?.createdAt??null,
      sources:phase?.sources??[],obligationIds:phase?.obligationIds??[],decision:{actions},
      nextAction:'Read the current case evidence. Select an accepted frozen repair with revise, sourceTaskId and explicit deliveryResolution {obligationId,generation,expectedSourceRunId}. Preserve composition and independent QA. Do not repeat delegation after completed recertification; a materially different need requires an explicit successor plan/case. If the current DELIVERY_RECERTIFICATION_READY sources are only accepted frozen diagnostics, do not substitute a diagnostic or repeat supervision.delegate. Re-read the same-scope source, writable integrator, DAG and alreadyPlanned. Propose the missing correction through existing coordination.backlog.tasks with workKind:"repair", lane:"glm", sourceTaskId of that diagnostic, deliverToTaskId of the original writable integrator, baseWorkspace of that scope, delivery:"deferred" and one stable coverageKey describing the missing repair. Reuse equivalent existing work; never rename coverage to evade deduplication. This requests isolated repair work, not source replacement or acceptance. After a repair is actually accepted with patch/report/source proof, submit a fresh revise with its exact sourceTaskId and deliveryResolution; composition and independent QA remain required.'};
    if(code==='CANONICAL_REPAIR_REVIEW_STALE')payload.nextAction=
      'Read the canonical repair rejection now attached to this existing case, including the exact code, source run and artifact hashes. Prepare a new review after that evidence. Preserve the immutable rejected delivery, original criteria, composition and independent QA. Do not replay the earlier frozen review, create a duplicate owner, or infer acceptance from packaging.';
    if(code==='OPENCODE_RECOVERY_REVIEW_STALE')payload.nextAction=
      'Prepare uma nova revisão após OPENCODE_RECOVERY_REVIEW_READY. '+operatorOpenCodeRecoveryNextAction;
    await event(db,kind,c.task_id,payload);
    if(reopen)await db.query(`UPDATE supervision_cases SET evidence=evidence||$2::jsonb WHERE id=$1`,
      [c.id,JSON.stringify([{kind,code,decisionRunId,phaseEventId:phase?.eventId??null}])]);
  }
  return true;
}
