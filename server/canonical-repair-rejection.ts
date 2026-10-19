import {createHash} from 'node:crypto';
import type {DB} from './db.js';

const digest=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const proofCodes=new Set(['SIZE','ENCODING','JSON','JSON_DUPLICATE_KEY','PATCH_FORMAT','PATCH_PATH','REPORT','COVERAGE']
  .map(code=>'REPAIR_SOURCE_PROOF_'+code));
const ref=(value:any)=>({id:value.id,name:value.name,digest:value.digest,size:value.size});
const rejectionKinds=new Set(['repair_source_proof_rejected','semantic_repair_rejected','semantic_repair_evidence_invalid']);

/** An old frozen review cannot decide a cause attached after it was created. */
export function canonicalRepairReviewIsFresh(evidence:any[],createdAt:string|Date){
  const dates=(Array.isArray(evidence)?evidence:[]).filter(item=>
    item?.authority==='review_only'&&rejectionKinds.has(item.kind)).map(item=>Date.parse(item.observedAt));
  const review=new Date(createdAt).getTime();
  return !dates.length||(Number.isFinite(review)&&dates.every(at=>Number.isFinite(at)&&review>at));
}

/** The applier calls this under the existing admission/case lock. PostgreSQL
 * timestamps close the read-then-record race without trusting model timestamps. */
export async function staleCanonicalRepairReview(db:DB,c:any,decisionRunId:string){
  const causes=(Array.isArray(c.evidence)?c.evidence:[]).filter((item:any)=>
    item?.authority==='review_only'&&rejectionKinds.has(item.kind)).map((item:any)=>item.causeDigest);
  if(!causes.length)return null;
  if(causes.some((value:any)=>typeof value!=='string'||!/^[a-f0-9]{64}$/.test(value)))
    return {eventId:null,createdAt:null,reviewIsFresh:false};
  const rows=(await db.query(`SELECT DISTINCT ON(e.payload->>'causeDigest') e.id::text event_id,
      e.created_at::text created_at,e.payload->>'causeDigest' cause_digest,
      COALESCE(r.created_at>e.created_at,false) review_is_fresh
    FROM events e LEFT JOIN runs r ON r.id=$4
    WHERE e.kind='CANONICAL_REPAIR_REJECTION_OBSERVED' AND e.entity_id=$1
      AND e.payload->>'ownerCaseId'=$2 AND e.payload->>'runId'=$3
      AND e.payload->>'routed'='true' AND e.payload->>'causeDigest'=ANY($5::text[])
    ORDER BY e.payload->>'causeDigest',e.id DESC`,[c.task_id,c.id,c.source_run_id,decisionRunId,causes])).rows;
  const missing=causes.some((cause:string)=>!rows.some(row=>row.cause_digest===cause));
  const stale=rows.filter(row=>row.review_is_fresh!==true).sort((a,b)=>
    BigInt(a.event_id)>BigInt(b.event_id)?-1:1)[0];
  return missing||stale?{eventId:stale?.event_id??null,createdAt:stale?.created_at??null,reviewIsFresh:false}:null;
}

/** The validator owns this code. Artifact prose is never a classification or
 * permission to retry. Missing/ambiguous artifacts keep their packaging path. */
export async function canonicalRepairProofRejection(db:DB,task:any,run:any,acceptance:any){
  if(run.state!=='submitted'||acceptance?.passed!==false||!Array.isArray(acceptance.checks))return null;
  const frozen=run.instruction_snapshot?.task;
  if(frozen&&(frozen.id!==task.id||frozen.goal_id!==task.goal_id))throw Error('SEMANTIC_RETRY_EVIDENCE_INVALID');
  const criteria=frozen?.criteria??task.criteria;
  if(!Array.isArray(criteria))return null;
  const failed=acceptance.checks.find((check:any)=>check.passed===false&&check.type==='repair_source_proof'&&
    criteria.some((criterion:any)=>criterion.id===check.id&&criterion.type==='repair_source_proof'&&
      criterion.version===1&&criterion.name==='repair-report.json'&&criterion.patch==='implementation.patch'));
  const match=typeof failed?.detail==='string'?/^(REPAIR_SOURCE_PROOF_[A-Z_]+)(?::|$)/.exec(failed.detail):null;
  const code=match?.[1];if(!code||!proofCodes.has(code))return null;
  const refs=(await db.query(`SELECT id,name,digest,size FROM artifacts WHERE run_id=$1
    AND name IN('implementation.patch','repair-report.json') ORDER BY id LIMIT 3`,[run.id])).rows;
  const patch=refs.filter(r=>r.name==='implementation.patch'),report=refs.filter(r=>r.name==='repair-report.json');
  if(patch.length!==1||report.length!==1||refs.some(r=>!Number.isSafeInteger(r.size)||r.size<0||
    !/^[a-f0-9]{64}$/.test(r.digest)))return null;
  return {version:1,kind:'repair_source_proof_rejected',code,taskId:task.id,goalId:task.goal_id,
    sourceRunId:run.id,sourceRunFence:run.fence,sourceContextVersion:run.context_id??null,
    sourceAttempt:run.attempt,acceptanceId:acceptance.id,report:ref(report[0]),patch:ref(patch[0]),
    failedCriterion:{id:failed.id,type:'repair_source_proof',name:'repair-report.json',
      patch:'implementation.patch',expected:true,actual:false},
    note:'The canonical structural check rejected this artifact pair. Preserve the source and original acceptance criteria; packaging does not prove applicability or independent QA.'};
}

export function canonicalRepairRejectionKey(rejection:any){
  return 'semantic:'+rejection.kind+':'+(rejection.kind==='repair_source_proof_rejected'
    ?digest([rejection.code,rejection.failedCriterion.id,rejection.patch.digest,rejection.report.digest])
    :rejection.report.digest);
}

export function canonicalRepairCause(rejection:any,run:any){
  return digest([rejection.goalId,rejection.taskId,run.id,run.fence,run.context_id??null,run.attempt,
    rejection.acceptanceId,rejection.kind,rejection.code??null,rejection.failedCriterion,
    rejection.report,rejection.patch??null]);
}

/** Caller holds the existing admission lock and has revalidated the current
 * actor/acceptance. This attaches facts only; no dispatch, stop or acceptance. */
export async function recordCanonicalRepairRejection(db:DB,task:any,run:any,rejection:any,owner:any){
  const requestKey=canonicalRepairRejectionKey(rejection),causeDigest=canonicalRepairCause(rejection,run);
  const item={...rejection,sourceRunFence:run.fence,sourceContextVersion:run.context_id??null,
    requestKey,causeDigest,authority:'review_only',observedAt:new Date().toISOString()};
  let disposition='deferred_existing_decision',attached=false;
  // Reopened cases retain their historical decision. Only the terminal reviewer
  // is historical; a live/unknown reviewer or an unbound decision remains deferred.
  const historicalReview=owner?.state==='open'&&owner.decision_run_id?
    ((await db.query(`SELECT r.id FROM runs r JOIN tasks t ON t.id=r.task_id
      WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate'
        AND r.state IN('submitted','failed','cancelled')`,[owner.decision_run_id,task.goal_id])).rowCount??0)>0:false;
  if(!owner)disposition='awaiting_owner';
  else if(owner.source_run_id!==run.id)disposition='deferred_owner_run_mismatch';
  else if(owner.state==='open'&&((owner.decision_run_id===null&&owner.decision==null)||historicalReview)){
    if(!Array.isArray(owner.evidence)||Buffer.byteLength(JSON.stringify(owner.evidence))>128*1024)
      disposition='deferred_owner_evidence_invalid';
    else{
      attached=owner.evidence.some((entry:any)=>entry?.sourceRunId===run.id&&
        (entry.causeDigest===causeDigest||(entry.requestKey===requestKey&&entry.acceptanceId===rejection.acceptanceId)));
      if(!attached){
        attached=Boolean((await db.query(`UPDATE supervision_cases SET evidence=evidence||$2::jsonb,updated_at=now()
          WHERE id=$1 AND task_id=$3 AND goal_id=$4 AND source_run_id=$5 AND state='open'
            AND decision_run_id IS NOT DISTINCT FROM $7::uuid AND decision IS NOT DISTINCT FROM $8::jsonb
            AND evidence=$6::jsonb RETURNING id`,
          [owner.id,JSON.stringify([item]),task.id,task.goal_id,run.id,JSON.stringify(owner.evidence),
            owner.decision_run_id,owner.decision===null?null:JSON.stringify(owner.decision)])).rowCount);
      }
      disposition=attached?'routed':'deferred_owner_changed';
    }
  }
  const key='canonical-repair-rejection:'+run.id;
  const prior=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',[key])).rows[0]?.value;
  const ownerCaseId=owner?.id??null,ownerState=owner?.state??null,ownerSourceRunId=owner?.source_run_id??null;
  if(prior?.causeDigest===causeDigest&&prior?.disposition===disposition&&
    prior?.ownerCaseId===ownerCaseId&&prior?.ownerState===ownerState&&prior?.ownerSourceRunId===ownerSourceRunId)
    return {changed:false,attached,disposition,causeDigest};
  const value={version:1,authority:'review_only',goalId:task.goal_id,taskId:task.id,runId:run.id,
    runFence:run.fence,contextVersion:run.context_id??null,expectedAttempt:run.attempt,
    acceptanceId:rejection.acceptanceId,causeDigest,requestKey,rejection:item,ownerCaseId,ownerState,ownerSourceRunId,
    disposition,routed:attached,observedAt:new Date().toISOString(),acceptance:false};
  await db.query(`INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value`,
    [key,JSON.stringify(value)]);
  // Transaction start can precede a review that froze while this transaction
  // waited for admission. The immutable event fences the actual attachment.
  await db.query('INSERT INTO events(kind,entity_id,payload,created_at) VALUES($1,$2,$3,clock_timestamp())',
    ['CANONICAL_REPAIR_REJECTION_OBSERVED',task.id,JSON.stringify(value)]);
  return {changed:true,attached,disposition,causeDigest};
}

/** Current-actor projection for the manager/API. An older observation is not
 * returned after a fence, context, attempt, acceptance or owner transition. */
async function readCurrentRepairRejections(db:DB,selector:'r.id'|'t.goal_id',id:string,limit:number){
  const rows=(await db.query(`SELECT s.value FROM runs r JOIN tasks t ON t.id=r.task_id
    JOIN settings s ON s.id='canonical-repair-rejection:'||r.id::text
    WHERE ${selector}=$1 AND r.state='submitted' AND t.attempt_count=r.attempt
      AND t.state NOT IN('accepted','cancelled','superseded','paused')
      AND s.value->>'goalId'=t.goal_id::text AND s.value->>'taskId'=t.id::text
      AND s.value->>'runFence'=r.fence::text
      AND s.value->>'contextVersion' IS NOT DISTINCT FROM r.context_id::text
      AND s.value->>'expectedAttempt'=r.attempt::text
      AND s.value->>'acceptanceId'=(SELECT id::text FROM acceptances WHERE run_id=r.id
        ORDER BY created_at DESC,id DESC LIMIT 1)
      AND (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)=false
      AND (s.value->>'ownerCaseId' IS NULL OR EXISTS(SELECT 1 FROM supervision_cases owner
        WHERE owner.id::text=s.value->>'ownerCaseId' AND owner.task_id=t.id AND owner.goal_id=t.goal_id
          AND owner.source_run_id::text IS NOT DISTINCT FROM CASE WHEN s.value?'ownerSourceRunId'
            THEN s.value->>'ownerSourceRunId' ELSE s.value->>'runId' END
          AND owner.state=s.value->>'ownerState'))
      AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id
        AND live.state IN('queued','running','reconciling','cancelling'))
      AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.payload->>'runId'=r.id::text)
    ORDER BY s.value->>'observedAt' DESC,r.id LIMIT $2`,[id,limit])).rows;
  return rows.map(row=>row.value);
}

export async function readCanonicalRepairRejection(db:DB,runId:string){
  return (await readCurrentRepairRejections(db,'r.id',runId,1))[0]??null;
}

/** Bounded goal-scoped manager projection. The shared current-actor guards keep
 * deferred facts visible without assigning a second owner or authorizing work. */
export async function readCanonicalRepairRejections(db:DB,goalId:string){
  return (await readCurrentRepairRejections(db,'t.goal_id',goalId,8)).map(value=>({...value,
    reason:value.rejection?.code??value.rejection?.kind??'CANONICAL_REPAIR_REJECTION'}));
}
