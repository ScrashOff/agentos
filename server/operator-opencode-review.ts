import type {DB} from './db.js';
import {hasOperatorOpenCodeRecoveryIntent} from './operator-opencode-recovery-contract.js';
import {operatorRecoveryCode} from './operator-recovery-contract.js';

const administrative=(e:any)=>e?.kind===operatorRecoveryCode&&e.runtime==='opencode';
export function operatorOpenCodeReviewIsFresh(evidence:any[],createdAt:string|Date){
 const entries=(Array.isArray(evidence)?evidence:[]).filter(administrative);
 const review=new Date(createdAt).getTime();
 // JavaScript loses PostgreSQL sub-millisecond precision. Equality here only
 // admits collection; the applier's strict PG comparison remains authoritative.
 return !entries.length||(Number.isFinite(review)&&entries.every(e=>
  Number.isFinite(Date.parse(e.observedAt))&&review>=Date.parse(e.observedAt)));
}
/** Timestamp authority is the immutable DB attachment event, not an operator
 * supplied time or model statement. Called under the canonical applier lock. */
export async function staleOperatorOpenCodeReview(db:DB,c:any,decisionRunId:string){
 const entries=(Array.isArray(c.evidence)?c.evidence:[]).filter(administrative);
 if(!entries.length)return null;
 const run=(await db.query('SELECT * FROM runs WHERE id=$1 AND task_id=$2',[c.source_run_id,c.task_id])).rows[0];
 if(!run||!await hasOperatorOpenCodeRecoveryIntent(db,run))
  return {eventId:null,createdAt:null,reviewIsFresh:false};
 const receipt=run.checkpoint.operatorRecoveryStop;
 if(entries.some((e:any)=>e.runId!==run.id||e.requestId!==receipt.request.requestId))
  return {eventId:null,createdAt:null,reviewIsFresh:false};
 const row=(await db.query(`SELECT e.id::text event_id,e.created_at::text created_at,
   COALESCE(r.created_at>e.created_at,false) review_is_fresh
  FROM events e LEFT JOIN runs r ON r.id=$4
  WHERE e.kind='OPENCODE_RECOVERY_REVIEW_READY' AND e.entity_id=$1
   AND e.payload->>'caseId'=$2 AND e.payload->>'sourceRunId'=$3
   AND e.payload->>'requestDigest'=$5 AND e.payload->>'requestId'=$6
  ORDER BY e.id DESC LIMIT 1`,[c.task_id,c.id,run.id,decisionRunId,receipt.requestDigest,receipt.request.requestId])).rows[0];
 return row?.review_is_fresh===true?null:
  {eventId:row?.event_id??null,createdAt:row?.created_at??null,reviewIsFresh:false};
}
export async function appliedOperatorOpenCodeReviewIsFresh(db:DB,c:any){
 return Boolean(c?.decision_run_id&&(Array.isArray(c.evidence)?c.evidence:[]).some(administrative)&&
  !await staleOperatorOpenCodeReview(db,c,c.decision_run_id));
}
export async function recordOperatorOpenCodeReviewReady(db:DB,taskId:string,runId:string,caseId:string,receipt:any){
 if(receipt?.request?.runtime!=='opencode')return;
 await db.query(`INSERT INTO events(kind,entity_id,payload,created_at)
  SELECT 'OPENCODE_RECOVERY_REVIEW_READY',$1,$2,clock_timestamp()
  WHERE NOT EXISTS(SELECT 1 FROM events WHERE kind='OPENCODE_RECOVERY_REVIEW_READY' AND entity_id=$1
   AND payload->>'caseId'=$3 AND payload->>'requestId'=$4)`,[taskId,JSON.stringify({caseId,sourceRunId:runId,
    requestId:receipt.request.requestId,requestDigest:receipt.requestDigest,acceptance:false}),caseId,receipt.request.requestId]);
}
