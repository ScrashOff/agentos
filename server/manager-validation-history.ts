import {createHash} from 'node:crypto';
import type {DB} from './db.js';

const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
const sha=(text:string)=>createHash('sha256').update(text).digest('hex');
const object=(value:any)=>value&&typeof value==='object'&&!Array.isArray(value);
const limit=1048576;

/** Only verbose diagnostics of demonstrably older completed validations move
 * behind a reader. Current/owned/uncertain/unknown records and all control,
 * outcome and identity fields remain inline. No settings or source is changed. */
export function projectManagerValidationHistory(snapshot:any) {
 const rows=snapshot.operation?.candidateValidations;
 if(!Array.isArray(rows))return snapshot;
 const attempts=new Map<string,number>();
 for(const task of snapshot.operation?.tasks??[])
  if(uuid.test(task?.id??'')&&Number.isSafeInteger(task.expectedAttempt))attempts.set(task.id,task.expectedAttempt);
 for(const entry of snapshot.cases??[])
  if(uuid.test(entry?.taskId??'')&&Number.isSafeInteger(entry.currentRun?.attempt))attempts.set(entry.taskId,entry.currentRun.attempt);
 const protectedText=JSON.stringify({cases:snapshot.cases,requiredDecision:snapshot.operation.requiredDecision,
  recoveredContext:snapshot.operation.recoveredContext,decisionFeedback:snapshot.operation.decisionFeedback});
 let changed=false;
 const projected=rows.map((row:any)=>{
  if(!object(row)||row.state!=='completed'||!uuid.test(row.actionId??'')||!uuid.test(row.goalId??'')||
   !uuid.test(row.taskId??'')||!uuid.test(row.runId??'')||!Number.isSafeInteger(row.expectedAttempt)||row.expectedAttempt<1||
   !(attempts.get(row.taskId)!>row.expectedAttempt)||!object(row.result)||
   protectedText.includes(row.actionId)||protectedText.includes(row.runId))return row;
  const text=JSON.stringify(row);
  if(Buffer.byteLength(text,'utf8')>limit)return row;
  const digest=sha(text),result={...row.result};let selected=false;
  for(const field of ['diagnostic','sourceDrift']){
   const original=row.result[field];
   if(original===undefined||JSON.stringify(original).length<2048)continue;
   result[field]={kind:'MANAGER_VALIDATION_HISTORY_DETAIL_V1',complete:false,fullSourcePreserved:true,
    field:'result.'+field,characters:JSON.stringify(original).length,
    recovery:{tool:'management.contextRequests',request:{kind:'candidate_validation',actionId:row.actionId,snapshotDigest:digest,offset:0}},
    guide:'Historical validation detail is not delivered here. Read the same-goal exact source with this request and follow nextOffset if needed. Outcome and identity stay inline. Neither this reference nor a historical passed result authorizes retry or acceptance.'};
   selected=true;
  }
  if(!selected||JSON.stringify(result).length>=JSON.stringify(row.result).length)return row;
  changed=true;return {...row,result};
 });
 return changed?{...snapshot,operation:{...snapshot.operation,candidateValidations:projected}}:snapshot;
}

/** The same JSON view as readCandidateValidations, bound to goal/action and
 * digest. A changed source is refused; callers never mix historical versions.
 * The request body (including private execution configuration) is never read. */
export async function readManagerValidationHistory(db:DB,goalId:string,
 request:{actionId:string;snapshotDigest:string;offset:number}) {
 if(!uuid.test(goalId)||!uuid.test(request.actionId)||!/^[a-f0-9]{64}$/.test(request.snapshotDigest)||
  !Number.isSafeInteger(request.offset)||request.offset<0)return {status:'INVALID_REQUEST' as const};
 const row=(await db.query(`SELECT CASE WHEN octet_length((value-'request')::text)<=$3
  THEN value-'request' END value FROM settings WHERE id=$1 AND value->>'goalId'=$2`,
  ['candidate-validation:'+request.actionId,goalId,limit])).rows[0];
 if(!row)return {status:'NOT_FOUND' as const};
 if(!row.value)return {status:'TOO_LARGE' as const};
 const value=row.value;
 if(value.goalId!==goalId||value.actionId!==request.actionId)return {status:'INTEGRITY_FAILED' as const};
 const text=JSON.stringify(value),digest=sha(text);
 if(digest!==request.snapshotDigest)return {status:'STALE_CONTEXT' as const,nextOffset:0};
 const identity={digest,snapshotDigest:digest,totalCharacters:text.length};
 if(request.offset>text.length)return {...identity,status:'PAGE_OUT_OF_RANGE' as const,nextOffset:0};
 const end=Math.min(text.length,request.offset+12000);
 return {...identity,status:'OK' as const,text:text.slice(request.offset,end),nextOffset:end<text.length?end:null,truncated:end<text.length};
}
