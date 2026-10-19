import {createHash} from 'node:crypto';
import type {DB} from './db.js';

const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
const hex=/^[a-f0-9]{64}$/;
const maximumEvidenceBytes=8*1024*1024;
const maximumEvidenceEntries=10000;
const canonical=(value:any):any=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'?
 Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const text=(value:any)=>JSON.stringify(canonical(value));
const hash=(value:string)=>createHash('sha256').update(value).digest('hex');

export type CaseEvidenceSource={version:1;caseId:string;taskId:string;sourceRunId:string|null;
 evidenceCount:number;arrayDigest:string;digest:string;entryDigests:string[]};

/** Bind the original JSONB array before any prompt-only summary/window. Entry
 * text uses canonical JSON keys; values and array order remain exact. This is
 * retrieval identity, never permission to apply any evidence or decision. */
export function managerCaseEvidenceSource(row:any):CaseEvidenceSource|null {
 if(!uuid.test(row?.id??'')||!uuid.test(row?.goal_id??'')||!uuid.test(row?.task_id??'')||
  !(row.source_run_id===null||uuid.test(row.source_run_id??''))||!Array.isArray(row.evidence)||
  row.evidence.length>maximumEvidenceEntries)return null;
 const arrayText=text(row.evidence);
 if(Buffer.byteLength(arrayText,'utf8')>maximumEvidenceBytes)return null;
 const identity={version:1 as const,caseId:row.id,taskId:row.task_id,sourceRunId:row.source_run_id,
  evidenceCount:row.evidence.length,arrayDigest:hash(arrayText)};
 return {...identity,digest:hash(text({...identity,goalId:row.goal_id})),entryDigests:row.evidence.map((entry:any)=>hash(text(entry)))};
}

export type CaseEvidenceRequest={caseId:string;evidenceIndex:number;digest:string;offset:number};

/** One goal-bound SQL snapshot. A changed array/owner/run cannot be spliced
 * into a page from an earlier case. Cases outside the current focus remain
 * readable only inside the same objective. No writes or artifact/provider IO. */
export async function readManagerCaseEvidence(db:DB,goalId:string,request:CaseEvidenceRequest) {
 if(!uuid.test(goalId)||!uuid.test(request.caseId)||!hex.test(request.digest)||
  !Number.isSafeInteger(request.evidenceIndex)||request.evidenceIndex<0||request.evidenceIndex>10000000||
  !Number.isSafeInteger(request.offset)||request.offset<0||request.offset>10000000)
  throw Error('CASE_EVIDENCE_REQUEST_INVALID');
 const row=(await db.query(`SELECT c.id,c.goal_id,c.task_id,c.source_run_id,
   octet_length(c.evidence::text) evidence_bytes,
   CASE WHEN jsonb_typeof(c.evidence)='array' THEN jsonb_array_length(c.evidence) ELSE NULL END evidence_count,
   CASE WHEN octet_length(c.evidence::text)<=$3 AND
     (CASE WHEN jsonb_typeof(c.evidence)='array' THEN jsonb_array_length(c.evidence)<=$4 ELSE true END)
     THEN c.evidence ELSE NULL END evidence
   FROM supervision_cases c JOIN tasks t ON t.id=c.task_id AND t.goal_id=c.goal_id
   WHERE c.id=$1::uuid AND c.goal_id=$2::uuid`,[request.caseId,goalId,maximumEvidenceBytes,maximumEvidenceEntries])).rows[0];
 if(!row)return {status:'NOT_FOUND'};
 if(row.evidence_bytes>maximumEvidenceBytes||row.evidence_count>maximumEvidenceEntries)return {status:'TOO_LARGE'};
 const source=managerCaseEvidenceSource(row);
 if(!source)return {status:'CASE_EVIDENCE_INVALID'};
 if(source.digest!==request.digest)return {status:'STALE_CASE_EVIDENCE',caseId:source.caseId,
  taskId:source.taskId,sourceRunId:source.sourceRunId,evidenceCount:source.evidenceCount,
  evidenceDigest:source.digest,arrayDigest:source.arrayDigest,nextOffset:0};
 if(request.evidenceIndex>=source.evidenceCount)return {status:'NOT_FOUND',caseId:source.caseId,evidenceDigest:source.digest};
 const entryText=text(row.evidence[request.evidenceIndex]),entryDigest=hash(entryText);
 if(entryDigest!==source.entryDigests[request.evidenceIndex])throw Error('CASE_EVIDENCE_INTEGRITY');
 // Do not expose a generic runId: these mutable case pages are not artifact
 // pages and must not enter the existing run-based evidence ledger.
 return {status:'OK',caseId:source.caseId,taskId:source.taskId,sourceRunId:source.sourceRunId,
  evidenceCount:source.evidenceCount,evidenceIndex:request.evidenceIndex,evidenceDigest:source.digest,
  arrayDigest:source.arrayDigest,entryDigest,digest:entryDigest,
  representation:'canonical_original_case_evidence',totalCharacters:entryText.length,
  text:entryText.slice(request.offset,request.offset+12000)};
}
