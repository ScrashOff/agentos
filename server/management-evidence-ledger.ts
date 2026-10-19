import type {DB} from './db.js';
import {hash} from './memory.js';
import {managementContextRequests,readManagementRetrievalRequest,sameLiveInstructionActor} from './management-retrieval.js';

const ledgerKey=(goalId:string)=>'management-evidence:'+goalId;
type Focus={kind?:string;caseId?:string;scope?:string|null;taskId?:string;goalId?:string;
 runId?:string;runFence?:string;contextVersion?:string;expectedAttempt?:number};
type ArtifactReader=(id:string)=>Promise<{bytes:Buffer}>;
const mutableKinds=new Set(['task','task_metadata','case_evidence','planning_request','delivery_consumption','candidate_validation']);
const scopedKinds=new Set([...mutableKinds,'context_section','live_task_instruction']);
const requestIdentity=(request:any)=>Object.fromEntries(Object.entries({
 ...request,...(request.kind==='task_metadata'?{part:request.part??'decision'}:{})})
 .filter(([key])=>key!=='offset'&&key!=='snapshotDigest').sort(([a],[b])=>a.localeCompare(b)));
// Scheduling observations can change between two pages of the same source.
// Bind evidence to the decision, not the selection reason or service history.
const stableFocus=(focus?:Focus)=>focus?{caseId:focus.caseId,scope:focus.scope,taskId:focus.taskId}:undefined;
const identity=(entry:any,focus?:Focus)=>JSON.stringify([requestIdentity(entry.request),
 entry.runId??null,entry.artifactId??null,entry.digest,entry.snapshotDigest??null,stableFocus(focus)??null]);
const sameFocus=(a:Focus,b?:Focus)=>!!b&&a.caseId===b.caseId&&a.scope===b.scope&&a.taskId===b.taskId;
const noArtifacts:ArtifactReader=async()=>{throw Error('MANAGEMENT_EVIDENCE_READER_REQUIRED');};
// Offsets refer to JavaScript UTF-16 code units. A valid page can start or end
// inside a surrogate pair; PostgreSQL jsonb rejects that raw JSON string (and
// U+0000). Store the JSON spelling as ordinary text, then recover the exact
// code units before comparison/use. Never replace evidence with U+FFFD.
const storedEntries=(entries:any[])=>entries.map(entry=>({...entry,
 pages:entry.pages?.map((page:any)=>({offset:page.offset,textJson:JSON.stringify(page.text)}))}));
const decodedEntries=(ledger:any):any[]=>!Array.isArray(ledger?.entries)?[]:ledger.entries.flatMap((entry:any)=>{
 if(ledger.version!==2)return [entry];
 if(!Array.isArray(entry?.pages))return [];
 try{
  const pages=entry.pages.map((page:any)=>{
   const text=JSON.parse(page.textJson);
   if(typeof text!=='string')throw Error('INVALID_EVIDENCE_PAGE');
   return {offset:page.offset,text};
  });
  return [{...entry,pages}];
 }catch{return [];}
});

type Page={offset:number;text:string};
function mergePages(pages:Page[],totalCharacters:number):Page[] {
 const merged:Page[]=[];
 for(const page of [...pages].sort((a,b)=>a.offset-b.offset)) {
  if(!Number.isSafeInteger(page?.offset)||page.offset<0||typeof page.text!=='string'||
    page.offset+page.text.length>totalCharacters)throw Error('MANAGEMENT_EVIDENCE_CONFLICT');
  const last=merged.at(-1),end=last?last.offset+last.text.length:0;
  if(!last||page.offset>end){merged.push({...page});continue;}
  const overlap=Math.min(end-page.offset,page.text.length);
  if(last.text.slice(page.offset-last.offset,page.offset-last.offset+overlap)!==page.text.slice(0,overlap))
   throw Error('MANAGEMENT_EVIDENCE_CONFLICT');
  last.text+=page.text.slice(overlap);
 }
 return merged;
}
const mergeableEntry=(entry:any)=>managementContextRequests.element.safeParse(entry?.request).success&&
 /^[a-f0-9]{64}$/.test(entry.digest??'')&&Number.isSafeInteger(entry.totalCharacters)&&entry.totalCharacters>=0&&
 Array.isArray(entry.pages)&&(!entry.focus||typeof entry.focus==='object'&&!Array.isArray(entry.focus)&&
  ['caseId','taskId'].every(key=>entry.focus[key]===undefined||typeof entry.focus[key]==='string')&&
  (entry.focus.scope==null||typeof entry.focus.scope==='string'));
/** Upgrade legacy scheduling-dependent keys in memory. The caller's goal-scoped
 * settings row is the partition; source identity and every overlap must agree.
 * A conflict throws before persistence, leaving the original ledger intact. */
function coalescedEntries(entries:any[]):any[] {
 const result:any[]=[],byKey=new Map<string,any>();
 for(const original of entries) {
  // Unknown/malformed records cannot acquire another source's canonical key.
  if(!mergeableEntry(original)){result.push(original);continue;}
  const focus=stableFocus(original.focus),key=identity(original,focus);
  const pages=mergePages(original.pages,original.totalCharacters),existing=byKey.get(key);
  if(!existing){
   const entry={...original,key,...(focus?{focus}:{}),pages};
   byKey.set(key,entry);result.push(entry);continue;
  }
  if(existing.totalCharacters!==original.totalCharacters)throw Error('MANAGEMENT_EVIDENCE_CONFLICT');
  existing.pages=mergePages([...existing.pages,...pages],existing.totalCharacters);
  existing.omittedCharacters=Math.max(existing.omittedCharacters??0,original.omittedCharacters??0);
  if(Date.parse(original.updatedAt)>Date.parse(existing.updatedAt))existing.updatedAt=original.updatedAt;
 }
 return result;
}
function boundedEntry(entry:any) {
 if(!mergeableEntry(entry))return entry;
 const pages=entry.pages.map((page:Page)=>({...page,text:page.text.slice(0,64000)})).slice(-8);
 const omitted=entry.pages.reduce((n:number,page:Page)=>n+page.text.length,0)-
  pages.reduce((n:number,page:Page)=>n+page.text.length,0);
 return {...entry,pages,omittedCharacters:Math.max(entry.omittedCharacters??0,omitted)};
}

/** Retain exact delivered pages, not model-generated recollections. Repeated or
 * overlapping pages coalesce only when their bytes agree; attempts never mix. */
export async function retainManagementEvidence(db:DB,goalId:string,recovered:any,readArtifact?:ArtifactReader,focus?:Focus) {
 const previous=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',[ledgerKey(goalId)])).rows[0]?.value;
 const entries:any[]=coalescedEntries(decodedEntries(previous));
 for(const result of recovered.results ?? []) {
  if(result.status!=='OK' || typeof result.text!=='string' || !/^[a-f0-9]{64}$/.test(result.digest??'') ||
    !Number.isSafeInteger(result.totalCharacters)||result.totalCharacters<0||
    (!result.runId&&!scopedKinds.has(result.request?.kind))) continue;
  // Decision pages and immutable sections have their own source identity, not an invented run.
  // A focus binding keeps another case's explicit requests out of this cache.
  const boundFocus=scopedKinds.has(result.request.kind)?stableFocus(focus):undefined;
  if(result.request.kind==='live_task_instruction'){
   if(focus?.kind!=='live_failure'||result.request.goalId!==goalId||
     !sameLiveInstructionActor(result.request,focus)||!managementContextRequests.element.safeParse(result.request).success)continue;
   // Revalidate the exact frozen source before caching model-visible bytes.
   // A run ending meanwhile makes this page unavailable, never authoritative.
   const fresh=await readManagementRetrievalRequest(db,goalId,noArtifacts,result.request);
   if(fresh.status!=='OK')continue;
   if(fresh.runId!==result.runId||fresh.digest!==result.digest||fresh.snapshotDigest!==result.snapshotDigest||
     fresh.totalCharacters!==result.totalCharacters||result.artifactId!==undefined||
     typeof fresh.text!=='string'||!fresh.text.startsWith(result.text))throw Error('MANAGEMENT_EVIDENCE_CONFLICT');
  }
  if(result.request.kind==='context_section'||result.request.kind==='candidate_validation') {
   // A section has no invented run. Retain only the exact same-goal page and
   // decision focus; neither caller-supplied bytes nor another case can bind it.
   if(!boundFocus || !managementContextRequests.element.safeParse(result.request).success)continue;
   const fresh=await readManagementRetrievalRequest(db,goalId,noArtifacts,result.request);
   if(fresh.status!=='OK'||fresh.digest!==result.digest||fresh.totalCharacters!==result.totalCharacters||
     result.runId!==undefined||result.artifactId!==undefined||fresh.snapshotDigest!==result.snapshotDigest||
     typeof fresh.text!=='string'||!fresh.text.startsWith(result.text))throw Error('MANAGEMENT_EVIDENCE_CONFLICT');
  }
  const key=identity(result,boundFocus);
  let offset=result.request.offset ?? 0,text=result.text;
  if(!Number.isSafeInteger(offset)||offset<0||offset+text.length>result.totalCharacters)continue;
  // A page request already read and verified the whole immutable artifact.
  // Cache that bounded source so later sessions need not reconstruct it from
  // serial pages; larger sources retain only the verified delivered interval.
  if(readArtifact && result.artifactId) {
   const {bytes}=await readArtifact(result.artifactId);
   if(hash(bytes)!==result.digest) throw Error('MANAGEMENT_EVIDENCE_CONFLICT');
   const source=bytes.toString('utf8');
   if(source.length<=64000) {offset=0;text=source;}
  }
  let entry=entries.find(item=>mergeableEntry(item)&&item.key===key);
  if(!entry) {entry={key,request:result.request,runId:result.runId,artifactId:result.artifactId,
    digest:result.digest,snapshotDigest:result.snapshotDigest,totalCharacters:result.totalCharacters,
    ...(boundFocus?{focus:boundFocus}:{}),pages:[]};entries.push(entry);}
  if(entry.totalCharacters!==result.totalCharacters)throw Error('MANAGEMENT_EVIDENCE_CONFLICT');
  const merged=mergePages([...entry.pages,{offset,text}],entry.totalCharacters);
  // Durable artifacts/sections retain everything. The cache stores bounded pages;
  // a huge source exposes explicit omissions rather than growing indefinitely.
  entry.pages=merged;
  entry.updatedAt=new Date().toISOString();
 }
 entries.sort((a,b)=>Date.parse(b.updatedAt)-Date.parse(a.updatedAt));
 // A legacy union can exceed the per-source bounds even without a new page
 // for that source. Apply the existing limits to every persisted entry.
 let kept=entries.slice(0,24).map(boundedEntry);
 while(JSON.stringify(storedEntries(kept)).length>192000) kept.pop();
 await db.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
  [ledgerKey(goalId),JSON.stringify({version:2,entries:storedEntries(kept),fullSourcesPreserved:true})]);
}

export async function readManagementEvidenceLedger(db:DB,goalId:string,scope?:string,focus?:Focus,readArtifact?:ArtifactReader) {
 const ledger=(await db.query('SELECT value FROM settings WHERE id=$1',[ledgerKey(goalId)])).rows[0]?.value;
 const result:any={version:2,characterLimit:24000,entries:[],omitted:0,fullSourcesPreserved:true,
  guide:'Only currently revalidated exact pages are included. availableRanges describe cached bytes, not pages necessarily included in this prompt or understood by a model. Reuse included pages; request only a missing range needed for the decision. Reading never grants authority or accepts a delivery.'};
 for(const entry of coalescedEntries(decodedEntries(ledger))) {
  if(!managementContextRequests.element.safeParse(entry?.request).success||!Array.isArray(entry.pages)||
    !entry.pages.every((page:any)=>Number.isSafeInteger(page?.offset)&&page.offset>=0&&typeof page.text==='string'))continue;
  if(entry.focus&&!sameFocus(entry.focus,focus))continue;
  const liveInstruction=entry.request.kind==='live_task_instruction';
  if(liveInstruction && (focus?.kind!=='live_failure'||entry.request.goalId!==goalId||
    !sameLiveInstructionActor(entry.request,focus)||!entry.focus))continue;
  if(scopedKinds.has(entry.request.kind)) {
   // Revalidate through the ordinary goal-scoped reader: immutable sections
   // verify their stored bytes; mutable sources keep current attempt, live,
   // case-array and planning checks. Cache reuse does not grant authority.
   if(entry.request.kind==='context_section'&&!entry.focus)continue;
   if(!entry.focus&&scope&&!entry.request.taskId)continue;
   const fresh=await readManagementRetrievalRequest(db,goalId,readArtifact??noArtifacts,
    {...entry.request,offset:0,...(liveInstruction?{snapshotDigest:entry.digest}:{})});
   if(fresh.status!=='OK'||fresh.digest!==entry.digest||fresh.totalCharacters!==entry.totalCharacters||
     fresh.runId!==entry.runId||fresh.snapshotDigest!==entry.snapshotDigest)continue;
  }
  // Only the dedicated frozen-live-instruction reader grants this exception.
  // Generic task/artifact caches keep their existing stopped-source rule.
  if(entry.request.taskId && !liveInstruction) {
   const task=(await db.query(`SELECT t.attempt_count,r.id run_id,
   EXISTS(SELECT 1 FROM runs l WHERE l.task_id=t.id AND l.state IN('queued','running','cancelling','reconciling')) live
   FROM tasks t LEFT JOIN LATERAL(SELECT id FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
   WHERE t.id=$1 AND t.goal_id=$2 AND ($3::text IS NULL OR t.execution->>'baseWorkspace'=$3)`,
   [entry.request.taskId,goalId,scope ?? null])).rows[0];
   if(!task || task.live || task.attempt_count!==entry.request.expectedAttempt || (task.run_id??null)!==(entry.runId??null)) continue;
  }
  const delivered:any={...entry,pages:entry.pages.map((page:any)=>({...page})),
   availableRanges:entry.pages.map((page:any)=>({offset:page.offset,characters:page.text.length})),
   complete:entry.pages.length===1 && entry.pages[0].offset===0 && entry.pages[0].text.length===entry.totalCharacters};
  result.entries.push(delivered);
  // A coalesced source larger than the prompt budget must not disappear as a
  // whole page. Keep an exact bounded interval with an explicit continuation.
  // Leave room for omission metadata before determining the final byte range.
  let omitted=false;
  while(JSON.stringify(result).length>23500 && delivered.pages.length) {
   const page=delivered.pages.at(-1),excess=JSON.stringify(result).length-23500;
   if(page.text.length>excess+256)page.text=page.text.slice(0,page.text.length-excess);
   else delivered.pages.pop();
   omitted=true;
  }
  if(omitted) {
   delivered.complete=false;
   delivered.omittedPages=entry.pages.length-delivered.pages.length;
   delivered.omittedCachedCharacters=entry.pages.reduce((n:number,p:any)=>n+p.text.length,0)-
    delivered.pages.reduce((n:number,p:any)=>n+p.text.length,0);
   const last=delivered.pages.at(-1);
   delivered.continueRequest={...entry.request,offset:last?last.offset+last.text.length:entry.pages[0]?.offset??0};
  }
  if(!delivered.pages.length||JSON.stringify(result).length>24000) {result.entries.pop();result.omitted++;}
 }
 return result;
}
