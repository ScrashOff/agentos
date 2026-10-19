// Observations for semantic review only. This module never stops an actor,
// executes a command, or treats a worker-authored report as an acceptance.
import {progressDigest} from './runtime-progress.mjs';
export const STAGE_POLICY='runtime-stage-observation-v1';
const sensitive=/(?:\.env\b|credentials?|auth\.json|id_rsa|private[_-]?key)/i;
export function stagePath(raw){
 if(typeof raw!=='string'||raw.length>240||sensitive.test(raw))return null;
 const value=raw.replace(/^\/workspace\//,'');
 return /^(?:data|evidence)\/[A-Za-z0-9_./-]+$/.test(value)&&!value.split('/').some(p=>!p||p==='.'||p==='..')?value:null;
}
export function observedWaitPath(command){
 if(typeof command!=='string'||command.length>2048||sensitive.test(command))return null;
 // Explicit read-only grammar. Date/process counts are not progress signals.
 // Recognizing this grammar grants observation only, never stop authority.
 const match=command.trim().replaceAll('\\"','"').match(/^(?:date;\s*)?sleep \d{1,4}(?:\.\d{1,3})?;\s*cat (\/workspace\/[A-Za-z0-9_./-]+)(?: 2>\/dev\/null)?;\s*ps aux\s*\|\s*grep -c "\[[A-Za-z]\][A-Za-z0-9_.:-]{0,79}";\s*date$/);
 return match?stagePath(match[1]):null;
}
export function stageTrace(text){
 let rows;try{rows=text.split('\n').filter(Boolean).map(line=>JSON.parse(line));}catch{return null;}
 let sequence=-1,time=0,pending=null,completed=[],lastSequence=0;
 const seen=new Set();
 for(const row of rows){
  if(['agent/inbox/spliced','turn/end'].includes(row.type)){pending=null;completed=[];continue;}
  if(!['tool/call','tool/result'].includes(row.type))continue;
  const at=typeof row.time==='number'?row.time:Date.parse(row.time);
  if(!Number.isSafeInteger(row.seq)||row.seq<=sequence||!Number.isFinite(at)||at<=0||at<time)return null;
  sequence=row.seq;lastSequence=row.seq;time=at;
  if(row.type==='tool/call'){
   let args;try{args=JSON.parse(row.data?.arguments);}catch{}
   const file=row.data?.name==='bash'?observedWaitPath(args?.command):null;
   const id=row.data?.callId;
   if(!file||typeof id!=='string'||!/^[A-Za-z0-9_.:-]{1,200}$/.test(id)||seen.has(id)){pending=null;completed=[];continue;}
   seen.add(id);
   const commandDigest=progressDigest(args.command.trim());
   if(pending||completed.length&&completed.at(-1).commandDigest!==commandDigest)completed=[];
   pending={callId:id,commandDigest,path:file,startedAt:at,sequence:row.seq};
  }else{
   const blocks=row.data?.message?.content,block=blocks?.[0];
   if(!pending||row.data?.truncated||row.data?.partial||!Array.isArray(blocks)||blocks.length!==1||block.type!=='tool-result'||block.toolCallId!==pending.callId||block.isError||block.truncated||block.partial||!Array.isArray(block.content)||!block.content.length||block.content.some(b=>b.type!=='text'||typeof b.text!=='string'||b.partial||b.truncated)){pending=null;completed=[];continue;}
   const output=block.content.map(b=>b.text).join('\n');
   // SDK versions differ: newer bash returns plain text on ordinary completion.
   // Count complete tool returns only; neither format proves a process stopped.
   // This observation MUST NOT be admitted to the separate stop-policy path.
   const explicitExit=output.match(/\[(?:Command finished with exit code |exit code: )(-?\d+)\]/);
   if(output.length>65536||explicitExit&&explicitExit[1]!=='0'||/\b(?:truncated|partial output|timed out|omitted|killed by signal)\b/i.test(output)){pending=null;completed=[];continue;}
   completed.push({...pending,finishedAt:at,resultSequence:row.seq});pending=null;
  }
 }
 return {lastSequence,completed,pending};
}
export function waitObservation(window,file,now=Date.now()){
 const calls=window?.completed;if(!calls||calls.length<4)return null;
 const first=calls[0],last=calls.at(-1);
 if(last.finishedAt>now||last.finishedAt-first.finishedAt<60000||file.mtimeMs>first.startedAt||file.ctimeMs>first.startedAt)return null;
 const report={path:first.path,digest:file.digest,size:file.size,mtimeMs:file.mtimeMs,passed:null,exitCode:null};
 return {category:'unproductive_wait',evidenceDigest:progressDigest(JSON.stringify([first.commandDigest,report.path,report.digest])),
  callId:last.callId,toolName:'bash',wait:{commandDigest:first.commandDigest,completedCalls:calls.length,
   firstCompletedAt:new Date(first.finishedAt).toISOString(),lastCompletedAt:new Date(last.finishedAt).toISOString()},reports:[report]};
}
function entropy(text){
 const frequencies=new Map();for(const c of text)frequencies.set(c,(frequencies.get(c)??0)+1);
 let score=0;for(const n of frequencies.values()){const p=n/text.length;score-=p*Math.log2(p);}return score;
}
function secretLikeName(text){
 if(/\bBearer\s+\S+|\b(?:password|passwd|pwd|secret|token|authorization|api[ _-]?key)\s*[:=]\s*\S+/i.test(text)||
    /\b(?:password|passwd|pwd|secret|authorization|api[ _-]?key)\s+(?:is\s+)?\S+/i.test(text)||
    /\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\b/.test(text)||
    /\b(?:sk-|sk_|ghp_|gho_|ghu_|ghs_|github_pat_|xox[baprs]-|AKIA|ASIA)[A-Za-z0-9_-]{8,}/.test(text))return true;
 for(const token of text.match(/[A-Za-z0-9+/_=-]{24,}/g)??[]){
  if(/^[a-f0-9]{32,}$/i.test(token))return true;
  const classes=[/[a-z]/,/[A-Z]/,/[0-9]/].filter(pattern=>pattern.test(token)).length;
  if((classes===3&&entropy(token)>=3.6)||(token.length>=32&&classes>=2&&entropy(token)>=4.2))return true;
 }
 return false;
}
/** Labels are untrusted data. Only a bounded sanitized case label is projected;
 * error text, commands, defects and limitations never become prompt material. */
export function stageCaseName(raw,index){
 if(typeof raw!=='string'||!raw.trim())return {name:'case_'+(index+1),truncated:false};
 let text=raw.normalize('NFKC').replace(/[\u0000-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e\u2060-\u206f]/g,'');
 let changed=text!==raw;
 if(secretLikeName(text))return {name:'[redacted case name]',truncated:true};
 const redacted=text.replace(/\b[a-z][a-z0-9+.-]{1,15}:\/\/[^\s<>]+/gi,'[redacted URL]')
  .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi,'[redacted email]')
  .replace(/\b(?:password|passwd|pwd|secret|authorization|api[ _-]?key)\s+(?:is\s+)?[^\s]+/gi,'[redacted credential]');
 changed||=redacted!==text;text=redacted.replace(/\s+/g,' ').trim();
 if(!text)return {name:'case_'+(index+1),truncated:true};
 if(text.length>160){text=text.slice(0,160);changed=true;}
 return {name:text,truncated:changed};
}
function detailClass(detail){
 if(typeof detail!=='string'||!detail)return 'unclassified';
 const text=detail.slice(0,65536);
 // Lexical categories only: they describe report text, not an inferred cause.
 if(/\b(?:timeout|timed out|deadline|ETIMEDOUT)\b/i.test(text))return 'timeout';
 if(/\b(?:ENOENT|not found|no such file|missing (?:input|file|artifact|resource))\b/i.test(text))return 'missing_input';
 if(/\b(?:TypeError|ReferenceError|SyntaxError|EACCES|EPERM|SIG[A-Z]+|process (?:exited|failed)|exit(?:Code| code)\s*[:=]?\s*[1-9])\b/i.test(text))return 'process_error';
 if(/\b(?:assert(?:ion)?(?:error)?|mismatch|expected|not equal|actual)\b/i.test(text))return 'assertion_mismatch';
 return 'unclassified';
}
const declaredCount=value=>Number.isSafeInteger(value)&&value>=0&&value<=1000000000?value:null;
export function reportFacts(report){
 if(!report||typeof report!=='object'||Array.isArray(report))return null;
 const cases=Array.isArray(report.cases)?report.cases:null;
 const totals=report.totals&&typeof report.totals==='object'&&!Array.isArray(report.totals)?report.totals:null;
 const reported=totals?{total:declaredCount(totals.cases),passed:declaredCount(totals.passed),failed:declaredCount(totals.failed)}:null;
 const reportedCounts=reported&&Object.values(reported).some(value=>value!==null)?reported:null;
 // A bare status flag does not turn every old stub into a new material event.
 if(!cases?.length&&!reportedCounts)return null;
 const examined=Math.min(cases?.length??0,2000);
 const observedCounts=cases?{total:cases.length,examined,passed:0,failed:0,unknown:0}:null;
 const failedCases=[];let truncated=!!cases&&examined<cases.length;
 for(let index=0;index<examined;index++){
  const entry=cases[index];
  if(entry?.ok===true)observedCounts.passed++;
  else if(entry?.ok===false){
   observedCounts.failed++;
   if(failedCases.length>=6){truncated=true;continue;}
   const label=stageCaseName(entry.name,index);truncated||=label.truncated;
   failedCases.push({index,name:label.name,ok:false,detailClass:detailClass(entry.detail),
    detailDigest:typeof entry.detail==='string'?progressDigest(entry.detail):null});
  }else observedCounts.unknown++;
 }
 let inconsistency=false;
 if(observedCounts&&reportedCounts){
  if(reportedCounts.total!==null&&reportedCounts.total!==observedCounts.total)inconsistency=true;
  if(observedCounts.examined===observedCounts.total)
   for(const key of ['passed','failed'])if(reportedCounts[key]!==null&&reportedCounts[key]!==observedCounts[key])inconsistency=true;
 }
 if(reportedCounts&&Object.values(reportedCounts).every(value=>value!==null)&&
   reportedCounts.total<reportedCounts.passed+reportedCounts.failed)inconsistency=true;
 return {version:1,suiteExecuted:typeof report.suiteExecuted==='boolean'?report.suiteExecuted:null,
  observedCounts,...(reportedCounts?{reportedCounts}:{}),inconsistency,failedCases,truncated};
}
/** Canonical arrays keep bridge/server hashes stable across object reordering. */
export function stageFactsDigest(facts){
 const o=facts.observedCounts,r=facts.reportedCounts;
 return progressDigest(JSON.stringify([facts.version,facts.suiteExecuted,
  o?[o.total,o.examined,o.passed,o.failed,o.unknown]:null,r?[r.total,r.passed,r.failed]:null,
  facts.inconsistency,facts.failedCases.map(c=>[c.index,c.name,c.ok,c.detailClass,c.detailDigest]),facts.truncated]));
}
export function partialReportObservation(relative,file,startedAt){
 if(!stagePath(relative)||file.mtimeMs<startedAt)return null;
 let report;try{report=JSON.parse(file.text);}catch{return null;}
 if(!report||Array.isArray(report))return null;
 const exitCode=Number.isInteger(report.exitCode)&&report.exitCode>=-1&&report.exitCode<=255?report.exitCode:null;
 if(report.passed!==false&&!(exitCode!==null&&exitCode!==0))return null;
 const facts=reportFacts(report);
 return {category:'stage_failure',evidenceDigest:progressDigest(JSON.stringify(facts?[relative,file.digest,stageFactsDigest(facts)]:[relative,file.digest])),
  callId:'report-'+file.digest.slice(0,32),toolName:'stage_report',reports:[{path:relative,digest:file.digest,size:file.size,
   mtimeMs:file.mtimeMs,passed:report.passed===false?false:null,exitCode,...(facts?{facts}:{})}]};
}
