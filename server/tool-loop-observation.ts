import {createHash} from 'node:crypto';

const hash=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const incomplete=/\[.*(?:omitid|redacted|truncat)|trecho omitido|partial output|timed out/i;
const runtimeDiagnostic=/(?:Error\s+\[|\bthrow new\s+)(ERR_MODULE_NOT_FOUND|ERR_UNKNOWN_FILE_EXTENSION|ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX)\b/g;
export type LoopSignal={version:1;pattern:'identical_returns'|'alternating_returns'|'recurring_runtime_error';
  evidenceDigest:string;materialDigest:string;firstSequence:number;lastSequence:number;completedCalls:number;
  firstStartedAt:string;lastCompletedAt:string;
  fingerprints:{tool:string;inputDigest:string;outputDigest:string;count:number}[];
  runtimeErrors?:{code:string;count:number}[]};

/** Evidence projection only. A suspicion grants no stop, retry, scope or acceptance authority.
 * Long calls, changing external state and incomplete traces stay inconclusive.
 * Repeated known diagnostics can request review even when commands differ. */
export function toolLoopObservation(observation:any):LoopSignal|null{
  if(!observation||observation.unavailable||observation.error||observation.traceTruncated||
    !Array.isArray(observation.actions)||observation.actions.length>100||
    observation.observedAt&&!Number.isFinite(Date.parse(observation.observedAt)))return null;
  for(const field of ['files','evidence'])if(observation[field]!=null&&
    (!Array.isArray(observation[field])||observation[field].length>100||observation[field].some((item:any)=>!item||typeof item!=='object')))return null;
  const completed=[],diagnostics=new Map<number,string[]>();
  let previous=-1,finished=0;
  for(const action of observation.actions){
    if(!action||!['returned','error'].includes(action.state))return null;
    const start=Date.parse(action.startedAt),end=Date.parse(action.finishedAt);
    if(typeof action.startedAt!=='string'||typeof action.finishedAt!=='string'||
      !Number.isSafeInteger(action.sequence)||action.sequence<=previous||!Number.isFinite(start)||
      !Number.isFinite(end)||start<finished||end<start||
      (observation.observedAt&&end>Date.parse(observation.observedAt))||action.partial||action.truncated||
      typeof action.tool!=='string'||!/^[A-Za-z0-9_.:-]{1,70}$/.test(action.tool)||
      typeof action.command!=='string'||action.command.length>1800||incomplete.test(action.command)||
      typeof action.output!=='string'||action.output.length>2600||incomplete.test(action.output))return null;
    previous=action.sequence;finished=end;
    diagnostics.set(action.sequence,[...new Set([...action.output.matchAll(runtimeDiagnostic)].map(match=>match[1]))]);
    completed.push({sequence:action.sequence,startedAt:action.startedAt,finishedAt:action.finishedAt,tool:action.tool,inputDigest:hash(action.command),outputDigest:hash([action.state,action.exitCode??null,action.output])});
  }
  if(completed.length<6)return null;
  const last=completed.at(-1)!;
  const same=(a:typeof last,b:typeof last)=>a.tool===b.tool&&a.inputDigest===b.inputDigest&&a.outputDigest===b.outputDigest;
  const consecutive=completed.slice(-6);
  const diagnosticWindow=completed.slice(-12),counts=new Map<string,number>();
  for(const call of diagnosticWindow)for(const code of diagnostics.get(call.sequence)??[])counts.set(code,(counts.get(code)??0)+1);
  const recurring=[...counts].filter(([,count])=>count>=6).map(([code,count])=>({code,count})).sort((a,b)=>a.code.localeCompare(b.code));
  const runtimeErrors=recurring.length&&diagnosticWindow.slice(-3).some(call=>(diagnostics.get(call.sequence)??[]).some(code=>recurring.some(error=>error.code===code)))?recurring:undefined;
  let window=runtimeErrors?diagnosticWindow:consecutive,pattern:LoopSignal['pattern']=runtimeErrors?'recurring_runtime_error':'identical_returns';
  if(!runtimeErrors&&!consecutive.every(c=>same(c,last))){
    window=completed.slice(-12);pattern='alternating_returns';
    if(window.length!==12)return null;
  }
  const groups=new Map<string,{tool:string;inputDigest:string;outputDigest:string;count:number}>();
  for(const {sequence,startedAt,finishedAt,...call} of window){
    const key=hash(call);const group=groups.get(key);
    if(group)group.count++;else groups.set(key,{...call,count:1});
  }
  if(pattern==='alternating_returns'&&(groups.size!==2||[...groups.values()].some(g=>g.count<3)))return null;
  const fingerprints=[...groups.values()].sort((a,b)=>hash(a).localeCompare(hash(b)));
  // File/evidence metadata can indicate new material, never prove a correct result.
  // Do not treat CPU, executor presence or wall-clock time as advancement.
  const materialDigest=hash({files:(observation.files??[]).map((f:any)=>[f.path,f.status,f.added,f.removed]),
    evidence:(observation.evidence??[]).map((f:any)=>[f.name,f.size,f.modifiedAt])});
  // Diagnostic identity is the known error family, not ever-changing commands,
  // paths, stack locations or retry count. Existing run/fence/context bindings
  // and material/consumption checks coalesce it without a second owner.
  const evidenceDigest=hash(runtimeErrors?[pattern,runtimeErrors.map(error=>error.code)]:[pattern,fingerprints.map(({count,...f})=>f)]);
  return {version:1,pattern,evidenceDigest,materialDigest,...(runtimeErrors?{runtimeErrors}:{}),
    firstSequence:window[0].sequence,lastSequence:last.sequence,firstStartedAt:window[0].startedAt,
    lastCompletedAt:last.finishedAt,completedCalls:window.length,fingerprints};
}
