// Deliberately narrow, evidence-based detector. Silence, age and heartbeats are
// never evidence of a stall. No shell command is executed by this module.
import {createHash} from 'node:crypto';
import {open, realpath} from 'node:fs/promises';
import {constants} from 'node:fs';
import path from 'node:path';

export const PROGRESS_POLICY = 'failed-evidence-poll-v1';
export const progressDigest = value => createHash('sha256').update(value).digest('hex');
const sensitive = /(?:\.env\b|credentials?|auth\.json|id_rsa|private[_-]?key)/i;
const fail = code => {throw Object.assign(Error(code),{code});};
const textDecoder = new TextDecoder('utf-8',{fatal:true});
function pollPath(value) {
  const file=value.replace(/^\/workspace\//,'').replace(/^\.\//,'');
  if(file.startsWith('/')||file.split('/').some(p=>!p||p==='.'||p==='..')||!file.includes('/')||sensitive.test(file))return null;
  return file;
}
export function parseReadOnlyPoll(command) {
  if(typeof command!=='string'||command.length>2048||sensitive.test(command))return null;
  // This is a grammar, not shell evaluation: the only pipeline allowed counts
  // matching processes. Its count is observational, never proof of termination.
  // The absolute tail path makes this form independent of a previous cwd.
  const composite=command.trim().match(/^(?:sleep (?:\d{1,4}(?:\.\d{1,3})?);\s*)?ps aux \| grep ([A-Za-z0-9_][A-Za-z0-9_.:-]{0,79}) \| grep -v grep \| wc -l;\s*tail (?:-n |\-)([1-9]\d{0,2}) (\/workspace\/[A-Za-z0-9_./-]+)$/);
  if(composite) {
    const file=pollPath(composite[3]);
    return file?{commandDigest:progressDigest(command.trim()),path:file,mode:'process-count-tail',
      pattern:null,lines:Number(composite[2])}:null;
  }
  // Require an explicit cwd, even when a previous shell command changed it.
  const match=command.trim().match(/^(?:sleep (?:\d{1,4}(?:\.\d{1,3})?);\s*)?cd \/workspace && (grep -c (?:(?:"([A-Za-z0-9 _:-]{1,80})")|(?:'([A-Za-z0-9 _:-]{1,80})')|([A-Za-z0-9_:-]{1,80}))|wc -l|tail -n ([1-9]\d{0,2})) ([A-Za-z0-9_./-]+)$/);
  if(!match)return null;
  const file=pollPath(match[6]);
  if(!file)return null;
  return {commandDigest:progressDigest(command.trim()),path:file,
    mode:match[1].startsWith('grep')?'grep-count':match[1]==='wc -l'?'line-count':'tail',
    pattern:match[2]??match[3]??match[4]??null,lines:match[5]?Number(match[5]):null};
}
const time = value => {const n=typeof value==='number'?value:Date.parse(value);return Number.isFinite(n)&&n>0?n:null;};
function completedOutput(result) {
  if(result.isError||result.truncated||result.partial||!Array.isArray(result.content)||!result.content.length)return null;
  if(result.content.some(b=>b.type!=='text'||typeof b.text!=='string'||b.truncated||b.partial))return null;
  const output=result.content.map(b=>b.text).join('\n');
  if(output.length>65536||/\b(truncated|partial output|timed out|out of memory|omitted)\b/i.test(output))return null;
  const footer=output.match(/\n?\[Command finished with exit code 0\]\s*$/);
  if(!footer)return null;
  return {digest:progressDigest(output),body:output.slice(0,footer.index).replace(/\n$/,'')};
}
export function analyzePollTrace(text) {
  let rows;
  try {rows=text.split('\n').filter(Boolean).map(line=>JSON.parse(line));}catch{return null;}
  let completed=[],pending=null,lastSequence=-1,lastTime=0;
  const seen=new Set();
  for(const row of rows) {
    if(row.type==='agent/inbox/spliced'||row.type==='turn/end'){completed=[];pending=null;continue;}
    if(!['tool/call','tool/result'].includes(row.type))continue;
    const seq=row.seq,at=time(row.time);
    if(!Number.isSafeInteger(seq)||seq<=lastSequence||!at||at<lastTime)return null;
    lastSequence=seq;lastTime=at;
    if(row.type==='tool/call') {
      const data=row.data;let args;
      try {args=JSON.parse(data?.arguments);}catch{}
      const poll=data?.name==='bash'?parseReadOnlyPoll(args?.command):null;
      if(pending||!poll||typeof data?.callId!=='string'||seen.has(data.callId)) {
        completed=[];pending=null;
        if(!poll||typeof data?.callId!=='string'||seen.has(data.callId))continue;
      }
      seen.add(data.callId);
      if(completed.length&&completed.at(-1).poll.commandDigest!==poll.commandDigest)completed=[];
      pending={id:data.callId,sequence:seq,startedAt:at,poll};
    } else {
      const data=row.data,blocks=data?.message?.content;
      if(data?.truncated||data?.partial||!Array.isArray(blocks)||blocks.length!==1||blocks[0].type!=='tool-result'||!pending||blocks[0].toolCallId!==pending.id) {
        completed=[];pending=null;continue;
      }
      const output=completedOutput(blocks[0]);
      if(!output){completed=[];pending=null;continue;}
      if(completed.length&&completed.at(-1).output.digest!==output.digest)completed=[];
      completed.push({...pending,finishedAt:at,resultSequence:seq,output});
      completed=completed.slice(-8);pending=null;
    }
  }
  if(completed.length<4||completed.at(-1).finishedAt-completed[0].finishedAt<60000)return null;
  // One unfinished identical read-only poll is harmless and is NOT counted.
  if(pending&&pending.poll.commandDigest!==completed[0].poll.commandDigest)return null;
  return {completed,pending,poll:completed[0].poll};
}
export async function readProgressFile(file,root,maxBytes) {
  if(!path.isAbsolute(file)||!file.startsWith(root+'/')||(await realpath(file))!==file)fail('UNSAFE_PROGRESS_PATH');
  const h=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
  try {
    if(await realpath(`/proc/self/fd/${h.fd}`)!==file)fail('UNSAFE_PROGRESS_PATH');
    const before=await h.stat();
    if(!before.isFile()||before.size>maxBytes)fail('PROGRESS_FILE_UNAVAILABLE');
    const bytes=Buffer.alloc(before.size);let offset=0;
    while(offset<bytes.length) {const n=await h.read(bytes,offset,bytes.length-offset,offset);if(!n.bytesRead)fail('PROGRESS_FILE_CHANGED');offset+=n.bytesRead;}
    const after=await h.stat();
    for(const k of ['dev','ino','size','mtimeMs','ctimeMs'])if(before[k]!==after[k])fail('PROGRESS_FILE_CHANGED');
    return {text:textDecoder.decode(bytes),digest:progressDigest(bytes),size:after.size,
      mtimeMs:after.mtimeMs,ctimeMs:after.ctimeMs,dev:after.dev,ino:after.ino};
  }finally{await h.close();}
}
export function sameProgressFile(a,b) {
  return ['digest','size','mtimeMs','ctimeMs','dev','ino'].every(k=>a[k]===b[k]);
}
export function proveFailedPolls(window,file,trace) {
  if(!window)return null;
  const {completed,pending,poll}=window,first=completed[0],last=completed.at(-1);
  // ctime cannot be backdated with utimes: the evidence must predate the whole
  // completed observation window, rather than simply exist at inspection time.
  if(file.mtimeMs>first.startedAt||file.ctimeMs>first.startedAt)return null;
  const failures=file.text.split('\n').filter(line=>/^(?:CASE FAIL(?:\s|:|\u2014)|not ok \d+(?:\s|$)|FAIL [^\s])/.test(line));
  if(!failures.length)return null;
  let expected;
  const lines=file.text.split('\n');
  if(poll.mode==='grep-count')expected=String(lines.filter(line=>line.includes(poll.pattern)).length);
  else if(poll.mode==='line-count')expected=String((file.text.match(/\n/g)??[]).length);
  else expected=(file.text.endsWith('\n')?lines.slice(0,-1):lines).slice(-poll.lines).join('\n');
  // wc can include the filename. Neither an acknowledgment nor partial tool
  // output is a completed observation of the referenced file.
  let observed=last.output.body;
  if(poll.mode==='process-count-tail') {
    // The whole result digest must already be identical across completed calls.
    // Consume exactly one wc count, then compare every remaining byte with tail.
    // No assumption is made about whether the counted test process is alive.
    const count=observed.match(/^[ \t]*\d{1,6}[ \t]*\n/);
    if(!count)return null;
    observed=observed.slice(count[0].length);
  }
  if(poll.mode==='line-count'?!new RegExp('^\\s*'+expected+'(?:\\s+'+poll.path.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+')?\\s*$').test(observed):observed!==expected)return null;
  return {completedPolls:completed.length,firstStartedAt:new Date(first.startedAt).toISOString(),
    firstCompletedAt:new Date(first.finishedAt).toISOString(),lastCompletedAt:new Date(last.finishedAt).toISOString(),
    firstSequence:first.sequence,lastSequence:last.resultSequence,callIds:completed.map(c=>c.id),
    commandDigest:poll.commandDigest,resultDigest:last.output.digest,
    pendingPollId:pending?.id??null,pendingPollSequence:pending?.sequence??null,
    file:{path:poll.path,digest:file.digest,size:file.size,mtimeMs:file.mtimeMs,ctimeMs:file.ctimeMs,dev:file.dev,ino:file.ino},
    failure:{kind:'explicit-test-failure',matches:failures.length,digest:progressDigest(failures.join('\n'))},
    trace:{digest:trace.digest,size:trace.size,mtimeMs:trace.mtimeMs}};
}
