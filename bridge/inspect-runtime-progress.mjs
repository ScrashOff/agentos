// Read-only inspection of an AgentOS-owned Harness. No process signals, provider
// requests, private RPC writes or arbitrary paths are accepted by this API/CLI.
import {readFile,writeFile} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify,isDeepStrictEqual} from 'node:util';
import {pathToFileURL} from 'node:url';
import {PROGRESS_POLICY,analyzePollTrace,readProgressFile,sameProgressFile,proveFailedPolls,progressDigest} from './runtime-progress.mjs';
const exec=promisify(execFile),DATA='/var/lib/agentos-bridge/worker-runs',ROOT='/srv/agentos/workspaces';
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const workspaceKey=/^[a-z0-9_-]+\/[a-z0-9_-]+$/;
const fail=code=>{throw Object.assign(Error(code),{code});};
const safeReasons=new Set(['UNSAFE_PROGRESS_PATH','PROGRESS_FILE_UNAVAILABLE','PROGRESS_FILE_CHANGED']);
async function inspectProcess(runId) {
  const unit=`agentos-run-${runId}.service`;
  const {stdout}=await exec('systemctl',['show',unit,'-p','ActiveState,SubState,MainPID,ControlGroup'],
    {timeout:4000,maxBuffer:8192,env:{PATH:'/usr/bin:/bin',LANG:'C.UTF-8'}});
  const props=Object.fromEntries(stdout.trim().split('\n').map(line=>{const i=line.indexOf('=');return[line.slice(0,i),line.slice(i+1)];}));
  const mainPid=Number(props.MainPID),controlGroup=`/system.slice/${unit}`;
  if(props.ActiveState!=='active'||props.SubState!=='running'||!Number.isSafeInteger(mainPid)||mainPid<1||props.ControlGroup!==controlGroup)fail('PROCESS_IDENTITY_UNAVAILABLE');
  const proc=await readFile(`/proc/${mainPid}/stat`,'utf8'),fields=proc.slice(proc.lastIndexOf(')')+2).trim().split(/\s+/);
  const startTicks=fields[19];
  if(!/^\d+$/.test(startTicks??'')||!(await readFile(`/proc/${mainPid}/cgroup`,'utf8')).trim().split('\n').includes(`0::${controlGroup}`))fail('PROCESS_IDENTITY_UNAVAILABLE');
  return {unit,mainPid,activeState:props.ActiveState,subState:props.SubState,startTicks,controlGroup};
}
export async function inspectRuntimeProgress(runId,workspace,io={}) {
  if(!uuid.test(runId)||!workspaceKey.test(workspace))fail('INVALID_PROGRESS_IDENTITY');
  const file=io.readFile??readProgressFile,processReader=io.inspectProcess??inspectProcess;
  const dir=`${DATA}/${runId}`,tracePath=`${dir}/home/sessions/--workspace--/${runId}/session.v3.jsonl`;
  const result={version:1,policy:PROGRESS_POLICY,runId,workspace,checkedAt:new Date().toISOString(),stalled:false,
    reason:'NO_STRONG_REPETITION',evidence:null,process:null,state:null};
  try {
    const stateFile=await file(`${dir}/state.json`,DATA,256*1024),state=JSON.parse(stateFile.text);
    result.state={runtime:state.runtime,state:state.state,runFence:state.runFence??null,digest:stateFile.digest};
    if(state.runId!==runId||state.workspace!==workspace||state.runtime!=='harness'||state.state!=='running')return {...result,reason:'RUN_IDENTITY_OR_STATE_CHANGED'};
    const trace=await file(tracePath,dir,16*1024*1024),window=analyzePollTrace(trace.text);
    if(!window)return result;
    if(window.completed.at(-1).finishedAt>Date.now()||window.pending?.startedAt>Date.now())return {...result,reason:'INVALID_OBSERVATION_TIME'};
    const evidenceFile=await file(`${ROOT}/${workspace}/${window.poll.path}`,`${ROOT}/${workspace}`,4*1024*1024);
    const evidence=proveFailedPolls(window,evidenceFile,trace);
    if(!evidence)return {...result,reason:'NO_UNCHANGED_FAILED_EVIDENCE'};
    const process=await processReader(runId);
    const [stateAfter,traceAfter,fileAfter]=await Promise.all([
      file(`${dir}/state.json`,DATA,256*1024),file(tracePath,dir,16*1024*1024),
      file(`${ROOT}/${workspace}/${window.poll.path}`,`${ROOT}/${workspace}`,4*1024*1024)]);
    if(!sameProgressFile(stateFile,stateAfter)||!sameProgressFile(trace,traceAfter)||!sameProgressFile(evidenceFile,fileAfter))return {...result,reason:'SAMPLE_CHANGED'};
    const processAfter=await processReader(runId);
    if(!isDeepStrictEqual(process,processAfter))return {...result,reason:'PROCESS_CHANGED'};
    return {...result,checkedAt:new Date().toISOString(),stalled:true,reason:'REPEATED_POLL_OF_UNCHANGED_FAILED_EVIDENCE',evidence,process};
  }catch(e){return {...result,checkedAt:new Date().toISOString(),reason:safeReasons.has(e.code)?e.code:'PROGRESS_OBSERVATION_UNAVAILABLE'};}
}
export function sameProgressProof(a,b) {
  return a?.stalled===true&&b?.stalled===true&&a.version===1&&b.version===1&&a.policy===PROGRESS_POLICY&&b.policy===PROGRESS_POLICY&&
    a.runId===b.runId&&a.workspace===b.workspace&&isDeepStrictEqual(a.evidence,b.evidence)&&
    isDeepStrictEqual(a.process,b.process)&&isDeepStrictEqual(a.state,b.state);
}
export async function checkpointRuntimeProgress(proof,identity,signal,io={}) {
  if(!uuid.test(identity?.runId??'')||!uuid.test(identity?.runFence??'')||!workspaceKey.test(identity?.workspace??''))fail('INVALID_PROGRESS_IDENTITY');
  const current=await(io.inspect??inspectRuntimeProgress)(identity.runId,identity.workspace);
  if(signal?.aborted)fail('CANCELLED');
  const age=Date.now()-Date.parse(current.checkedAt);
  if(!sameProgressProof(proof,current)||current.runId!==identity.runId||current.workspace!==identity.workspace||current.state.runFence!==identity.runFence||!Number.isFinite(age)||age<0||age>10000)fail('PROGRESS_PROOF_STALE');
  const checkpoint={version:1,code:'RUNTIME_PROGRESS_STALLED',runId:identity.runId,runFence:identity.runFence,
    workspace:identity.workspace,proof:current};
  const bytes=JSON.stringify(checkpoint),digest=progressDigest(bytes),name='progress-stall.json';
  await(io.writeCheckpoint??(async value=>writeFile(`${DATA}/${identity.runId}/${name}`,value,{flag:'wx',mode:0o600})))(bytes);
  // The caller must still check cancellation before signalling its own runtime.
  return {version:1,name,digest,runId:identity.runId,runFence:identity.runFence,workspace:identity.workspace,policy:PROGRESS_POLICY};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href) {
  try {console.log(JSON.stringify(await inspectRuntimeProgress(process.argv[2],process.argv[3])));}
  catch {console.log(JSON.stringify({error:'INVALID_PROGRESS_IDENTITY'}));process.exitCode=1;}
}
