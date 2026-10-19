// Identity only. No signals, raw trace/prompt, workspace scan or model calls.
import {readFile,realpath} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify,isDeepStrictEqual} from 'node:util';
import {pathToFileURL} from 'node:url';
import {readProgressFile} from './runtime-progress.mjs';
const exec=promisify(execFile),ROOT='/var/lib/agentos-bridge/worker-runs';
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const files=['server/run-heartbeat.ts','server/service.ts','server/harness-supervision.ts',
 'server/bridge.ts','server/runtime.ts','server/native-supervision.ts'];
async function processIdentity(unit,cgroup){
 const {stdout}=await exec('systemctl',['show',unit,'-p','ActiveState,SubState,MainPID,ControlGroup'],
  {timeout:4000,maxBuffer:8192,env:{PATH:'/usr/bin:/bin',LANG:'C.UTF-8'}});
 const props=Object.fromEntries(stdout.trim().split('\n').map(s=>{const p=s.indexOf('=');return[s.slice(0,p),s.slice(p+1)];}));
 const pid=Number(props.MainPID);
 if(props.ActiveState!=='active'||props.SubState!=='running'||!Number.isSafeInteger(pid)||pid<1||props.ControlGroup!==cgroup)
  throw Error('OPERATOR_RECOVERY_PROCESS_CHANGED');
 const stat=await readFile(`/proc/${pid}/stat`,'utf8'),startTicks=stat.slice(stat.lastIndexOf(')')+2).trim().split(/\s+/)[19];
 if(!/^\d+$/.test(startTicks??'')||!(await readFile(`/proc/${pid}/cgroup`,'utf8')).trim().split('\n').includes('0::'+cgroup))
  throw Error('OPERATOR_RECOVERY_PROCESS_CHANGED');
 return {unit,mainPid:pid,startTicks,activeState:props.ActiveState,subState:props.SubState,controlGroup:cgroup};
}
export async function inspectOperatorRecovery(runId,workspace,runFence,candidate,io={}){
 if(!uuid.test(runId)||!uuid.test(runFence)||!/^[-a-z0-9_]+\/[-a-z0-9_]+$/.test(workspace)||
  !/^\/opt\/agentos\/releases\/managed-[a-f0-9-]{36}$/.test(candidate))throw Error('OPERATOR_RECOVERY_IDENTITY_INVALID');
 const read=io.read??readProgressFile,inspect=io.inspect??processIdentity,resolve=io.realpath??realpath;
 const sample=async()=>{
  if(await resolve(candidate)!==candidate)throw Error('OPERATOR_RECOVERY_SOURCE_CHANGED');
  const file=await read(`${ROOT}/${runId}/state.json`,ROOT,262144),state=JSON.parse(file.text);
  if(state.runId!==runId||state.runFence!==runFence||state.workspace!==workspace||state.runtime!=='harness'||state.state!=='running')
   throw Error('OPERATOR_RECOVERY_IDENTITY_CHANGED');
  const unit=`agentos-run-${runId}.service`,ownerUnit=`agentos-native-supervisor-${runId}.service`;
  const process=await inspect(unit,'/system.slice/'+unit),owner=await inspect(ownerUnit,'/agentos_native_supervisors.slice/'+ownerUnit);
  if(await resolve(`/proc/${owner.mainPid}/cwd`)!==candidate)throw Error('OPERATOR_RECOVERY_OWNER_CHANGED');
  const sourceHashes={};for(const name of files)sourceHashes[name]=(await read(candidate+'/'+name,candidate,1048576)).digest;
  return {runId,runFence,workspace,state:'running',runtime:'harness',stateDigest:file.digest,process,owner:{...owner,candidate,sourceHashes}};
 };
 const first=await sample(),second=await sample();
 if(!isDeepStrictEqual(first,second))throw Error('OPERATOR_RECOVERY_OBSERVATION_CHANGED');
 return {version:1,...second,checkedAt:new Date().toISOString()};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 try{if(process.argv.length!==6)throw Error('ARGUMENTS_INVALID');
  console.log(JSON.stringify(await inspectOperatorRecovery(...process.argv.slice(2))));}
 catch{console.error(JSON.stringify({code:'OPERATOR_RECOVERY_INSPECTION_FAILED'}));process.exitCode=1;}
}
