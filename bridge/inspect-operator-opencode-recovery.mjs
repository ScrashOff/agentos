// Administrative identity inspection only: no signals, DB, model or raw prompt.
import {readFile,realpath} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify,isDeepStrictEqual} from 'node:util';
import {pathToFileURL} from 'node:url';
import {readProgressFile} from './runtime-progress.mjs';

const exec=promisify(execFile);
export const operatorOpenCodeRunRoot='/var/lib/agentos-bridge/worker-runs';
export const operatorOpenCodeSourceFiles=Object.freeze(['scripts/opencode-batch.ts','server/run-heartbeat.ts',
 'server/service.ts','server/native-supervision.ts','server/bridge-path.ts',
 'bridge/opencode-executor.mjs','bridge/process-stop.mjs']);
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const digest=/^[a-f0-9]{64}$/;
const release=/^\/opt\/agentos\/releases\/managed-[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const fail=code=>{throw Object.assign(Error(code),{code});};
const keys=(value,names)=>value&&typeof value==='object'&&!Array.isArray(value)&&
 Object.keys(value).sort().join(',')===[...names].sort().join(',');
function liveProcess(value,unit,group){
 return keys(value,['unit','mainPid','startTicks','activeState','subState','controlGroup'])&&
 value.unit===unit&&value.controlGroup===group&&Number.isSafeInteger(value.mainPid)&&value.mainPid>0&&
 typeof value.startTicks==='string'&&/^\d+$/.test(value.startTicks)&&value.activeState==='active'&&value.subState==='running';
}
/** Validates the closed data shape, not its administrative authority. The
 * server must bind these arguments to its current frozen reservation/fence. */
export function validateOperatorOpenCodeIdentity(value){
 if(!keys(value,['version','runId','runFence','contextVersion','workspace','state','runtime','stateDigest',
  'checkedAt','stateIdentityFields','process','owner'])||value.version!==1||value.runtime!=='opencode'||value.state!=='running'||
  ![value.runId,value.runFence,value.contextVersion].every(v=>uuid.test(v??''))||
  !/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(value.workspace??'')||!digest.test(value.stateDigest??'')||
  typeof value.checkedAt!=='string'||!Number.isFinite(Date.parse(value.checkedAt))||
  !keys(value.stateIdentityFields,['runFence','contextVersion'])||
  !Object.values(value.stateIdentityFields).every(v=>typeof v==='boolean'))fail('OPENCODE_RECOVERY_IDENTITY_INVALID');
 const processUnit=`agentos-run-${value.runId}.service`,ownerUnit=`agentos-native-supervisor-${value.runId}.service`;
 if(!liveProcess(value.process,processUnit,'/system.slice/'+processUnit)||
  !keys(value.owner,['unit','mainPid','startTicks','activeState','subState','controlGroup','candidate','sourceHashes'])||
  !release.test(value.owner.candidate??'')||!keys(value.owner.sourceHashes,operatorOpenCodeSourceFiles)||
  !Object.values(value.owner.sourceHashes).every(v=>digest.test(v)))fail('OPENCODE_RECOVERY_IDENTITY_INVALID');
 const {candidate,sourceHashes,...owner}=value.owner;
 if(!liveProcess(owner,ownerUnit,'/agentos_native_supervisors.slice/'+ownerUnit))fail('OPENCODE_RECOVERY_IDENTITY_INVALID');
 return value;
}
async function processIdentity(unit,controlGroup){
 const {stdout}=await exec('systemctl',['show',unit,'--property=ActiveState,SubState,MainPID,ControlGroup'],
  {timeout:4000,maxBuffer:8192,env:{PATH:'/usr/bin:/bin',LANG:'C.UTF-8'}});
 const props=Object.fromEntries(stdout.trim().split('\n').map(line=>{const p=line.indexOf('=');return [line.slice(0,p),line.slice(p+1)];}));
 const mainPid=Number(props.MainPID);
 if(props.ActiveState!=='active'||props.SubState!=='running'||!Number.isSafeInteger(mainPid)||mainPid<1||
  props.ControlGroup!==controlGroup)fail('OPENCODE_RECOVERY_PROCESS_CHANGED');
 const stat=await readFile(`/proc/${mainPid}/stat`,'utf8'),startTicks=stat.slice(stat.lastIndexOf(')')+2).trim().split(/\s+/)[19];
 if(!/^\d+$/.test(startTicks??'')||!(await readFile(`/proc/${mainPid}/cgroup`,'utf8')).trim().split('\n').includes('0::'+controlGroup))
  fail('OPENCODE_RECOVERY_PROCESS_CHANGED');
 return {unit,mainPid,startTicks,activeState:props.ActiveState,subState:props.SubState,controlGroup};
}
export async function inspectOperatorOpenCodeRecovery(runId,workspace,runFence,contextVersion,candidate,io={}){
 if(![runId,runFence,contextVersion].every(v=>uuid.test(v??''))||
  !/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(workspace??'')||!release.test(candidate??''))fail('OPENCODE_RECOVERY_ARGUMENTS_INVALID');
 const read=io.read??readProgressFile,inspect=io.inspect??processIdentity,resolve=io.realpath??realpath;
 const sample=async()=>{
  if(await resolve(candidate)!==candidate)fail('OPENCODE_RECOVERY_SOURCE_CHANGED');
  const file=await read(`${operatorOpenCodeRunRoot}/${runId}/state.json`,operatorOpenCodeRunRoot,262144);
  const state=JSON.parse(file.text);
  if(state.runId!==runId||state.workspace!==workspace||state.runtime!=='opencode'||state.state!=='running'||
   (Object.hasOwn(state,'runFence')&&state.runFence!==runFence)||
   (Object.hasOwn(state,'contextVersion')&&state.contextVersion!==contextVersion))fail('OPENCODE_RECOVERY_STATE_CHANGED');
  const processUnit=`agentos-run-${runId}.service`,ownerUnit=`agentos-native-supervisor-${runId}.service`;
  const process=await inspect(processUnit,'/system.slice/'+processUnit);
  const owner=await inspect(ownerUnit,'/agentos_native_supervisors.slice/'+ownerUnit);
  if(await resolve(`/proc/${owner.mainPid}/cwd`)!==candidate)fail('OPENCODE_RECOVERY_OWNER_CHANGED');
  const sourceHashes={};for(const name of operatorOpenCodeSourceFiles)sourceHashes[name]=(await read(candidate+'/'+name,candidate,1048576)).digest;
  return {runId,runFence,contextVersion,workspace,state:'running',runtime:'opencode',stateDigest:file.digest,
   stateIdentityFields:{runFence:Object.hasOwn(state,'runFence'),contextVersion:Object.hasOwn(state,'contextVersion')},
   process,owner:{...owner,candidate,sourceHashes}};
 };
 const first=await sample(),second=await sample();
 if(!isDeepStrictEqual(first,second))fail('OPENCODE_RECOVERY_OBSERVATION_CHANGED');
 return validateOperatorOpenCodeIdentity({version:1,...second,checkedAt:new Date().toISOString()});
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 try{if(process.argv.length!==7)fail('OPENCODE_RECOVERY_ARGUMENTS_INVALID');
  console.log(JSON.stringify(await inspectOperatorOpenCodeRecovery(...process.argv.slice(2))));}
 catch{console.error(JSON.stringify({code:'OPENCODE_RECOVERY_INSPECTION_FAILED'}));process.exitCode=1;}
}
