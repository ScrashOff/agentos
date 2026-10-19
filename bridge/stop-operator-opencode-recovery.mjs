// One explicitly owned administrative stop of the exact sandbox. Never stops
// its native owner, fabricates a terminal receipt, submits output or readmits.
import {open,readFile,realpath} from 'node:fs/promises';
import {constants} from 'node:fs';
import {createHash} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify,isDeepStrictEqual} from 'node:util';
import {setTimeout as delay} from 'node:timers/promises';
import {pathToFileURL} from 'node:url';
import {inspectUnit,stopUnit} from './process-stop.mjs';
import {readProgressFile} from './runtime-progress.mjs';
import {inspectOperatorOpenCodeRecovery,validateOperatorOpenCodeIdentity,operatorOpenCodeRunRoot} from './inspect-operator-opencode-recovery.mjs';
const exec=promisify(execFile),uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const canonical=value=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'
 ?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const digest=value=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const fail=code=>{throw Object.assign(Error(code),{code});};
const sameLive=(a,b)=>{const {checkedAt:at,...av}=a,{checkedAt:bt,...bv}=b;return isDeepStrictEqual(av,bv);};
async function readIntent(filename){
 try{return JSON.parse((await readProgressFile(filename,operatorOpenCodeRunRoot,16384)).text);}
 catch(error){if(error.code==='ENOENT')return null;fail('OPENCODE_RECOVERY_INTENT_UNREADABLE');}
}
async function createIntent(filename,receipt){
 const directory=filename.slice(0,filename.lastIndexOf('/'));
 if(await realpath(directory)!==directory)fail('OPENCODE_RECOVERY_INTENT_PATH');
 let handle;
 try{handle=await open(filename,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);}
 catch(error){if(error.code==='EEXIST')return false;throw error;}
 try{await handle.writeFile(JSON.stringify(receipt)+'\n');await handle.sync();}finally{await handle.close();}
 const parent=await open(directory,constants.O_RDONLY|constants.O_DIRECTORY);
 try{await parent.sync();}finally{await parent.close();}
 return true;
}
async function observeOwner(identity){
 const unit=identity.owner.unit,expected=identity.owner.controlGroup;let stdout;
 try{({stdout}=await exec('systemctl',['show',unit,'--property=ActiveState,SubState,LoadState,MainPID,Job,ControlGroup'],
  {timeout:4000,maxBuffer:4096,env:{PATH:'/usr/bin:/bin',LANG:'C.UTF-8'}}));}
 catch(error){if(error.code!==1||typeof error.stdout!=='string'||!/^LoadState=not-found$/m.test(error.stdout))throw error;stdout=error.stdout;}
 const props=Object.fromEntries(stdout.trim().split('\n').map(line=>{const p=line.indexOf('=');return [line.slice(0,p),line.slice(p+1)];}));
 const mainPid=Number(props.MainPID);if((props.ControlGroup&&props.ControlGroup!==expected)||!Number.isSafeInteger(mainPid)||mainPid<0)
  fail('OPENCODE_RECOVERY_OWNER_CHANGED');
 let cgroupEmpty;
 try{const text=await readFile('/sys/fs/cgroup'+expected+'/cgroup.events','utf8');
  if(!/^populated [01]$/m.test(text))fail('OPENCODE_RECOVERY_OWNER_UNCERTAIN');cgroupEmpty=/^populated 0$/m.test(text);}
 catch(error){if(error.code!=='ENOENT')throw error;cgroupEmpty=true;}
 return {unit,checkedAt:new Date().toISOString(),loadState:props.LoadState,activeState:props.ActiveState,
  subState:props.SubState,mainPid,pendingJob:!['','0'].includes(props.Job??'unknown'),cgroupEmpty};
}
async function actorReceipt(identity){
 try{
  const file=await readProgressFile(`${operatorOpenCodeRunRoot}/${identity.runId}/state.json`,operatorOpenCodeRunRoot,262144);
  const state=JSON.parse(file.text);
  if(state.runId!==identity.runId||state.runtime!=='opencode'||state.workspace!==identity.workspace||
   !['failed','cancelled'].includes(state.state)||state.stopped!==true||!/^[A-Z][A-Z0-9_]{1,80}$/.test(state.code??''))return null;
  const stopFile=await readProgressFile(`${operatorOpenCodeRunRoot}/${identity.runId}/stop-receipt.json`,operatorOpenCodeRunRoot,16384);
  const proof=JSON.parse(stopFile.text);
  if(proof.unit!==`agentos-run-${identity.runId}`||proof.stopped!==true||proof.mainPid!==0||proof.populated!==false||
   !['inactive','failed'].includes(proof.activeState))return null;
  return {state:state.state,code:state.code,stateDigest:file.digest,stopReceiptDigest:stopFile.digest};
 }catch{return null;}
}
/** IO injection is only a test boundary. Production callers cannot provide
 * commands, filenames, replacement identities or a native-owner stop. */
export async function stopOperatorOpenCodeRecovery(request,io={}){
 if(!request||typeof request!=='object'||Array.isArray(request)||Object.keys(request).sort().join(',')!=='identity,requestId'||
  !uuid.test(request.requestId??''))fail('OPENCODE_RECOVERY_REQUEST_INVALID');
 const identity=validateOperatorOpenCodeIdentity(request.identity),requestDigest=digest(request);
 const filename=`${operatorOpenCodeRunRoot}/${identity.runId}/operator-opencode-recovery-intent.json`;
 const read=io.readIntent??readIntent,claim=io.createIntent??createIntent,inspect=io.inspectIdentity??inspectOperatorOpenCodeRecovery;
 const inspectSandbox=io.inspectSandbox??inspectUnit,inspectOwner=io.inspectOwner??observeOwner,readActor=io.readActorReceipt??actorReceipt;
 const wait=io.wait??delay;
 let duplicate=false,signalAttempted=false;
 const checkReceipt=receipt=>{
  if(receipt?.version!==1||receipt.requestId!==request.requestId||receipt.requestDigest!==requestDigest||
   receipt.runId!==identity.runId||receipt.runFence!==identity.runFence||receipt.contextVersion!==identity.contextVersion||
   receipt.identityDigest!==digest(identity)||receipt.authority!=='administrative_request')fail('OPENCODE_RECOVERY_REQUEST_CONFLICT');
 };
 const prior=await read(filename);
 if(prior){checkReceipt(prior);duplicate=true;}
 else{
  const current=await inspect(identity.runId,identity.workspace,identity.runFence,identity.contextVersion,identity.owner.candidate);
  if(!sameLive(current,identity))fail('OPENCODE_RECOVERY_IDENTITY_CHANGED');
  const receipt={version:1,authority:'administrative_request',requestId:request.requestId,requestDigest,
   runId:identity.runId,runFence:identity.runFence,contextVersion:identity.contextVersion,identityDigest:digest(identity),
   requestedAt:new Date().toISOString(),signalOwnership:'single_attempt'};
  if(!await claim(filename,receipt)){const racing=await read(filename);checkReceipt(racing);duplicate=true;}
 }
 const result={version:1,requestId:request.requestId,runId:identity.runId,runFence:identity.runFence,
  contextVersion:identity.contextVersion,workspace:identity.workspace,state:'uncertain',duplicate,signalAttempted:false,
  process:null,owner:null,actorReceipt:null,stopConfirmed:false,newAttemptQueued:false};
 if(!duplicate){
  try{
   const current=await inspect(identity.runId,identity.workspace,identity.runFence,identity.contextVersion,identity.owner.candidate);
   if(!sameLive(current,identity))fail('OPENCODE_RECOVERY_IDENTITY_CHANGED');
   let terminal=null;signalAttempted=true;
   await stopUnit(`agentos-run-${identity.runId}`,()=>terminal!==null,{
    inspect:async unit=>{const process=await inspectSandbox(unit);terminal=await readActor(identity);return process;},
    wait,attempts:io.attempts??50,...(io.stopSandbox?{stop:()=>io.stopSandbox(`agentos-run-${identity.runId}`)}:{}),
   });
  }catch{result.code='OPENCODE_RECOVERY_STOP_UNCONFIRMED';}
 }
 result.signalAttempted=signalAttempted;
 for(let attempt=0;attempt<(io.observationAttempts??4);attempt++){
  try{
   result.process=await inspectSandbox(`agentos-run-${identity.runId}`);
   result.owner=await inspectOwner(identity);result.actorReceipt=await readActor(identity);
   const p=result.process,o=result.owner;
   result.stopConfirmed=Boolean(p?.unit===`agentos-run-${identity.runId}`&&p.stopped===true&&p.mainPid===0&&p.populated===false&&
    ['inactive','failed'].includes(p.activeState)&&['loaded','not-found'].includes(p.loadState)&&
    o?.unit===identity.owner.unit&&o.mainPid===0&&o.pendingJob===false&&o.cgroupEmpty===true&&
    ['inactive','failed'].includes(o.activeState)&&['loaded','not-found'].includes(o.loadState)&&result.actorReceipt);
   if(result.stopConfirmed){result.state='stopped';delete result.code;break;}
  }catch{result.code='OPENCODE_RECOVERY_STOP_UNCONFIRMED';}
  if(attempt+1<(io.observationAttempts??4))await wait(250);
 }
 return result;
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 try{if(process.argv.length!==2)fail('OPENCODE_RECOVERY_REQUEST_INVALID');let body='';
  for await(const chunk of process.stdin){body+=chunk;if(Buffer.byteLength(body)>16384)fail('OPENCODE_RECOVERY_REQUEST_INVALID');}
  console.log(JSON.stringify(await stopOperatorOpenCodeRecovery(JSON.parse(body))));}
 catch{console.error(JSON.stringify({code:'OPENCODE_RECOVERY_STOP_REQUEST_FAILED'}));process.exitCode=1;}
}
