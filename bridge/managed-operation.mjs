// Runs inside the existing worker sandbox. These receipts are hash-bound
// observations, never authority to accept a task or launch another worker.
import {spawn} from 'node:child_process';
import {constants,createWriteStream} from 'node:fs';
import {open,mkdir,readFile,rename,realpath,readdir,lstat} from 'node:fs/promises';
import {Transform} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {createHash,randomUUID} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import path from 'node:path';

export const OPERATION_POLICY_PATH='/run/operation-policy.json';
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i,sha=/^[a-f0-9]{64}$/;
const identityKeys=['goalId','taskId','runId','runFence','contextVersion'];
const executableHashes=new Map();
const fail=code=>Object.assign(Error(code),{code});
const canonical=v=>Array.isArray(v)?'['+v.map(canonical).join(',')+']':v&&typeof v==='object'?'{'+Object.keys(v).sort().map(k=>JSON.stringify(k)+':'+canonical(v[k])).join(',')+'}':JSON.stringify(v);
export const operationDigest=value=>createHash('sha256').update(canonical(value)).digest('hex');
const bytesDigest=value=>createHash('sha256').update(value).digest('hex');
const within=(root,target)=>target===root||target.startsWith(root+path.sep);

export function validateOperationPolicy(value){
  if(!value||value.version!==1||!identityKeys.every(k=>uuid.test(value[k]??''))||
    value.workspace!=='/workspace'||!sha.test(value.sourceSnapshotDigest??'')||
    !Number.isSafeInteger(value.maxDeadlineMs)||value.maxDeadlineMs<100||value.maxDeadlineMs>3600000)
    throw fail('OPERATION_POLICY_INVALID');
  return {version:1,...Object.fromEntries(identityKeys.map(k=>[k,value[k]])),workspace:value.workspace,
    sourceSnapshotDigest:value.sourceSnapshotDigest,maxDeadlineMs:value.maxDeadlineMs};
}
export function validateOperationSpecification(raw,policy){
  if(!raw||Object.keys(raw).some(k=>!['stageKey','argv','cwd','deadlineMs','inputFiles'].includes(k))||
    !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$/.test(raw.stageKey??'')||
    !Array.isArray(raw.argv)||raw.argv.length<1||raw.argv.length>128||
    raw.argv.some(a=>typeof a!=='string'||a.includes('\0')||a.length>8192)||
    !path.posix.isAbsolute(raw.argv[0])||typeof(raw.cwd??policy.workspace)!=='string'||
    !Number.isSafeInteger(raw.deadlineMs)||raw.deadlineMs<100||raw.deadlineMs>policy.maxDeadlineMs||
    (raw.inputFiles!==undefined&&(!Array.isArray(raw.inputFiles)||raw.inputFiles.length>32||
      raw.inputFiles.some(p=>typeof p!=='string'||!p||p.length>1024||p.includes('\0')))))
    throw fail('OPERATION_SPEC_INVALID');
  const cwd=path.resolve(raw.cwd??policy.workspace);
  if(!within(policy.workspace,cwd))throw fail('OPERATION_CWD_OUTSIDE_WORKSPACE');
  return {stageKey:raw.stageKey,argv:[...raw.argv],cwd,deadlineMs:raw.deadlineMs,
    ...(raw.inputFiles?{inputFiles:[...new Set(raw.inputFiles)].sort()}:{} )};
}
export function operationIdentity(policy,specification){
  return operationDigest({version:1,...Object.fromEntries(identityKeys.map(k=>[k,policy[k]])),
    sourceSnapshotDigest:policy.sourceSnapshotDigest,stageKey:specification.stageKey});
}
async function readJson(file,max=65536){
  const fd=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);
  try{const stat=await fd.stat();if(!stat.isFile()||stat.size>max)throw fail('OPERATION_FILE_INVALID');
    return JSON.parse(await fd.readFile('utf8'));}finally{await fd.close();}
}
async function atomicJson(file,value){
  const temporary=file+'.'+randomUUID()+'.tmp';
  const fd=await open(temporary,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
  try{await fd.writeFile(JSON.stringify(value));await fd.sync();}finally{await fd.close();}
  await rename(temporary,file);
  const directory=await open(path.dirname(file),constants.O_RDONLY|constants.O_DIRECTORY);
  try{await directory.sync();}finally{await directory.close();}
}
async function secureDirectory(directory){
  if(await realpath(path.dirname(directory))!==path.dirname(directory))throw fail('OPERATION_UNSAFE_DIRECTORY');
  await mkdir(directory,{recursive:true,mode:0o700});
  if(await realpath(directory)!==directory||(await lstat(directory)).isSymbolicLink())throw fail('OPERATION_UNSAFE_DIRECTORY');
}
async function hashBoundFile(file,{limit=16777216,cache=false}={}){
  const fd=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);
  try{
    const before=await fd.stat({bigint:true});
    if(!before.isFile()||before.size>BigInt(limit))throw fail('OPERATION_INPUT_FILE_INVALID');
    const key=[file,before.dev,before.ino,before.size,before.mtimeNs,before.ctimeNs].join(':');
    let digest=cache?executableHashes.get(key):null;
    if(!digest){const hash=createHash('sha256'),buffer=Buffer.alloc(65536);let offset=0;
      while(true){const {bytesRead}=await fd.read(buffer,0,buffer.length,offset);if(!bytesRead)break;offset+=bytesRead;
        if(offset>limit)throw fail('OPERATION_INPUT_FILE_INVALID');hash.update(buffer.subarray(0,bytesRead));}
      digest=hash.digest('hex');
    }
    const after=await fd.stat({bigint:true});
    if([after.dev,after.ino,after.size,after.mtimeNs,after.ctimeNs].join(':')!==[before.dev,before.ino,before.size,before.mtimeNs,before.ctimeNs].join(':'))
      throw fail('OPERATION_INPUT_FILE_CHANGED');
    if(cache){if(executableHashes.size>=8)executableHashes.clear();executableHashes.set(key,digest);}
    return {sha256:digest,bytes:Number(before.size)};
  }finally{await fd.close();}
}
async function bindInputFiles(specification,workspaceRoot,cwd){
  const executable=await realpath(specification.argv[0]).catch(error=>{if(error.code==='ENOENT')return null;throw error;});
  const inputs=new Map();
  const resolve=p=>path.isAbsolute(p)?(within('/workspace',p)?path.join(workspaceRoot,path.relative('/workspace',p)):p):path.resolve(cwd,p);
  async function add(input,required){
    const candidate=resolve(input);
    if(!within(workspaceRoot,candidate)){if(required)throw fail('OPERATION_INPUT_OUTSIDE_WORKSPACE');return;}
    let actual;
    try{actual=await realpath(candidate);}catch(error){if(!required&&['ENOENT','ENOTDIR','ENAMETOOLONG'].includes(error.code))return;throw error;}
    if(!within(workspaceRoot,actual)||actual!==candidate)throw fail('OPERATION_UNSAFE_INPUT');
    if(!(await lstat(actual)).isFile()){if(required)throw fail('OPERATION_INPUT_FILE_INVALID');return;}
    if(!inputs.has(actual))inputs.set(actual,{path:path.relative(workspaceRoot,actual),...await hashBoundFile(actual)});
  }
  for(const input of specification.inputFiles??[])await add(input,true);
  // Only literal argv entries are inferred. Imports/dynamic reads must be
  // declared via inputFiles; a snapshot manifest is not a transitive file hash.
  for(const input of specification.argv.slice(1))if(input&&!input.startsWith('-')&&input.length<=1024&&!/[\r\n]/.test(input))await add(input,false);
  return {...specification,inputBindings:[...inputs.values()].sort((a,b)=>a.path.localeCompare(b.path)),
    executableBinding:executable?{path:executable,...await hashBoundFile(executable,{limit:268435456,cache:true})}:null};
}
async function processIdentity(pid){
  if(!Number.isSafeInteger(pid)||pid<2)throw fail('OPERATION_PROCESS_INVALID');
  const text=await readFile('/proc/'+pid+'/stat','utf8'),parts=text.slice(text.lastIndexOf(')')+2).trim().split(/\s+/);
  return {pid,state:parts[0],ppid:Number(parts[1]),pgrp:Number(parts[2]),session:Number(parts[3]),startTicks:parts[19],
    cgroup:await readFile('/proc/'+pid+'/cgroup','utf8')};
}
async function groupMembers(pgrp){
  const members=[];
  for(const name of await readdir('/proc')){
    if(!/^[1-9][0-9]*$/.test(name)||Number(name)<2)continue;
    try{const value=await processIdentity(Number(name));if(value.pgrp===pgrp&&!['Z','X'].includes(value.state))members.push(value);}
    catch(error){if(!['ENOENT','ESRCH'].includes(error.code))throw error;}
  }
  return members;
}
function sameProcess(a,b){return a&&b&&a.pid===b.pid&&a.pgrp===b.pgrp&&a.session===b.session&&a.startTicks===b.startTicks&&a.cgroup===b.cgroup;}
async function confirmOwnedLeader(owner){
  try{return sameProcess(owner,await processIdentity(owner.pid));}catch{return false;}
}
function capture(stream,file,maxBytes){
  let seen=0,stored=0;
  const hash=createHash('sha256');
  const limit=new Transform({transform(chunk,_encoding,done){
    seen+=chunk.length;const keep=chunk.subarray(0,Math.max(0,maxBytes-stored));
    if(keep.length){stored+=keep.length;hash.update(keep);this.push(keep);}done();
  }});
  const writing=pipeline(stream,limit,createWriteStream(file,{flags:'wx',mode:0o600}));
  writing.catch(()=>{});
  return {finish:async()=>{await writing;const fd=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);
    try{await fd.sync();}finally{await fd.close();}
    return {name:path.basename(file),sha256:hash.digest('hex'),bytes:stored,observedBytes:seen,truncated:seen>stored};}};
}
async function verifyReceipt(directory,receipt,binding){
  if(!receipt||receipt.version!==1||receipt.operationId!==binding.operationId||receipt.policyDigest!==binding.policyDigest||
    receipt.specificationDigest!==binding.specificationDigest||!identityKeys.every(k=>receipt[k]===binding[k])||
    receipt.sourceSnapshotDigest!==binding.sourceSnapshotDigest||receipt.stageKey!==binding.stageKey||
    receipt.acceptance!==false||receipt.verification!=='unverified_observation'||
    receipt.passed===true||!['exited','deadline_exceeded','cancelled','launch_failed','uncertain'].includes(receipt.outcome))throw fail('OPERATION_RECEIPT_INVALID');
  if(receipt.outcome==='uncertain'){
    if(receipt.physicalExitConfirmed!==false||receipt.code!=='STOP_UNCONFIRMED')throw fail('OPERATION_RECEIPT_INVALID');
    return receipt;
  }
  if(receipt.physicalExitConfirmed!==true)throw fail('OPERATION_RECEIPT_INVALID');
  for(const entry of receipt.logs??[]){
    if(!['stdout.log','stderr.log'].includes(entry.name)||!sha.test(entry.sha256??''))throw fail('OPERATION_RECEIPT_INVALID');
    const file=path.join(directory,entry.name),fd=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);
    try{const stat=await fd.stat();if(!stat.isFile()||stat.size!==entry.bytes||stat.size>16777216)throw fail('OPERATION_LOG_CHANGED');
      if(bytesDigest(await fd.readFile())!==entry.sha256)throw fail('OPERATION_LOG_CHANGED');}finally{await fd.close();}
  }
  return receipt;
}
/** Options are an in-process test seam only. CLI always reads the fixed policy
 * and uses the worker's own home. No user-selected host policy or output root. */
export async function executeManagedOperation(raw,options={}){
  if(process.platform!=='linux')throw fail('OPERATION_LINUX_REQUIRED');
  options.signal?.throwIfAborted();
  const policy=validateOperationPolicy(options.policy??await readJson(OPERATION_POLICY_PATH));
  let specification=validateOperationSpecification(raw,policy);
  const cwd=options.workspaceRoot?path.join(options.workspaceRoot,path.relative(policy.workspace,specification.cwd)):specification.cwd;
  if(await realpath(cwd)!==cwd)throw fail('OPERATION_UNSAFE_CWD');
  const snapshot=path.join(options.workspaceRoot??policy.workspace,'.agentos-snapshot.json');
  const snapshotFd=await open(snapshot,constants.O_RDONLY|constants.O_NOFOLLOW);
  try{const stat=await snapshotFd.stat();if(!stat.isFile()||stat.size>16777216||bytesDigest(await snapshotFd.readFile())!==policy.sourceSnapshotDigest)
    throw fail('OPERATION_SOURCE_SNAPSHOT_CHANGED');}finally{await snapshotFd.close();}
  specification=await bindInputFiles(specification,options.workspaceRoot??policy.workspace,cwd);
  const root=path.resolve(options.root??'/workspace/evidence/managed-operations');await secureDirectory(root);
  const operationId=operationIdentity(policy,specification),directory=path.join(root,operationId);
  const binding={version:1,operationId,policyDigest:operationDigest(policy),specificationDigest:operationDigest(specification),
    ...Object.fromEntries(identityKeys.map(k=>[k,policy[k]])),sourceSnapshotDigest:policy.sourceSnapshotDigest,stageKey:specification.stageKey};
  const observation={...binding,acceptance:false,verification:'unverified_observation'};
  try{await mkdir(directory,{mode:0o700});}
  catch(error){
    if(error.code!=='EEXIST')throw error;
    if(await realpath(directory)!==directory||(await lstat(directory)).isSymbolicLink())throw fail('OPERATION_UNSAFE_DIRECTORY');
    let prior;try{prior=await readJson(path.join(directory,'claim.json'));}catch{return {...observation,outcome:'uncertain',code:'STOP_UNCONFIRMED',physicalExitConfirmed:false,passed:false,reused:true};}
    if(prior.policyDigest!==binding.policyDigest||prior.specificationDigest!==binding.specificationDigest)throw fail('OPERATION_INPUT_CHANGED');
    try{return {...await verifyReceipt(directory,await readJson(path.join(directory,'operation-report.json')),binding),reused:true};}
    catch(error){if(error.code!=='ENOENT')throw error;}
    // A live or lost owner may have crossed spawn. Never repeat its effect.
    return {...observation,outcome:'uncertain',code:'STOP_UNCONFIRMED',physicalExitConfirmed:false,passed:false,reused:true};
  }
  const claimedAt=new Date().toISOString();
  await atomicJson(path.join(directory,'claim.json'),{...binding,claimedAt});
  await atomicJson(path.join(directory,'state.json'),{...observation,state:'claimed',claimedAt});
  // argv never appears in receipts/events. It may contain private test inputs.
  // A separate PID namespace is required: PGID alone misses descendants that
  // call setsid(). Its PID-1 exiting terminates all remaining owned descendants.
  // Recursive bind retains the enclosing sandbox's existing read-only mounts,
  // network namespace, credentials and visibility; no host path is introduced.
  const child=spawn('/usr/bin/bwrap',['--unshare-pid','--die-with-parent','--bind','/','/','--proc','/proc','--dev','/dev','--',...specification.argv],
    {cwd,stdio:['ignore','pipe','pipe'],detached:true,env:process.env});
  let spawnError=null,closed=false,exitCode=null,exitSignal=null,deadlineReached=false,cancelRequested=false,owner=null;
  let resolveClosed;const exited=new Promise(resolve=>{resolveClosed=resolve;});
  child.once('error',error=>{spawnError=error.code??'SPAWN_FAILED';});
  child.once('close',(code,signal)=>{closed=true;exitCode=code;exitSignal=signal;resolveClosed();});
  const maxLogBytes=Number.isSafeInteger(options.maxLogBytes)&&options.maxLogBytes>0&&options.maxLogBytes<=16777216?options.maxLogBytes:16777216;
  const stdout=capture(child.stdout,path.join(directory,'stdout.log'),maxLogBytes);
  const stderr=capture(child.stderr,path.join(directory,'stderr.log'),maxLogBytes);
  try{if(child.pid)owner=await processIdentity(child.pid);}catch{}
  if(owner&&(owner.ppid!==process.pid||owner.pgrp!==child.pid||owner.session!==child.pid))owner=null;
  const graceMs=options.graceMs??2000;
  let timeout,escalation,finalObservation,resolveDeadline,stopRequested=false;
  const deadlineEnded=new Promise(resolve=>{resolveDeadline=resolve;});
  const requestStop=async reason=>{
    if(reason==='deadline')deadlineReached=true;else cancelRequested=true;
    if(stopRequested||closed)return;stopRequested=true;
    // The policy permits stopping this newly spawned operation, never the run,
    // the worker cgroup, an inherited browser, or an actor found by name.
    if(owner&&await confirmOwnedLeader(owner))try{process.kill(-owner.pgrp,'SIGTERM');}catch{}
    if(closed)return;
    escalation=setTimeout(async()=>{
      if(!closed&&owner&&await confirmOwnedLeader(owner))try{process.kill(-owner.pgrp,'SIGKILL');}catch{}
      // Failure to confirm ownership/exit is explicit uncertainty, not retry.
      finalObservation=setTimeout(resolveDeadline,graceMs);
    },graceMs);
  };
  timeout=setTimeout(()=>void requestStop('deadline'),specification.deadlineMs);
  const abort=()=>void requestStop('cancel');
  options.signal?.addEventListener('abort',abort,{once:true});
  if(options.signal?.aborted)abort();
  // Observation persistence cannot orphan a command that already crossed spawn.
  // Its owned timer and process observer stay installed even on a disk error.
  let persistenceFailed=false;
  try{await atomicJson(path.join(directory,'state.json'),{...observation,state:'running',claimedAt,startedAt:new Date().toISOString(),
    process:owner?{pid:owner.pid,pgrp:owner.pgrp,session:owner.session,startTicks:owner.startTicks}:null});}
  catch{persistenceFailed=true;}
  await Promise.race([exited,deadlineEnded]);
  clearTimeout(timeout);clearTimeout(escalation);clearTimeout(finalObservation);
  options.signal?.removeEventListener('abort',abort);
  let physicalExitConfirmed=false;
  if(closed){
    try{physicalExitConfirmed=!child.pid||(await groupMembers(child.pid)).length===0;}catch{}
  }
  if(!physicalExitConfirmed){
    const receipt={...observation,outcome:'uncertain',code:'STOP_UNCONFIRMED',physicalExitConfirmed:false,passed:false,
      deadlineReached,cancelRequested,observedAt:new Date().toISOString()};
    await atomicJson(path.join(directory,'state.json'),{...receipt,state:'uncertain'});
    await atomicJson(path.join(directory,'operation-report.json'),receipt);
    return receipt;
  }
  const logs=await Promise.all([stdout.finish(),stderr.finish()]);
  const receipt={...observation,outcome:spawnError?'launch_failed':cancelRequested?'cancelled':deadlineReached?'deadline_exceeded':'exited',
    exitCode,exitSignal,code:spawnError??(persistenceFailed?'OPERATION_OBSERVATION_PERSISTENCE_FAILED':null),physicalExitConfirmed:true,deadlineReached,cancelRequested,
    ...((spawnError||cancelRequested||deadlineReached||exitCode!==0)?{passed:false}:{}),
    outputTruncated:logs.some(log=>log.truncated),logs,finishedAt:new Date().toISOString()};
  await atomicJson(path.join(directory,'operation-report.json'),receipt);
  await atomicJson(path.join(directory,'state.json'),{...receipt,state:'completed'});
  return receipt;
}

if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  try{
    if(process.argv.length!==4||process.argv[2]!=='--spec')throw fail('OPERATION_ARGUMENTS_INVALID');
    const input=path.resolve(process.argv[3]);if(!within('/workspace',input)||await realpath(input)!==input)throw fail('OPERATION_SPEC_PATH_INVALID');
    const receipt=await executeManagedOperation(await readJson(input));console.log(JSON.stringify(receipt));
    process.exitCode=receipt.outcome==='exited'&&receipt.exitCode===0?0:1;
  }catch(error){console.log(JSON.stringify({code:/^[A-Z][A-Z0-9_]+$/.test(error.code??'')?error.code:'OPERATION_FAILED',acceptance:false}));process.exitCode=1;}
}
