import {constants} from 'node:fs';
import {open,realpath} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import {inspectUnit} from './process-stop.mjs';
import {inspectUnitExit} from './unit-exit.mjs';

const uuid=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const workspaceKey=/^[a-z0-9_-]+\/[a-z0-9_-]+$/;
const root='/var/lib/agentos-bridge/worker-runs';
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');

async function readState(runId) {
  const dir=root+'/'+runId,file=dir+'/state.json';
  if(await realpath(dir)!==dir)throw Error('RUN_PATH_INVALID');
  const h=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
  try {
    if(await realpath('/proc/self/fd/'+h.fd)!==file)throw Error('RUN_PATH_INVALID');
    const before=await h.stat();
    if(!before.isFile()||before.size>262144)throw Error('RUN_STATE_INVALID');
    // Read a bounded amount even if a writer grows the file after stat.
    const bytes=Buffer.alloc(before.size+1);
    let offset=0;
    while(offset<bytes.length){const n=await h.read(bytes,offset,bytes.length-offset,offset);if(!n.bytesRead)break;offset+=n.bytesRead;}
    const after=await h.stat();
    if(offset!==before.size||['dev','ino','size','mtimeMs','ctimeMs'].some(k=>before[k]!==after[k]))
      throw Error('RUN_STATE_CHANGED');
    const value=bytes.subarray(0,offset);
    return {state:JSON.parse(value.toString('utf8')),digest:sha(value)};
  } finally {await h.close();}
}

function requireStopped(proof,unit) {
  const age=Date.now()-Date.parse(proof?.checkedAt);
  if(proof?.unit!==unit||proof.stopped!==true||proof.mainPid!==0||proof.populated!==false||
    !['inactive','failed'].includes(proof.activeState)||!Number.isFinite(age)||age<0||age>10000)
    throw Error('STOP_UNCONFIRMED');
}

// Read-only evidence for a historical Harness exit. No workspace scan, provider
// call, process signal, artifact collection, state rewrite or event insertion.
export async function inspectHarnessTermination(runId,workspace,runFence,io={readState,inspectUnit,inspectUnitExit}) {
  if(!uuid.test(runId)||!uuid.test(runFence)||!workspaceKey.test(workspace))throw Error('RUN_IDENTITY_INVALID');
  const before=await io.readState(runId),state=before.state;
  if(state.runId!==runId||state.runFence!==runFence||state.workspace!==workspace||state.runtime!=='harness'||
    state.state!=='failed'||state.stopped!==true||!['RUNTIME_EXITED','HARNESS_SANDBOX_OOM'].includes(state.code)||
    !/^[a-f0-9]{64}$/.test(before.digest))throw Error('RUN_IDENTITY_MISMATCH');
  const unit='agentos-run-'+runId;
  const first=await io.inspectUnit(unit);requireStopped(first,unit);
  const observed=await io.inspectUnitExit(unit);
  const termination=observed?.source==='systemd-journal'&&observed.unit===unit+'.service'&&observed.result==='oom-kill'
    ? {source:observed.source,unit:observed.unit,result:observed.result} : null;
  const after=await io.readState(runId);
  if(before.digest!==after.digest)throw Error('RUN_STATE_CHANGED');
  const process=await io.inspectUnit(unit);requireStopped(process,unit);
  return {version:1,runId,runFence,workspace,runtime:'harness',state:state.state,code:state.code,
    stateDigest:before.digest,termination,process,checkedAt:process.checkedAt};
}

if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href) {
  try {
    if(process.argv.length!==5)throw Error('INVALID_INSPECTION');
    console.log(JSON.stringify(await inspectHarnessTermination(process.argv[2],process.argv[3],process.argv[4])));
  } catch(error) {
    const known=new Set(['INVALID_INSPECTION','RUN_IDENTITY_INVALID','RUN_IDENTITY_MISMATCH','RUN_PATH_INVALID',
      'RUN_STATE_INVALID','RUN_STATE_CHANGED','STOP_UNCONFIRMED']);
    console.log(JSON.stringify({error:known.has(error.message)?error.message:'INSPECTION_UNAVAILABLE'}));
    process.exitCode=1;
  }
}
