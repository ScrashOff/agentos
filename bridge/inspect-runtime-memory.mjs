import { constants } from 'node:fs';
import { open, realpath } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { inspectUnit } from './process-stop.mjs';
const uuid=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const workspaceKey=/^[a-z0-9_-]+\/[a-z0-9_-]+$/;
async function readState(id) {
  const dir='/var/lib/agentos-bridge/worker-runs/'+id;
  if(await realpath(dir)!==dir)throw Error('RUN_PATH_INVALID');
  const handle=await open(dir+'/state.json',constants.O_RDONLY|constants.O_NOFOLLOW);
  try {
    const info=await handle.stat();
    if(!info.isFile()||info.size>262144)throw Error('RUN_STATE_INVALID');
    return JSON.parse(await handle.readFile('utf8'));
  } finally {await handle.close();}
}
// Deliberately no workspace-manager, candidate, file inventory or patch scan.
// The workspace supplied by the controller is bound to its durable reservation.
export async function inspectRuntimeMemory(runId,workspace,notStarted=false,io={readState,inspectUnit},runtime='opencode') {
  if(!uuid.test(runId)||!workspaceKey.test(workspace)||!['opencode','harness'].includes(runtime))throw Error('RUN_IDENTITY_INVALID');
  let state;
  try {state=await io.readState(runId);}
  catch(error) {if(!notStarted||error.code!=='ENOENT')throw error;}
  if(state&&(state.runId!==runId||state.runtime!==runtime||state.workspace!==workspace))
    throw Error('RUN_IDENTITY_MISMATCH');
  const processStatus=await io.inspectUnit('agentos-run-'+runId);
  if(!processStatus.stopped)throw Error('STOP_UNCONFIRMED');
  return {runId,executionRunId:runId,runtime,workspace,
    checkedAt:processStatus.checkedAt,process:processStatus};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href) {
  const options=process.argv.slice(4);
  if(process.argv.length<4||options.some(x=>!['--not-started','--runtime=harness','--runtime=opencode'].includes(x))||
    new Set(options).size!==options.length||options.filter(x=>x.startsWith('--runtime=')).length>1)
    throw Error('INVALID_INSPECTION');
  console.log(JSON.stringify(await inspectRuntimeMemory(process.argv[2],process.argv[3],options.includes('--not-started'),
    undefined,options.includes('--runtime=harness')?'harness':'opencode')));
}
