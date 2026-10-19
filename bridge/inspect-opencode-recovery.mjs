import { readFile } from 'node:fs/promises';
import { inspectUnit } from './process-stop.mjs';
import { workspaceOperation } from './workspaces.mjs';
import {readProgressFile} from './runtime-progress.mjs';

const runId = process.argv[2];
if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(runId ?? '')) throw Error('INVALID_RUN');
const state = JSON.parse(
  await readFile(`/var/lib/agentos-bridge/worker-runs/${runId}/state.json`, 'utf8'),
);
if (
  state.runId !== runId ||
  state.runtime !== 'opencode'
)
  throw Error('RUN_IDENTITY_MISMATCH');
const processStatus = await inspectUnit(`agentos-run-${runId}`);
if (!processStatus.stopped) throw Error('STOP_UNCONFIRMED');
const candidate = await workspaceOperation({ action: 'inspect', workspace: state.workspace });
let bridgeReceipt=null;
try{
  const root='/var/lib/agentos-bridge/worker-runs';
  const file=await readProgressFile(`${root}/${runId}/stop-receipt.json`,root,32768);
  bridgeReceipt={digest:file.digest,proof:JSON.parse(file.text)};
}catch{/* Missing or unreadable bridge receipt is not a successful stop claim. */}
const after = await inspectUnit(`agentos-run-${runId}`);
if (!after.stopped) throw Error('STOP_UNCONFIRMED');
console.log(
  JSON.stringify({
    runId,
    runtime: 'opencode',
    workspace: state.workspace,
    state,
    process: after,
    candidate,
    bridgeReceipt,
    checkedAt: after.checkedAt,
  }),
);
