// A bounded, explicit runtime upgrade for only the inventoried pre-gate runs.
// The shared semaphore retains their reservations until the gateway owner exits.
import { readFile, writeFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { inspectUnit } from './process-stop.mjs';
import { legacyExecutorIsActive } from './glm-request-gate.mjs';

const runId = process.argv[2];
if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(runId ?? '')) throw Error('INVALID_RUN');
const { reservations } = JSON.parse(
  await readFile('/var/lib/agentos-bridge/glm-legacy-reservations.json', 'utf8'),
);
const owner = reservations.find((item) => item.runId === runId);
if (!owner) throw Error('NOT_AN_INVENTORIED_LEGACY_RUN');
const dir = `/var/lib/agentos-bridge/worker-runs/${runId}`;
const before = JSON.parse(await readFile(`${dir}/state.json`, 'utf8'));
if (
  before.runId !== runId ||
  before.runtime !== 'opencode' ||
  before.unit !== `agentos-run-${runId}`
)
  throw Error('IDENTITY_MISMATCH');
await writeFile(
  `${dir}/gate-migration-intent.json`,
  JSON.stringify({
    runId,
    owner,
    workspace: before.workspace,
    at: new Date().toISOString(),
    reason: 'Upgrade to request-level concurrency; preserve workspace and resume unfinished work.',
  }),
  { mode: 0o600 },
);
if (await legacyExecutorIsActive(owner)) {
  await promisify(execFile)('systemctl', ['stop', before.unit + '.service'], { timeout: 15000 });
}
let receipt;
for (let attempt = 0; attempt < 150; attempt++) {
  const process = await inspectUnit(before.unit);
  const ownerActive = await legacyExecutorIsActive(owner);
  if (process.stopped && !ownerActive) {
    receipt = {
      runId,
      workspace: before.workspace,
      process,
      ownerStopped: true,
      reason: 'request_gate_upgrade',
      at: new Date().toISOString(),
    };
    break;
  }
  await delay(100);
}
if (!receipt) throw Error('STOP_UNCONFIRMED');
await writeFile(`${dir}/gate-migration-stop.json`, JSON.stringify(receipt), { mode: 0o600 });
console.log(JSON.stringify(receipt));
