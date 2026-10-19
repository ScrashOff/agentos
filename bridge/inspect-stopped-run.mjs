import { readFile } from 'node:fs/promises';
import { inspectUnit } from './process-stop.mjs';
import { workspaceOperation } from './workspaces.mjs';
import { stoppedRunProof } from './stopped-run-proof.mjs';
const id = process.argv[2];
if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(id ?? '')) throw Error('INVALID_RUN');
const notStartedWorkspace = process.argv[3] === '--not-started' ? process.argv[4] : undefined;
if (process.argv.length > 3 && (!notStartedWorkspace || process.argv.length !== 5))
  throw Error('INVALID_INSPECTION');
console.log(
  JSON.stringify({
    ...(await stoppedRunProof(id, notStartedWorkspace, {
      readState: async (runId) => JSON.parse(
        await readFile(`/var/lib/agentos-bridge/worker-runs/${runId}/state.json`, 'utf8'),
      ),
      inspectUnit,
      inspectWorkspace: (workspace) => workspaceOperation({ action: 'inspect', workspace }),
    })),
  }),
);
