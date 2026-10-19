import { pool } from '../server/db.js';
import { requireSchemaReady } from '../server/schema-readiness.js';
import { CycleEngine } from '../server/cycles/engine.js';
import { accountBridge } from '../server/bridge.js';
import { recordWorkerReadiness } from '../server/worker-readiness.js';
await requireSchemaReady();
const engine = new CycleEngine();
await engine.start();
if (engine.owner) await recordWorkerReadiness('agentos-cycles', [4280088]);
let closing = false;
async function close() {
  if (closing) return;
  closing = true;
  await engine.close();
  accountBridge.close();
  await pool.end();
  process.exit(0);
}
process.on('SIGTERM', () => void close());
process.on('SIGINT', () => void close());
