import { ContinuationSupervisor } from './continuation.js';
import { pool } from './db.js';
const supervisor = new ContinuationSupervisor();
if (!(await supervisor.start())) {
  await pool.end();
  process.exit(0);
}
console.log('Automatic continuation supervisor started.');
let closing = false;
async function close() {
  if (closing) return;
  closing = true;
  await supervisor.close();
  await pool.end();
}
process.on('SIGINT', () => void close());
process.on('SIGTERM', () => void close());
