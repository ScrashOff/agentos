import { RunObserver } from './observations.js';
import { pool } from './db.js';
const observer = new RunObserver();
if (!(await observer.start())) {
  console.log('Observer already running or remote execution disabled.');
  await pool.end();
} else {
  console.log('Read-only execution observer started.');
  const close = async () => {
    await observer.close();
    await pool.end();
    process.exit(0);
  };
  process.on('SIGINT', () => void close());
  process.on('SIGTERM', () => void close());
}
