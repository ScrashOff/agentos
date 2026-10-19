import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { pool } from '../server/db.js';
import { config } from '../server/config.js';
import { recoverOpenCodeRun } from '../server/opencode-recovery.js';
const exec = promisify(execFile);
try {
  const id = process.argv[2];
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(id ?? ''))
    throw Error('Provide the failed OpenCode run UUID.');
  const run = (await pool.query('SELECT fence FROM runs WHERE id=$1', [id])).rows[0];
  if (!run) throw Error('RUN_NOT_FOUND');
  const result = await exec(
    'ssh',
    [
      '-T',
      '-o',
      'BatchMode=yes',
      '-o',
      'ConnectTimeout=8',
      config.bridgeHost,
      '/opt/deepseek-harness/node/bin/node',
      '/opt/agentos-bridge/inspect-opencode-recovery.mjs',
      id,
    ],
    { timeout: 45000, maxBuffer: 20 * 1024 * 1024, windowsHide: true },
  );
  console.log(JSON.stringify(await recoverOpenCodeRun(id, run.fence, JSON.parse(result.stdout))));
} finally {
  await pool.end();
}
