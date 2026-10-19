import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash } from 'node:crypto';
import { pool, event, type DB } from './db.js';
import { checkedExecutionMetadata, executionMetadataColumns, taskExecutionMetadataVersion } from './task-execution-metadata.js';
import { config } from './config.js';
import type { PoolClient } from 'pg';
const exec = promisify(execFile);

export async function readObservedRunCandidates(db: DB) {
  const rows = checkedExecutionMetadata((await db.query(`SELECT t.id,r.id run_id,r.task_id,r.state,
    ${executionMetadataColumns} FROM runs r JOIN tasks t ON t.id=r.task_id
    LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    WHERE (metadata.control->>'kind'='harness' OR metadata.task_id IS NULL OR metadata.version<>${taskExecutionMetadataVersion} OR metadata.goal_id<>t.goal_id)
      AND (r.state IN('running','cancelling','reconciling') OR r.finished_at>now()-interval '2 hours')
    ORDER BY (r.state IN('running','cancelling','reconciling')) DESC,r.created_at DESC LIMIT 20`)).rows,
    { objectRequired: false });
  return rows.map(({ id: _taskId, run_id, ...row }) => ({ ...row, id: run_id }));
}

// One observer owns the advisory lock. It never changes runs, leases or tasks.
// Snapshots use the existing authenticated event transport, including on an
// already running controller. This lets us observe without restarting workers.
export class RunObserver {
  private client?: PoolClient;
  private timer?: ReturnType<typeof setTimeout>;
  private closing = false;
  private pending?: Promise<void>;
  private previous = new Map<string, { hash: string; at: number; final: boolean }>();
  async start() {
    if (!config.bridgeHost || !config.remoteExecution) return false;
    this.client = await pool.connect();
    const lock = await this.client.query('SELECT pg_try_advisory_lock(4280027) AS owned');
    if (!lock.rows[0].owned) {
      this.client.release();
      this.client = undefined;
      return false;
    }
    this.client.on('error', () => {
      this.closing = true;
      clearTimeout(this.timer);
    });
    const loop = () => {
      if (this.closing) return;
      this.pending = this.scan()
        .catch(() => {})
        .finally(() => {
          if (!this.closing) this.timer = setTimeout(loop, 12000);
        });
    };
    loop();
    return true;
  }
  async scan() {
    if (!this.client) return;
    await this.client.query('SELECT 1'); // Fail closed if the lock connection is lost.
    const runs = await readObservedRunCandidates(this.client);
    for (const run of runs) {
      if (this.closing) return;
      const final = !['running', 'cancelling', 'reconciling'].includes(run.state);
      if (final && this.previous.get(run.id)?.final) continue;
      let observation: any;
      try {
        const { stdout } = await exec(
          'ssh',
          [
            '-T',
            '-o',
            'BatchMode=yes',
            '-o',
            'ConnectTimeout=8',
            config.bridgeHost,
            '/opt/deepseek-harness/node/bin/node',
            '/opt/agentos-bridge/observe-run.mjs',
            run.id,
          ],
          { timeout: 20000, maxBuffer: 1024 * 1024, windowsHide: true },
        );
        observation = JSON.parse(stdout);
        if (
          observation.error ||
          observation.runId !== run.id ||
          !Array.isArray(observation.actions)
        )
          throw Error('INVALID_OBSERVATION');
      } catch {
        observation = {
          runId: run.id,
          observedAt: new Date().toISOString(),
          unavailable: true,
          message:
            'Não foi possível consultar o executor remoto. O estado da tarefa não foi alterado.',
        };
      }
      const fingerprint = {
        ...observation,
        observedAt: undefined,
        process: observation.process
          ? {
              ...observation.process,
              cpuNs: undefined,
              memoryBytes: undefined,
              memoryPeakBytes: undefined,
            }
          : undefined,
      };
      const hash = createHash('sha256').update(JSON.stringify(fingerprint)).digest('hex');
      const old = this.previous.get(run.id);
      if (old?.hash === hash && Date.now() - old.at < 60000 && !final) continue;
      await event(this.client, 'EXECUTION_OBSERVED', run.task_id, { runId: run.id, observation });
      this.previous.set(run.id, { hash, at: Date.now(), final: final && !observation.unavailable });
    }
  }
  async close() {
    this.closing = true;
    clearTimeout(this.timer);
    await this.pending;
    if (this.client) {
      await this.client.query('SELECT pg_advisory_unlock(4280027)').catch(() => {});
      this.client.release();
      this.client = undefined;
    }
  }
}
