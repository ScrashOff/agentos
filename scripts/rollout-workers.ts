import { readFile, writeFile, rename, unlink, realpath, mkdir, readdir } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash, randomUUID } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { pool } from '../server/db.js';
import { DomainError } from '../server/contracts.js';
import { workerSourceDigest } from '../server/worker-readiness.js';
import { restartStoppedWorkerService } from '../server/worker-service-stop.js';
import { schedule } from '../server/service.js';
import {
  reconcileWorkerRollout,
  dispatchAdmittedWorkerRuns,
  type PendingWorkerRelease,
  type WorkerService,
  type WorkerServiceProof,
} from '../server/worker-rollout.js';

const exec = promisify(execFile),
  state = '/var/lib/agentos/releases',
  file = path.join(state, 'pending-workers.json');
const services = ['agentos-workers', 'agentos-opencode', 'agentos-backlog', 'agentos-cycles'];
const fingerprint = (text: string) => createHash('sha256').update(text).digest('hex');
async function serviceStatus(service: WorkerService) {
  const { stdout } = await exec(
    'systemctl',
    ['show', service, '--property=MainPID,ActiveState,ControlGroup'],
    { timeout: 10000 },
  );
  return Object.fromEntries(
    stdout
      .trim()
      .split('\n')
      .map((line) => {
        const at = line.indexOf('=');
        return [line.slice(0, at), line.slice(at + 1)];
      }),
  );
}
async function pidGone(pid: number) {
  if (!pid) return true;
  try {
    await realpath('/proc/' + pid);
    return false;
  } catch (error: any) {
    if (error.code === 'ENOENT') return true;
    throw error;
  }
}
async function cgroupEmpty(directory: string): Promise<boolean> {
  try {
    if ((await readFile(path.join(directory, 'cgroup.procs'), 'utf8')).trim()) return false;
    for (const child of await readdir(directory, { withFileTypes: true }))
      if (child.isDirectory() && !(await cgroupEmpty(path.join(directory, child.name))))
        return false;
    return true;
  } catch (error: any) {
    if (error.code === 'ENOENT') return true;
    throw error;
  }
}
async function restartService(service: WorkerService) {
  return restartStoppedWorkerService(service, {
    serviceStatus,
    pidGone,
    cgroupEmpty,
    command: async (action, unit) => {
      await exec('systemctl', [action, unit], {
        timeout: action === 'stop' ? 60000 : action === 'start' ? 30000 : 10000,
      });
    },
  });
}
async function serviceProof(
  service: WorkerService,
  record: any,
  phase: any,
  options?: { once?: boolean },
): Promise<WorkerServiceProof> {
  const deadline = Date.now() + (options?.once ? 0 : 30000);
  do {
    const observed = await serviceStatus(service),
      pid = Number(observed.MainPID);
    const readiness = (
      await pool.query('SELECT value FROM settings WHERE id=$1', ['worker-readiness:' + service])
    ).rows[0]?.value;
    const ownersAlive =
      readiness?.locks &&
      (
        await pool.query(
          `SELECT count(*)::int n FROM
      jsonb_to_recordset($1::jsonb) AS owner(key int,"backendPid" int) JOIN pg_locks l
      ON l.locktype='advisory' AND l.classid=0 AND l.objsubid=1 AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND l.objid=owner.key::oid AND l.pid=owner."backendPid" AND l.granted`,
          [JSON.stringify(readiness.locks)],
        )
      ).rows[0].n === readiness.locks.length;
    if (
      observed.ActiveState === 'active' &&
      pid > 0 &&
      pid !== phase.previousPid &&
      ownersAlive &&
      readiness?.pid === pid &&
      readiness.service === service &&
      Date.parse(readiness.readyAt) >= Date.parse(phase.restartAt)
    ) {
      const cwd = await realpath('/proc/' + pid + '/cwd'),
        digest = await workerSourceDigest(cwd);
      const after = await serviceStatus(service);
      if (
        after.ActiveState === 'active' &&
        Number(after.MainPID) === pid &&
        cwd === record.candidate &&
        digest === record.digest &&
        readiness.candidate === cwd &&
        readiness.digest === digest &&
        (await pidGone(phase.previousPid ?? 0))
      )
        return {
          unit: service,
          active: true,
          pid,
          cwd,
          digest,
          oldPidGone: true,
          readiness,
          stopped: phase.stopped,
          checkedAt: new Date().toISOString(),
        };
    }
    if(options?.once)break;
    await new Promise((resolve) => setTimeout(resolve, 500));
  } while (Date.now() < deadline);
  // An alive process without readiness keeps its durable start receipt. Only a
  // confirmed stopped unit may re-enter the existing quiet/stop/start path.
  const final=await serviceStatus(service);
  if(['inactive','failed'].includes(final.ActiveState)&&Number(final.MainPID)===0&&
    (await pidGone(phase.previousPid??0))&&(!final.ControlGroup||
      (final.ControlGroup==='/system.slice/'+service+'.service'&&
        await cgroupEmpty('/sys/fs/cgroup'+final.ControlGroup))))
    throw new DomainError('ROLLOUT_SERVICE_START_FAILED','Unidade nova encerrada antes da prontidão.');
  throw new DomainError(
    'ROLLOUT_SERVICE_PROOF',
    'Processo novo ainda sem prontidão e origem confirmadas.',
  );
}
async function readPending(): Promise<PendingWorkerRelease | null> {
  let text: string;
  try {
    text = await readFile(file, 'utf8');
  } catch (error: any) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
  return { record: JSON.parse(text), version: fingerprint(text) };
}
try {
  await mkdir(state, { recursive: true });
  if (!process.argv.includes('--publication-lock-held')) {
    // Same lock as publication: prevent source swaps and pending-file writes
    // between the quiet proof, service restart and acknowledgement.
    try {
      const { stdout } = await exec(
        'flock',
        [
          '--no-fork',
          '--nonblock',
          '--conflict-exit-code',
          '75',
          path.join(state, 'publication.lock'),
          process.execPath,
          ...process.execArgv,
          fileURLToPath(import.meta.url),
          '--publication-lock-held',
        ],
        { maxBuffer: 1024 * 1024 },
      );
      if (stdout) process.stdout.write(stdout);
    } catch (error: any) {
      if (error.code !== 75) throw error;
    }
  } else {
    const result = await reconcileWorkerRollout({
      readPending,
      readLast: () =>
        readFile(path.join(state, 'last-worker-update.json'), 'utf8').then(JSON.parse, (error) => {
          if (error.code === 'ENOENT') return null;
          throw error;
        }),
      verifyRelease: async (record) => {
        if (
          (await realpath('/opt/agentos/current')) !== record.candidate ||
          (await workerSourceDigest(record.candidate)) !== record.digest
        )
          throw new DomainError('ROLLOUT_RELEASE_CHANGED', 'A versão publicada ou seu hash mudou.');
      },
      dispatchAdmitted: dispatchAdmittedWorkerRuns,
      scheduleAdmitted: schedule,
      restartService,
      serviceProof,
      restart: async () => {
        await exec('systemctl', ['restart', ...services], { timeout: 150000 });
      },
      healthy: async () => {
        await exec('systemctl', ['is-active', '--quiet', ...services], { timeout: 20000 });
      },
      archive: async (expected, completed) => {
        if ((await readPending())?.version !== expected.version)
          throw new DomainError('ROLLOUT_PENDING_CHANGED', 'Atualização pendente mudou.');
        const temp = path.join(state, 'worker-update-' + randomUUID() + '.json');
        await writeFile(temp, JSON.stringify(completed), { flag: 'wx', mode: 0o600 });
        await rename(temp, path.join(state, 'last-worker-update.json'));
        await unlink(file);
      },
    });
    console.log(JSON.stringify(result));
  }
} finally {
  await pool.end();
}
