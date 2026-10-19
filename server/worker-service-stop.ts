import { DomainError } from './contracts.js';
import type { WorkerService, WorkerServiceStopProof } from './worker-rollout.js';

type Status = { MainPID?: string; ActiveState?: string; ControlGroup?: string };
export type WorkerServiceStopIO = {
  serviceStatus(service: WorkerService): Promise<Status>;
  command(action: 'stop' | 'reset-failed' | 'start', service: WorkerService): Promise<void>;
  pidGone(pid: number): Promise<boolean>;
  cgroupEmpty(directory: string): Promise<boolean>;
};
const services: readonly WorkerService[] = [
  'agentos-opencode', 'agentos-backlog', 'agentos-cycles', 'agentos-workers',
];
function reject(): never {
  throw new DomainError('ROLLOUT_SERVICE_STOP_PROOF', 'Encerramento da unidade antiga não foi comprovado.');
}
function parsePid(value: unknown): number {
  if (typeof value !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value)) reject();
  const pid = Number(value);
  if (!Number.isSafeInteger(pid)) reject();
  return pid;
}

/** Called only after the existing rollout admission and publication fence.
 * A failed systemd latch can survive a completed stop. Clearing that latch is
 * permitted only after physical absence, and never substitutes for observing
 * inactive and proving physical absence again before starting a new process.
 */
export async function restartStoppedWorkerService(service: WorkerService, io: WorkerServiceStopIO) {
  if (!services.includes(service)) reject();
  const expectedGroup = '/system.slice/' + service + '.service';
  const checkGroup = (status: Status) => {
    if (status.ControlGroup !== undefined && typeof status.ControlGroup !== 'string') reject();
    if (status.ControlGroup && status.ControlGroup !== expectedGroup) reject();
  };
  const before = await io.serviceStatus(service);
  const previousPid = parsePid(before.MainPID);
  checkGroup(before);
  await io.command('stop', service);

  const physicallyStopped = async (status: Status) => {
    checkGroup(status);
    if (parsePid(status.MainPID) !== 0) reject();
    // Check the canonical group even if systemd no longer reports its path;
    // the real adapter recursively checks descendants and treats ENOENT only
    // as absent. Permission/read failures propagate without a receipt.
    if (!(await io.cgroupEmpty('/sys/fs/cgroup' + expectedGroup))) reject();
    if (!(await io.pidGone(previousPid))) reject();
  };
  let stopped = await io.serviceStatus(service);
  await physicallyStopped(stopped);
  if (stopped.ActiveState === 'failed') {
    await io.command('reset-failed', service);
    stopped = await io.serviceStatus(service);
    await physicallyStopped(stopped);
  }
  if (stopped.ActiveState !== 'inactive') reject();
  const stopProof: WorkerServiceStopProof = {
    unit: service,
    previousPid,
    activeState: 'inactive',
    mainPid: 0,
    oldPidGone: true,
    cgroupEmpty: true,
    checkedAt: new Date().toISOString(),
  };
  await io.command('start', service);
  return { previousPid, stopped: stopProof };
}
