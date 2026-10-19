import { describe, expect, it, vi } from 'vitest';
import { restartStoppedWorkerService } from '../server/worker-service-stop.js';
import type { WorkerService } from '../server/worker-rollout.js';

const unit = 'agentos-backlog' as const;
const group = '/system.slice/agentos-backlog.service';
const active = () => ({ MainPID: '77', ActiveState: 'active', ControlGroup: group });
const stopped = (state = 'failed') => ({ MainPID: '0', ActiveState: state, ControlGroup: '' });
function fixture(statuses: any[] = [active(), stopped(), stopped('inactive')]) {
  const calls: string[] = [];
  let i = 0;
  const io = {
    serviceStatus: vi.fn(async (_unit: WorkerService) => {
      calls.push('status');
      if (i >= statuses.length) throw new Error('unexpected status read');
      return statuses[i++];
    }),
    command: vi.fn(async (action: 'stop' | 'reset-failed' | 'start', service: WorkerService) => {
      expect(service).toBe(unit);
      calls.push(action);
    }),
    cgroupEmpty: vi.fn(async (directory: string) => {
      expect(directory).toBe('/sys/fs/cgroup' + group);
      calls.push('group');
      return true;
    }),
    pidGone: vi.fn(async (pid: number) => {
      expect(pid).toBe(Number(statuses[0].MainPID));
      calls.push('pid');
      return true;
    }),
  };
  return { io, calls };
}
const code = { code: 'ROLLOUT_SERVICE_STOP_PROOF' };
function noStart(io: ReturnType<typeof fixture>['io']) {
  expect(io.command.mock.calls.some(([action]) => action === 'start')).toBe(false);
}
function noReset(io: ReturnType<typeof fixture>['io']) {
  expect(io.command.mock.calls.some(([action]) => action === 'reset-failed')).toBe(false);
  noStart(io);
}

it('clears only the failed latch after physical absence and proves inactive afresh before start', async () => {
  const { io, calls } = fixture();
  const before = Date.now();
  const result = await restartStoppedWorkerService(unit, io);
  expect(calls).toEqual(['status', 'stop', 'status', 'group', 'pid', 'reset-failed', 'status', 'group', 'pid', 'start']);
  expect(result).toMatchObject({ previousPid: 77, stopped: { unit, previousPid: 77,
    mainPid: 0, activeState: 'inactive', oldPidGone: true, cgroupEmpty: true } });
  expect(Date.parse(result.stopped.checkedAt)).toBeGreaterThanOrEqual(before);
  expect(Date.parse(result.stopped.checkedAt)).toBeLessThanOrEqual(Date.now());
});
it('preserves normal inactive stop/start without reset', async () => {
  const { io, calls } = fixture([active(), stopped('inactive')]);
  await restartStoppedWorkerService(unit, io);
  expect(calls).toEqual(['status', 'stop', 'status', 'group', 'pid', 'start']);
});
it('proves canonical cgroup absence even when both systemd group fields are empty', async () => {
  const { io } = fixture([{ ...stopped(), MainPID: '0' }, stopped(), stopped('inactive')]);
  await restartStoppedWorkerService(unit, io);
  expect(io.cgroupEmpty).toHaveBeenCalledTimes(2);
  expect(io.pidGone).toHaveBeenCalledWith(0);
});
it('rejects any service outside the worker allowlist before IO', async () => {
  const { io } = fixture();
  await expect(restartStoppedWorkerService('other' as WorkerService, io)).rejects.toMatchObject(code);
  expect(io.serviceStatus).not.toHaveBeenCalled();
  expect(io.command).not.toHaveBeenCalled();
});
describe.each(['before', 'stopped', 'after-reset'] as const)('invalid metadata at %s', (phase) => {
  const index = { before: 0, stopped: 1, 'after-reset': 2 }[phase];
  it.each([undefined, null, '', ' ', 'NaN', '-1', '1.5', '00', '9007199254740992'])('rejects malformed MainPID %s', async (pid) => {
    const rows = [active(), stopped(), stopped('inactive')];
    const { io } = fixture(rows);
    rows[index].MainPID = pid as any;
    await expect(restartStoppedWorkerService(unit, io)).rejects.toMatchObject(code);
    noStart(io);
    if (phase !== 'after-reset') noReset(io);
    if (phase === 'before') expect(io.command).not.toHaveBeenCalled();
  });
  it.each(['/system.slice/other.service', '/system.slice/agentos-backlog.service/child', null])('rejects foreign or malformed cgroup %s', async (badGroup) => {
    const rows = [active(), stopped(), stopped('inactive')];
    rows[index].ControlGroup = badGroup as any;
    const { io } = fixture(rows);
    await expect(restartStoppedWorkerService(unit, io)).rejects.toMatchObject(code);
    noStart(io);
    if (phase !== 'after-reset') noReset(io);
    if (phase === 'before') expect(io.command).not.toHaveBeenCalled();
  });
});
describe.each(['stopped', 'after-reset'] as const)('physical guards at %s', (phase) => {
  const post = phase === 'after-reset';
  it('rejects a live MainPID without starting', async () => {
    const rows = [active(), stopped(), stopped('inactive')];
    rows[post ? 2 : 1].MainPID = '88';
    const { io } = fixture(rows);
    await expect(restartStoppedWorkerService(unit, io)).rejects.toMatchObject(code);
    if (post) noStart(io); else noReset(io);
  });
  it('rejects a live process in the canonical cgroup or descendants', async () => {
    const { io } = fixture();
    if (post) io.cgroupEmpty.mockResolvedValueOnce(true);
    io.cgroupEmpty.mockResolvedValueOnce(false);
    await expect(restartStoppedWorkerService(unit, io)).rejects.toMatchObject(code);
    if (post) noStart(io); else noReset(io);
  });
  it('rejects the old PID still present or reappearing', async () => {
    const { io } = fixture();
    if (post) io.pidGone.mockResolvedValueOnce(true);
    io.pidGone.mockResolvedValueOnce(false);
    await expect(restartStoppedWorkerService(unit, io)).rejects.toMatchObject(code);
    if (post) noStart(io); else noReset(io);
  });
  it('propagates cgroup read errors without start or receipt', async () => {
    const { io } = fixture();
    if (post) io.cgroupEmpty.mockResolvedValueOnce(true);
    io.cgroupEmpty.mockRejectedValueOnce(new Error('read failed'));
    await expect(restartStoppedWorkerService(unit, io)).rejects.toThrow('read failed');
    if (post) noStart(io); else noReset(io);
  });
  it('propagates process read errors without start or receipt', async () => {
    const { io } = fixture();
    if (post) io.pidGone.mockResolvedValueOnce(true);
    io.pidGone.mockRejectedValueOnce(new Error('process read failed'));
    await expect(restartStoppedWorkerService(unit, io)).rejects.toThrow('process read failed');
    if (post) noStart(io); else noReset(io);
  });
});
it.each(['failed', 'active', 'deactivating', undefined])('does not assume inactive after reset returned: %s', async (state) => {
  const { io } = fixture([active(), stopped(), { ...stopped(), ActiveState: state }]);
  await expect(restartStoppedWorkerService(unit, io)).rejects.toMatchObject(code);
  expect(io.command.mock.calls.filter(([action]) => action === 'reset-failed')).toHaveLength(1);
  noStart(io);
});
it.each(['active', 'deactivating', undefined])('does not reset other stopped states: %s', async (state) => {
  const { io } = fixture([active(), { ...stopped(), ActiveState: state }]);
  await expect(restartStoppedWorkerService(unit, io)).rejects.toMatchObject(code);
  noReset(io);
});
it('does not proceed when stop fails or times out', async () => {
  const { io } = fixture();
  io.command.mockRejectedValueOnce(new Error('timeout'));
  await expect(restartStoppedWorkerService(unit, io)).rejects.toThrow('timeout');
  expect(io.serviceStatus).toHaveBeenCalledTimes(1);
  noReset(io);
});
it('does not proceed when reset fails', async () => {
  const { io } = fixture();
  io.command.mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error('reset failed'));
  await expect(restartStoppedWorkerService(unit, io)).rejects.toThrow('reset failed');
  expect(io.serviceStatus).toHaveBeenCalledTimes(2);
  noStart(io);
});
it('does not return a restart receipt when start fails', async () => {
  const { io } = fixture();
  io.command.mockResolvedValueOnce(undefined).mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error('start failed'));
  await expect(restartStoppedWorkerService(unit, io)).rejects.toThrow('start failed');
});
