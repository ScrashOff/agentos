import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { readFile, readdir, realpath } from 'node:fs/promises';
import path from 'node:path';
import {
  assertNativeRelease,
  nativeSupervisorCgroup,
  nativeSupervisorUnit,
  type NativeReservation,
  type NativeUnitObservation,
  type NativeUnitProof,
} from '../server/native-supervision.js';
import { workerSourceDigest } from '../server/worker-readiness.js';

const exec = promisify(execFile);
const node = '/opt/deepseek-harness/node/bin/node';

/** No task text, credentials or provider headers occur in process arguments.
 * The supervisor is its own top-level systemd unit, never a child in the
 * dispatcher's cgroup and never PartOf/BindsTo the dispatcher. */
export function nativeSupervisorArguments(reservation: NativeReservation) {
  assertNativeRelease(reservation);
  if (
    reservation.unit !== nativeSupervisorUnit(reservation.runId) ||
    !/^[0-9a-f-]{36}$/.test(reservation.launchToken)
  )
    throw Error('NATIVE_LAUNCH_INVALID');
  return [
    '--quiet',
    '--no-block',
    '--collect',
    '--unit=' + reservation.unit,
    '--property=Type=exec',
    '--property=Slice=agentos_native_supervisors.slice',
    '--property=WorkingDirectory=' + reservation.candidate,
    '--property=EnvironmentFile=/etc/agentos/server.env',
    '--property=Environment=PATH=/opt/deepseek-harness/node/bin:/usr/bin:/bin',
    '--property=NoNewPrivileges=yes',
    '--property=KillMode=control-group',
    '--property=TimeoutStopSec=10',
    '--property=UMask=0077',
    '--property=Restart=no',
    '--',
    node,
    '--import',
    reservation.candidate + '/node_modules/tsx/dist/loader.mjs',
    reservation.candidate + (reservation.runtime==='harness'?'/scripts/harness-supervisor.ts':'/scripts/opencode-batch.ts'),
    'supervise',
    reservation.runId,
    reservation.launchToken,
  ];
}
export async function launchNativeSupervisor(reservation: NativeReservation) {
  await exec('systemd-run', nativeSupervisorArguments(reservation), {
    timeout: 20000,
    maxBuffer: 1024 * 1024,
    windowsHide: true,
  });
}
async function emptyCgroup(directory: string): Promise<boolean> {
  try {
    if ((await readFile(path.join(directory, 'cgroup.procs'), 'utf8')).trim()) return false;
    for (const entry of await readdir(directory, { withFileTypes: true }))
      if (entry.isDirectory() && !(await emptyCgroup(path.join(directory, entry.name))))
        return false;
    return true;
  } catch (error: any) {
    if (error.code === 'ENOENT') return true;
    throw error;
  }
}
export async function observeNativeSupervisor(
  reservation: NativeReservation,
): Promise<NativeUnitObservation> {
  if (reservation.unit !== nativeSupervisorUnit(reservation.runId))
    throw Error('NATIVE_UNIT_INVALID');
  let stdout: string;
  try {
    ({ stdout } = await exec(
      'systemctl',
      ['show', reservation.unit, '--property=ActiveState,LoadState,MainPID,Job,ControlGroup'],
      { timeout: 10000, maxBuffer: 4096, windowsHide: true },
    ));
  } catch (error: any) {
    // systemctl returns 1 for a collected/unknown unit but still provides its
    // explicit not-found/inactive/zero-PID properties. Other errors are unknown.
    if (
      error.code !== 1 ||
      typeof error.stdout !== 'string' ||
      !/^LoadState=not-found$/m.test(error.stdout)
    )
      throw error;
    stdout = error.stdout;
  }
  const props = Object.fromEntries(
    stdout
      .trim()
      .split('\n')
      .map((line) => {
        const at = line.indexOf('=');
        return [line.slice(0, at), line.slice(at + 1)];
      }),
  );
  const expected = nativeSupervisorCgroup(reservation.runId);
  if (props.ControlGroup && props.ControlGroup !== expected) throw Error('NATIVE_CGROUP_INVALID');
  const mainPid = Number(props.MainPID);
  if (!Number.isSafeInteger(mainPid) || mainPid < 0) throw Error('NATIVE_PID_INVALID');
  return {
    unit: reservation.unit,
    checkedAt: new Date().toISOString(),
    activeState: props.ActiveState,
    loadState: props.LoadState,
    mainPid,
    pendingJob: !['', '0'].includes(props.Job ?? 'unknown'),
    cgroupEmpty: await emptyCgroup('/sys/fs/cgroup' + expected),
  };
}
/** Do not trust argv, a metadata label or current symlink as readiness. */
export async function nativeSupervisorSelfProof(
  reservation: NativeReservation,
): Promise<NativeUnitProof> {
  assertNativeRelease(reservation);
  const cwd = await realpath(process.cwd());
  const cgroup = (await readFile('/proc/self/cgroup', 'utf8'))
    .split('\n')
    .find((line) => line.startsWith('0::'))
    ?.slice(3);
  if (
    cwd !== reservation.candidate ||
    cgroup !== nativeSupervisorCgroup(reservation.runId) ||
    (await workerSourceDigest(cwd)) !== reservation.digest
  )
    throw Error('NATIVE_SUPERVISOR_SELF_PROOF');
  const observed = await observeNativeSupervisor(reservation);
  if (
    !['active', 'activating'].includes(observed.activeState) ||
    observed.mainPid !== process.pid ||
    observed.cgroupEmpty
  )
    throw Error('NATIVE_SUPERVISOR_MAIN_PID_PROOF');
  return { unit: reservation.unit, pid: process.pid, cwd, cgroup };
}
