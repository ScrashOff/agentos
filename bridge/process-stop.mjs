import { execFile } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';

const exec = promisify(execFile);
const validUnit = /^agentos-run-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;

export async function inspectUnit(unit) {
  if (!validUnit.test(unit)) throw new Error('INVALID_UNIT');
  const { stdout } = await exec(
    'systemctl',
    ['show', unit + '.service', '--property=LoadState,ActiveState,SubState,MainPID,ControlGroup'],
    { timeout: 4000 },
  );
  const values = Object.fromEntries(
    stdout
      .trim()
      .split('\n')
      .map((line) => {
        const split = line.indexOf('=');
        return [line.slice(0, split), line.slice(split + 1)];
      }),
  );
  const cgroupPath = `/sys/fs/cgroup/system.slice/${unit}.service`;
  if (values.ControlGroup && values.ControlGroup !== `/system.slice/${unit}.service`)
    throw new Error('UNEXPECTED_CGROUP');
  let populated = false,
    cgroupExists = true;
  try {
    const content = await readFile(cgroupPath + '/cgroup.events', 'utf8');
    if (!/^populated [01]$/m.test(content)) throw new Error('INVALID_CGROUP_STATE');
    populated = /^populated 1$/m.test(content);
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    cgroupExists = false;
  }
  const stopped =
    ['loaded', 'not-found'].includes(values.LoadState) &&
    ['inactive', 'failed'].includes(values.ActiveState) &&
    values.MainPID === '0' &&
    !populated;
  return {
    unit,
    checkedAt: new Date().toISOString(),
    stopped,
    loadState: values.LoadState,
    activeState: values.ActiveState,
    subState: values.SubState,
    mainPid: Number(values.MainPID),
    cgroupExists,
    populated,
  };
}

// systemd and child-pipe closure can become observable at different times.
// Confirmation requires both, and transient inspection failure is never success.
export async function stopUnit(unit, hasExited, options = {}) {
  const inspect = options.inspect ?? inspectUnit;
  const wait = options.wait ?? delay;
  const stop =
    options.stop ?? (() => exec('systemctl', ['stop', unit + '.service'], { timeout: 10000 }));
  let lastProbe;
  try {
    await stop();
  } catch {
    /* An already collected unit can still be confirmed below. */
  }
  for (let i = 0; i < (options.attempts ?? 50); i++) {
    try {
      lastProbe = await inspect(unit);
      if (lastProbe.stopped && hasExited()) return lastProbe;
    } catch {
      lastProbe = { unit, stopped: false, inspectionUnavailable: true };
    }
    await wait(100);
  }
  throw Object.assign(new Error('STOP_UNCONFIRMED'), { code: 'STOP_UNCONFIRMED', lastProbe });
}
