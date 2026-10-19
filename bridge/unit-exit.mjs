import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const exec = promisify(execFile);
const unitPattern = /^agentos-run-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?:\.service)?$/;

// Trust typed records from PID 1 for this exact unit, never application output.
export function unitExitFromJournal(unit, text) {
  if (!unitPattern.test(unit)) throw Error('INVALID_UNIT');
  const name = unit.endsWith('.service') ? unit : unit + '.service';
  for (const line of text.split('\n').reverse()) {
    try {
      const record = JSON.parse(line);
      if (record._PID !== '1' || record._COMM !== 'systemd' || record.UNIT !== name ||
          record.UNIT_RESULT !== 'oom-kill') continue;
      return { source: 'systemd-journal', unit: name, result: 'oom-kill' };
    } catch { /* Ignore malformed or unrelated records. */ }
  }
  return null;
}

export async function inspectUnitExit(unit, execute = exec) {
  if (!unitPattern.test(unit)) throw Error('INVALID_UNIT');
  try {
    const { stdout } = await execute('journalctl', ['--unit', unit, '--no-pager', '--output=json', '--lines=16'],
      { timeout: 4000, maxBuffer: 256 * 1024, windowsHide: true });
    return unitExitFromJournal(unit, stdout);
  } catch { return null; }
}
