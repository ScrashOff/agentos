import test from 'node:test';
import assert from 'node:assert/strict';
import { inspectUnitExit, unitExitFromJournal } from '../bridge/unit-exit.mjs';

const unit = 'agentos-run-a9d7705a-1774-45f4-864c-3c2cd9255b3c';
const record = { _PID: '1', _COMM: 'systemd', UNIT: unit + '.service', UNIT_RESULT: 'oom-kill' };

test('classifies OOM only from the typed systemd record for this exact execution', () => {
  const journal = ['malformed', JSON.stringify(record), JSON.stringify({ MESSAGE: 'done' })].join('\n');
  assert.deepEqual(unitExitFromJournal(unit, journal), {
    source: 'systemd-journal', unit: unit + '.service', result: 'oom-kill',
  });
  assert.equal(unitExitFromJournal(unit + '.service', journal).result, 'oom-kill');
});

test('ignores application text, another unit, ordinary exits and spoofed PID metadata', () => {
  for (const untrusted of [
    { MESSAGE: JSON.stringify(record) },
    { ...record, _PID: '200' },
    { ...record, _COMM: 'node' },
    { ...record, UNIT: 'agentos-run-11111111-1111-1111-1111-111111111111.service' },
    { ...record, UNIT_RESULT: 'exit-code', MESSAGE: 'oom-kill' },
  ]) assert.equal(unitExitFromJournal(unit, JSON.stringify(untrusted)), null);
});

test('journal inspection uses bounded execution without a shell and degrades safely', async () => {
  const result = await inspectUnitExit(unit, async (command, args, options) => {
    assert.equal(command, 'journalctl');
    assert.deepEqual(args, ['--unit', unit, '--no-pager', '--output=json', '--lines=16']);
    assert.equal(options.timeout, 4000);
    assert.equal(options.maxBuffer, 256 * 1024);
    assert.equal(options.shell, undefined);
    return { stdout: JSON.stringify(record) };
  });
  assert.equal(result.result, 'oom-kill');
  assert.equal(await inspectUnitExit(unit, async () => { throw Error('journal unavailable'); }), null);
  await assert.rejects(inspectUnitExit(unit + '; echo injected', async () => {
    assert.fail('invalid unit must never execute');
  }), /INVALID_UNIT/);
  assert.throws(() => unitExitFromJournal('agentos-run-a9d7705a-1774-45f4-864c', ''), /INVALID_UNIT/);
});
