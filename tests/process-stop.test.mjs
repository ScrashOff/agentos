import test from 'node:test';
import assert from 'node:assert/strict';
import { stopUnit } from '../bridge/process-stop.mjs';
const unit = 'agentos-run-11111111-1111-1111-1111-111111111111';
const wait = async () => {};
test('rechecks a transitional unit and waits for child pipes before confirming', async () => {
  let samples = 0;
  const result = await stopUnit(unit, () => samples >= 3, {
    wait,
    stop: async () => {},
    inspect: async () => ({ unit, stopped: ++samples >= 2 }),
    attempts: 5,
  });
  assert.equal(result.stopped, true);
  assert.equal(samples, 3);
});
test('does not confuse unavailable inspection or a populated unit with termination', async () => {
  for (const inspect of [
    async () => {
      throw Error('ssh/systemd unavailable');
    },
    async () => ({ stopped: false, populated: true }),
  ])
    await assert.rejects(
      stopUnit(unit, () => true, { wait, inspect, stop: async () => {}, attempts: 2 }),
      { code: 'STOP_UNCONFIRMED' },
    );
});
test('already collected unit can be confirmed despite stop returning not-found', async () => {
  const result = await stopUnit(unit, () => true, {
    wait,
    stop: async () => {
      throw Error('not-found');
    },
    inspect: async () => ({ unit, stopped: true, mainPid: 0, populated: false }),
    attempts: 1,
  });
  assert.equal(result.stopped, true);
});
