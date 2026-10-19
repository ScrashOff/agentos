import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { createGlmRequestGate } from '../bridge/glm-request-gate.mjs';
import { acquireGlmRequestSlot, getGlmRequestGateStatus } from '../bridge/glm-request-slot.mjs';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(predicate, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (!(await predicate())) {
    if (Date.now() > deadline) assert.fail('Timed out waiting for gate state');
    await sleep(5);
  }
}
async function setup(t, options = {}) {
  const directory = await mkdtemp(path.join(tmpdir(), 'glm-gate-'));
  const socketPath =
    process.platform === 'win32'
      ? `\\\\.\\pipe\\agentos-glm-${randomUUID()}`
      : path.join(directory, 'gate.sock');
  const gate = await createGlmRequestGate({ socketPath, refreshMs: 20, ...options });
  t.after(async () => {
    await gate.close();
    await rm(directory, { recursive: true, force: true });
  });
  return { gate, socketPath, directory };
}
const acquire = (socketPath, runId, options = {}) =>
  acquireGlmRequestSlot({ socketPath, runId, ...options });

test('more than six sessions share six permits, preserve FIFO and hold through stream lifetime', async (t) => {
  const { gate, socketPath } = await setup(t);
  const first = await Promise.all(
    Array.from({ length: 6 }, (_, i) => acquire(socketPath, `initial-${i}`)),
  );
  const waiting = [];
  const order = [];
  for (let i = 0; i < 5; i++) {
    let queued = false;
    waiting.push(
      acquire(socketPath, `waiting-${i}`, {
        onState: (state) => {
          if (state.state === 'queued') queued = true;
        },
      }).then((slot) => {
        order.push(i);
        return slot;
      }),
    );
    await until(() => queued);
  }
  assert.equal((await gate.status()).active, 6);
  assert.equal((await getGlmRequestGateStatus({ socketPath })).queued, 5);
  // Receiving headers/partial SSE chunks must not release a permit.
  await sleep(40);
  assert.deepEqual(order, []);
  for (let i = 0; i < waiting.length; i++) {
    first[i].release();
    const replacement = await waiting[i];
    assert.deepEqual(
      order,
      Array.from({ length: i + 1 }, (_, n) => n),
    );
    assert.equal((await gate.status()).active, 6);
    waiting[i] = Promise.resolve(replacement);
  }
  for (const slot of await Promise.all(waiting)) slot.release();
  first[5].release();
  await until(async () => (await gate.status()).active === 0);
  assert.equal((await gate.status()).maxObserved, 6);
  assert.equal((await gate.status()).granted, 11);
});

test('queued cancellation removes the request without taking a slot or blocking its successor', async (t) => {
  const { gate, socketPath } = await setup(t, { maxConcurrent: 1 });
  const holder = await acquire(socketPath, 'holder');
  const controller = new AbortController();
  const cancelled = acquire(socketPath, 'cancelled', { signal: controller.signal });
  const rejection = assert.rejects(cancelled, { code: 'GLM_REQUEST_ABORTED' });
  const next = acquire(socketPath, 'successor');
  await until(async () => (await gate.status()).queued === 2);
  controller.abort();
  await rejection;
  await until(async () => (await gate.status()).queued === 1);
  holder.release();
  const successor = await next;
  successor.release();
  await until(async () => (await gate.status()).active === 0);
  assert.equal((await gate.status()).granted, 2);
  assert.equal((await gate.status()).cancelled, 1);
});

test('gate loss aborts an in-flight provider and rejects queued work; release is not reported as loss', async (t) => {
  const { gate, socketPath } = await setup(t, { maxConcurrent: 1 });
  const provider = new AbortController();
  let losses = 0;
  const holder = await acquire(socketPath, 'streaming', {
    onLost: (error) => {
      losses++;
      provider.abort(error);
    },
  });
  const pending = acquire(socketPath, 'waiting');
  const rejection = assert.rejects(pending, { code: 'GLM_GATE_LOST' });
  await until(async () => (await gate.status()).queued === 1);
  await gate.close();
  await rejection;
  await until(() => provider.signal.aborted);
  assert.equal(losses, 1);
  holder.release();
  assert.equal(losses, 1);

  const second = await setup(t, { maxConcurrent: 1 });
  let normalLoss = false;
  const normal = await acquire(second.socketPath, 'normal', {
    onLost: () => {
      normalLoss = true;
    },
  });
  normal.release();
  await until(async () => (await second.gate.status()).active === 0);
  assert.equal(normalLoss, false);
});

test('legacy executors reserve capacity until identity is proved stopped, with read errors failing closed', async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), 'glm-reservations-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const reservationsPath = path.join(directory, 'legacy.json');
  const records = Array.from({ length: 6 }, (_, i) => ({
    runId: `legacy-${i}`,
    pid: 100 + i,
    startTime: `${1000 + i}`,
  }));
  await writeFile(reservationsPath, JSON.stringify({ reservations: records }));
  const active = new Set(records.map((record) => record.runId));
  let unreadable = false;
  const { gate, socketPath } = await setup(t, {
    reservationsPath,
    legacyActive: async (record) => {
      if (unreadable) throw new Error('EACCES');
      return active.has(record.runId);
    },
  });
  const waiting = acquire(socketPath, 'new-worker');
  await until(async () => (await gate.status()).queued === 1);
  await sleep(50);
  assert.equal((await gate.status()).active, 0);
  assert.equal((await gate.status()).legacyReserved, 6);
  unreadable = true;
  active.delete('legacy-0');
  assert.equal((await gate.status()).reservationState, 'unconfirmed');
  assert.equal((await gate.status()).active, 0);
  unreadable = false;
  const slot = await waiting;
  assert.equal((await gate.status()).effectiveActive, 6);
  slot.release();
  await until(async () => (await gate.status()).active === 0);

  await writeFile(reservationsPath, '{invalid json');
  const controller = new AbortController();
  const blocked = acquire(socketPath, 'blocked', { signal: controller.signal });
  const rejection = assert.rejects(blocked, { code: 'GLM_REQUEST_ABORTED' });
  await until(async () => (await gate.status()).queued === 1);
  assert.equal((await gate.status()).reservationState, 'unconfirmed');
  assert.equal((await gate.status()).active, 0);
  controller.abort();
  await rejection;
});

test('eight independent processes never exceed six simultaneous provider request permits', async (t) => {
  const { gate, socketPath } = await setup(t);
  const moduleUrl = new URL('../bridge/glm-request-slot.mjs', import.meta.url).href;
  const children = [];
  const acquired = [];
  const failures = [];
  t.after(() => {
    for (const child of children) if (child.exitCode === null) child.kill();
  });
  for (let i = 0; i < 8; i++) {
    const script = `
      import { acquireGlmRequestSlot } from ${JSON.stringify(moduleUrl)};
      const slot = await acquireGlmRequestSlot({socketPath:${JSON.stringify(socketPath)},runId:'child-${i}',onLost:()=>process.exit(2)});
      process.send({acquired:${i}});
      process.once('message',()=>{ slot.release(); process.disconnect(); });
    `;
    const child = spawn(process.execPath, ['--input-type=module', '-e', script], {
      stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
    });
    children.push(child);
    child.on('message', (message) => acquired.push(message.acquired));
    child.stderr.on('data', (data) => failures.push(data.toString()));
  }
  await until(async () => acquired.length === 6 && (await gate.status()).queued === 2, 15000);
  assert.equal((await gate.status()).active, 6);
  children[acquired[0]].send('release');
  await until(() => acquired.length === 7);
  children[acquired[1]].send('release');
  await until(() => acquired.length === 8);
  for (const index of acquired.slice(2)) children[index].send('release');
  await until(async () => (await gate.status()).active === 0);
  assert.equal((await gate.status()).maxObserved, 6);
  assert.deepEqual(failures, []);
});

test('unavailable gate rejects acquisition instead of allowing an uncounted provider request', async () => {
  const socketPath =
    process.platform === 'win32'
      ? `\\\\.\\pipe\\missing-${randomUUID()}`
      : path.join(tmpdir(), `missing-${randomUUID()}.sock`);
  await assert.rejects(acquire(socketPath, 'must-wait'), { code: 'GLM_GATE_UNAVAILABLE' });
});

test('abort on admission rejects promptly and frees the just-granted permit', async (t) => {
  const { gate, socketPath } = await setup(t);
  const controller = new AbortController();
  await assert.rejects(
    acquire(socketPath, 'admission-abort', {
      signal: controller.signal,
      onState: (state) => {
        if (state.state === 'acquired') controller.abort();
      },
    }),
    { code: 'GLM_REQUEST_ABORTED' },
  );
  await until(async () => (await gate.status()).active === 0);
});
