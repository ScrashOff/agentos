import test from 'node:test';
import assert from 'node:assert/strict';
import { requestModel } from '../bridge/model-request.mjs';

const enc = new TextEncoder();
const drip = (chunks, gapMs, stallAfter = -1) => new ReadableStream({
  async start(controller) {
    for (let i = 0; i < chunks.length; i++) {
      if (i === stallAfter) return; // stall: never enqueue/close again
      controller.enqueue(enc.encode(chunks[i]));
      await new Promise((r) => setTimeout(r, gapMs));
    }
    controller.close();
  },
});

test('slow but progressing streams are not killed by a total-duration cap', async () => {
  const chunks = Array.from({ length: 8 }, (_, i) => `data: ${i}\n\n`);
  const fetch = async () => new Response(drip(chunks, 25), { status: 200 });
  const res = await requestModel('http://provider.invalid/v1', { signal: new AbortController().signal }, () => {}, {
    fetch,
    wait: async () => {},
    idleTimeoutMs: 60,
    totalTimeoutMs: 5000,
  });
  assert.equal(await res.text(), chunks.join(''));
});

test('a stalled stream aborts on idle instead of hanging forever', async () => {
  const fetch = async () => new Response(drip(['a', 'b'], 5, 1), { status: 200 });
  const res = await requestModel('http://provider.invalid/v1', { signal: new AbortController().signal }, () => {}, {
    fetch,
    wait: async () => {},
    idleTimeoutMs: 60,
    totalTimeoutMs: 5000,
  });
  // The response was already returned to the runtime; a mid-body stall must
  // surface as a TimeoutError on the body, never as clean EOF or a hang.
  await assert.rejects(res.text(), (e) => e.name === 'TimeoutError');
});

test('a pre-response transport timeout is retried before returning', async () => {
  let calls = 0;
  const events = [];
  const fetch = async () => {
    calls++;
    if (calls === 1) throw new DOMException('timed out', 'TimeoutError');
    return new Response(drip(['ok'], 1), { status: 200 });
  };
  const res = await requestModel('http://provider.invalid/v1', { signal: new AbortController().signal }, (k, v) => events.push([k, v]), {
    fetch,
    wait: async () => {},
    idleTimeoutMs: 60000,
    totalTimeoutMs: 60000,
  });
  assert.equal(await res.text(), 'ok');
  assert.equal(calls, 2);
  assert.equal(events.some(([k]) => k === 'MODEL_RETRY'), true);
});

test('caller abort wins and is never retried', async () => {
  const caller = new AbortController();
  let calls = 0;
  const fetch = async () => {
    calls++;
    return new Response(drip(['x'], 5, 0), { status: 200 });
  };
  const pending = requestModel('http://provider.invalid/v1', { signal: caller.signal }, () => {}, {
    fetch,
    wait: async () => {},
    idleTimeoutMs: 60000,
    totalTimeoutMs: 60000,
  });
  setTimeout(() => caller.abort(new Error('caller-stop')), 30);
  await assert.rejects(pending.then((r) => r.text()));
  assert.equal(calls, 1);
});
