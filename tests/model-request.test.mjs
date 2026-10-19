import test from 'node:test';
import assert from 'node:assert/strict';
import { requestModel } from '../bridge/model-request.mjs';
test('retries transient failures before handing any response to the runtime', async () => {
  let calls = 0;
  const events = [];
  const response = await requestModel(
    'https://example.invalid',
    { signal: new AbortController().signal },
    (...e) => events.push(e),
    {
      wait: async () => {},
      fetch: async () => {
        calls++;
        if (calls === 1) throw new TypeError('fetch failed');
        return new Response('{}', { status: calls === 2 ? 503 : 200 });
      },
    },
  );
  assert.equal(response.status, 200);
  assert.equal(calls, 3);
  assert.equal(events.length, 2);
});
test('quota and authentication responses are not retried; terminal transport failures stay bounded', async () => {
  for (const status of [401, 403, 402, 429]) {
    let calls = 0;
    const response = await requestModel(
      'https://example.invalid',
      { signal: new AbortController().signal },
      () => {},
      {
        fetch: async () => {
          calls++;
          return new Response('{}', { status });
        },
      },
    );
    assert.equal(response.status, status);
    assert.equal(calls, 1);
  }
  let calls = 0;
  await assert.rejects(
    requestModel('https://example.invalid', { signal: new AbortController().signal }, () => {}, {
      wait: async () => {},
      fetch: async () => {
        calls++;
        throw Error('offline');
      },
    }),
    /offline/,
  );
  assert.equal(calls, 3);
});
test('cancellation never starts a retry', async () => {
  const controller = new AbortController();
  controller.abort();
  let calls = 0;
  await assert.rejects(
    requestModel('https://example.invalid', { signal: controller.signal }, () => {}, {
      fetch: async () => {
        calls++;
        controller.signal.throwIfAborted();
      },
    }),
  );
  assert.equal(calls, 1);
});
