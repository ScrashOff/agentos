import { afterEach, expect, it } from 'vitest';
import { createServer, type Server, type RequestListener } from 'node:http';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
// @ts-expect-error Runtime ESM has no generated declarations.
import { bufferedModelChannel } from '../bridge/runtime-model-channel.mjs';
// @ts-expect-error Runtime ESM has no generated declarations.
import { withGlmRequestSlot } from '../bridge/gated-generation.mjs';

const servers: Server[] = [];
afterEach(async () => {
  for (const server of servers.splice(0)) {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
async function endpoint(handler: RequestListener) {
  const server = createServer(handler);
  servers.push(server);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('NO_LISTENER');
  return `http://127.0.0.1:${address.port}`;
}
function pending<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

it('keeps a buffered response alive beyond the consumer idle deadline without publishing partial tools', async () => {
  const finish = pending(), stopped = pending();
  const caller = new AbortController();
  const url = await endpoint(async (_req, res) => {
    const channel = bufferedModelChannel(res, caller.signal, { heartbeatMs: 10 });
    try {
      channel.start();
      await finish.promise;
      channel.finish([{ choices: [{ delta: { tool_calls: [{ id: 'complete', function: { arguments: '{"command":"pwd"}' } }] } }] }]);
    } finally { channel.dispose(); stopped.resolve(); }
  });
  const response = await fetch(url);
  expect(response.headers.get('content-type')).toBe('text/event-stream');
  const reader = response.body!.getReader();
  let received = '', timer: ReturnType<typeof setTimeout> | undefined;
  const expired = pending<boolean>();
  const watchdog = () => {
    clearTimeout(timer);
    timer = setTimeout(() => expired.resolve(true), 200);
  };
  watchdog();
  const consume = (async () => {
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      received += new TextDecoder().decode(part.value);
      watchdog();
    }
    clearTimeout(timer);
  })();
  await delay(500);
  expect(received).toContain(': agentos transport keepalive');
  expect(received).not.toContain('data:');
  expect(await Promise.race([expired.promise, Promise.resolve(false)])).toBe(false);
  finish.resolve();
  await consume;
  await stopped.promise;
  expect(received).toContain('"id":"complete"');
  expect(received.endsWith('data: [DONE]\n\n')).toBe(true);
  expect(caller.signal.aborted).toBe(false);
});

it('reports provider timeout after headers and retains the permit until abort unwinds', async () => {
  const aborted = pending(), unwind = pending(), finished = pending();
  let released = false, code: string | undefined;
  const url = await endpoint(async (_req, res) => {
    const channel = bufferedModelChannel(res, new AbortController().signal, { heartbeatMs: 10 });
    try {
      channel.start();
      await withGlmRequestSlot({ signal: channel.signal, requestTimeoutMs: 25 }, async (signal: AbortSignal) => {
        signal.addEventListener('abort', () => aborted.resolve(), { once: true });
        await unwind.promise;
        // A transport may normalize AbortError; the gate retains timeout reason.
        throw Object.assign(new Error('CANCELLED'), { code: 'CANCELLED' });
      }, { acquire: async () => ({ release: () => { released = true; } }) });
    } catch (error) { code = channel.failureCode(error); channel.fail(code); }
    finally { channel.dispose(); finished.resolve(); }
  });
  const response = await fetch(url);
  await aborted.promise;
  expect(released).toBe(false);
  unwind.resolve();
  const body = await response.text();
  await finished.promise;
  expect(released).toBe(true);
  expect(code).toBe('MODEL_TIMEOUT');
  expect(body).toContain('"message":"MODEL_TIMEOUT"');
  expect(body).not.toContain('[DONE]');
});

it('classifies a dropped consumer as disconnection rather than user cancellation', async () => {
  const result = pending<string>();
  const url = await endpoint(async (_req, res) => {
    const channel = bufferedModelChannel(res, new AbortController().signal, { heartbeatMs: 10 });
    try {
      channel.start();
      await once(channel.signal, 'abort');
      result.resolve(channel.failureCode(Object.assign(new Error('CANCELLED'), { code: 'CANCELLED' })));
    } finally { channel.dispose(); }
  });
  const client = new AbortController();
  const response = await fetch(url, { signal: client.signal });
  await response.body!.getReader().read();
  client.abort();
  expect(await result.promise).toBe('MODEL_CLIENT_DISCONNECTED');
});

it('preserves explicit caller cancellation even when it races a provider timeout', async () => {
  const caller = new AbortController(), ready = pending(), result = pending<string>();
  const url = await endpoint(async (_req, res) => {
    const channel = bufferedModelChannel(res, caller.signal);
    try {
      channel.start();
      ready.resolve();
      await once(channel.signal, 'abort');
      const code = channel.failureCode(new DOMException('Timeout', 'TimeoutError'));
      channel.fail(code);
      result.resolve(code);
    } finally { channel.dispose(); }
  });
  const response = await fetch(url);
  await ready.promise;
  caller.abort();
  expect(await result.promise).toBe('CANCELLED');
  expect(await response.text()).toContain('"message":"CANCELLED"');
});
