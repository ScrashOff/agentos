import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { MuseEvents, museRoute, createMuseGateway } from '../bridge/muse-protocol.mjs';
import { parseArtifactResult } from '../bridge/artifact-collector.mjs';

const session = 'a129d1c1-925f-4ac4-8489-19f546f98773';
const event = (type, payload, id = session) => ({
  schema_version: 1,
  stream: { kind: 'session', id },
  payload_type: type,
  payload,
});
const key = 'test-host-only-secret';
const credential = { key, baseUrl: 'https://api.meta.ai/v1' };
const body = JSON.stringify({ model: 'muse-spark-1.3', input: 'test', stream: true });
const stream = (value) =>
  new Response(`data: ${JSON.stringify(value)}\n\n`, {
    headers: { 'content-type': 'text/event-stream' },
  });
const completed = {
  type: 'response.completed',
  response: {
    status: 'completed',
    usage: {
      input_tokens: 12,
      output_tokens: 3,
      total_tokens: 15,
      input_tokens_details: { cached_tokens: 4 },
    },
  },
};
async function gateway(t, options = {}) {
  const controller = new AbortController();
  const g = createMuseGateway({
    credential,
    model: 'muse-spark-1.3',
    accountId: 'muse-test',
    signal: controller.signal,
    ...options,
  });
  g.server.listen(0, '127.0.0.1');
  await once(g.server, 'listening');
  t.after(() => {
    controller.abort();
    g.server.closeAllConnections();
    g.server.close();
  });
  const port = g.server.address().port;
  const request = (path = '/v1/responses', method = 'POST', headers = {}, payload = body) =>
    new Promise((resolve, reject) => {
      const req = http.request({ host: '127.0.0.1', port, path, method, headers }, (res) => {
        let result = '';
        res.on('data', (chunk) => (result += chunk));
        res.on('end', () =>
          resolve({ status: res.statusCode, body: result, headers: res.headers }),
        );
      });
      req.on('error', reject);
      req.end(payload);
    });
  return { ...g, controller, request, port };
}

test('native final requires the matching session and genuine successful terminal event', () => {
  const state = new MuseEvents(session);
  const text = JSON.stringify({ summary: 'Verified files', artifacts: [] });
  state.accept(event('run.lifecycle.started', {}));
  state.accept(event('run.output.delta', { text }));
  assert.throws(() => state.finalText(), { code: 'INCOMPLETE_TURN' });
  state.accept(
    event('run.terminal.completed', { terminal: 'completed', text }, 'unrelated-session'),
  );
  assert.throws(() => state.finalText(), { code: 'INCOMPLETE_TURN' });
  state.accept(event('run.terminal.completed', { terminal: 'completed', text }));
  assert.equal(parseArtifactResult(state.finalText()).summary, 'Verified files');
  state.accept(event('run.terminal.failed', { terminal: 'failed', text }));
  assert.throws(() => state.finalText(), { code: 'INCOMPLETE_TURN' });
});

test('gateway restricts operations and pins the provider origin', () => {
  assert.equal(
    museRoute('POST', '/v1/responses', credential.baseUrl),
    'https://api.meta.ai/v1/responses',
  );
  assert.equal(
    museRoute('GET', '/muse-code/models', credential.baseUrl),
    'https://api.meta.ai/muse-code/models',
  );
  for (const route of [
    'https://attacker.invalid/v1/responses',
    '/v1/responses?redirect=evil',
    '/v1/../accounts',
    '/v1/files',
  ])
    assert.throws(() => museRoute('POST', route, credential.baseUrl), { code: 'INVALID_ROUTE' });
  assert.throws(() => museRoute('GET', '/v1/responses', credential.baseUrl), {
    code: 'INVALID_ROUTE',
  });
  assert.throws(() => museRoute('POST', '/v1/responses', 'http://provider.invalid/v1'), {
    code: 'ACCOUNT_CONFIGURATION',
  });
});

test('observer or nested run events do not replace the submitted command outcome', () => {
  const state = new MuseEvents(session);
  state.accept(
    event('runtime.command.accepted', { command_kind: 'turn.submit', command_id: 'primary' }),
  );
  state.accept(event('run.lifecycle.started', { command_id: 'primary' }));
  state.accept(event('run.lifecycle.started', { command_id: 'observer' }));
  state.accept(
    event('run.terminal.completed', {
      command_id: 'observer',
      terminal: 'completed',
      text: 'not the task',
    }),
  );
  assert.throws(() => state.finalText(), { code: 'INCOMPLETE_TURN' });
  state.accept(
    event('run.terminal.completed', {
      command_id: 'primary',
      terminal: 'completed',
      text: 'task result',
    }),
  );
  state.accept(
    event('run.terminal.failed', {
      command_id: 'observer',
      terminal: 'failed',
      text: 'observer failed',
    }),
  );
  assert.equal(state.finalText(), 'task result');
});

test('catalog uses its native root path and drops compression headers after fetch decoding', async (t) => {
  let upstream;
  const g = await gateway(t, {
    fetchImpl: async (url) => {
      upstream = url;
      return new Response('{"models":[{"id":"muse-spark-1.3"}]}', {
        headers: { 'content-type': 'application/json', 'content-encoding': 'gzip' },
      });
    },
  });
  const result = await g.request('/muse-code/models', 'GET', {}, '');
  assert.equal(upstream, 'https://api.meta.ai/muse-code/models');
  assert.equal(result.status, 200);
  assert.equal(result.headers['content-encoding'], undefined);
  assert.equal(JSON.parse(result.body).models[0].id, 'muse-spark-1.3');
  assert.equal(g.stats.requests, 0);
});

test('native body and protocol headers are preserved; real credential exists only upstream', async (t) => {
  const events = [];
  let upstream;
  const g = await gateway(t, {
    emit: (...item) => events.push(item),
    fetchImpl: async (url, options) => {
      upstream = { url, options };
      return stream(completed);
    },
  });
  const result = await g.request('/v1/responses', 'POST', {
    authorization: 'Bearer dummy',
    'x-client-id': 'native-cli',
    'user-agent': 'muse/1.4.3',
    cookie: 'untrusted=1',
    'x-api-key': 'untrusted',
  });
  assert.equal(result.status, 200);
  assert.equal(upstream.url, 'https://api.meta.ai/v1/responses');
  assert.equal(upstream.options.headers.authorization, `Bearer ${key}`);
  assert.equal(upstream.options.headers['x-client-id'], 'native-cli');
  assert.equal(upstream.options.headers['user-agent'], 'muse/1.4.3');
  assert.equal(upstream.options.headers.cookie, undefined);
  assert.equal(upstream.options.headers['x-api-key'], undefined);
  assert.equal(upstream.options.body.toString(), body);
  assert.equal(upstream.options.redirect, 'error');
  assert.equal(g.stats.responses, 1);
  assert.equal(g.stats.usage.totalTokens, 15);
  assert.equal(g.stats.usage.cacheRead, 4);
  assert.equal(JSON.stringify({ result, events, stats: g.stats }).includes(key), false);
});

test('provider failures never expose response secrets or raw errors', async (t) => {
  const failures = [],
    events = [];
  const g = await gateway(t, {
    onFailure: (e) => failures.push(e.code),
    emit: (...e) => events.push(e),
    fetchImpl: async () =>
      new Response(`sensitive ${key}`, { status: 401, headers: { 'x-private': key } }),
  });
  const result = await g.request();
  assert.equal(result.status, 502);
  assert.deepEqual(failures, ['AUTH_REQUIRED']);
  assert.equal(JSON.stringify({ result, events }).includes(key), false);
  assert.equal(result.headers['x-private'], undefined);
  assert.equal(g.stats.responses, 0);
});

test('attempts to change the model never reach the provider', async (t) => {
  let calls = 0;
  const g = await gateway(t, {
    fetchImpl: async () => {
      calls++;
      return stream(completed);
    },
  });
  assert.equal(
    (await g.request('/v1/responses', 'POST', {}, JSON.stringify({ model: 'other-model' }))).status,
    404,
  );
  assert.equal((await g.request('/v1/files', 'POST')).status, 404);
  assert.equal(calls, 0);
});

test('cancellation aborts the live provider transport and cannot count as completed', async (t) => {
  let started;
  const inFlight = new Promise((resolve) => (started = resolve));
  let aborted = false;
  const failures = [];
  const g = await gateway(t, {
    onFailure: (e) => failures.push(e.code),
    fetchImpl: (_url, options) =>
      new Promise((_resolve, reject) => {
        options.signal.addEventListener(
          'abort',
          () => {
            aborted = true;
            reject(options.signal.reason);
          },
          { once: true },
        );
        started();
      }),
  });
  const result = g.request();
  await inFlight;
  g.controller.abort();
  await result;
  assert.equal(aborted, true);
  assert.equal(g.stats.responses, 0);
  assert.deepEqual(failures, ['CANCELLED']);
});

test('stream EOF or failed terminal cannot masquerade as model completion', async (t) => {
  const failures = [];
  const g = await gateway(t, {
    onFailure: (e) => failures.push(e.code),
    fetchImpl: async () => stream({ type: 'response.output_text.delta', delta: 'partial' }),
  });
  await g.request();
  assert.equal(g.stats.responses, 0);
  assert.deepEqual(failures, ['MODEL_INCOMPLETE_RESPONSE']);
});
