import test from 'node:test';
import assert from 'node:assert/strict';
import {
  openAICompletionsFallback,
  toCompletionsMessages,
  fromCompletionsResponse,
  backoffDelay,
  readCompletionsStream,
  RETRY_ATTEMPTS,
} from '../bridge/openai-completions-fallback.mjs';

const model = { id: 'glm-5.3-flash', baseUrl: 'https://provider.example/v1', maxTokens: 512 };
const reply = (content = 'ok', extra = {}) =>
  new Response(
    JSON.stringify({ choices: [{ message: { content }, finish_reason: 'stop', ...extra }], usage: null }),
    { status: 200, headers: { 'content-type': 'application/json' } },
  );
const stub = (responses) => {
  const calls = [];
  const queue = [...responses];
  return {
    calls,
    fetch: async (url, request) => {
      calls.push({ url, body: JSON.parse(request.body), headers: request.headers });
      const next = queue.shift();
      if (typeof next === 'function') return next();
      return next;
    },
  };
};
const run = (fetchImpl, options = {}, content = {}) =>
  openAICompletionsFallback({ fetch: fetchImpl, random: () => 0 })
    .streamSimple(model, { systemPrompt: 'be exact', messages: [], tools: [], ...content }, { apiKey: 'k', ...options })
    .result();

test('an assistant turn with tool calls survives the round trip to the wire format', () => {
  const messages = [
    { role: 'user', content: [{ type: 'text', text: 'refactor auth' }] },
    {
      role: 'assistant',
      content: [
        { type: 'text', text: 'reading the file' },
        { type: 'toolCall', id: 'call-1', name: 'read', arguments: { path: 'server/auth.ts' } },
      ],
    },
    { role: 'toolResult', toolCallId: 'call-1', content: [{ type: 'text', text: 'file body' }] },
  ];
  assert.deepEqual(toCompletionsMessages('system text', messages), [
    { role: 'system', content: 'system text' },
    { role: 'user', content: 'refactor auth' },
    {
      role: 'assistant',
      content: 'reading the file',
      tool_calls: [
        { id: 'call-1', type: 'function', function: { name: 'read', arguments: '{"path":"server/auth.ts"}' } },
      ],
    },
    { role: 'tool', tool_call_id: 'call-1', content: 'file body' },
  ]);
});

test('a tool call reply becomes internal content and a toolUse stop reason', () => {
  const message = fromCompletionsResponse({
    choices: [
      {
        message: {
          content: 'calling',
          tool_calls: [{ id: 'c1', function: { name: 'write', arguments: '{"path":"a.ts"}' } }],
        },
        finish_reason: 'tool_calls',
      },
    ],
    usage: { prompt_tokens: 10, completion_tokens: 4, total_tokens: 14 },
  });
  assert.equal(message.stopReason, 'toolUse');
  assert.deepEqual(message.content, [
    { type: 'text', text: 'calling' },
    { type: 'toolCall', id: 'c1', name: 'write', arguments: { path: 'a.ts' } },
  ]);
  assert.deepEqual(message.usage, { input: 10, output: 4, totalTokens: 14 });
  // Unparseable arguments must not abort the turn.
  const broken = fromCompletionsResponse({
    choices: [{ message: { tool_calls: [{ id: 'c2', function: { name: 'x', arguments: '{' } }] } }],
  });
  assert.deepEqual(broken.content[0].arguments, {});
  assert.equal(fromCompletionsResponse({ choices: [{ message: { content: 'a' }, finish_reason: 'length' }] }).stopReason, 'length');
});

test('a credential never leaves the host over cleartext', async () => {
  await assert.rejects(
    openAICompletionsFallback({ fetch: async () => reply() })
      .streamSimple({ ...model, baseUrl: 'http://provider.example/v1' }, { messages: [] }, { apiKey: 'k' })
      .result(),
    { code: 'INSECURE_PROVIDER_URL' },
  );
  const local = stub([reply()]);
  await openAICompletionsFallback({ fetch: local.fetch })
    .streamSimple({ ...model, baseUrl: 'http://127.0.0.1:17820/v1' }, { messages: [] }, { apiKey: 'k' })
    .result();
  assert.equal(local.calls[0].url, 'http://127.0.0.1:17820/v1/chat/completions');
});

test('a rate limit is retried once the stated wait has passed', async () => {
  const limited = new Response('{}', { status: 429, headers: { 'retry-after': '0' } });
  const s = stub([limited, reply('done')]);
  const message = await run(s.fetch);
  assert.equal(s.calls.length, 2);
  assert.deepEqual(message.content, [{ type: 'text', text: 'done' }]);
});

test('exhausted quota and stale authentication are account states, never retried', async () => {
  const quota = stub([
    new Response(JSON.stringify({ error: { code: 'insufficient_quota' } }), { status: 429 }),
  ]);
  await assert.rejects(run(quota.fetch), { code: 'QUOTA_EXCEEDED' });
  assert.equal(quota.calls.length, 1, 'a quota failure must not spend another permit');

  const auth = stub([new Response('{}', { status: 401 })]);
  await assert.rejects(run(auth.fetch), { code: 'AUTH_REQUIRED' });
  assert.equal(auth.calls.length, 1);
});

test('a server failure is retried a bounded number of times and then surfaced', async () => {
  const s = stub(Array.from({ length: RETRY_ATTEMPTS }, () => new Response('{}', { status: 503 })));
  await assert.rejects(run(s.fetch), { code: 'MODEL_REQUEST_FAILED', status: 503 });
  assert.equal(s.calls.length, RETRY_ATTEMPTS);
});

test('a stated wait longer than the budget is handed back instead of retried early', async () => {
  const s = stub([new Response('{}', { status: 429, headers: { 'retry-after': '600' } })]);
  await assert.rejects(run(s.fetch), (error) => {
    assert.equal(error.code, 'RATE_LIMIT');
    assert.equal(error.retryAfterSeconds, 600);
    return true;
  });
  assert.equal(s.calls.length, 1);
  assert.equal(backoffDelay(0, 600), 600000, 'a stated wait is never shortened');
  assert.ok(backoffDelay(0, null, () => 0) === 500 && backoffDelay(2, null, () => 0) === 2000);
  assert.equal(backoffDelay(9, null, () => 0), 15000, 'exponential backoff stays capped');
});

test('cancellation during a provider wait rejects instead of sleeping on', async () => {
  const controller = new AbortController();
  const s = stub([
    () => {
      setTimeout(() => controller.abort(), 10);
      return new Response('{}', { status: 429, headers: { 'retry-after': '5' } });
    },
  ]);
  await assert.rejects(run(s.fetch, { signal: controller.signal }), { code: 'CANCELLED' });
  assert.equal(s.calls.length, 1);
});

test('the request carries the credential, the model and bounded output', async () => {
  const s = stub([reply()]);
  await run(s.fetch, { maxTokens: 777 }, { tools: [{ name: 'read', parameters: {} }] });
  const [call] = s.calls;
  assert.equal(call.headers.authorization, 'Bearer k');
  assert.equal(call.body.model, 'glm-5.3-flash');
  assert.equal(call.body.max_tokens, 777);
  assert.equal(call.body.stream, true);
  assert.deepEqual(call.body.tools, [{ type: 'function', function: { name: 'read', parameters: {} } }]);
});

// This provider emits a few tokens per second, so events arrive split across
// chunk boundaries and a tool call is assembled from many argument fragments.
const sse = (events) => {
  const text = events.map((e) => `data: ${typeof e === 'string' ? e : JSON.stringify(e)}\n\n`).join('');
  const bytes = Buffer.from(text);
  return {
    async *[Symbol.asyncIterator]() {
      for (let at = 0; at < bytes.length; at += 7) yield bytes.subarray(at, at + 7);
    },
  };
};

test('a streamed reply is assembled from deltas split across chunk boundaries', async () => {
  const message = await readCompletionsStream(sse([
    { choices: [{ delta: { content: 'Plano ' } }] },
    { choices: [{ delta: { content: 'em partes' } }] },
    { choices: [{ delta: { tool_calls: [{ index: 0, id: 'c1', function: { name: 'write', arguments: '{"path":' } }] } }] },
    { choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: '"a.php"}' } }] } }] },
    { choices: [{ delta: {}, finish_reason: 'tool_calls' }], usage: { prompt_tokens: 7, completion_tokens: 9, total_tokens: 16 } },
    '[DONE]',
  ]));
  assert.deepEqual(message.content, [
    { type: 'text', text: 'Plano em partes' },
    { type: 'toolCall', id: 'c1', name: 'write', arguments: { path: 'a.php' } },
  ]);
  assert.equal(message.stopReason, 'toolUse');
  assert.deepEqual(message.usage, { input: 7, output: 9, totalTokens: 16 });
});

test('an event-stream response is consumed as a stream, not parsed as one JSON body', async () => {
  const streamed = new Response(null, { status: 200, headers: { 'content-type': 'text/event-stream' } });
  Object.defineProperty(streamed, 'body', { value: sse([
    { choices: [{ delta: { content: 'ok' }, finish_reason: 'stop' }] }, '[DONE]',
  ]) });
  const message = await openAICompletionsFallback({ fetch: async () => streamed })
    .streamSimple(model, { messages: [] }, { apiKey: 'k' })
    .result();
  assert.deepEqual(message.content, [{ type: 'text', text: 'ok' }]);
  assert.equal(message.stopReason, 'stop');
});

test('cancellation stops consuming a stream in flight', async () => {
  const controller = new AbortController();
  const slow = {
    async *[Symbol.asyncIterator]() {
      yield Buffer.from('data: {"choices":[{"delta":{"content":"a"}}]}\n\n');
      controller.abort();
      yield Buffer.from('data: {"choices":[{"delta":{"content":"b"}}]}\n\n');
    },
  };
  await assert.rejects(readCompletionsStream(slow, controller.signal), { code: 'CANCELLED' });
});
