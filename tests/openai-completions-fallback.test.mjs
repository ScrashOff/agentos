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
  assert.deepEqual(message.usage, { input: 10, output: 4, cacheRead:0, totalTokens: 14 });
  assert.throws(()=>fromCompletionsResponse({
    choices: [{ message: { tool_calls: [{ id: 'c2', function: { name: 'x', arguments: '{' } }] },finish_reason:'tool_calls' }],
  }),{code:'MODEL_INCOMPLETE_RESPONSE'});
  assert.throws(()=>fromCompletionsResponse({ choices: [{ message: { content: 'a' }, finish_reason: 'length' }] }),{code:'MODEL_OUTPUT_LIMIT'});
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
  assert.deepEqual(message.usage, { input: 7, output: 9, cacheRead:0, totalTokens: 16 });
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

test('streaming reports production without ever carrying generated content', async () => {
  const reports = [];
  let clock = 0;
  const message = await readCompletionsStream(
    sse([
      { choices: [{ delta: { content: 'primeiro' } }] },
      { choices: [{ delta: { content: ' segundo' } }] },
      { choices: [{ delta: {}, finish_reason: 'stop' }] },
      '[DONE]',
    ]),
    undefined,
    (p) => reports.push(p),
    () => (clock += 4000),
  );
  assert.equal(message.content[0].text, 'primeiro segundo');
  assert.ok(reports.length >= 2, 'progresso precisa ser reportado durante o fluxo');
  assert.equal(reports.at(-1).done, true);
  assert.ok(reports.at(-1).characters === 'primeiro segundo'.length);
  assert.ok(reports.every((p) => Number.isFinite(p.chunks) && Number.isFinite(p.elapsedMs)));
  // Nothing in a report may reveal what was generated.
  const serialized = JSON.stringify(reports);
  assert.ok(!serialized.includes('primeiro') && !serialized.includes('segundo'));
});


// Offline clock and controlled streams: no provider, real-time sleep or database.
function semanticClock() {
  let time=0, sequence=0;
  const pending=new Map();
  return {now:()=>time, pending,
    timers:{
      setTimeout(fn,delay){ const id=++sequence; pending.set(id,{at:time+delay,fn}); return id; },
      clearTimeout(id){ pending.delete(id); },
    },
    advance(ms){
      time+=ms;
      while(true){
        const next=[...pending].filter(([,t])=>t.at<=time).sort((a,b)=>a[1].at-b[1].at)[0];
        if(!next) break;
        pending.delete(next[0]); next[1].fn();
      }
    },
  };
}
const flushStream=()=>new Promise(resolve=>setImmediate(resolve));
const eventBytes=value=>Buffer.from(`data: ${typeof value==='string'?value:JSON.stringify(value)}\n\n`);
function semanticFixture({idleTimeoutMs=100,signal,cancel}={}) {
  const clock=semanticClock(), reports=[], failures=[];
  let control, calls=0, cancelled=0;
  const response=new Response(new ReadableStream({
    start(controller){control=controller;},
    cancel(reason){cancelled++;return cancel?.(reason);},
  }),{headers:{'content-type':'text/event-stream'}});
  const provider=openAICompletionsFallback({fetch:async()=>{calls++;return response;},
    idleTimeoutMs,now:clock.now,timers:clock.timers,random:()=>0});
  const result=provider.streamSimple(model,{messages:[]},{apiKey:'private-test-key',signal,
    onProgress:value=>reports.push(value),onFailure:value=>failures.push(value)}).result();
  const outcome=result.then(value=>({value}),error=>({error}));
  return {clock,reports,failures,outcome,result,
    enqueue:value=>control.enqueue(eventBytes(value)),
    raw:value=>control.enqueue(Buffer.from(value)),
    close:()=>control.close(),error:error=>control.error(error),
    calls:()=>calls,cancellations:()=>cancelled};
}

test('semantic progress permits a stream longer than twelve minutes and clears its timer',async()=>{
  const f=semanticFixture({idleTimeoutMs:300000});
  await flushStream();
  for(let i=0;i<8;i++){
    f.clock.advance(200000);
    f.enqueue({choices:[{delta:{content:'x'}}]});
    await flushStream();
  }
  assert.ok(f.clock.now()>720000,'exercise the old absolute deadline');
  f.enqueue({choices:[{delta:{},finish_reason:'stop'}]});
  f.enqueue('[DONE]'); f.close();
  const result=await f.outcome;
  assert.equal(result.error,undefined);
  assert.equal(result.value.content[0].text,'xxxxxxxx');
  assert.equal(f.calls(),1);
  assert.equal(f.clock.pending.size,0);
  assert.equal(f.reports.at(-1).done,true);
  assert.equal(f.reports.at(-1).semanticCharacters,8);
  assert.equal(f.reports.at(-1).toolCharacters,0);
  assert.ok(f.reports.every(r=>r.semanticCharacters>0));
});

test('idle watchdog covers silent response headers and exposes only its typed diagnostic',async()=>{
  const clock=semanticClock(),diagnostics=[];let calls=0,transportEnded=false;
  const provider=openAICompletionsFallback({idleTimeoutMs:100,now:clock.now,timers:clock.timers,
    fetch:async(_url,{signal})=>{calls++;return new Promise((_resolve,reject)=>{
      signal.addEventListener('abort',()=>{transportEnded=true;reject(signal.reason);},{once:true});
    });}});
  const outcome=provider.streamSimple(model,{messages:[]},{apiKey:'private-test-key',
    onFailure:value=>diagnostics.push(value)}).result().then(value=>({value}),error=>({error}));
  clock.advance(101);
  const result=await outcome;
  assert.equal(result.error.code,'MODEL_IDLE_TIMEOUT');
  assert.equal(transportEnded,true);assert.equal(calls,1);
  assert.deepEqual(diagnostics,[{code:'MODEL_IDLE_TIMEOUT'}]);
  assert.equal(clock.pending.size,0);
  assert.ok(!JSON.stringify(diagnostics).includes('private-test-key'));
});

test('a semantic delta renews idle time, but a later silent body fails without replay',async()=>{
  const f=semanticFixture();await flushStream();
  f.clock.advance(70);f.enqueue({choices:[{delta:{content:'a'}}]});await flushStream();
  f.clock.advance(70);
  assert.equal(f.cancellations(),0,'a real delta renewed the window');
  f.clock.advance(31);
  const result=await f.outcome;
  assert.equal(result.error.code,'MODEL_IDLE_TIMEOUT');
  assert.equal(f.cancellations(),1);assert.equal(f.calls(),1);
  assert.equal(f.clock.pending.size,0);
});

test('keepalives, usage, empty deltas and repeated tool identity cannot renew semantic idle',async()=>{
  const f=semanticFixture();await flushStream();
  f.enqueue({choices:[{delta:{tool_calls:[{index:0,id:'c1',function:{name:'write'}}]}}]});
  await flushStream();
  const initial=f.reports[0].semanticCharacters;
  f.clock.advance(60);
  f.raw(': heartbeat\n\n');
  f.enqueue({usage:{completion_tokens:999},choices:[]});
  f.enqueue({choices:[{delta:{content:'',reasoning_content:'',tool_calls:[{index:0,id:'c1',function:{name:'write',arguments:''}}]}}]});
  await flushStream();
  assert.equal(f.reports.length,1,'byte/chunk activity must not emit semantic progress');
  assert.equal(initial,'c1write'.length);
  f.clock.advance(41);
  const result=await f.outcome;
  assert.equal(result.error.code,'MODEL_IDLE_TIMEOUT');assert.equal(f.calls(),1);
  assert.equal(f.clock.pending.size,0);
});

test('new reasoning, tool name, id and arguments each renew the watchdog',async()=>{
  const f=semanticFixture();await flushStream();
  for(const delta of [
    {reasoning_content:'analyze'},
    {tool_calls:[{index:0,id:'c1'}]},
    {tool_calls:[{index:0,function:{name:'write'}}]},
    {tool_calls:[{index:0,function:{arguments:'{'}}]},
    {tool_calls:[{index:0,function:{arguments:'}'}}]},
  ]){
    f.clock.advance(70);f.enqueue({choices:[{delta}]});await flushStream();
  }
  f.enqueue({choices:[{delta:{},finish_reason:'tool_calls'}]});f.enqueue('[DONE]');f.close();
  const result=await f.outcome;
  assert.equal(result.error,undefined);assert.equal(result.value.stopReason,'toolUse');
  assert.equal(f.reports.at(-1).toolCharacters,2);
  assert.equal(f.reports.at(-1).semanticCharacters,'analyzec1write{}'.length);
  assert.equal(f.clock.pending.size,0);
});

test('caller abort awaits transport cancellation before rejection and never replays',async()=>{
  const caller=new AbortController();let finishCancel,settled=false;
  const f=semanticFixture({signal:caller.signal,cancel:()=>new Promise(resolve=>{finishCancel=resolve;})});
  f.outcome.then(()=>{settled=true;});await flushStream();
  f.enqueue({choices:[{delta:{content:'partial'}}]});await flushStream();
  caller.abort();await flushStream();
  assert.equal(f.cancellations(),1);
  assert.equal(settled,false,'the outer gate must keep its permit until cancel settles');
  finishCancel();
  const result=await f.outcome;
  assert.equal(result.error.code,'CANCELLED');assert.equal(f.calls(),1);
  assert.equal(f.clock.pending.size,0);
});

test('caller cancellation wins when it races an idle timeout during transport cleanup',async()=>{
  const caller=new AbortController();let finishCancel;
  const f=semanticFixture({signal:caller.signal,cancel:()=>new Promise(resolve=>{finishCancel=resolve;})});
  await flushStream();f.clock.advance(101);await flushStream();
  caller.abort();finishCancel();
  const result=await f.outcome;
  assert.equal(result.error.code,'CANCELLED');assert.equal(f.calls(),1);
  assert.equal(f.clock.pending.size,0);
});

test('transport failure after any semantic delta is terminal even inside the retry budget',async()=>{
  const f=semanticFixture();await flushStream();
  f.enqueue({choices:[{delta:{reasoning_content:'private reasoning fragment'}}]});await flushStream();
  f.error(Error('private upstream failure'));
  const result=await f.outcome;
  assert.equal(result.error.code,'MODEL_INCOMPLETE_RESPONSE');assert.equal(f.calls(),1);
  assert.equal(f.clock.pending.size,0);
  assert.deepEqual(f.failures,[{code:'MODEL_INCOMPLETE_RESPONSE'}]);
  assert.ok(!JSON.stringify(f.failures).includes('private'));
});

test('EOF with partial tool arguments is terminal and never replayed as a fresh request',async()=>{
  const f=semanticFixture();await flushStream();
  f.enqueue({choices:[{delta:{tool_calls:[{index:0,id:'c1',function:{name:'write',arguments:'{'}}]}}]});
  await flushStream();f.close();
  const result=await f.outcome;
  assert.equal(result.error.code,'MODEL_INCOMPLETE_RESPONSE');assert.equal(f.calls(),1);
  assert.equal(f.clock.pending.size,0);
});

test('a truly empty SSE body may still use the existing bounded retry policy',async()=>{
  const s=stub([
    new Response('',{headers:{'content-type':'text/event-stream'}}),reply('recovered'),
  ]);
  const result=await run(s.fetch);
  assert.equal(result.content[0].text,'recovered');assert.equal(s.calls.length,2);
});

test('empty SSE retries stop at the existing attempt limit',async()=>{
  const s=stub(Array.from({length:RETRY_ATTEMPTS},()=>
    new Response('',{headers:{'content-type':'text/event-stream'}})));
  await assert.rejects(run(s.fetch),{code:'MODEL_INCOMPLETE_RESPONSE'});
  assert.equal(s.calls.length,RETRY_ATTEMPTS);
});

test('a truncated first SSE frame never replays before it can be parsed semantically',async()=>{
  const f=semanticFixture();await flushStream();
  f.raw('data: {"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{');
  await flushStream();f.close();
  const result=await f.outcome;
  assert.equal(result.error.code,'MODEL_INCOMPLETE_RESPONSE');
  assert.equal(f.calls(),1);assert.equal(f.reports.length,0);
  assert.equal(f.clock.pending.size,0);
});

test('a malformed first SSE frame is terminal even without a valid content delta',async()=>{
  const f=semanticFixture();await flushStream();
  f.raw('data: invalid-json\n\n');
  const result=await f.outcome;
  assert.equal(result.error.code,'MODEL_INCOMPLETE_RESPONSE');
  assert.equal(f.calls(),1);assert.equal(f.reports.length,0);
  assert.equal(f.cancellations(),1);assert.equal(f.clock.pending.size,0);
});

test('partial SSE framing prevents replay but cannot renew idle time',async()=>{
  const f=semanticFixture();await flushStream();
  f.clock.advance(70);f.raw('data: {"choices":');await flushStream();
  f.clock.advance(31);
  const result=await f.outcome;
  assert.equal(result.error.code,'MODEL_IDLE_TIMEOUT');
  assert.equal(f.calls(),1);assert.equal(f.reports.length,0);
  assert.equal(f.clock.pending.size,0);
});

test('already cancelled callers make no fetch and create no timer',async()=>{
  const clock=semanticClock(),caller=new AbortController();let calls=0;caller.abort();
  const result=openAICompletionsFallback({fetch:async()=>{calls++;return reply();},timers:clock.timers,now:clock.now})
    .streamSimple(model,{messages:[]},{signal:caller.signal}).result();
  await assert.rejects(result,{code:'CANCELLED'});
  assert.equal(calls,0);assert.equal(clock.pending.size,0);
});
