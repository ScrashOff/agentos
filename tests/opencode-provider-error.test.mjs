import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createGlmRequestGate } from '../bridge/glm-request-gate.mjs';
import { requestGatedModel } from '../bridge/gated-generation.mjs';
import {
  classifyProviderError, retryAfterSeconds, temporaryProviderReply,
  providerBackoff, pendingProviderBackoff, isRateLimitClientError, mergeProviderBackoff, successfulProviderRequest, requestWithProviderBackoff,
} from '../bridge/opencode-provider-error.mjs';

const now = Date.parse('2026-10-11T21:00:00.000Z');
const identity = { runId: 'mock-run', workspace: 'mock/work', model: 'glm' };
const jsonResponse = (status, error, headers = {}) => new Response(JSON.stringify({ error }), { status, headers });

for (const entry of [
  { status: 402, error: { message: 'payment required' }, code: 'QUOTA_EXCEEDED' },
  { status: 429, error: { code: 'insufficient_quota', message: 'private message' }, code: 'QUOTA_EXCEEDED' },
  { status: 429, error: { type: 'insufficient_quota' }, code: 'QUOTA_EXCEEDED' },
  { status: 429, error: { code: 'organization_spend_limit_exceeded' }, code: 'QUOTA_EXCEEDED' },
  { status: 429, error: { code: 'rate_limit_error' }, code: 'RATE_LIMIT' },
  { status: 429, error: { message: 'quota exhausted; sk-private' }, code: 'RATE_LIMIT' },
  { status: 429, error: { code: '1113' }, code: 'RATE_LIMIT' },
  { status: 401, error: { code: 'insufficient_quota' }, code: 'AUTH_REQUIRED' },
  { status: 503, error: { code: 'insufficient_quota' }, code: 'MODEL_REQUEST_FAILED' },
]) test(`classifies ${entry.status} ${JSON.stringify(entry.error)} by explicit structured evidence`, async () => {
  const result = await classifyProviderError(jsonResponse(entry.status, entry.error));
  assert.equal(result.code, entry.code);
  assert.equal(result.status, entry.status);
  assert.deepEqual(Object.keys(result).sort(), ['category','code','retryAfterSeconds','status']);
  assert.ok(!JSON.stringify(result).includes('private'));
});

test('opaque/oversized 429 bodies remain rate limits and cancel the body without retaining it', async () => {
  let cancelled = false;
  const stream = new ReadableStream({ start(c) { c.enqueue(new TextEncoder().encode('x'.repeat(9000))); }, cancel() { cancelled = true; } });
  const value = await classifyProviderError(new Response(stream, { status: 429 }));
  assert.equal(value.code, 'RATE_LIMIT'); assert.equal(cancelled, true);
  assert.equal((await classifyProviderError(new Response('<html>insufficient_quota</html>', { status: 429 }))).code, 'RATE_LIMIT');
});

test('a stalled error body is bounded and cancelled, without changing known HTTP semantics', async () => {
  let cancelled = false;
  const stream = new ReadableStream({ cancel() { cancelled = true; } });
  const value = await classifyProviderError(new Response(stream, { status: 429 }), { timeoutMs: 10 });
  assert.equal(value.code, 'RATE_LIMIT'); assert.equal(cancelled, true);
});

test('Retry-After accepts integer seconds and all HTTP-date forms without shortening a long valid wait', () => {
  assert.equal(retryAfterSeconds('120', now), 120);
  assert.equal(retryAfterSeconds('Sun, 11 Oct 2026 21:00:04 GMT', now), 4);
  assert.equal(retryAfterSeconds('Sunday, 11-Oct-26 21:00:04 GMT', now), 4);
  assert.equal(retryAfterSeconds('Sun Oct 11 21:00:04 2026', now), 4);
  assert.equal(retryAfterSeconds('Sun, 11 Oct 2026 21:00:01 GMT', now + 1), 1);
  assert.equal(retryAfterSeconds('Sun, 11 Oct 2026 20:00:00 GMT', now), 0);
  assert.equal(retryAfterSeconds('3000000', now), 3000000);
  assert.equal(retryAfterSeconds('Sunday, 11-Oct-60 21:00:04 GMT', now),
    Math.ceil((Date.parse('2060-10-11T21:00:04Z')-now)/1000));
  for (const bad of ['-1','0.5','1e6','Infinity','2026-10-12','60,120','2\r\nAuthorization:secret','9'.repeat(30)])
    assert.equal(retryAfterSeconds(bad, now), null, bad);
});

test('gateway response contains only a fixed error and canonical retry seconds', () => {
  const reply = temporaryProviderReply({ code: 'RATE_LIMIT', status: 429, retryAfterSeconds: 67, body: 'secret', headers: { authorization: 'secret' } });
  assert.equal(reply.status, 429);
  assert.deepEqual(reply.headers, { 'content-type': 'application/json', 'cache-control': 'no-store', 'retry-after': '67' });
  assert.deepEqual(JSON.parse(reply.body), { error: { message: 'RATE_LIMIT', type: 'rate_limit_error', code: 'rate_limit_error' } });
  assert.equal(temporaryProviderReply({ code: 'RATE_LIMIT', status: 429, retryAfterSeconds: null }).headers['retry-after'], undefined);
  assert.throws(() => temporaryProviderReply({ code: 'QUOTA_EXCEEDED', status: 429 }), /INVALID_PROVIDER_FAILURE/);
});

test('provider minimum wait survives a private runtime state roundtrip and expires without quota estimates', () => {
  const value = providerBackoff({ code: 'RATE_LIMIT', status: 429, retryAfterSeconds: 3000000 }, identity, now);
  const state = JSON.parse(JSON.stringify({ ...identity, providerBackoff: value }));
  assert.equal(pendingProviderBackoff(state, identity, now + 1000).seconds, 2999999);
  assert.equal(pendingProviderBackoff(state, identity, now + 3000000000), null);
  assert.equal(pendingProviderBackoff({ ...identity }, identity, now), null);
  assert.equal(providerBackoff({ code: 'RATE_LIMIT', status: 429, retryAfterSeconds: null }, identity, now), null);
  assert.equal(value.remaining, undefined); assert.equal(value.resetAt, undefined);
});

for (const field of ['runId','workspace','model','version','source','status','observedAt','notBefore'])
  test(`recovery rejects an unbound or invalid ${field} instead of admitting early`, () => {
    const wait = providerBackoff({ code: 'RATE_LIMIT', status: 429, retryAfterSeconds: 120 }, identity, now);
    const state = { ...identity, providerBackoff: { ...wait, [field]: field === 'observedAt' ? new Date(now + 60000).toISOString() : 'foreign' } };
    assert.throws(() => pendingProviderBackoff(state, identity, now), /INVALID_PROVIDER_BACKOFF_PROOF/);
  });

test('terminal rate classification requires the actual SDK APIError 429, not previous unrelated failures', () => {
  assert.equal(isRateLimitClientError({ type: 'error', error: { name: 'APIError', data: { statusCode: 429 } } }), true);
  for (const error of [{ name:'UnknownError' },{ name:'APIError',data:{statusCode:500} },{ name:'APIError',data:{message:'429'} }])
    assert.equal(isRateLimitClientError({ type: 'error', error }), false);
});

test('an older in-flight success cannot erase a newer rate error or its provider deadline', () => {
  const failure = { code: 'RATE_LIMIT', observedAt: now + 1000 };
  const wait = providerBackoff({ code:'RATE_LIMIT',status:429,retryAfterSeconds:90 },identity,now + 1000);
  assert.deepEqual(successfulProviderRequest(failure,wait,now), { failure, wait });
  assert.deepEqual(successfulProviderRequest(failure,wait,now + 1000), { failure, wait });
  assert.deepEqual(successfulProviderRequest(failure,wait,now + 2000), { failure:null, wait:null });
  assert.deepEqual(successfulProviderRequest({ ...failure, observedAt:now+3000 },wait,now+2000),
    { failure:{ ...failure, observedAt:now+3000 },wait });
});

test('a concurrent later 429 without or with shorter Retry-After retains the stronger known wait', () => {
  const previous = providerBackoff({ code:'RATE_LIMIT',status:429,retryAfterSeconds:90 },identity,now);
  const shorter = providerBackoff({ code:'RATE_LIMIT',status:429,retryAfterSeconds:5 },identity,now+1000);
  const longer = providerBackoff({ code:'RATE_LIMIT',status:429,retryAfterSeconds:180 },identity,now+1000);
  assert.equal(mergeProviderBackoff(previous,null,now+1000),previous);
  assert.equal(mergeProviderBackoff(previous,shorter,now+1000),previous);
  assert.equal(mergeProviderBackoff(previous,longer,now+1000),longer);
  assert.equal(mergeProviderBackoff(previous,null,now+91000),null);
});

test('SDK early retries during a long wait never reach gate admission or upstream and retain RATE_LIMIT', async () => {
  const wait = providerBackoff({ code:'RATE_LIMIT',status:429,retryAfterSeconds:3000000 },identity,now);
  const state = { ...identity, providerBackoff:wait };
  let calls = 0;
  const upstream = () => { calls++; return 'CONTACTED'; };
  const results = await Promise.all(Array.from({length:12},()=>requestWithProviderBackoff(state,identity,upstream,now+2147483647)));
  assert.equal(calls,0);
  assert.ok(results.every(value=>value.code==='RATE_LIMIT' && value.localBackoff && value.retryAfterSeconds>0));
  for (const result of results) {
    const reply=temporaryProviderReply(result);
    assert.equal(reply.status,429);
    assert.equal(isRateLimitClientError({type:'error',error:{name:'APIError',data:{statusCode:reply.status}}}),true);
  }
  assert.equal(wait.notBefore,new Date(now+3000000000).toISOString());
  assert.equal(requestWithProviderBackoff(state,identity,upstream,now+3000000000),'CONTACTED');
  assert.equal(calls,1);
});

test('temporary 429 releases the actual GLM gate before SDK backoff and permits another waiting session', async t => {
  const directory = await mkdtemp(path.join(tmpdir(), 'glm-rate-mock-'));
  const socketPath = path.join(directory, 'gate.sock');
  const gate = await createGlmRequestGate({ socketPath, maxConcurrent: 1, refreshMs: 15 });
  t.after(async () => { await gate.close(); await rm(directory,{recursive:true,force:true}); });
  const events=[];
  const result = await requestGatedModel({ runId:'rate',socketPath,emit:(kind)=>events.push(kind) }, 'https://unused.invalid', {},
    response => classifyProviderError(response), { fetch:async()=>jsonResponse(429,{code:'rate_limit_error'},{'retry-after':'120'}) });
  assert.equal(result.code,'RATE_LIMIT');
  assert.ok(events.includes('MODEL_REQUEST_RELEASED'));
  const successor = await requestGatedModel({runId:'other',socketPath},'https://unused.invalid',{},r=>r.text(),{fetch:async()=>new Response('OTHER_OK')});
  assert.equal(successor,'OTHER_OK');
  assert.equal(temporaryProviderReply(result).headers['retry-after'],'120');
});

test('a request already queued at the GLM gate rechecks the new 429 wait after admission before any fetch', async t => {
  const directory=await mkdtemp(path.join(tmpdir(),'glm-rate-queued-'));
  const socketPath=path.join(directory,'gate.sock');
  const gate=await createGlmRequestGate({socketPath,maxConcurrent:1,refreshMs:15});
  t.after(async()=>{await gate.close();await rm(directory,{recursive:true,force:true});});
  let stream,wait=null,fetches=0,start;
  const started=new Promise(resolve=>{start=resolve;});
  const first=requestGatedModel({runId:'first',socketPath},'https://unused.invalid',{},async response=>{
    const observed=await classifyProviderError(response);
    // Same ordering as the executor: visible in memory before permit release.
    wait=providerBackoff(observed,identity);
    return observed;
  },{fetch:async()=>{
    fetches++;start();
    return new Response(new ReadableStream({start(controller){stream=controller;}}),{status:429,headers:{'retry-after':'3600'}});
  }});
  await started;
  const second=requestWithProviderBackoff({...identity,providerBackoff:wait},identity,()=>
    requestGatedModel({runId:'second',socketPath,
      beforeRequest:()=>requestWithProviderBackoff({...identity,providerBackoff:wait},identity,()=>undefined)},
    'https://unused.invalid',{},response=>response.text(),{fetch:async()=>{fetches++;return new Response('EARLY');}}));
  const limit=Date.now()+2000;
  while((await gate.status()).queued!==1){assert.ok(Date.now()<limit);await new Promise(resolve=>setTimeout(resolve,5));}
  stream.enqueue(new TextEncoder().encode('{"error":{"code":"rate_limit_error"}}'));stream.close();
  assert.equal((await first).code,'RATE_LIMIT');
  const deferred=await second;
  assert.equal(deferred.code,'RATE_LIMIT');assert.equal(deferred.localBackoff,true);
  assert.equal(fetches,1);
  assert.ok(deferred.retryAfterSeconds>=3599);
});

test('an internal 5xx retry rechecks a concurrent 429 wait before its next fetch', async t => {
  const directory=await mkdtemp(path.join(tmpdir(),'glm-rate-retry-'));
  const socketPath=path.join(directory,'gate.sock');
  const gate=await createGlmRequestGate({socketPath,maxConcurrent:2,refreshMs:15});
  t.after(async()=>{await gate.close();await rm(directory,{recursive:true,force:true});});
  let wait=null,firstFetches=0,secondFetches=0,releaseSleep,notifySleep;
  const sleeping=new Promise(resolve=>{notifySleep=resolve;});
  const resume=new Promise(resolve=>{releaseSleep=resolve;});
  const first=requestGatedModel({runId:'retrying',socketPath,
    beforeRequest:()=>requestWithProviderBackoff({...identity,providerBackoff:wait},identity,()=>undefined)},
    'https://unused.invalid',{},response=>response.text(),{
      fetch:async()=>{firstFetches++;return new Response('temporary',{status:503});},
      wait:async()=>{notifySleep();await resume;},
    });
  await sleeping;
  const observed=await requestGatedModel({runId:'rate',socketPath},'https://unused.invalid',{},async response=>{
    const error=await classifyProviderError(response);
    wait=providerBackoff(error,identity);
    return error;
  },{fetch:async()=>{secondFetches++;return jsonResponse(429,{code:'rate_limit_error'},{'retry-after':'3600'});}});
  assert.equal(observed.code,'RATE_LIMIT');
  releaseSleep();
  const result=await first;
  assert.equal(firstFetches,1);assert.equal(secondFetches,1);
  assert.equal(result.code,'RATE_LIMIT');assert.equal(result.localBackoff,true);
  assert.ok(result.retryAfterSeconds>=3599);
});
