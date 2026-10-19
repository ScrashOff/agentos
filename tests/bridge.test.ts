import { it, expect, vi, afterEach } from 'vitest';
import { PassThrough } from 'node:stream';
import { EventEmitter } from 'node:events';
vi.mock('node:child_process', async (importOriginal) => ({
  ...(await importOriginal<typeof import('node:child_process')>()),
  spawn: vi.fn(),
}));
import { spawn } from 'node:child_process';
import { AccountBridge } from '../server/bridge.js';
import { config } from '../server/config.js';
import { taskDeliveryContract } from '../bridge/task-delivery-contract.mjs';
function fixture() {
  config.bridgeHost = 'test-only';
  const child = Object.assign(new EventEmitter(), {
    stdin: new PassThrough(),
    stdout: new PassThrough(),
    stderr: new PassThrough(),
    killed: false,
  });
  vi.mocked(spawn).mockReturnValue(child as any);
  return { bridge: new AccountBridge(), child };
}
afterEach(() => {
  config.bridgeHost = '';
  vi.restoreAllMocks();
  vi.useRealTimers();
});
it('preserves the typed task delivery contract in the worker.execute SSH envelope', async () => {
  const { bridge, child } = fixture();
  const deliveryContract = taskDeliveryContract([
    { type: 'artifact', name: 'change.diff' },
    { type: 'artifact', name: 'manifest.json' },
    { type: 'json_field', name: 'verification.json' },
  ]);
  const result = bridge.request('worker.execute', { runId: 'fixture', deliveryContract });
  const sent = JSON.parse(child.stdin.read().toString());
  expect(sent).toMatchObject({ method: 'worker.execute', params: { runId: 'fixture', deliveryContract } });
  child.stdout.write(JSON.stringify({ id: sent.id, result: { evidence: { stopped: true } } }) + '\n');
  expect(await result).toEqual({ evidence: { stopped: true } });
  bridge.close();
});
it('keeps cancellation pending until the remote request finishes', async () => {
  const { bridge, child } = fixture();
  const controller = new AbortController();
  const result = bridge.request('generate', {}, { signal: controller.signal }).catch((e) => e);
  const id = [...bridge.pending.keys()][0];
  controller.abort();
  expect(bridge.pending.size).toBe(1);
  child.stdout.write(
    JSON.stringify({ id, error: { code: 'CANCELLED', message: 'Stopped' } }) + '\n',
  );
  expect(await result).toMatchObject({ code: 'CANCELLED' });
  expect(bridge.pending.size).toBe(0);
  bridge.close();
});
it('marks a broken connection as uncertain instead of claiming cancellation', async () => {
  const { bridge, child } = fixture();
  const result = bridge.request('generate').catch((e) => e);
  child.emit('close', 255);
  expect(await result).toMatchObject({ code: 'STOP_UNCONFIRMED' });
  expect(bridge.pending.size).toBe(0);
  bridge.close();
});
it('keeps worker events pending and never masks unconfirmed stop with a requested cancellation', async () => {
  const { bridge, child } = fixture();
  const c = new AbortController(),
    onEvent = vi.fn();
  const result = bridge
    .request('worker.execute', {}, { signal: c.signal, onEvent })
    .catch((e) => e);
  const id = [...bridge.pending.keys()][0];
  child.stdout.write(
    JSON.stringify({ id, event: { kind: 'HARNESS_STARTED', payload: { ready: true } } }) + '\n',
  );
  expect(onEvent).toHaveBeenCalledWith('HARNESS_STARTED', { ready: true });
  expect(bridge.pending.size).toBe(1);
  c.abort();
  child.stdout.write(
    JSON.stringify({ id, error: { code: 'STOP_UNCONFIRMED', message: 'Still uncertain' } }) + '\n',
  );
  expect(await result).toMatchObject({ code: 'STOP_UNCONFIRMED' });
  bridge.close();
});

it('keeps an active worker beyond the original deadline and clears its timer on confirmed completion', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100});
  const {id}=JSON.parse(child.stdin.read().toString());
  await vi.advanceTimersByTimeAsync(60);
  child.stdout.write(JSON.stringify({id,event:{kind:'MODEL_RESPONSE',payload:{requests:1}}})+'\n');
  await vi.advanceTimersByTimeAsync(60);
  child.stdout.write(JSON.stringify({id,event:{kind:'HARNESS_TOOL_EVENT',payload:{toolEvents:1}}})+'\n');
  await vi.advanceTimersByTimeAsync(60);
  expect(bridge.pending.get(id)?.cancelError).toBeUndefined();
  expect(child.stdin.read()).toBeNull();
  child.stdout.write(JSON.stringify({id,result:{summary:'Actual result',evidence:{stopped:true}}})+'\n');
  expect(await result).toMatchObject({summary:'Actual result',evidence:{stopped:true}});
  expect(bridge.pending.size).toBe(0);
  expect(vi.getTimerCount()).toBe(0);
  bridge.close();
});

it('heartbeat and queued/request-start notifications do not extend inactivity or prove completion', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  for (const kind of ['HEARTBEAT','GLM_REQUEST_SLOT_QUEUED','MODEL_REQUEST']) {
    await vi.advanceTimersByTimeAsync(30);
    child.stdout.write(JSON.stringify({id,event:{kind,payload:{}}})+'\n');
  }
  await vi.advanceTimersByTimeAsync(10);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
  expect(JSON.parse(child.stdin.read().toString())).toEqual({method:'cancel',params:{id}});
  expect(bridge.pending.size).toBe(1);
  child.stdout.write(JSON.stringify({id,error:{code:'STOP_UNCONFIRMED',message:'Still alive'}})+'\n');
  expect(await result).toMatchObject({code:'STOP_UNCONFIRMED'});
  bridge.close();
});

it('late progress cannot renew the remote stop-confirmation deadline', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  await vi.advanceTimersByTimeAsync(100+14999);
  child.stdout.write(JSON.stringify({id,event:{kind:'MODEL_RESPONSE',payload:{}}})+'\n');
  await vi.advanceTimersByTimeAsync(1);
  expect(await result).toMatchObject({code:'STOP_UNCONFIRMED'});
  expect(bridge.pending.size).toBe(0);
  bridge.close();
});

it('explicit cancellation still takes precedence over worker progress and requires remote confirmation', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture(), controller=new AbortController();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100,signal:controller.signal}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  controller.abort();
  child.stdout.write(JSON.stringify({id,event:{kind:'HARNESS_TOOL_EVENT',payload:{}}})+'\n');
  expect(bridge.pending.size).toBe(1);
  child.stdout.write(JSON.stringify({id,error:{code:'CANCELLED',message:'Stopped'}})+'\n');
  expect(await result).toMatchObject({code:'CANCELLED'});
  expect(vi.getTimerCount()).toBe(0);
  bridge.close();
});

it('ordinary model calls retain their absolute response timeout despite events', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('generate',{}, {timeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  await vi.advanceTimersByTimeAsync(90);
  child.stdout.write(JSON.stringify({id,event:{kind:'MODEL_RESPONSE',payload:{}}})+'\n');
  await vi.advanceTimersByTimeAsync(10);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('PROVIDER_TIMEOUT');
  child.stdout.write(JSON.stringify({id,error:{code:'CANCELLED',message:'Stopped'}})+'\n');
  expect(await result).toMatchObject({code:'PROVIDER_TIMEOUT'});
  bridge.close();
});
