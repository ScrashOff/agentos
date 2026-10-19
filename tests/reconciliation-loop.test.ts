import { afterEach, expect, it, vi } from 'vitest';
import { ReconciliationLoop } from '../server/reconciliation-loop.js';

afterEach(() => vi.useRealTimers());

it('coalesces events during a slow pass and wakes idle work without overlap',async()=>{
  vi.useFakeTimers();let release!:()=>void;
  const reconcile=vi.fn().mockImplementationOnce(()=>new Promise<void>(r=>{release=r;})).mockResolvedValue(undefined);
  const loop=new ReconciliationLoop(reconcile,async()=>{},5000);loop.start();
  for(let i=0;i<100;i++)loop.requestWake();
  expect(reconcile).toHaveBeenCalledTimes(1);
  release();await vi.advanceTimersByTimeAsync(0);
  expect(reconcile).toHaveBeenCalledTimes(2);
  loop.requestWake();loop.requestWake();await vi.advanceTimersByTimeAsync(0);
  expect(reconcile).toHaveBeenCalledTimes(3);
  await loop.close();loop.requestWake();await vi.advanceTimersByTimeAsync(60000);
  expect(reconcile).toHaveBeenCalledTimes(3);
});

it('keeps preparation/refill advancing while a different reconciliation lane is blocked', async () => {
  vi.useFakeTimers();
  let release!: () => void;
  const blocked = vi.fn(() => new Promise<void>(resolve => { release = resolve; }));
  const refill = vi.fn(async () => {});
  const slow = new ReconciliationLoop(blocked, async () => {}, 5000);
  const fast = new ReconciliationLoop(refill, async () => {}, 5000);
  slow.start(); fast.start();
  try {
    await vi.advanceTimersByTimeAsync(20000);
    expect(blocked).toHaveBeenCalledTimes(1);
    expect(refill).toHaveBeenCalledTimes(5);
  } finally {
    slow.requestStop(); fast.requestStop(); release();
    await Promise.all([slow.close(), fast.close()]);
  }
});

it('never overlaps the same lane and drains its actual operation before closing', async () => {
  vi.useFakeTimers();
  let release!: () => void;
  const reconcile = vi.fn(() => new Promise<void>(resolve => { release = resolve; }));
  const loop = new ReconciliationLoop(reconcile, async () => {}, 5000);
  loop.start(); loop.start();
  let closed = false;
  const completion = loop.close().then(() => { closed = true; });
  await vi.advanceTimersByTimeAsync(20000);
  expect(closed).toBe(false);
  expect(reconcile).toHaveBeenCalledTimes(1);
  release(); await completion;
  expect(closed).toBe(true);
  await vi.advanceTimersByTimeAsync(20000);
  expect(reconcile).toHaveBeenCalledTimes(1);
});

it('recovers the next reconciliation after a failed operation and failed diagnostic write', async () => {
  vi.useFakeTimers();
  const reconcile = vi.fn().mockRejectedValueOnce(Error('INPUT_UNAVAILABLE')).mockResolvedValue(undefined);
  const diagnostic = vi.fn().mockRejectedValueOnce(Error('DIAGNOSTIC_UNAVAILABLE'));
  const loop = new ReconciliationLoop(reconcile, diagnostic, 5000);
  loop.start();
  try {
    await vi.advanceTimersByTimeAsync(5000);
    expect(reconcile).toHaveBeenCalledTimes(2);
    expect(diagnostic).toHaveBeenCalledTimes(1);
  } finally { await loop.close(); }
});

it('cancels idle timers on shutdown without admitting a later iteration', async () => {
  vi.useFakeTimers();
  const reconcile = vi.fn(async () => {});
  const loop = new ReconciliationLoop(reconcile, async () => {}, 5000);
  loop.start(); await vi.advanceTimersByTimeAsync(0);
  await loop.close(); await vi.advanceTimersByTimeAsync(60000);
  expect(reconcile).toHaveBeenCalledTimes(1);
  expect(() => loop.start()).toThrow('RECONCILIATION_STOPPED');
});
