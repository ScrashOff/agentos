import {EventEmitter} from 'node:events';
import {afterEach,expect,it,vi} from 'vitest';
import {PlanningWake} from '../server/planning-wake.js';
const client=()=>Object.assign(new EventEmitter(),{query:vi.fn(async()=>({rows:[]})),release:vi.fn()});
afterEach(()=>vi.useRealTimers());
it('coalesces committed notification bursts and ignores unrelated channels without calling models',async()=>{
  const db=client(),wake=vi.fn(),listener=new PlanningWake(wake,async()=>db as any);
  await listener.start();
  db.emit('notification',{channel:'other'});expect(wake).not.toHaveBeenCalled();
  for(let i=0;i<10;i++)db.emit('notification',{channel:'agentos_planning_wake'});
  await Promise.resolve();expect(wake).toHaveBeenCalledTimes(1);
  await listener.close();db.emit('notification',{channel:'agentos_planning_wake'});
  await Promise.resolve();expect(wake).toHaveBeenCalledTimes(1);
  expect(db.query).toHaveBeenCalledWith('UNLISTEN agentos_planning_wake');expect(db.release).toHaveBeenCalledTimes(1);
});
it('reconnects a lost listener while leaving reconciliation and worker state alone',async()=>{
  vi.useFakeTimers();const first=client(),second=client(),wake=vi.fn();
  const connect=vi.fn().mockResolvedValueOnce(first).mockResolvedValueOnce(second);
  const listener=new PlanningWake(wake,connect);
  await listener.start();first.emit('error',Error('connection lost'));
  expect(first.release).toHaveBeenCalledWith(true);
  await vi.advanceTimersByTimeAsync(5000);
  second.emit('notification',{channel:'agentos_planning_wake'});await vi.advanceTimersByTimeAsync(0);
  expect(wake).toHaveBeenCalledTimes(1);expect(connect).toHaveBeenCalledTimes(2);
  await listener.close();
});
it('does not leak a connection that arrives after shutdown',async()=>{
  let resolve!:(value:any)=>void;const db=client();
  const listener=new PlanningWake(vi.fn(),()=>new Promise(r=>{resolve=r;}));
  const opening=listener.start(),closing=listener.close();resolve(db);
  expect(await opening).toBe(false);await closing;
  expect(db.release).toHaveBeenCalledTimes(1);expect(db.query).not.toHaveBeenCalled();
});
