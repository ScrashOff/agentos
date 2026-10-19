import {it,expect,vi,afterAll} from 'vitest';
import pg from 'pg';
import {randomUUID,createHash} from 'node:crypto';
import {pool} from '../server/db.js';
import {config} from '../server/config.js';
import {RunHeartbeat} from '../server/run-heartbeat.js';
afterAll(async()=>{await pool.end();});

it('keeps native executor reads available in a four-connection PostgreSQL pool while one heartbeat is blocked',async()=>{
 const native=new pg.Pool({connectionString:config.databaseUrl,max:4,connectionTimeoutMillis:1000});
 const blocker=await pool.connect(),owner=await native.connect();
 const key=createHash('sha256').update(randomUUID()).digest().readUInt32BE(0)&2147483647;
 let started!:()=>void;const entered=new Promise<void>(resolve=>{started=resolve;});
 const check=vi.fn(async()=>{started();await native.query('SELECT pg_advisory_xact_lock(4280125,$1)',[key]);return {state:'running'};});
 const stop=vi.fn(),pulse=new RunHeartbeat(check,stop);
 let unlocked=false;
 try{
   await blocker.query('SELECT pg_advisory_lock(4280125,$1)',[key]);
   const pending=Array.from({length:40},()=>pulse.tick());await entered;
   const read=await native.query('SELECT 42::int ready');
   expect(read.rows[0].ready).toBe(42);expect(native.waitingCount).toBe(0);
   expect(check).toHaveBeenCalledTimes(1);expect(stop).not.toHaveBeenCalled();
   await blocker.query('SELECT pg_advisory_unlock(4280125,$1)',[key]);unlocked=true;
   await Promise.all(pending);expect(stop).not.toHaveBeenCalled();
 }finally{
   if(!unlocked)await blocker.query('SELECT pg_advisory_unlock(4280125,$1)',[key]);
   await pulse.close();owner.release();blocker.release();await native.end();
 }
});

it('a slow database observation occupies one slot regardless of the timer ticks',async()=>{
  let resolve!:(value:{state:string}|null)=>void;
  const observed=new Promise<{state:string}|null>(r=>{resolve=r;});
  const check=vi.fn(()=>observed),stop=vi.fn(),pulse=new RunHeartbeat(check,stop);
  const pending=Array.from({length:30},()=>pulse.tick());
  await Promise.resolve();
  expect(check).toHaveBeenCalledTimes(1);expect(stop).not.toHaveBeenCalled();
  resolve({state:'running'});await Promise.all(pending);
  expect(stop).not.toHaveBeenCalled();await pulse.close();
});
it('a connection timeout does not cancel the actor and the next observation can confirm cancellation',async()=>{
  const check=vi.fn().mockRejectedValueOnce(Error('Connection timeout')).mockResolvedValueOnce({state:'cancelling'});
  const stop=vi.fn(),pulse=new RunHeartbeat(check,stop);
  await pulse.tick();expect(stop).not.toHaveBeenCalled();
  await pulse.tick();expect(stop).toHaveBeenCalledTimes(1);await pulse.close();
});
it('a successfully observed lost fence stops the actor',async()=>{
  const stop=vi.fn(),pulse=new RunHeartbeat(async()=>null,stop);
  await pulse.tick();expect(stop).toHaveBeenCalledTimes(1);await pulse.close();
});
it('closing waits for the outstanding observation without treating a late terminal response as cancellation',async()=>{
  let resolve!:(value:{state:string}|null)=>void;
  const check=vi.fn(()=>new Promise<{state:string}|null>(r=>{resolve=r;})),stop=vi.fn();
  const pulse=new RunHeartbeat(check,stop),pending=pulse.tick();await Promise.resolve();
  let closed=false;const closing=pulse.close().then(()=>{closed=true;});
  await pulse.tick();expect(closed).toBe(false);expect(check).toHaveBeenCalledTimes(1);
  resolve(null);await Promise.all([pending,closing]);expect(closed).toBe(true);expect(stop).not.toHaveBeenCalled();
});
it('repeated failed observations never cause overlapping queries or imply a stop receipt',async()=>{
  const check=vi.fn(async()=>{throw Error('Database unavailable');}),stop=vi.fn(),pulse=new RunHeartbeat(check,stop);
  await Promise.all(Array.from({length:12},()=>pulse.tick()));
  expect(check).toHaveBeenCalledTimes(1);expect(stop).not.toHaveBeenCalled();
  await pulse.tick();expect(check).toHaveBeenCalledTimes(2);expect(stop).not.toHaveBeenCalled();await pulse.close();
});
