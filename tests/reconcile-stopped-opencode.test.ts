// Server-only PostgreSQL regression. Run only against a disposable test DB.
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import { pool, migrate, lock } from '../server/db.js';
import { reconcileStoppedDetachedOpenCode } from '../server/reconcile-stopped-opencode.js';
import { installNativeSupervision, nativeSupervisorLockKey, type NativeReservation, type NativeUnitObservation } from '../server/native-supervision.js';
import { allocateRuntimeMemory } from '../server/runtime-memory.js';
import { seedRegistry } from '../server/registry.js';

const candidate='/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111';
let f:any;
beforeAll(async()=>{
  const url=new URL(process.env.DATABASE_URL??'');
  if(!['agentos_test','agentos_reconcile_test'].includes(url.pathname.slice(1)))throw Error('DISPOSABLE_TEST_DATABASE_REQUIRED');
  await migrate();
  await installNativeSupervision(pool);
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),launchToken=randomUUID();
  const reservation:NativeReservation={version:1,mode:'detached',runId:run,runFence:fence,launchToken,
    unit:'agentos-native-supervisor-'+run,candidate,digest:'a'.repeat(64),launchAttempt:1,reservedAt:new Date().toISOString()};
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'reconcile test','disposable','reconcile-test',$2)",[goal,'reconcile-'+goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count)
    VALUES($1,$2,$3,'task','test','qa.backend','reconciling','[]',$4,1)`,[task,goal,'task-'+task,JSON.stringify({kind:'opencode',workspace:'reconcile-tests/'+run})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,checkpoint,instruction_snapshot)
    VALUES($1,$2,1,$3,'opencode',1,'reconciling','SUPERVISOR_LOST','{}',$4)`,[run,task,fence,
      JSON.stringify({schema:1,task:{id:task,goal_id:goal,execution:{kind:'opencode',workspace:'reconcile-tests/'+run}}})]);
  const client=await pool.connect();let allocation:any;
  try{await client.query('BEGIN');await lock(client);allocation=await allocateRuntimeMemory(client,
    {runId:run,runFence:fence,launchToken,candidate,digest:'a'.repeat(64)},
    {runtime:'opencode',checkpoint:{}},'reconcile-tests/'+run);await client.query('COMMIT');}
  catch(error){await client.query('ROLLBACK');throw error;}finally{client.release();}
  if(!allocation)throw Error('FIXTURE_MEMORY_ALLOCATION_FAILED');
  reservation.resources=allocation;
  await pool.query("INSERT INTO opencode_jobs(run_id,state,supervision) VALUES($1,'orphaned',$2)",[run,JSON.stringify(reservation)]);
  await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2),($3,$2)',['remote-slot',run,'workspace:reconcile-tests/'+run]);
  f={goal,task,run,fence,reservation,inspection:{runId:run,runtime:'opencode',workspace:'reconcile-tests/'+run,
    checkedAt:new Date().toISOString(),state:{runId:run,runtime:'opencode',workspace:'reconcile-tests/'+run,state:'failed',code:'OPENCODE_ERROR'},
    process:{unit:'agentos-run-'+run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}}};
});
afterAll(()=>pool.end());
const observation=(reservation:NativeReservation):NativeUnitObservation=>({unit:reservation.unit,checkedAt:new Date().toISOString(),
  activeState:'inactive',loadState:'not-found',mainPid:0,pendingJob:false,cgroupEmpty:true});

it('serializes concurrent closure, records terminal evidence and never queues another attempt',async()=>{
  const outcomes=await Promise.all(Array.from({length:6},()=>reconcileStoppedDetachedOpenCode(f.reservation,observation(f.reservation),f.inspection)));
  expect(outcomes.filter(Boolean)).toHaveLength(1);
  expect((await pool.query('SELECT state,error_code,attempt FROM runs WHERE id=$1',[f.run])).rows[0])
    .toMatchObject({state:'failed',error_code:'SUPERVISOR_LOST',attempt:1});
  expect((await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual({state:'failed',attempt_count:1});
  expect((await pool.query('SELECT state FROM opencode_jobs WHERE run_id=$1',[f.run])).rows[0].state).toBe('finished');
  expect((await pool.query('SELECT count(*)::int n FROM leases WHERE run_id=$1',[f.run])).rows[0].n).toBe(0);
  expect((await pool.query("SELECT checkpoint->'nativeStopReceipt' receipt FROM runs WHERE id=$1",[f.run])).rows[0].receipt)
    .toMatchObject({actorState:'failed',actorCode:'OPENCODE_ERROR',runFence:f.fence});
  expect((await pool.query("SELECT count(*)::int n FROM runs WHERE task_id=$1 AND state IN('queued','running','reconciling')",[f.task])).rows[0].n).toBe(0);
});

it('preserves cancellation only when AgentOS recorded CANCEL_REQUESTED',async()=>{
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.task,JSON.stringify({runId:f.run})]);
  f.inspection.state.state='cancelled';f.inspection.state.code='CANCELLED';
  expect(await reconcileStoppedDetachedOpenCode(f.reservation,observation(f.reservation),f.inspection)).toBe(true);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('cancelled');
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe('cancelled');
});

it('does not infer AgentOS cancellation from a remote CANCELLED code alone, and preserves that code as evidence',async()=>{
  f.inspection.state.state='cancelled';f.inspection.state.code='CANCELLED';
  expect(await reconcileStoppedDetachedOpenCode(f.reservation,observation(f.reservation),f.inspection)).toBe(true);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('failed');
  expect((await pool.query("SELECT checkpoint->'nativeStopReceipt'->>'actorCode' actor_code FROM runs WHERE id=$1",[f.run])).rows[0].actor_code).toBe('CANCELLED');
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe('failed');
});

it.each(['live_owner','wrong_identity','stale_actor_receipt','completed_not_terminal','wrong_unit','pending_job','cgroup_live','allocation_changed'])(
  'retains the reservation and lease with %s proof',async(kind)=>{
    const owner=await pool.connect();
    if(kind==='live_owner')await owner.query('SELECT pg_advisory_lock($1::int,$2::int)',[4280041,
      (await import('../server/native-supervision.js')).nativeSupervisorLockKey(f.run)]);
    const inspection=structuredClone(f.inspection);
    if(kind==='wrong_identity')inspection.state.runId=randomUUID();
    if(kind==='stale_actor_receipt')inspection.checkedAt='2000-01-01T00:00:00Z';
    if(kind==='completed_not_terminal')inspection.state.state='completed';
    const observed=observation(f.reservation);
    if(kind==='wrong_unit')observed.unit='agentos-native-supervisor-'+randomUUID();
    if(kind==='pending_job')observed.pendingJob=true;
    if(kind==='cgroup_live')observed.cgroupEmpty=false;
    if(kind==='allocation_changed')await pool.query("UPDATE opencode_jobs SET supervision=jsonb_set(supervision,'{resources,id}',to_jsonb($2::text)) WHERE run_id=$1",[f.run,randomUUID()]);
    const call=reconcileStoppedDetachedOpenCode(f.reservation,observed,inspection);
    if(['live_owner','wrong_unit','pending_job','cgroup_live','allocation_changed'].includes(kind))expect(await call).toBe(false);
    else await expect(call).rejects.toThrow();
    await owner.query('SELECT pg_advisory_unlock_all()');owner.release();
    expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('reconciling');
    expect((await pool.query('SELECT count(*)::int n FROM leases WHERE run_id=$1',[f.run])).rows[0].n).toBe(2);
  });
