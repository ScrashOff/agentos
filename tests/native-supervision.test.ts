import { afterAll, afterEach, beforeAll, beforeEach, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import pg from 'pg';
import { pool, migrate, tx, lock } from '../server/db.js';
import { config } from '../server/config.js';
import {executeDetachedHarness,reconcileStoppedDetachedHarness} from '../server/harness-supervision.js';
import * as memory from '../server/memory.js';
import { seedRegistry } from '../server/registry.js';
import {
  startRun,
  heartbeat,
  markUncertain,
  cancelTask,
  confirmStopped,
  writeArtifact,
} from '../server/service.js';
import {
  installNativeSupervision,
  reserveNativeSupervisor,
  startDetachedNativeRun,
  registerNativeSupervisor,
  reconcileNativeSupervisor,
  superviseNativeRun,
  nativeSupervisorLockNamespace,
  nativeSupervisorLockKey,
  nativeSupervisorUnit,
  nativeSupervisorCgroup,
  independentNativeSupervisorSql,
  type NativeReservation,
  type NativeUnitObservation,
} from '../server/native-supervision.js';
import { claimWorkerDrain, admitWorkerServiceRestart } from '../server/worker-rollout.js';
import {
  nativeSupervisorArguments,
  nativeSupervisorSelfProof,
} from '../scripts/native-supervisor-unit.js';

const release = {
  candidate: '/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',
  digest: 'a'.repeat(64),
};
const held: PoolClient[] = [];
beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await installNativeSupervision(pool);
});
beforeEach(async () => {
  await pool.query("DELETE FROM settings WHERE id='maintenance' OR id LIKE 'worker-readiness:%'");
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
});
afterEach(async () => {
  vi.restoreAllMocks();
  for (const client of held.splice(0)) {
    await client.query('SELECT pg_advisory_unlock_all()');
    client.release();
  }
  await pool.query("DELETE FROM settings WHERE id='maintenance'");
});
afterAll(() => pool.end());

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}
async function fixture(runtime='opencode') {
  const goal = randomUUID(),
    task = randomUUID(),
    run = randomUUID(),
    fence = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Native fixture','No real agents','native-supervisor-test',$2)",
    [goal, goal],
  );
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count)
    VALUES($1,$2,$4,'Fixture','Immutable contract','qa.backend','queued','[]',$3,1)`,
    [
      task,
      goal,
      JSON.stringify({
        kind: runtime,
        workspace: 'native-tests/' + run,
        writePaths: ['evidence/'],
      }),
      task,
    ],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,checkpoint) VALUES($1,$2,1,$3,$4,1,'queued','{\"verified\":true,\"cursor\":7}')",
    [run, task, fence,runtime],
  );
  await pool.query('INSERT INTO leases VALUES($1,$2),($3,$2)', [
    'remote-slot',
    run,
    'workspace:native-tests/' + run,
  ]);
  await pool.query('INSERT INTO opencode_jobs(run_id) VALUES($1)', [run]);
  return { goal, task, run, fence };
}

it.each([
  ['harness','running'],['harness','cancelling'],['opencode','running'],['opencode','cancelling'],
])('a stale heartbeat does not replace the confirmed %s owner in %s, but real owner loss remains uncertain',async(runtime,state)=>{
  const f=await fixture(runtime);
  if(runtime==='harness')await pool.query('INSERT INTO settings VALUES($1,$2)',
    ['workforce:'+f.goal,'{"enabled":true,"detachedHarnessEnabled":true}']);
  const reservation=(await tx(db=>reserveNativeSupervisor(db,f.run,release)))!;
  const heldOwner=await owner(reservation);
  expect(await tx(db=>registerNativeSupervisor(db,reservation,proof(reservation),heldOwner.pid))).toBe(true);
  await tx(db=>startDetachedNativeRun(db,reservation,runtime+'-native-12345'));
  if(state==='cancelling')await cancelTask(f.task);
  await pool.query("UPDATE runs SET heartbeat_at=clock_timestamp()-interval '10 minutes' WHERE id=$1",[f.run]);
  const before=(await pool.query('SELECT checkpoint,fence,attempt FROM runs WHERE id=$1',[f.run])).rows[0];
  await markUncertain();
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe(state);
  expect((await pool.query('SELECT 1 FROM events WHERE kind=$1 AND payload->>\'runId\'=$2',['RUN_RECONCILING',f.run])).rowCount).toBe(0);
  await heldOwner.c.query('SELECT pg_advisory_unlock_all()');
  await markUncertain();
  expect((await pool.query('SELECT state,error_code FROM runs WHERE id=$1',[f.run])).rows[0])
    .toEqual({state:'reconciling',error_code:'HEARTBEAT_LOST'});
  expect((await pool.query('SELECT checkpoint,fence,attempt FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1',[f.run])).rowCount).toBe(2);
  expect(await tx(db=>reserveNativeSupervisor(db,f.run,release))).toBeNull();
});

it('pins Harness work independently during controller rollout without changing its runtime, criteria or inputs',async()=>{
  const f=await fixture('harness');
  expect(await tx(db=>reserveNativeSupervisor(db,f.run,release))).toBeNull();
  await pool.query('INSERT INTO settings VALUES($1,$2)',[
    'workforce:'+f.goal,JSON.stringify({enabled:true,detachedHarnessEnabled:true})]);
  const reservations=await Promise.all(Array.from({length:6},()=>tx(db=>reserveNativeSupervisor(db,f.run,release))));
  expect(reservations.filter(Boolean)).toHaveLength(1);
  const reservation=reservations.find(Boolean)!;
  expect(reservation.runtime).toBe('harness');
  expect(nativeSupervisorArguments(reservation)).toContain(release.candidate+'/scripts/harness-supervisor.ts');
  expect(nativeSupervisorArguments(reservation)).not.toContain(release.candidate+'/scripts/opencode-batch.ts');
  const heldOwner=await owner(reservation);
  expect(await tx(db=>registerNativeSupervisor(db,reservation,proof(reservation),heldOwner.pid))).toBe(true);
  const run=await tx(db=>startDetachedNativeRun(db,reservation,'harness-native-12345'));
  expect(run?.runtime).toBe('harness');
  expect(await tx(db=>startDetachedNativeRun(db,reservation,'harness-native-12345'))).toBeNull();
  await tx(db=>claimWorkerDrain(db,release,'per_service'));
  expect((await tx(async db=>admitWorkerServiceRestart(db,
    ''+(await db.query("SELECT value->'workerRollout'->>'token' token FROM settings WHERE id='maintenance'")).rows[0].token,
    'agentos-opencode'))).ready).toBe(true);
  expect((await pool.query('SELECT criteria,execution FROM tasks WHERE id=$1',[f.task])).rows[0]).toMatchObject({
    criteria:[],execution:{kind:'harness',workspace:'native-tests/'+f.run,writePaths:['evidence/']}});
});

it('rejects a forged runtime owner and respects a paused Harness goal between reservation and start',async()=>{
  const f=await fixture('harness');
  await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+f.goal,'{"enabled":true,"detachedHarnessEnabled":true}']);
  const reservation=(await tx(db=>reserveNativeSupervisor(db,f.run,release)))!;
  const heldOwner=await owner(reservation);
  expect(await tx(db=>registerNativeSupervisor(db,{...reservation,runtime:'opencode'},proof(reservation),heldOwner.pid))).toBe(false);
  expect(await tx(db=>registerNativeSupervisor(db,reservation,proof(reservation),heldOwner.pid))).toBe(true);
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  expect(await tx(db=>startDetachedNativeRun(db,reservation,'harness-native-12345'))).toBeNull();
  expect((await pool.query('SELECT state,context_id FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual({state:'queued',context_id:null});
});

it('preserves uncertain Harness work and leases instead of retrying after a detached transport loses its stop receipt',async()=>{
  const f=await fixture('harness');
  await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+f.goal,'{"enabled":true,"detachedHarnessEnabled":true}']);
  const reservation=(await tx(db=>reserveNativeSupervisor(db,f.run,release)))!;
  const executor=vi.fn(async()=>{throw Object.assign(Error('Lost transport'),{code:'STOP_UNCONFIRMED'});});
  await superviseNativeRun(f.run,reservation.launchToken,async()=>proof(reservation,process.pid),
    (id,r)=>executeDetachedHarness(id,r,executor));
  expect(executor).toHaveBeenCalledTimes(1);
  expect((await pool.query('SELECT state,error_code FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual({state:'reconciling',error_code:'STOP_UNCONFIRMED'});
  expect((await job(f.run)).state).toBe('orphaned');
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1',[f.run])).rowCount).toBe(2);
  expect(await tx(db=>reserveNativeSupervisor(db,f.run,release))).toBeNull();
  expect((await pool.query("SELECT 1 FROM events WHERE kind='RUN_INPUT_FAILURE' AND entity_id=$1",[f.task])).rowCount).toBe(0);
});

it.each([false,true])('the real detached Harness catch handles preflight before optional diagnosis (diagnostic DB failure=%s)',async failObservation=>{
  const f=await fixture('harness');
  await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+f.goal,'{"enabled":true,"detachedHarnessEnabled":true}']);
  const reservation=(await tx(db=>reserveNativeSupervisor(db,f.run,release)))!;
  const error=Object.assign(Error('PRIVATE_CONTEXT_PAYLOAD'),{code:'INPUT_MANIFEST_CHANGED',
    inputFailure:{phase:'effective_source_manifest',subcode:'EFFECTIVE_INPUT_MISSING',command:'PRIVATE_COMMAND'}});
  vi.spyOn(memory,'assembleContext').mockRejectedValueOnce(error);
  const executor=vi.fn(async()=>{throw Error('must not launch');});
  let observationAttempts=0;
  const originalQuery=pg.Client.prototype.query;
  const spy=vi.spyOn(pg.Client.prototype,'query').mockImplementation((function(this:pg.Client,...args:any[]){
    if(String(args[0]).includes("SELECT 'RUN_INPUT_FAILURE'")){
      observationAttempts++;
      if(failObservation)return Promise.reject(Error('PRIVATE_TELEMETRY_DATABASE'));
    }
    return (originalQuery as any).apply(this,args);
  }) as typeof pg.Client.prototype.query);
  try{
    await superviseNativeRun(f.run,reservation.launchToken,async()=>proof(reservation,process.pid),
      (id,r)=>executeDetachedHarness(id,r,executor));
  }finally{spy.mockRestore();}
  expect(executor).not.toHaveBeenCalled();expect(observationAttempts).toBe(1);
  expect((await pool.query('SELECT state,error_code,context_id FROM runs WHERE id=$1',[f.run])).rows[0])
    .toEqual({state:'failed',error_code:'INPUT_MANIFEST_CHANGED',context_id:null});
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1',[f.run])).rowCount).toBe(0);
  expect((await pool.query('SELECT attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0].attempt_count).toBe(1);
  expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id=$1',[f.run])).rowCount).toBe(0);
  const events=(await pool.query('SELECT id::text,kind,payload FROM events WHERE entity_id=$1 ORDER BY id',[f.task])).rows;
  expect(events.some(e=>e.kind==='HARNESS_NOT_STARTED'&&e.payload.confirmed===true)).toBe(true);
  expect(events.some(e=>e.kind==='HARNESS_LAUNCH_ATTEMPTED'||e.kind==='TASK_RETRY_SCHEDULED')).toBe(false);
  const diagnosis=events.filter(e=>e.kind==='RUN_INPUT_FAILURE');
  expect(diagnosis).toHaveLength(failObservation?0:1);
  if(!failObservation){
    expect(diagnosis[0].payload).toMatchObject({runId:f.run,runFence:f.fence,attempt:1,contextVersion:null,
      code:'INPUT_MANIFEST_CHANGED',phase:'effective_source_manifest',subcode:'EFFECTIVE_INPUT_MISSING'});
    expect(BigInt(diagnosis[0].id)>BigInt(events.find(e=>e.kind==='RUN_FAILED')!.id)).toBe(true);
  }
  expect(JSON.stringify(events)).not.toContain('PRIVATE_');
});
async function reserved() {
  const f = await fixture();
  const reservation = (await tx((db) => reserveNativeSupervisor(db, f.run, release)))!;
  return { ...f, reservation };
}
function proof(r: NativeReservation, pid = 12345) {
  return { unit: r.unit, pid, cwd: r.candidate, cgroup: nativeSupervisorCgroup(r.runId) };
}
function stopped(
  r: NativeReservation,
  extra: Partial<NativeUnitObservation> = {},
): NativeUnitObservation {
  return {
    unit: r.unit,
    checkedAt: new Date().toISOString(),
    activeState: 'inactive',
    loadState: 'not-found',
    mainPid: 0,
    pendingJob: false,
    cgroupEmpty: true,
    ...extra,
  };
}
async function owner(r: NativeReservation) {
  const c = await pool.connect();
  held.push(c);
  await c.query('SELECT pg_advisory_lock($1::int,$2::int)', [
    nativeSupervisorLockNamespace,
    nativeSupervisorLockKey(r.runId),
  ]);
  return { c, pid: (await c.query('SELECT pg_backend_pid() pid')).rows[0].pid };
}
async function job(id: string) {
  return (await pool.query('SELECT * FROM opencode_jobs WHERE run_id=$1', [id])).rows[0];
}

async function stoppedHarnessFixture(){
  const f=await fixture('harness');
  await pool.query('INSERT INTO settings VALUES($1,$2)',
    ['workforce:'+f.goal,'{"enabled":true,"detachedHarnessEnabled":true}']);
  const reservation=(await tx(db=>reserveNativeSupervisor(db,f.run,release)))!;
  const heldOwner=await owner(reservation);
  await tx(db=>registerNativeSupervisor(db,reservation,proof(reservation),heldOwner.pid));
  await tx(db=>startDetachedNativeRun(db,reservation,'harness-native-12345'));
  const inspection={runId:f.run,checkedAt:new Date().toISOString(),workspace:'native-tests/'+f.run,
    process:{unit:'agentos-run-'+f.run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};
  return {...f,reservation,heldOwner,inspection};
}
it('records a stopped detached owner and sandbox without a retry, accepting an artifact or losing prior evidence',async()=>{
  const f=await stoppedHarnessFixture();
  await writeArtifact(f.run,f.fence,'partial.json','{"partial":true}');
  const before=(await pool.query('SELECT instruction_snapshot,checkpoint FROM runs WHERE id=$1',[f.run])).rows[0];
  await f.heldOwner.c.query('SELECT pg_advisory_unlock_all()');
  expect(await reconcileStoppedDetachedHarness(f.reservation,stopped(f.reservation),async()=>f.inspection)).toBe(true);
  const after=(await pool.query('SELECT state,instruction_snapshot,checkpoint FROM runs WHERE id=$1',[f.run])).rows[0];
  expect(after.state).toBe('failed');expect(after.instruction_snapshot).toEqual(before.instruction_snapshot);
  expect(after.checkpoint).toMatchObject({...before.checkpoint,nativeStopReceipt:{workspace:f.inspection.workspace,runFence:f.fence}});
  expect((await job(f.run)).state).toBe('finished');
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1',[f.run])).rowCount).toBe(0);
  expect((await pool.query('SELECT attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0].attempt_count).toBe(1);
  expect((await pool.query('SELECT name FROM artifacts WHERE run_id=$1',[f.run])).rows).toEqual([{name:'partial.json'}]);
  expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id=$1',[f.run])).rowCount).toBe(0);
  const drain=await tx(db=>claimWorkerDrain(db,release,'per_service'));
  if(drain.status!=='draining')throw Error('Drain not owned');
  expect(await tx(db=>admitWorkerServiceRestart(db,drain.token,'agentos-opencode'))).toMatchObject({ready:true});
});
it('confirmed explicit cancellation stays cancelled after detached stop reconciliation',async()=>{
  const f=await stoppedHarnessFixture();await cancelTask(f.task);await f.heldOwner.c.query('SELECT pg_advisory_unlock_all()');
  expect(await reconcileStoppedDetachedHarness(f.reservation,stopped(f.reservation),async()=>f.inspection)).toBe(true);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('cancelled');
  expect((await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0])
    .toEqual({state:'cancelled',attempt_count:1});
});
it.each(['owner_alive','sandbox_alive','wrong_workspace','wrong_unit','stale_proof','forged_token'])(
  'does not release a detached reservation or produce a retry with %s evidence',async(kind)=>{
    const f=await stoppedHarnessFixture();
    if(kind!=='owner_alive')await f.heldOwner.c.query('SELECT pg_advisory_unlock_all()');
    const inspection={...f.inspection,process:{...f.inspection.process}};
    if(kind==='sandbox_alive')inspection.process.mainPid=18;
    if(kind==='wrong_workspace')inspection.workspace='native-tests/foreign';
    if(kind==='wrong_unit')inspection.process.unit='agentos-run-'+randomUUID();
    if(kind==='stale_proof')inspection.checkedAt='2000-01-01T00:00:00Z';
    const reservation=kind==='forged_token'?{...f.reservation,launchToken:randomUUID()}:f.reservation;
    const before=(await pool.query('SELECT state,checkpoint,instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0];
    const operation=reconcileStoppedDetachedHarness(reservation,stopped(reservation),async()=>inspection);
    if(['owner_alive','forged_token'].includes(kind))expect(await operation).toBe(false);
    else await expect(operation).rejects.toThrow('STOP_UNCONFIRMED');
    expect((await pool.query('SELECT state,checkpoint,instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
    expect((await job(f.run)).state).toBe('running');
    expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1',[f.run])).rowCount).toBe(2);
  });

it('atomically reserves a queued native job once under concurrent dispatchers without consuming its context or leases', async () => {
  const f = await fixture(),
    before = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0];
  const attempts = await Promise.all(
    Array.from({ length: 8 }, () => tx((db) => reserveNativeSupervisor(db, f.run, release))),
  );
  expect(attempts.filter(Boolean)).toHaveLength(1);
  expect((await job(f.run)).state).toBe('launching');
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
});

it('runs one supervisor per run even while the global dispatcher owner changes and rollout admits a new dispatcher', async () => {
  const f = await reserved(),
    started = barrier(),
    finish = barrier(),
    actor = vi.fn(async () => {
      expect(await startRun(f.run, 'opencode-native-12345')).not.toBeNull();
      await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)', [
        randomUUID(),
        f.run,
        JSON.stringify({ owned: true }),
        'Frozen context',
      ]);
      started.resolve();
      await finish.promise;
    });
  const dispatcher = await pool.connect();
  await dispatcher.query('SELECT pg_advisory_lock(4280040)');
  const running = superviseNativeRun(
    f.run,
    f.reservation.launchToken,
    async (r) => proof(r),
    actor,
  );
  await started.promise;
  try {
    const before = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0];
    const contexts = (await pool.query('SELECT * FROM contexts WHERE run_id=$1', [f.run])).rows;
    expect(
      await superviseNativeRun(f.run, f.reservation.launchToken, async (r) => proof(r), actor),
    ).toBe('duplicate_owner');
    await dispatcher.query('SELECT pg_advisory_unlock(4280040)');
    dispatcher.release();
    const successor = await pool.connect();
    held.push(successor);
    expect(
      (await successor.query('SELECT pg_try_advisory_lock(4280040) owned')).rows[0].owned,
    ).toBe(true);
    const drain = await tx((db) => claimWorkerDrain(db, release, 'per_service'));
    expect(drain.status).toBe('draining');
    if (drain.status !== 'draining') throw Error('Own drain failed');
    expect(
      await tx((db) => admitWorkerServiceRestart(db, drain.token, 'agentos-opencode')),
    ).toMatchObject({ ready: true, counts: { runs: 0, jobs: 0 } });
    expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual(before);
    expect((await pool.query('SELECT * FROM contexts WHERE run_id=$1', [f.run])).rows).toEqual(
      contexts,
    );
    expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
    expect((await heartbeat(f.run, f.fence))?.state).toBe('running');
    expect(actor).toHaveBeenCalledTimes(1);
  } finally {
    finish.resolve();
    await running;
  }
});

it.each(['pid', 'cgroup', 'cwd', 'unit'] as const)(
  'rejects supervisor identity with wrong %s before any actor starts',
  async (field) => {
    const f = await reserved(),
      owned = await owner(f.reservation),
      invalid: any = proof(f.reservation);
    invalid[field] = field === 'pid' ? 0 : 'not-the-frozen-identity';
    await expect(
      tx((db) => registerNativeSupervisor(db, f.reservation, invalid, owned.pid)),
    ).rejects.toThrow('NATIVE_SUPERVISOR_IDENTITY_INVALID');
    expect(
      (await pool.query('SELECT state,worker_id FROM runs WHERE id=$1', [f.run])).rows[0],
    ).toEqual({ state: 'queued', worker_id: null });
  },
);

it('rejects registration without the actual per-run PostgreSQL owner', async () => {
  const f = await reserved();
  await expect(
    tx((db) => registerNativeSupervisor(db, f.reservation, proof(f.reservation), 123)),
  ).rejects.toThrow('NATIVE_SUPERVISOR_OWNER_MISSING');
});

it('fences a delayed launch after a confirmed unit-start failure and retains the original run fence and leases', async () => {
  const f = await reserved(),
    old = f.reservation;
  expect(await tx((db) => reconcileNativeSupervisor(db, old, stopped(old)))).toBe('queued');
  const queued = await job(f.run);
  expect(queued.supervision.launchToken).not.toBe(old.launchToken);
  const actor = vi.fn(async () => {});
  expect(await superviseNativeRun(f.run, old.launchToken, async (r) => proof(r), actor)).toBe(
    'stale_launch',
  );
  expect(actor).not.toHaveBeenCalled();
  expect((await pool.query('SELECT fence,state FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual({
    fence: f.fence,
    state: 'queued',
  });
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
  await pool.query(
    "UPDATE opencode_jobs SET supervision=supervision-'nextLaunchAt' WHERE run_id=$1",
    [f.run],
  );
  const next = await tx((db) => reserveNativeSupervisor(db, f.run, release));
  expect(next?.launchToken).not.toBe(old.launchToken);
  expect(next?.runFence).toBe(f.fence);
});

it.each([
  { pendingJob: true },
  { mainPid: 44 },
  { cgroupEmpty: false },
  { activeState: 'active' },
  { unit: 'agentos-opencode' },
  { checkedAt: '2000-01-01T00:00:00Z' },
  { loadState: 'unknown' },
])('never requeues an ambiguous supervisor stop: %j', async (invalid) => {
  const f = await reserved();
  expect(
    await tx((db) => reconcileNativeSupervisor(db, f.reservation, stopped(f.reservation, invalid))),
  ).toBe('observing');
  expect((await job(f.run)).state).toBe('launching');
  expect((await job(f.run)).supervision.launchToken).toBe(f.reservation.launchToken);
});

it('does not requeue a startup whose PostgreSQL owner is still alive despite a false absent-unit observation', async () => {
  const f = await reserved();
  await owner(f.reservation);
  expect(
    await tx((db) => reconcileNativeSupervisor(db, f.reservation, stopped(f.reservation))),
  ).toBe('observing');
});

it('recovers a dispatcher crash after launch by observing its independent owner, without a duplicate actor', async () => {
  const f = await reserved(),
    owned = await owner(f.reservation);
  expect(
    await tx((db) => registerNativeSupervisor(db, f.reservation, proof(f.reservation), owned.pid)),
  ).toBe(true);
  await startRun(f.run, 'opencode-native-12345');
  expect(
    await tx((db) =>
      reconcileNativeSupervisor(
        db,
        f.reservation,
        stopped(f.reservation, {
          activeState: 'active',
          mainPid: 12345,
          cgroupEmpty: false,
          loadState: 'loaded',
        }),
      ),
    ),
  ).toBe('observing');
  expect(await tx((db) => reserveNativeSupervisor(db, f.run, release))).toBeNull();
  expect(
    (
      await pool.query(
        "SELECT count(*)::int n FROM events WHERE kind='RUN_STARTED' AND payload->>'runId'=$1",
        [f.run],
      )
    ).rows[0].n,
  ).toBe(1);
});

it('keeps an actor crash uncertain instead of relaunching or releasing its frozen context, checkpoints and leases', async () => {
  const f = await reserved();
  await expect(
    superviseNativeRun(
      f.run,
      f.reservation.launchToken,
      async (r) => proof(r),
      async () => {
        await startRun(f.run, 'opencode-native-12345');
        await pool.query(
          "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Preserved')",
          [randomUUID(), f.run],
        );
        throw Error('Simulated supervisor crash after remote start');
      },
    ),
  ).rejects.toThrow('Simulated supervisor crash');
  expect((await job(f.run)).state).toBe('orphaned');
  const before = (
    await pool.query('SELECT fence,checkpoint,instruction_snapshot FROM runs WHERE id=$1', [f.run])
  ).rows[0];
  expect(
    await tx((db) => reconcileNativeSupervisor(db, f.reservation, stopped(f.reservation))),
  ).toBe('reconciling');
  expect(await tx((db) => reserveNativeSupervisor(db, f.run, release))).toBeNull();
  expect(
    (
      await pool.query('SELECT fence,checkpoint,instruction_snapshot FROM runs WHERE id=$1', [
        f.run,
      ])
    ).rows[0],
  ).toEqual(before);
  expect((await pool.query('SELECT 1 FROM contexts WHERE run_id=$1', [f.run])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
  expect(
    (await pool.query('SELECT state,error_code FROM runs WHERE id=$1', [f.run])).rows[0],
  ).toEqual({ state: 'reconciling', error_code: 'SUPERVISOR_LOST' });
});

it('preserves cancellation until the existing run fence confirms a real stop', async () => {
  const f = await reserved(),
    started = barrier(),
    finish = barrier();
  const running = superviseNativeRun(
    f.run,
    f.reservation.launchToken,
    async (r) => proof(r),
    async () => {
      await startRun(f.run, 'opencode-native-12345');
      started.resolve();
      await finish.promise;
    },
  );
  await started.promise;
  try {
    await cancelTask(f.task);
    expect((await heartbeat(f.run, f.fence))?.state).toBe('cancelling');
    expect(await heartbeat(f.run, randomUUID())).toBeNull();
    expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
    await confirmStopped(f.run, randomUUID());
    expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
      'cancelling',
    );
    await expect(writeArtifact(f.run, f.fence, 'wrong.json', '{}')).rejects.toThrow();
    await confirmStopped(f.run, f.fence);
    expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
      'cancelled',
    );
  } finally {
    finish.resolve();
    await running;
  }
  expect((await job(f.run)).state).toBe('finished');
});

it.each(['legacy', 'metadata_only', 'missing_owner', 'wrong_fence', 'wrong_cgroup'])(
  'retains rollout blocking for %s native work',
  async (kind) => {
    const f = await reserved();
    if (kind !== 'legacy') {
      const owned = await owner(f.reservation);
      if (kind !== 'metadata_only')
        await tx((db) =>
          registerNativeSupervisor(db, f.reservation, proof(f.reservation), owned.pid),
        );
      if (kind === 'missing_owner') await owned.c.query('SELECT pg_advisory_unlock_all()');
      if (kind === 'wrong_fence')
        await pool.query(
          "UPDATE opencode_jobs SET supervision=jsonb_set(supervision,'{owner,runFence}',to_jsonb($2::text)) WHERE run_id=$1",
          [f.run, randomUUID()],
        );
      if (kind === 'wrong_cgroup')
        await pool.query(
          "UPDATE opencode_jobs SET supervision=jsonb_set(supervision,'{owner,cgroup}','\"/system.slice/agentos-opencode.service\"') WHERE run_id=$1",
          [f.run],
        );
    } else
      await pool.query(
        "UPDATE opencode_jobs SET state='running',supervision=NULL WHERE run_id=$1",
        [f.run],
      );
    await startRun(f.run, 'opencode-native-12345');
    const drain = await tx((db) => claimWorkerDrain(db, release, 'per_service'));
    if (drain.status !== 'draining') throw Error('Drain not owned');
    expect(
      await tx((db) => admitWorkerServiceRestart(db, drain.token, 'agentos-opencode')),
    ).toMatchObject({ ready: false });
    expect(
      (
        await pool.query(
          `SELECT ${independentNativeSupervisorSql()} valid FROM runs r JOIN opencode_jobs j ON j.run_id=r.id WHERE r.id=$1`,
          [f.run],
        )
      ).rows[0].valid,
    ).toBe(false);
    await pool.query("UPDATE runs SET heartbeat_at=clock_timestamp()-interval '10 minutes' WHERE id=$1",[f.run]);
    await markUncertain();
    expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('reconciling');
  },
);

it('uses an independent systemd unit and frozen absolute source with no secrets, runtime cap or dispatcher lifetime binding', async () => {
  const f = await reserved(),
    args = nativeSupervisorArguments(f.reservation);
  expect(args).toContain('--unit=' + nativeSupervisorUnit(f.run));
  expect(args).toContain('--property=Slice=agentos_native_supervisors.slice');
  expect(args).toContain(release.candidate + '/scripts/opencode-batch.ts');
  expect(args).toContain('--property=EnvironmentFile=/etc/agentos/server.env');
  expect(args).toContain('--property=NoNewPrivileges=yes');
  expect(args).toContain('--property=Type=exec');
  expect(args.join(' ')).not.toMatch(
    /PartOf=|BindsTo=|RuntimeMaxSec|TokenLimit|Bearer|api[_-]?key/i,
  );
  expect(args).not.toContain('--wait');
  expect(() =>
    nativeSupervisorArguments({ ...f.reservation, candidate: '/opt/agentos/current' }),
  ).toThrow('NATIVE_RELEASE_INVALID');
});

it('rejects a standalone invocation outside its proven published source and independent systemd cgroup', async () => {
  const f = await reserved();
  await expect(nativeSupervisorSelfProof(f.reservation)).rejects.toThrow(
    'NATIVE_SUPERVISOR_SELF_PROOF',
  );
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
    'queued',
  );
});

it('does not start a reserved run that the user cancelled before its supervisor registered', async () => {
  const f = await reserved(),
    actor = vi.fn(async () => {});
  await cancelTask(f.task);
  expect(
    await superviseNativeRun(f.run, f.reservation.launchToken, async (r) => proof(r), actor),
  ).toBe('not_admitted');
  expect(actor).not.toHaveBeenCalled();
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
    'cancelled',
  );
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(0);
  expect(
    await tx((db) => reconcileNativeSupervisor(db, f.reservation, stopped(f.reservation))),
  ).toBe('finished');
});

it.each(['paused', 'cancelled'])(
  'does not reserve native work from a %s goal and preserves the queued run',
  async (state) => {
    const f = await fixture();
    await tx(async (db) => {
      await lock(db);
      await db.query('UPDATE goals SET state=$2 WHERE id=$1', [f.goal, state]);
    });
    const before = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0];
    expect(await tx((db) => reserveNativeSupervisor(db, f.run, release))).toBeNull();
    expect((await job(f.run)).state).toBe('queued');
    expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual(before);
    expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
    expect((await pool.query('SELECT 1 FROM contexts WHERE run_id=$1', [f.run])).rowCount).toBe(0);
  },
);

it('rechecks pause after registration at the actual fenced native start and resumes that same run only after resume', async () => {
  const f = await reserved(),
    owned = await owner(f.reservation);
  expect(
    await tx((db) => registerNativeSupervisor(db, f.reservation, proof(f.reservation), owned.pid)),
  ).toBe(true);
  await tx(async (db) => {
    await lock(db);
    await db.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
  });
  const before = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0];
  expect(
    await tx((db) => startDetachedNativeRun(db, f.reservation, 'opencode-native-12345')),
  ).toBeNull();
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
  expect((await pool.query('SELECT 1 FROM contexts WHERE run_id=$1', [f.run])).rowCount).toBe(0);
  await tx(async (db) => {
    await lock(db);
    await db.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  });
  expect(
    await tx((db) => startDetachedNativeRun(db, f.reservation, 'opencode-native-12345')),
  ).toMatchObject({ id: f.run, fence: f.fence, state: 'running' });
  expect(
    await tx((db) => startDetachedNativeRun(db, f.reservation, 'opencode-native-12345')),
  ).toBeNull();
});

it('serializes competing native starts with a concurrent goal pause without a duplicate start or context/lease change', async () => {
  const f = await reserved(),
    owned = await owner(f.reservation);
  await tx((db) => registerNativeSupervisor(db, f.reservation, proof(f.reservation), owned.pid));
  const work = Array.from({ length: 4 }, () =>
    tx((db) => startDetachedNativeRun(db, f.reservation, 'opencode-native-12345')),
  );
  work.push(
    tx(async (db) => {
      await lock(db);
      await db.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
      return null;
    }),
  );
  const results = await Promise.all(work);
  expect(results.filter(Boolean).length).toBeLessThanOrEqual(1);
  const state = (await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state;
  const starts = (
    await pool.query(
      "SELECT count(*)::int n FROM events WHERE kind='RUN_STARTED' AND payload->>'runId'=$1",
      [f.run],
    )
  ).rows[0].n;
  expect(starts).toBe(state === 'running' ? 1 : 0);
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
  expect((await pool.query('SELECT 1 FROM contexts WHERE run_id=$1', [f.run])).rowCount).toBe(0);
});

it('does not turn a queued legacy record with a remote launch event into a new supervisor launch', async () => {
  const f = await fixture();
  await pool.query(
    "INSERT INTO events(kind,entity_id,payload) VALUES('OPENCODE_LAUNCH_ATTEMPTED',$1,$2)",
    [f.task, JSON.stringify({ runId: f.run })],
  );
  expect(await tx((db) => reserveNativeSupervisor(db, f.run, release))).toBeNull();
  expect((await job(f.run)).state).toBe('queued');
});
