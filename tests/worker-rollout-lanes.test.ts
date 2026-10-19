import { beforeAll, beforeEach, afterEach, afterAll, it, expect } from 'vitest';
import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import { pool, migrate, tx } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { config } from '../server/config.js';
import { schedule, heartbeat } from '../server/service.js';
import { installSupervision } from '../server/supervision.js';
import { installBacklogPlanner } from '../server/backlog.js';
import { createCycle, runCycle, occurrenceDetail } from '../server/cycles/store.js';
import {
  installWorkerRolloutAdmission,
  claimWorkerDrain,
  admitWorkerServiceRestart,
  adoptWorkerService,
  type WorkerService,
  type WorkerRelease,
  type WorkerServiceProof,
} from '../server/worker-rollout.js';

const record: WorkerRelease = {
  candidate: '/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',
  digest: 'a'.repeat(64),
  workersPendingRestart: true,
};
const accounts = { muse: 'lane-muse', glm: 'lane-glm', sol: 'lane-sol' };
let goal: string;
const owners = new Map<WorkerService, { client: PoolClient; key: number; backendPid: number }>();
const alteredTriggers = new Set<string>();
beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await installBacklogPlanner();
  await installSupervision();
  await installWorkerRolloutAdmission();
});
beforeEach(async () => {
  await pool.query("DELETE FROM settings WHERE id='maintenance' OR id LIKE 'worker-readiness:%'");
  await pool.query('TRUNCATE goals,agents,accounts,memories,contexts,cycles,events CASCADE');
  await seedRegistry();
  config.bridgeHost = 'test-only';
  config.remoteExecution = true;
  for (const [id, provider, model] of [
    [accounts.muse, 'muse', 'muse-spark-1.3'],
    [accounts.glm, 'openai', 'glm-5.3-flash'],
    [accounts.sol, 'codex', 'gpt-6.1-sol'],
  ])
    await pool.query(
      'INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id) VALUES($1,$1,$2,$3,$4,true,true,20,$1)',
      [id, provider, model, JSON.stringify([model])],
    );
  goal = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Generic lane handoff','Own admission fixture','rollout-lanes',$2)",
    [goal, goal],
  );
  await pool.query('INSERT INTO settings VALUES($1,$2),($3,$4)', [
    'workforce:' + goal,
    JSON.stringify({
      enabled: true,
      glmAccountId: accounts.glm,
      glmTarget: 12,
      museTarget: 6,
      workerAccounts: [accounts.muse],
      coordinatorAccountId: accounts.sol,
      coordinatorModel: 'gpt-6.1-sol',
    }),
    'supervision:' + goal,
    JSON.stringify({ enabled: true }),
  ]);
  await pool.query(
    "INSERT INTO settings VALUES('capacity','{\"maxRuns\":20,\"browserSlots\":4}'),('remote-capacity','{\"maxRuns\":20}') ON CONFLICT(id) DO UPDATE SET value=excluded.value",
  );
});
afterEach(async () => {
  for (const name of alteredTriggers) await pool.query('ALTER TABLE outbox ENABLE TRIGGER ' + name);
  alteredTriggers.clear();
  for (const { client, key } of owners.values()) {
    await client.query('SELECT pg_advisory_unlock($1)', [key]);
    client.release();
  }
  owners.clear();
  await pool.query(
    'DROP TRIGGER IF EXISTS agentos_maintenance_admission ON runs; DROP TRIGGER IF EXISTS agentos_maintenance_admission ON cycle_attempts; DROP FUNCTION IF EXISTS agentos_maintenance_admission()',
  );
  await pool.query("DELETE FROM settings WHERE id='maintenance' OR id LIKE 'worker-readiness:%'");
  config.bridgeHost = '';
  config.remoteExecution = false;
});
afterAll(() => pool.end());

async function task(lane: 'muse' | 'glm' | 'sol', priority = 50, state = 'ready') {
  const id = randomUUID();
  const execution =
    lane === 'sol'
      ? null
      : {
          kind: lane === 'glm' ? 'opencode' : 'harness',
          profile: 'code',
          baseWorkspace: 'rollout-lanes/base',
          workspace: 'rollout-lanes/o' + id.replaceAll('-', ''),
        };
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,priority,criteria,execution,attempt_count) VALUES($1,$2,$3,'Own lane fixture','Preserve original contract','qa.backend',$4,$5,$6,$7,$8,'[]',$9,$10)",
    [
      id,
      goal,
      id,
      accounts[lane],
      lane === 'glm' ? 'glm-5.3-flash' : lane === 'sol' ? 'gpt-6.1-sol' : 'muse-spark-1.3',
      state,
      lane === 'sol' ? 'coordinate' : 'work',
      priority,
      execution ? JSON.stringify(execution) : null,
      state === 'ready' ? 0 : 1,
    ],
  );
  return id;
}
async function rawRun(taskId: string, state = 'queued', runtime = 'harness') {
  const id = randomUUID(),
    fence = randomUUID();
  await pool.query(
    'INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,heartbeat_at) VALUES($1,$2,1,$3,$4,1,$5,now())',
    [id, taskId, fence, runtime, state],
  );
  return { id, fence };
}
async function phasedDrain() {
  const drain = await tx((db) => claimWorkerDrain(db, record, 'per_service'));
  if (drain.status !== 'draining') throw Error('Own phased drain was not claimed');
  return drain.token;
}
async function cycleStep() {
  const cycle = await createCycle(
    {
      name: 'Own lane cycle',
      project: 'rollout-lanes',
      trigger: { kind: 'manual' },
      steps: [{ key: 'wait', title: 'Own cycle', kind: 'wait', seconds: 1 }],
    },
    randomUUID(),
  );
  const occurrence = await runCycle(cycle.id, randomUUID());
  return (await occurrenceDetail(occurrence.id)).steps[0].id;
}
async function rawCycle(step: string, number = 1) {
  const id = randomUUID();
  await pool.query(
    "INSERT INTO cycle_attempts(id,step_id,number,fence,owner,state) VALUES($1,$2,$3,$4,'own-cycle-owner','running')",
    [id, step, number, randomUUID()],
  );
  return id;
}
async function adopt(token: string, service: WorkerService, pid: number) {
  const gate = await tx((db) => admitWorkerServiceRestart(db, token, service));
  expect(gate.ready).toBe(true);
  const now = new Date().toISOString();
  const key =
    service === 'agentos-opencode' ? 4280040 : service === 'agentos-backlog' ? 4280066 : 4280088;
  const client = await pool.connect();
  const owner = (
    await client.query('SELECT pg_try_advisory_lock($1) owned,pg_backend_pid() pid', [key])
  ).rows[0];
  if (!owner.owned) {
    client.release();
    throw Error('Own test owner unavailable');
  }
  owners.set(service, { client, key, backendPid: owner.pid });
  const readiness = {
    service,
    pid,
    candidate: record.candidate,
    digest: record.digest,
    startupId: randomUUID(),
    readyAt: now,
    locks: [{ key, backendPid: owner.pid }],
  };
  await pool.query(
    'INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    ['worker-readiness:' + service, JSON.stringify(readiness)],
  );
  const proof: WorkerServiceProof = {
    unit: service,
    active: true,
    pid,
    cwd: record.candidate,
    digest: record.digest,
    checkedAt: now,
    oldPidGone: true,
    readiness,
    stopped: {
      unit: service,
      previousPid: 0,
      activeState: 'inactive',
      mainPid: 0,
      oldPidGone: true,
      cgroupEmpty: true,
      checkedAt: now,
    },
  };
  await tx((db) => adoptWorkerService(db, token, record, service, proof));
  const state = (await pool.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0]
    .value;
  expect(state.workerRollout.services[service].phase).toBe('adopted');
  return proof;
}

it('uses the task native contract when the raw run still arrives with runtime harness', async () => {
  const native = await task('glm');
  const token = await phasedDrain();
  await adopt(token, 'agentos-opencode', 81001);
  const run = await rawRun(native, 'queued', 'harness');
  expect(
    (await pool.query('SELECT runtime,state FROM runs WHERE id=$1', [run.id])).rows[0],
  ).toEqual({ runtime: 'harness', state: 'queued' });
  await pool.query('INSERT INTO outbox(run_id) VALUES($1)', [run.id]);
  expect((await pool.query('SELECT runtime FROM runs WHERE id=$1', [run.id])).rows[0].runtime).toBe(
    'opencode',
  );
  expect(
    (await pool.query('SELECT state FROM opencode_jobs WHERE run_id=$1', [run.id])).rows[0].state,
  ).toBe('queued');
  expect((await pool.query('SELECT 1 FROM outbox WHERE run_id=$1', [run.id])).rowCount).toBe(0);
});

it('admits native and inline coordination despite a higher priority Muse waiting for its old controller', async () => {
  const blockedMuse = await task('muse', 79),
    coordinator = await task('sol', 70),
    native = await task('glm', 60);
  const token = await phasedDrain();
  await adopt(token, 'agentos-opencode', 81002);
  await adopt(token, 'agentos-backlog', 81003);
  const admitted = await schedule();
  const rows = (await pool.query('SELECT task_id FROM runs WHERE id=ANY($1::uuid[])', [admitted]))
    .rows;
  expect(new Set(rows.map((r) => r.task_id))).toEqual(new Set([coordinator, native]));
  expect(
    (await pool.query('SELECT attempt_count FROM tasks WHERE id=$1', [blockedMuse])).rows[0]
      .attempt_count,
  ).toBe(0);
  expect(
    (
      await pool.query(
        'SELECT 1 FROM supervisor_jobs j JOIN runs r ON r.id=j.run_id WHERE r.task_id=$1',
        [coordinator],
      )
    ).rowCount,
  ).toBe(1);
  expect(
    (
      await pool.query(
        'SELECT 1 FROM opencode_jobs j JOIN runs r ON r.id=j.run_id WHERE r.task_id=$1',
        [native],
      )
    ).rowCount,
  ).toBe(1);
});

it.each(['queued', 'running', 'reconciling'])(
  'denies a raw %s admission to an unadopted lane without changing its task',
  async (state) => {
    const generic = await task('muse');
    await phasedDrain();
    const before = (await pool.query('SELECT * FROM tasks WHERE id=$1', [generic])).rows[0];
    await expect(rawRun(generic, state)).rejects.toThrow('MAINTENANCE_DRAIN');
    expect((await pool.query('SELECT * FROM tasks WHERE id=$1', [generic])).rows[0]).toEqual(
      before,
    );
    expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1', [generic])).rowCount).toBe(0);
  },
);

it('does not revive a terminal generic run as reconciling while its controller is unadopted', async () => {
  const generic = await task('muse', 50, 'failed'),
    run = await rawRun(generic, 'failed');
  await phasedDrain();
  await expect(
    pool.query("UPDATE runs SET state='reconciling' WHERE id=$1", [run.id]),
  ).rejects.toThrow('MAINTENANCE_DRAIN');
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [run.id])).rows[0].state).toBe(
    'failed',
  );
});

it('preserves the old live generic owner, context, fence and leases when native/backlog are adopted', async () => {
  const generic = await task('muse', 79, 'running'),
    run = await rawRun(generic, 'running');
  const context = randomUUID();
  await pool.query(
    "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{\"fixture\":true}','Original immutable fixture context')",
    [context, run.id],
  );
  await pool.query("UPDATE runs SET context_id=$2,worker_id='old-generic-owner' WHERE id=$1", [
    run.id,
    context,
  ]);
  await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2),($3,$2)', [
    'remote-slot',
    run.id,
    'workspace:own-live-fixture',
  ]);
  const before = (await pool.query('SELECT * FROM runs WHERE id=$1', [run.id])).rows[0];
  const leases = (
    await pool.query('SELECT * FROM leases WHERE run_id=$1 ORDER BY resource', [run.id])
  ).rows;
  const token = await phasedDrain();
  await adopt(token, 'agentos-opencode', 81004);
  await adopt(token, 'agentos-backlog', 81005);
  const blocked = await tx((db) => admitWorkerServiceRestart(db, token, 'agentos-workers'));
  expect(blocked.ready).toBe(false);
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [run.id])).rows[0]).toEqual(before);
  expect(
    (await pool.query('SELECT * FROM leases WHERE run_id=$1 ORDER BY resource', [run.id])).rows,
  ).toEqual(leases);
  expect((await heartbeat(run.id, run.fence))?.state).toBe('running');
});

it('fails closed if foreign maintenance replaces the rollout after native adoption', async () => {
  const native = await task('glm'),
    token = await phasedDrain();
  await adopt(token, 'agentos-opencode', 81006);
  await pool.query(
    'UPDATE settings SET value=value||\'{"owner":"foreign-maintenance","draining":true}\'::jsonb WHERE id=\'maintenance\'',
  );
  await expect(rawRun(native)).rejects.toThrow('MAINTENANCE_DRAIN');
  await expect(schedule()).resolves.toEqual([]);
  expect((await pool.query('SELECT 1 FROM runs')).rowCount).toBe(0);
  expect(
    (await pool.query("SELECT value->>'owner' owner FROM settings WHERE id='maintenance'")).rows[0]
      .owner,
  ).toBe('foreign-maintenance');
});

it('rejects changed readiness and a digest that no longer matches the adopted controller', async () => {
  const native = await task('glm', 70),
    coordinator = await task('sol', 60),
    token = await phasedDrain();
  await adopt(token, 'agentos-opencode', 81007);
  await adopt(token, 'agentos-backlog', 81107);
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{digest}',$2::jsonb) WHERE id=$1", [
    'worker-readiness:agentos-opencode',
    JSON.stringify('b'.repeat(64)),
  ]);
  await expect(rawRun(native)).rejects.toThrow('MAINTENANCE_DRAIN');
  expect((await pool.query('SELECT 1 FROM runs')).rowCount).toBe(0);
  const admitted = await schedule();
  expect(
    (await pool.query('SELECT task_id FROM runs WHERE id=ANY($1::uuid[])', [admitted])).rows.map(
      (r) => r.task_id,
    ),
  ).toEqual([coordinator]);
});

it('rejects a runtime-only native disguise without a routable task protocol', async () => {
  const generic = await task('muse'),
    token = await phasedDrain();
  await adopt(token, 'agentos-opencode', 81008);
  await expect(rawRun(generic, 'queued', 'opencode')).rejects.toThrow('MAINTENANCE_DRAIN');
});

it('rejects native admission when its workforce routing contract has been disabled', async () => {
  const native = await task('glm'),
    token = await phasedDrain();
  await adopt(token, 'agentos-opencode', 81009);
  await pool.query('UPDATE settings SET value=value||\'{"enabled":false}\'::jsonb WHERE id=$1', [
    'workforce:' + goal,
  ]);
  await expect(rawRun(native)).rejects.toThrow('MAINTENANCE_DRAIN');
});

it.each(['DISABLE', 'ENABLE REPLICA'])(
  'keeps native admissions closed when its dispatch trigger is %s for an origin connection',
  async (mode) => {
    const native = await task('glm'),
      token = await phasedDrain();
    await adopt(token, 'agentos-opencode', 81201);
    alteredTriggers.add('agentos_workforce_native_dispatch');
    await pool.query('ALTER TABLE outbox ' + mode + ' TRIGGER agentos_workforce_native_dispatch');
    await expect(rawRun(native)).rejects.toThrow('MAINTENANCE_DRAIN');
    await expect(schedule()).resolves.toEqual([]);
  },
);

it.each(['DISABLE', 'ENABLE REPLICA'])(
  'keeps inline coordinator admissions closed when its dispatch trigger is %s for an origin connection',
  async (mode) => {
    const coordinator = await task('sol'),
      token = await phasedDrain();
    await adopt(token, 'agentos-backlog', 81202);
    alteredTriggers.add('agentos_supervisor_dispatch');
    await pool.query('ALTER TABLE outbox ' + mode + ' TRIGGER agentos_supervisor_dispatch');
    await expect(rawRun(coordinator)).rejects.toThrow('MAINTENANCE_DRAIN');
    await expect(schedule()).resolves.toEqual([]);
  },
);

it('does not admit another native job after its matching readiness owner connection has lost the lock', async () => {
  const native = await task('glm', 70),
    coordinator = await task('sol', 60),
    token = await phasedDrain();
  await adopt(token, 'agentos-opencode', 81010);
  await adopt(token, 'agentos-backlog', 81011);
  const owner = owners.get('agentos-opencode')!;
  await owner.client.query('SELECT pg_advisory_unlock($1)', [owner.key]);
  owner.client.release();
  owners.delete('agentos-opencode');
  await expect(rawRun(native)).rejects.toThrow('MAINTENANCE_DRAIN');
  const admitted = await schedule();
  expect(
    (await pool.query('SELECT task_id FROM runs WHERE id=ANY($1::uuid[])', [admitted])).rows.map(
      (r) => r.task_id,
    ),
  ).toEqual([coordinator]);
});

it('does not adopt a controller that holds only the two-key advisory namespace rather than the native single-key owner', async () => {
  const token = await phasedDrain();
  const gate = await tx((db) => admitWorkerServiceRestart(db, token, 'agentos-opencode'));
  expect(gate.ready).toBe(true);
  const client = await pool.connect();
  try {
    const owner = (
      await client.query('SELECT pg_try_advisory_lock(0,4280040) owned,pg_backend_pid() pid')
    ).rows[0];
    expect(owner.owned).toBe(true);
    const now = new Date().toISOString();
    const readiness = {
      service: 'agentos-opencode' as const,
      pid: 81301,
      candidate: record.candidate,
      digest: record.digest,
      startupId: randomUUID(),
      readyAt: now,
      locks: [{ key: 4280040, backendPid: owner.pid }],
    };
    await pool.query('INSERT INTO settings VALUES($1,$2)', [
      'worker-readiness:agentos-opencode',
      JSON.stringify(readiness),
    ]);
    const proof: WorkerServiceProof = {
      unit: 'agentos-opencode',
      active: true,
      pid: readiness.pid,
      cwd: record.candidate,
      digest: record.digest,
      checkedAt: now,
      oldPidGone: true,
      readiness,
      stopped: {
        unit: 'agentos-opencode',
        previousPid: 0,
        activeState: 'inactive',
        mainPid: 0,
        oldPidGone: true,
        cgroupEmpty: true,
        checkedAt: now,
      },
    };
    await expect(
      tx((db) => adoptWorkerService(db, token, record, 'agentos-opencode', proof)),
    ).rejects.toMatchObject({ code: 'ROLLOUT_SERVICE_PROOF' });
    expect(
      (await pool.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0].value
        .workerRollout.services['agentos-opencode'].phase,
    ).toBe('restarting');
  } finally {
    await client.query('SELECT pg_advisory_unlock(0,4280040)');
    client.release();
  }
});

it('migrates the exact legacy global maintenance guard without disabling protection for foreign maintenance', async () => {
  await pool.query(`CREATE OR REPLACE FUNCTION agentos_maintenance_admission() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF EXISTS(SELECT 1 FROM settings WHERE id='maintenance' AND value->>'draining'='true') THEN
      RAISE EXCEPTION 'MAINTENANCE_DRAIN: admissions suspended for controlled transfer';
    END IF; RETURN NEW; END $$;
    CREATE TRIGGER agentos_maintenance_admission BEFORE INSERT ON runs FOR EACH ROW EXECUTE FUNCTION agentos_maintenance_admission();
    CREATE TRIGGER agentos_maintenance_admission BEFORE INSERT ON cycle_attempts FOR EACH ROW EXECUTE FUNCTION agentos_maintenance_admission();`);
  await installWorkerRolloutAdmission();
  const enabled = (
    await pool.query(
      "SELECT tgenabled FROM pg_trigger WHERE tgrelid='runs'::regclass AND tgname='agentos_maintenance_admission'",
    )
  ).rows[0];
  expect(enabled.tgenabled).not.toBe('D');
  const native = await task('glm'),
    token = await phasedDrain();
  await adopt(token, 'agentos-opencode', 81012);
  const allowed = await rawRun(native, 'queued', 'harness');
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [allowed.id])).rows[0].state).toBe(
    'queued',
  );
  const next = await task('glm');
  await pool.query(
    'UPDATE settings SET value=value||\'{"owner":"foreign-maintenance","draining":true}\'::jsonb WHERE id=\'maintenance\'',
  );
  await expect(rawRun(next)).rejects.toThrow('MAINTENANCE_DRAIN');
});

it('preserves the legacy cycle trigger while opening only its adopted owner and keeping foreign maintenance closed', async () => {
  const step = await cycleStep();
  await pool.query(`CREATE OR REPLACE FUNCTION agentos_maintenance_admission() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF EXISTS(SELECT 1 FROM settings WHERE id='maintenance' AND value->>'draining'='true') THEN
      RAISE EXCEPTION 'MAINTENANCE_DRAIN: admissions suspended for controlled transfer';
    END IF; RETURN NEW; END $$;
    CREATE TRIGGER agentos_maintenance_admission BEFORE INSERT ON cycle_attempts FOR EACH ROW EXECUTE FUNCTION agentos_maintenance_admission();`);
  await installWorkerRolloutAdmission();
  expect(
    (
      await pool.query(
        "SELECT tgenabled FROM pg_trigger WHERE tgrelid='cycle_attempts'::regclass AND tgname='agentos_maintenance_admission'",
      )
    ).rows[0].tgenabled,
  ).not.toBe('D');
  const token = await phasedDrain();
  await expect(rawCycle(step)).rejects.toThrow('MAINTENANCE_DRAIN');
  await adopt(token, 'agentos-cycles', 81013);
  const admitted = await rawCycle(step);
  expect(
    (await pool.query('SELECT state FROM cycle_attempts WHERE id=$1', [admitted])).rows[0].state,
  ).toBe('running');
  await pool.query(
    'UPDATE settings SET value=value||\'{"owner":"foreign-maintenance","draining":true}\'::jsonb WHERE id=\'maintenance\'',
  );
  await expect(rawCycle(step, 2)).rejects.toThrow('MAINTENANCE_DRAIN');
});
