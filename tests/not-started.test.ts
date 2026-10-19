import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate, tx, lock } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { config } from '../server/config.js';
import {
  auditedLegacyOpenCodeControllerDigest,
  legacyOpenCodeNotStartedCandidate,
  recordLegacyOpenCodeNotStarted,
  recordOpenCodeNotStarted,
} from '../server/not-started.js';

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
});
afterAll(async () => {
  await pool.end();
});

async function fixture(legacy = false) {
  const goal = randomUUID(),
    task = randomUUID(),
    run = randomUUID(),
    fence = randomUUID();
  const workerId = 'opencode-native-123';
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Test','Test','not-started',$2)",
    [goal, goal],
  );
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,attempt_count,criteria,execution)
    VALUES($1,$2,'one','One','One','qa.backend',$3,1,'[]','{"kind":"opencode","workspace":"not-started/one"}')`,
    [task, goal, legacy ? 'recovery_blocked' : 'running'],
  );
  await pool.query(
    `INSERT INTO runs(id,task_id,attempt,fence,worker_id,agent_version,runtime,state,error_code,started_at,finished_at)
    VALUES($1,$2,1,$3,$4,1,'opencode',$5,$6,now()-interval '20 seconds',CASE WHEN $5='failed' THEN now()-interval '10 seconds' ELSE NULL END)`,
    [
      run,
      task,
      fence,
      workerId,
      legacy ? 'failed' : 'running',
      legacy ? 'SUPERVISION_CONTEXT_CHANGED' : null,
    ],
  );
  await pool.query(
    "INSERT INTO opencode_jobs(run_id,state,started_at,finished_at) VALUES($1,$2,now()-interval '20 seconds',CASE WHEN $2='finished' THEN now()-interval '9 seconds' ELSE NULL END)",
    [run, legacy ? 'finished' : 'running'],
  );
  if (legacy)
    for (const kind of ['RUN_QUEUED', 'RUN_STARTED', 'RUN_FAILED'])
      await pool.query(
        'INSERT INTO events(kind,entity_id,payload,created_at) VALUES($1,$2,$3,now()-$4::interval)',
        [
          kind,
          task,
          JSON.stringify({ runId: run, code: 'SUPERVISION_CONTEXT_CHANGED' }),
          kind === 'RUN_QUEUED'
            ? '25 seconds'
            : kind === 'RUN_STARTED'
              ? '20 seconds'
              : '10 seconds',
        ],
      );
  const receipt = {
    runId: run,
    fence,
    workerId,
    remoteStartAttempted: false,
    cancelRequested: false,
    errorCode: 'SUPERVISION_CONTEXT_CHANGED',
  };
  return { goal, task, run, fence, workerId, receipt };
}
const record = (input: Parameters<typeof recordOpenCodeNotStarted>[1]) =>
  tx(async (db) => {
    await lock(db);
    return recordOpenCodeNotStarted(db, input);
  });

it('records a fence-owned preflight failure once without representing a cancelled or attempted launch as not-started', async () => {
  const f = await fixture();
  expect(await record({ ...f.receipt, remoteStartAttempted: true })).toBe(false);
  expect(await record({ ...f.receipt, cancelRequested: true })).toBe(false);
  expect(await record(f.receipt)).toBe(true);
  expect(await record(f.receipt)).toBe(true);
  const events = (await pool.query("SELECT payload FROM events WHERE kind='OPENCODE_NOT_STARTED'"))
    .rows;
  expect(events).toHaveLength(1);
  expect(events[0].payload).not.toHaveProperty('fence');
  expect(events[0].payload).toMatchObject({
    runId: f.run,
    workerId: f.workerId,
    confirmed: true,
    runtime: 'opencode',
    remoteStartAttempted: false,
    cancelRequested: false,
    phase: 'before_remote_spawn',
  });
});

it('rejects stale fences, owners, live cancellation and durable attempted-launch evidence', async () => {
  const f = await fixture();
  expect(await record({ ...f.receipt, fence: randomUUID() })).toBe(false);
  expect(await record({ ...f.receipt, workerId: 'opencode-native-456' })).toBe(false);
  await pool.query("INSERT INTO events(kind,payload) VALUES('OPENCODE_LAUNCH_ATTEMPTED',$1)", [
    JSON.stringify({ runId: f.run }),
  ]);
  expect(await record(f.receipt)).toBe(false);
  await pool.query("DELETE FROM events WHERE kind='OPENCODE_LAUNCH_ATTEMPTED'");
  await pool.query("INSERT INTO events(kind,payload) VALUES('CANCEL_REQUESTED',$1)", [
    JSON.stringify({ runId: f.run }),
  ]);
  expect(await record(f.receipt)).toBe(false);
  await pool.query("DELETE FROM events WHERE kind='CANCEL_REQUESTED'");
  await pool.query("UPDATE runs SET state='cancelling' WHERE id=$1", [f.run]);
  expect(await record(f.receipt)).toBe(false);
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='OPENCODE_NOT_STARTED'")).rowCount,
  ).toBe(0);
});

it('requires the audited legacy controller and lifecycle sequence, not merely an error or missing context', async () => {
  const f = await fixture(true);
  expect(await legacyOpenCodeNotStartedCandidate(pool, f.run, '0'.repeat(64))).toBeNull();
  const candidate = await legacyOpenCodeNotStartedCandidate(
    pool,
    f.run,
    auditedLegacyOpenCodeControllerDigest,
  );
  expect(candidate).toMatchObject({
    runId: f.run,
    taskId: f.task,
    workspace: 'not-started/one',
    errorCode: 'SUPERVISION_CONTEXT_CHANGED',
  });
  await pool.query("DELETE FROM events WHERE kind='RUN_STARTED'");
  expect(
    await legacyOpenCodeNotStartedCandidate(pool, f.run, auditedLegacyOpenCodeControllerDigest),
  ).toBeNull();
});

it('allows only later continuation metadata while rejecting activity at every time and metadata during the original job', async () => {
  const f = await fixture(true);
  const eligible = () =>
    legacyOpenCodeNotStartedCandidate(pool, f.run, auditedLegacyOpenCodeControllerDigest);
  await pool.query("INSERT INTO events(kind,payload) VALUES('CONTINUATION_STATUS',$1)", [
    JSON.stringify({ runId: f.run, state: 'supervising' }),
  ]);
  expect(await eligible()).not.toBeNull();
  await pool.query(
    "UPDATE events SET created_at=now()-interval '15 seconds' WHERE kind='CONTINUATION_STATUS'",
  );
  expect(await eligible()).toBeNull();
  await pool.query("UPDATE events SET created_at=now() WHERE kind='CONTINUATION_STATUS'");
  await pool.query("INSERT INTO events(kind,payload) VALUES('OPENCODE_LAUNCH_ATTEMPTED',$1)", [
    JSON.stringify({ runId: f.run }),
  ]);
  expect(await eligible()).toBeNull();
});

it('rejects runtime activity, any committed context, live jobs, non-whitelisted errors and stale attempts', async () => {
  const f = await fixture(true);
  const eligible = () =>
    legacyOpenCodeNotStartedCandidate(pool, f.run, auditedLegacyOpenCodeControllerDigest);
  await pool.query("INSERT INTO events(kind,payload) VALUES('EXECUTION_OBSERVED',$1)", [
    JSON.stringify({ runId: f.run }),
  ]);
  expect(await eligible()).toBeNull();
  await pool.query("DELETE FROM events WHERE kind='EXECUTION_OBSERVED'");
  const context = randomUUID();
  await pool.query(
    "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Committed context')",
    [context, f.run],
  );
  expect(await eligible()).toBeNull();
  await pool.query('DELETE FROM contexts WHERE id=$1', [context]);
  await pool.query("UPDATE opencode_jobs SET state='running' WHERE run_id=$1", [f.run]);
  expect(await eligible()).toBeNull();
  await pool.query("UPDATE opencode_jobs SET state='finished' WHERE run_id=$1", [f.run]);
  await pool.query("UPDATE runs SET error_code='MODEL_REQUEST_FAILED' WHERE id=$1", [f.run]);
  expect(await eligible()).toBeNull();
  await pool.query("UPDATE runs SET error_code='SUPERVISION_CONTEXT_CHANGED' WHERE id=$1", [f.run]);
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [f.task]);
  expect(await eligible()).toBeNull();
});

it('requires a fresh genuine not-started proof with absent process and matching workspace before retrospective receipt', async () => {
  const f = await fixture(true);
  const candidate = (await legacyOpenCodeNotStartedCandidate(
    pool,
    f.run,
    auditedLegacyOpenCodeControllerDigest,
  ))!;
  const now = Date.now(),
    proof = {
      runId: f.run,
      workspace: 'not-started/one',
      state: 'not_started',
      process: {
        unit: 'agentos-run-' + f.run,
        stopped: true,
        mainPid: 0,
        populated: false,
        activeState: 'inactive',
      },
      checkedAt: new Date(now).toISOString(),
    };
  const retrospective = (value: any) =>
    tx(async (db) => {
      await lock(db);
      return recordLegacyOpenCodeNotStarted(db, candidate, value, now);
    });
  for (const bad of [
    { ...proof, state: 'running' },
    { ...proof, workspace: 'not-started/other' },
    { ...proof, process: { ...proof.process, mainPid: 1 } },
    { ...proof, process: { ...proof.process, populated: true } },
    { ...proof, process: { ...proof.process, stopped: false } },
    { ...proof, checkedAt: new Date(now - 61000).toISOString() },
  ])
    expect(await retrospective(bad)).toBe(false);
  expect(await retrospective(proof)).toBe(true);
  const receipt = (await pool.query("SELECT payload FROM events WHERE kind='OPENCODE_NOT_STARTED'"))
    .rows[0].payload;
  expect(receipt).toMatchObject({
    confirmed: true,
    retrospective: true,
    runId: f.run,
    controllerDigest: auditedLegacyOpenCodeControllerDigest,
  });
  expect(await retrospective(proof)).toBe(false); // The original receipt is immutable, not duplicated.
});

it('rechecks historical eligibility after the remote probe so concurrent evidence cannot authorize a receipt', async () => {
  const f = await fixture(true);
  const candidate = (await legacyOpenCodeNotStartedCandidate(
    pool,
    f.run,
    auditedLegacyOpenCodeControllerDigest,
  ))!;
  await pool.query("INSERT INTO events(kind,payload) VALUES('CANCEL_REQUESTED',$1)", [
    JSON.stringify({ runId: f.run }),
  ]);
  const proof = {
    runId: f.run,
    workspace: 'not-started/one',
    state: 'not_started',
    process: {
      unit: 'agentos-run-' + f.run,
      stopped: true,
      mainPid: 0,
      populated: false,
      activeState: 'inactive',
    },
    checkedAt: new Date().toISOString(),
  };
  expect(
    await tx(async (db) => {
      await lock(db);
      return recordLegacyOpenCodeNotStarted(db, candidate, proof);
    }),
  ).toBe(false);
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='OPENCODE_NOT_STARTED'")).rowCount,
  ).toBe(0);
});
