import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { writeArtifact } from '../server/service.js';
import { reconcileWorkforces, workforceFingerprint,readWorkforceRoutingTasks } from '../server/workforce.js';

const now = Date.parse('2026-10-05T23:00:00Z');
let goalId: string;
const policy = {
  enabled: true,
  glmAccountId: 'glm',
  glmTarget: 6,
  workerAccounts: ['qwen', 'kimi', 'agy1', 'agy2'],
  coordinatorAccountId: 'codex1',
  coordinatorModel: 'gpt-6-sol',
};

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await seedRegistry();
  for (const [id, provider, model] of [
    ['codex1', 'codex', 'gpt-6-sol'],
    ['codex2', 'codex', 'gpt-6-sol'],
    ['agy1', 'agy', 'gemini-3.8-flash-high'],
    ['agy2', 'agy', 'gemini-3.8-flash-high'],
    ['qwen', 'qwen', 'deepseek-v4-flash-0731'],
    ['kimi', 'kimi', 'k3'],
  ])
    await pool.query(
      'INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,bridge_id) VALUES($1,$1,$2,$3,$4,true,true,$1)',
      [id, provider, model, JSON.stringify([model])],
    );
  goalId = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'test','test','workforce',$1::text)",
    [goalId],
  );
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)', [
    `workforce:${goalId}`,
    JSON.stringify(policy),
  ]);
});
afterAll(async () => {
  await pool.end();
});

async function task(options: any = {}) {
  const id = randomUUID(),
    stage = options.stage ?? 'work';
  const account =
    options.account ?? (stage === 'coordinate' ? 'codex1' : stage === 'review' ? 'agy1' : 'qwen');
  const model = account.startsWith('codex')
    ? 'gpt-6-sol'
    : account.startsWith('agy')
      ? 'gemini-3.8-flash-high'
      : 'deepseek-v4-flash-0731';
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,stage,state,criteria,execution,attempt_count,max_attempts,priority)
    VALUES($1::uuid,$2,$1::text,'test','original','qa.backend',$3,$4,$5,$6,'[]',$7,$8,$9,50)`,
    [
      id,
      goalId,
      account,
      model,
      stage,
      options.state ?? 'ready',
      options.execution === undefined ? null : JSON.stringify(options.execution),
      options.attempts ?? 0,
      options.maxAttempts ?? 3,
    ],
  );
  return id;
}
async function run(taskId: string, options: any = {}) {
  const id = randomUUID(),
    fence = randomUUID();
  await pool.query(
    `INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state,finished_at,error_code)
    SELECT $1,id,$2,$3,account_id,model,'deepseek-harness',1,$4,$5,$6 FROM tasks WHERE id=$7`,
    [
      id,
      options.attempt ?? 1,
      fence,
      options.state ?? 'failed',
      new Date(options.finishedAt ?? now),
      options.error ?? 'PROVIDER_ERROR',
      taskId,
    ],
  );
  return { id, fence };
}
const row = async (id: string) =>
  (await pool.query('SELECT * FROM tasks WHERE id=$1', [id])).rows[0];
const events = async (kind: string) =>
  (await pool.query('SELECT * FROM events WHERE kind=$1', [kind])).rows;

it('routes from bounded metadata while retaining cancellation and failure counts without loading worker contexts',async()=>{
  const execution={kind:'harness',workforceLane:'kimi',continuationRoutes:[{accountId:'qwen',model:'deepseek-v4-flash-0731',until:'2099-01-01'}],
    contextSnapshot:'immutable context '.repeat(60000)};
  const current=await task({execution,state:'failed',attempts:1}),closed=await task({execution,state:'accepted'});
  const analytical=await task({stage:'review',state:'failed',attempts:1});
  const running=await task({execution,state:'running',attempts:1});
  const failure=await run(analytical),active=await run(running,{state:'running'});
  const stopped=await run(current);
  await pool.query('UPDATE runs SET summary=$2 WHERE id=$1',[stopped.id,'report '.repeat(10000)]);
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[analytical,JSON.stringify({runId:failure.id})]);
  const before=await row(current),rows=await readWorkforceRoutingTasks(pool,goalId);
  expect(rows.find(t=>t.id===current)).toMatchObject({execution:{kind:'harness',workforceLane:'kimi',continuationRoutes:execution.continuationRoutes},summary:null});
  expect(rows.find(t=>t.id===closed)).toMatchObject({state:'accepted',execution:null});
  expect(rows.find(t=>t.id===analytical)).toMatchObject({cancel_requested:true,failure_count:1,execution:null});
  expect(rows.find(t=>t.id===running)).toMatchObject({has_live_run:true,run_id:active.id,run_state:'running'});
  expect(JSON.stringify(rows).length).toBeLessThan(10000);
  expect((await readWorkforceRoutingTasks(pool,goalId,true)).find(t=>t.id===current).summary).toHaveLength(1400);
  expect(await row(current)).toEqual(before);
  await reconcileWorkforces(now);
  expect((await row(current)).account_id).toBe('kimi');
  expect((await row(current)).execution).toEqual(execution);
  expect((await row(analytical)).state).toBe('failed');
  expect((await row(running)).state).toBe('running');
});

it('backs off terminal analytical failures, changes eligible account, and never refreshes retry deadline each tick', async () => {
  const id = await task({ stage: 'review', state: 'failed', attempts: 1 });
  const source = await run(id);
  await pool.query("UPDATE goals SET state='failed' WHERE id=$1", [goalId]);
  await reconcileWorkforces(now);
  let current = await row(id);
  expect(current).toMatchObject({
    state: 'retry_scheduled',
    account_id: 'agy2',
    attempt_count: 1,
    max_attempts: 3,
    execution: null,
  });
  expect(current.next_attempt_at.getTime()).toBe(now + 30000);
  expect((await events('ANALYTICAL_RETRY_SCHEDULED'))[0].payload.runId).toBe(source.id);
  await reconcileWorkforces(now + 5000);
  current = await row(id);
  expect(current.next_attempt_at.getTime()).toBe(now + 30000);
  expect(current.account_id).toBe('agy2');
  expect(await events('ANALYTICAL_RETRY_SCHEDULED')).toHaveLength(1);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1', [goalId])).rows[0].state).toBe(
    'active',
  );
});

it('caps consecutive failures even if task has a large historical allowance', async () => {
  const id = await task({ stage: 'coordinate', state: 'failed', attempts: 3, maxAttempts: 100 });
  for (let attempt = 1; attempt <= 3; attempt++) await run(id, { attempt });
  await task(); // Remaining work must not reset the coordinator's exhausted cycle.
  await reconcileWorkforces(now);
  await reconcileWorkforces(now + 600000);
  expect(await row(id)).toMatchObject({ state: 'failed', attempt_count: 3, max_attempts: 100 });
  expect(await events('ANALYTICAL_RETRY_EXHAUSTED')).toHaveLength(1);
  expect(await events('ANALYTICAL_RETRY_SCHEDULED')).toHaveLength(0);
});

it('respects a lower configured max_attempts without extending the failed coordinator', async () => {
  const id = await task({ stage: 'coordinate', state: 'failed', attempts: 1, maxAttempts: 1 });
  await run(id);
  await task();
  await reconcileWorkforces(now);
  expect(await row(id)).toMatchObject({ state: 'failed', max_attempts: 1 });
});

it('never retries cancelled, uncertain, live, context-invalid or workspace executions', async () => {
  const ids: string[] = [];
  for (const options of [
    { state: 'cancelled' },
    { state: 'reconciling' },
    { state: 'running' },
    { state: 'failed', error: 'STOP_UNCONFIRMED' },
    { state: 'failed', error: 'PROVIDER_TIMEOUT' },
    { state: 'failed', error: 'CONTEXT_TOO_LARGE' },
    { state: 'failed', execution: { kind: 'harness', workspace: 'test/x' } },
    { state: 'failed', cancel: true },
    { state: 'failed', live: true },
  ]) {
    const id = await task({
      stage: 'review',
      state: options.state,
      attempts: 1,
      execution: options.execution,
    });
    const source = await run(id, {
      state: options.live
        ? 'running'
        : ['cancelled', 'reconciling', 'running'].includes(options.state)
          ? options.state
          : 'failed',
      error: options.error,
    });
    if (options.cancel)
      await pool.query(
        "INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",
        [id, JSON.stringify({ runId: source.id })],
      );
    ids.push(id);
  }
  const before = await Promise.all(ids.map(row));
  await reconcileWorkforces(now);
  const after = await Promise.all(ids.map(row));
  expect(after.map((t) => [t.state, t.attempt_count, t.max_attempts])).toEqual(
    before.map((t) => [t.state, t.attempt_count, t.max_attempts]),
  );
  expect(await events('ANALYTICAL_RETRY_SCHEDULED')).toHaveLength(0);
});

it('waits for an eligible route instead of sending a retry through an exhausted account', async () => {
  await pool.query("UPDATE accounts SET enabled=false WHERE id='agy2'");
  const id = await task({ stage: 'review', state: 'failed', attempts: 1 });
  await run(id, { error: 'QUOTA_EXCEEDED' });
  await reconcileWorkforces(now);
  expect((await row(id)).state).toBe('failed');
  await reconcileWorkforces(now + 5000);
  expect((await row(id)).state).toBe('failed');
  await pool.query("UPDATE accounts SET enabled=true WHERE id='agy2'");
  await reconcileWorkforces(now + 10000);
  expect(await row(id)).toMatchObject({
    state: 'retry_scheduled',
    account_id: 'agy2',
    execution: null,
  });
});

it('refreshes successful coordination only on changed work and applies bounded priorities once', async () => {
  const worker = await task();
  const coordinator = await task({
    stage: 'coordinate',
    state: 'running',
    attempts: 3,
    maxAttempts: 3,
  });
  const source = await run(coordinator, { state: 'running', attempt: 3, finishedAt: now - 100000 });
  await writeArtifact(
    source.id,
    source.fence,
    'coordination.json',
    JSON.stringify({
      summary: 'Order real work',
      priorities: [
        null,
        { taskId: worker, priority: 79 },
        { taskId: coordinator, priority: 2 },
        { taskId: worker, priority: 101 },
        { taskId: worker, priority: 3.5 },
        { taskId: randomUUID(), priority: 1 },
      ],
    }),
    'application/json',
  );
  await pool.query("UPDATE runs SET state='submitted' WHERE id=$1", [source.id]);
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1", [coordinator]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')",
    [randomUUID(), coordinator, source.id],
  );
  const tasks = (await pool.query('SELECT * FROM tasks WHERE goal_id=$1', [goalId])).rows;
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)', [
    `workforce-state:${goalId}`,
    JSON.stringify({ fingerprint: workforceFingerprint(tasks) }),
  ]);
  await reconcileWorkforces(now);
  expect((await row(coordinator)).state).toBe('accepted');
  expect((await row(worker)).priority).toBe(79);
  await pool.query("UPDATE tasks SET state='waiting_resource' WHERE id=$1", [worker]);
  await reconcileWorkforces(now + 1000);
  expect(await row(coordinator)).toMatchObject({ state: 'ready', max_attempts: 6, priority: 100 });
  await reconcileWorkforces(now + 2000);
  expect((await row(coordinator)).max_attempts).toBe(6);
  expect(await events('COORDINATION_APPLIED')).toHaveLength(1);
});

it('leaves paused goals completely untouched', async () => {
  const id = await task({ stage: 'review', state: 'failed', attempts: 1 });
  await run(id);
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [goalId]);
  await reconcileWorkforces(now);
  expect(await row(id)).toMatchObject({ state: 'failed', account_id: 'agy1' });
  expect(await events('ANALYTICAL_RETRY_SCHEDULED')).toHaveLength(0);
});

async function periodicQuotaFixture(state = 'waiting_quota') {
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)', [
    'management:' + goalId, JSON.stringify({ enabled: true, intervalSeconds: 120 }),
  ]);
  await pool.query('UPDATE settings SET value=$2 WHERE id=$1', [
    'workforce:' + goalId, JSON.stringify({ ...policy, coordinatorModel: 'gpt-6.1-sol' }),
  ]);
  for (const [account, remaining] of [['codex1', 0], ['codex2', 99]] as const)
    await pool.query(`UPDATE accounts SET model='gpt-6.1-sol',models='["gpt-6.1-sol"]',
      allow_unknown=false,quota=$2 WHERE id=$1`, [account,
      JSON.stringify({ status: 'ok', groups: [{ label: 'codex', windows: [{ remainingPercent: remaining }] }] })]);
  const id = await task({ stage: 'coordinate', state, attempts: 1, maxAttempts: 4 });
  await pool.query("UPDATE tasks SET model='gpt-6.1-sol',description='MANAGEMENT_V1 preserve exact instructions' WHERE id=$1", [id]);
  return id;
}

it('reroutes an idle periodic manager from exhausted Codex to the eligible account without changing lifecycle or contract', async () => {
  const id = await periodicQuotaFixture(), before = await row(id);
  await reconcileWorkforces(now);
  expect(await row(id)).toEqual({ ...before, account_id: 'codex2' });
  await reconcileWorkforces(now + 5000);
  expect(await row(id)).toEqual({ ...before, account_id: 'codex2' });
  expect(await events('WORKFORCE_ROUTE_CHANGED')).toHaveLength(1);
  expect(await events('ANALYTICAL_RETRY_SCHEDULED')).toHaveLength(0);
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1', [id])).rowCount).toBe(0);
});

it.each(['allowed_accounts', 'all_exhausted', 'wrong_model'])(
  'keeps the periodic manager waiting without inventing a route when %s', async mode => {
    const id = await periodicQuotaFixture();
    if (mode === 'allowed_accounts') await pool.query("UPDATE tasks SET allowed_accounts='[\"codex1\"]' WHERE id=$1", [id]);
    if (mode === 'all_exhausted') await pool.query("UPDATE accounts SET quota=(SELECT quota FROM accounts WHERE id='codex1') WHERE id='codex2'");
    if (mode === 'wrong_model') await pool.query("UPDATE accounts SET models='[\"gpt-6-luna\"]' WHERE id='codex2'");
    const before = await row(id), quotas = (await pool.query('SELECT id,quota FROM accounts ORDER BY id')).rows;
    await reconcileWorkforces(now);
    expect(await row(id)).toEqual(before);
    expect((await pool.query('SELECT id,quota FROM accounts ORDER BY id')).rows).toEqual(quotas);
    expect(await events('WORKFORCE_ROUTE_CHANGED')).toHaveLength(0);
  },
);

it.each(['goal_paused', 'task_paused', 'cancelled', 'cancel_requested', 'run_cancelled', 'workspace_execution',
  'queued', 'running', 'cancelling', 'reconciling', 'manual', 'autonomy_off'])(
  'does not reroute a periodic manager with %s protection', async mode => {
    const id = await periodicQuotaFixture();
    if (mode === 'goal_paused') await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [goalId]);
    if (mode === 'task_paused' || mode === 'cancelled')
      await pool.query('UPDATE tasks SET state=$2 WHERE id=$1', [id, mode === 'task_paused' ? 'paused' : 'cancelled']);
    if (['queued', 'running', 'cancelling', 'reconciling', 'run_cancelled', 'cancel_requested'].includes(mode)) {
      const created = await run(id, { state: mode === 'run_cancelled' ? 'cancelled' : mode === 'cancel_requested' ? 'failed' : mode });
      if (mode === 'cancel_requested') await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [id, JSON.stringify({ runId: created.id })]);
    }
    if (mode === 'manual') await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1", [id]);
    if (mode === 'workspace_execution') await pool.query("UPDATE tasks SET execution='{\"kind\":\"harness\",\"workspace\":\"workforce/live-source\"}' WHERE id=$1", [id]);
    if (mode === 'autonomy_off') await pool.query("INSERT INTO settings VALUES('autonomy:workforce','{\"enabled\":false}') ON CONFLICT(id) DO UPDATE SET value=excluded.value");
    const before = await row(id);
    try {
      await reconcileWorkforces(now);
      expect(await row(id)).toEqual(before);
      expect(await events('WORKFORCE_ROUTE_CHANGED')).toHaveLength(0);
    } finally {
      if (mode === 'autonomy_off') await pool.query("DELETE FROM settings WHERE id='autonomy:workforce'");
    }
  },
);

it('rechecks periodic manager policy after waiting for the admission lock', async () => {
  const id = await periodicQuotaFixture(), before = await row(id);
  const owner = await pool.connect();
  let pending: Promise<void> | undefined;
  try {
    await owner.query('BEGIN');
    await owner.query('SELECT pg_advisory_xact_lock(4280001)');
    pending = reconcileWorkforces(now);
    let waiting = false;
    for (let attempt = 0; attempt < 100; attempt++) {
      waiting = (await pool.query("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=4280001 AND NOT granted) waiting")).rows[0].waiting;
      if (waiting) break;
      await delay(10);
    }
    expect(waiting).toBe(true);
    await owner.query("UPDATE settings SET value=jsonb_set(value,'{enabled}','false') WHERE id=$1", ['workforce:' + goalId]);
    await owner.query('COMMIT');
    await pending;
    expect(await row(id)).toEqual(before);
    expect(await events('WORKFORCE_ROUTE_CHANGED')).toHaveLength(0);
  } finally {
    await owner.query('ROLLBACK');
    owner.release();
    await pending;
  }
});
