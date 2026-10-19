import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { schedule } from '../server/service.js';
import {
  chooseWorkforceRoute,
  installWorkforcePolicy,
  workforceCapacityReason,
  type WorkforcePolicy,
} from '../server/workforce-policy.js';

const policy: WorkforcePolicy = {
  enabled: true,
  glmAccountId: 'glm',
  glmTarget: 6,
  workerAccounts: ['qwen', 'kimi', 'agy'],
  coordinatorAccountId: 'codex',
  coordinatorModel: 'gpt-6-sol',
};
const account = (id: string, provider: string, model: string, extra: any = {}) => ({
  id,
  provider,
  model,
  models: [model],
  enabled: true,
  bridge_id: id,
  allow_unknown: true,
  ...extra,
});
const accounts = () => [
  account('codex', 'codex', 'gpt-6-sol'),
  account('qwen', 'qwen', 'deepseek-v4-flash-0731'),
  account('kimi', 'kimi', 'k3'),
  account('agy', 'agy', 'gemini-3.8-flash-high'),
  account('glm', 'openai', 'glm-5.3-flash'),
];

it('routes an explicitly authorized Harness task to GLM while preserving restrictions and quota holds', () => {
  const task = { model: 'glm-5.3-flash', allowed_accounts: ['glm'],
    execution: { kind: 'harness', workforceLane: 'glm' } };
  const authorized = { ...policy, glmHarnessEnabled: true };
  expect(chooseWorkforceRoute(task, accounts(), policy)).toBeNull();
  expect(chooseWorkforceRoute(task, accounts(), authorized)).toEqual({accountId:'glm',model:'glm-5.3-flash'});
  expect(chooseWorkforceRoute({...task,allowed_accounts:['qwen']},accounts(),authorized)).toBeNull();
  const unavailable = accounts().map(a => a.id === 'glm' ? {...a,quota:{ordinaryUsageAllowed:false}} : a);
  expect(chooseWorkforceRoute(task,unavailable,authorized)).toBeNull();
  const held = {...task,execution:{...task.execution,continuationRoutes:[{accountId:'glm',model:task.model,until:'2099-01-01'}]}};
  expect(chooseWorkforceRoute(held,accounts(),authorized)).toBeNull();
});

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await installWorkforcePolicy(pool);
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await seedRegistry();
  for (const a of accounts())
    await pool.query(
      'INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,bridge_id) VALUES($1,$1,$2,$3,$4,true,true,$1)',
      [a.id, a.provider, a.model, JSON.stringify(a.models)],
    );
});
afterAll(async () => {
  await pool.end();
});

it('refills the independent executor without admitting legacy work and puts a ready delivery ahead of auxiliary diagnostics',async()=>{
 const previous={bridgeHost:config.bridgeHost,remoteExecution:config.remoteExecution};
 const capacity=(await pool.query("SELECT value FROM settings WHERE id='capacity'")).rows[0].value;
 try{
   config.bridgeHost='fixture';config.remoteExecution=true;
   await pool.query("UPDATE settings SET value='{"+'"maxRuns":1,"browserSlots":1'+"}' WHERE id='capacity'");
   await pool.query("UPDATE accounts SET max_concurrency=12 WHERE id='glm'");
   const enabled=await goal(),other=await goal(false);
   await pool.query('UPDATE settings SET value=value||$2::jsonb WHERE id=$1',['workforce:'+enabled,
     JSON.stringify({glmHarnessEnabled:true,detachedHarnessEnabled:true,deliveryFirstEnabled:true,glmTarget:12})]);
   const diagnostic=await task(enabled,{accountId:'glm',model:'glm-5.3-flash',stage:'work',resources:[],
     execution:{kind:'harness',workforceLane:'glm',workKind:'diagnostic',workspace:'independent/diagnostic'}});
   const delivery=await task(enabled,{accountId:'glm',model:'glm-5.3-flash',stage:'integrate',resources:[],
     execution:{kind:'harness',workforceLane:'glm',workspace:'independent/delivery'}});
   const legacy=await task(other,{resources:[]});
   const results=await Promise.all([schedule('agentos-opencode'),schedule('agentos-opencode')]);
   expect(results.flat()).toHaveLength(1);
   expect((await pool.query('SELECT task_id,runtime FROM runs')).rows).toEqual([{task_id:delivery,runtime:'harness'}]);
   expect((await pool.query('SELECT state FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY id',[[diagnostic,legacy]])).rows.every(row=>row.state!=='queued')).toBe(true);
   expect((await pool.query('SELECT 1 FROM opencode_jobs')).rowCount).toBe(1);
   expect((await pool.query('SELECT 1 FROM outbox')).rowCount).toBe(0);
 }finally{
   Object.assign(config,previous);
   await pool.query("UPDATE settings SET value=$1 WHERE id='capacity'",[JSON.stringify(capacity)]);
 }
});

it('reserves Sol for coordination and sends native work only to the configured GLM account', () => {
  expect(chooseWorkforceRoute({ stage: 'coordinate' }, accounts(), policy)).toEqual({
    accountId: 'codex',
    model: 'gpt-6-sol',
  });
  expect(
    chooseWorkforceRoute({ model: 'gpt-6-sol', execution: { kind: 'harness' } }, accounts(), policy)
      ?.accountId,
  ).toBe('qwen');
  expect(chooseWorkforceRoute({ execution: { kind: 'opencode' } }, accounts(), policy)).toEqual({
    accountId: 'glm',
    model: 'glm-5.3-flash',
  });
  expect(
    chooseWorkforceRoute(
      { execution: { kind: 'harness', workforceLane: 'glm' } },
      accounts(),
      policy,
    ),
  ).toBeNull();
});

it('keeps AGY limited to supported review work and fails over Kimi/Qwen on quota or auth', () => {
  expect(
    chooseWorkforceRoute(
      { execution: { kind: 'harness', workforceLane: 'agy' } },
      accounts(),
      policy,
    ),
  ).toBeNull();
  expect(
    chooseWorkforceRoute({ execution: { workforceLane: 'agy' } }, accounts(), policy)?.accountId,
  ).toBe('agy');
  const list = accounts();
  list[2].connection_status = 'reconnect_required';
  expect(
    chooseWorkforceRoute({ execution: { kind: 'harness', workforceLane: 'kimi' } }, list, policy)
      ?.accountId,
  ).toBe('qwen');
  list[1].quota = { ordinaryUsageAllowed: false };
  expect(
    chooseWorkforceRoute({ execution: { kind: 'harness', workforceLane: 'kimi' } }, list, policy),
  ).toBeNull();
});

it('evaluates AGY quota by selected model rather than account default and respects cooldowns and permissions', () => {
  const agy = account('agy', 'agy', 'claude-sonnet', {
    models: ['claude-sonnet', 'gemini-3.8-flash-high'],
    quota: {
      status: 'ok',
      groups: [
        { label: 'Claude e GPT', windows: [{ remainingPercent: 0 }] },
        { label: 'Gemini', windows: [{ remainingPercent: 50 }] },
      ],
    },
  });
  const task = {
    model: 'gemini-3.8-flash-high',
    allowed_accounts: ['agy'],
    execution: { workforceLane: 'agy' },
  };
  expect(chooseWorkforceRoute(task, [agy], policy)?.model).toBe('gemini-3.8-flash-high');
  expect(chooseWorkforceRoute({ ...task, allowed_accounts: ['other'] }, [agy], policy)).toBeNull();
  expect(
    chooseWorkforceRoute(
      {
        ...task,
        execution: {
          ...task.execution,
          continuationRoutes: [{ accountId: 'agy', model: task.model, until: '2030-01-01' }],
        },
      },
      [agy],
      policy,
      Date.parse('2026-10-05'),
    ),
  ).toBeNull();
  expect(
    chooseWorkforceRoute(
      { execution: { kind: 'opencode' } },
      [account('glm', 'openai', 'glm-5.3-flash', { allow_unknown: false })],
      policy,
    ),
  ).toBeNull();
});

it('routes the explicit Luna lane only to authorized Luna accounts and never substitutes Sol', () => {
  const lunaPolicy = { ...policy, lunaTarget: 2, lunaAccounts: ['codex', 'codex-2'] };
  const list = [
    ...accounts().filter((a) => a.id !== 'codex'),
    account('codex', 'codex', 'gpt-6-sol', { models: ['gpt-6-sol', 'gpt-6-luna'] }),
    account('codex-2', 'codex', 'gpt-6-sol', { models: ['gpt-6-sol', 'gpt-6-luna'] }),
    account('codex-unapproved', 'codex', 'gpt-6-luna'),
  ];
  const worker = {
    account_id: 'codex',
    model: 'gpt-6-luna',
    execution: { kind: 'harness', workforceLane: 'luna' },
  };
  expect(chooseWorkforceRoute(worker, list, lunaPolicy)).toEqual({
    accountId: 'codex',
    model: 'gpt-6-luna',
  });
  list.find((a) => a.id === 'codex')!.quota = { ordinaryUsageAllowed: false };
  expect(chooseWorkforceRoute(worker, list, lunaPolicy)).toEqual({
    accountId: 'codex-2',
    model: 'gpt-6-luna',
  });
  expect(
    chooseWorkforceRoute({ ...worker, allowed_accounts: ['codex'] }, list, lunaPolicy),
  ).toBeNull();
  list.find((a) => a.id === 'codex-2')!.models = ['gpt-6-sol'];
  expect(chooseWorkforceRoute(worker, list, lunaPolicy)).toBeNull();
  expect(chooseWorkforceRoute(worker, list, policy)).toBeNull();
  expect(chooseWorkforceRoute(worker, list, { ...lunaPolicy, lunaTarget: 0 })).toBeNull();
  expect(
    chooseWorkforceRoute({ model: 'gpt-6-sol', execution: { kind: 'harness' } }, list, lunaPolicy)
      ?.accountId,
  ).toBe('qwen');
});

it('preserves the Luna model during continuation route cooldowns', () => {
  const lunaPolicy = { ...policy, lunaTarget: 2, lunaAccounts: ['codex', 'codex-2'] };
  const list = ['codex', 'codex-2'].map((id) =>
    account(id, 'codex', 'gpt-6-sol', { models: ['gpt-6-sol', 'gpt-6-luna'] }),
  );
  const worker = {
    account_id: 'codex',
    model: 'gpt-6-luna',
    execution: {
      kind: 'harness',
      workforceLane: 'luna',
      continuationRoutes: [{ accountId: 'codex', model: 'gpt-6-luna', until: '2030-01-01' }],
    },
  };
  expect(chooseWorkforceRoute(worker, list, lunaPolicy, Date.parse('2026-10-05'))).toEqual({
    accountId: 'codex-2',
    model: 'gpt-6-luna',
  });
  expect(chooseWorkforceRoute(worker, list, lunaPolicy, Date.parse('2030-01-02'))).toEqual({
    accountId: 'codex',
    model: 'gpt-6-luna',
  });
});

it('uses optional Luna fallback only after authorized Kimi and Qwen routes are unavailable', () => {
  const fallbackPolicy = {
    ...policy,
    lunaTarget: 2,
    lunaAccounts: ['codex'],
    lunaFallback: true,
  };
  const list = [
    ...accounts().filter((a) => a.id !== 'codex'),
    account('codex', 'codex', 'gpt-6-sol', { models: ['gpt-6-sol', 'gpt-6-luna'] }),
  ];
  const worker = {
    account_id: 'codex',
    model: 'gpt-6-luna',
    allowed_accounts: ['codex', 'kimi', 'qwen'],
    execution: { kind: 'harness', workforceLane: 'kimi' },
  };
  expect(chooseWorkforceRoute(worker, list, fallbackPolicy)?.accountId).toBe('kimi');
  list.find((a) => a.id === 'kimi')!.quota = { ordinaryUsageAllowed: false };
  expect(chooseWorkforceRoute(worker, list, fallbackPolicy)?.accountId).toBe('qwen');
  list.find((a) => a.id === 'qwen')!.connection_status = 'reconnect_required';
  expect(chooseWorkforceRoute(worker, list, fallbackPolicy)).toEqual({
    accountId: 'codex',
    model: 'gpt-6-luna',
  });
  expect(
    chooseWorkforceRoute({ ...worker, execution: { kind: 'harness' } }, list, fallbackPolicy),
  ).toEqual({ accountId: 'codex', model: 'gpt-6-luna' });
  expect(
    chooseWorkforceRoute({ ...worker, allowed_accounts: [] }, list, fallbackPolicy),
  ).toBeNull();
  expect(chooseWorkforceRoute(worker, list, { ...fallbackPolicy, lunaFallback: false })).toBeNull();
  expect(
    chooseWorkforceRoute({ ...worker, execution: { workforceLane: 'agy' } }, list, fallbackPolicy),
  ).toBeNull();
  list.find((a) => a.id === 'codex')!.quota = { ordinaryUsageAllowed: false };
  expect(chooseWorkforceRoute(worker, list, fallbackPolicy)).toBeNull();
});

it('lets the scheduler defer Luna at capacity without aborting the transaction or blocking unrelated lanes', () => {
  const lunaPolicy = { ...policy, lunaTarget: 2, lunaAccounts: ['codex'] };
  const worker = {
    goal_id: 'goal',
    model: 'gpt-6-luna',
    execution: { kind: 'harness', workforceLane: 'luna' },
  };
  const running = [
    { state: 'running', model: 'gpt-6-luna', goal_id: 'goal' },
    { state: 'reconciling', model: 'gpt-6-luna', goal_id: 'goal' },
    { state: 'submitted', model: 'gpt-6-luna', goal_id: 'goal' },
    { state: 'running', model: 'gpt-6-luna', goal_id: 'other-goal' },
  ];
  expect(workforceCapacityReason(worker, running, lunaPolicy)).toBe('WORKFORCE_LUNA_CAPACITY');
  expect(workforceCapacityReason(worker, running.slice(1), lunaPolicy)).toBeNull();
  expect(workforceCapacityReason(worker, running, policy)).toBeNull();
  expect(
    workforceCapacityReason({ model: 'deepseek-v4-flash-0731' }, running, lunaPolicy),
  ).toBeNull();
  const coordinator = { stage: 'coordinate', model: 'gpt-6-sol' };
  expect(workforceCapacityReason(coordinator, running, lunaPolicy)).toBeNull();
  expect(
    workforceCapacityReason(
      coordinator,
      [...running, { state: 'queued', model: 'gpt-6-sol' }],
      lunaPolicy,
    ),
  ).toBe('WORKFORCE_COORDINATOR_BUSY');
});

async function goal(enabled = true) {
  const id = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'test','test','workforce-test',$1::text)",
    [id],
  );
  if (enabled)
    await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)', [
      `workforce:${id}`,
      JSON.stringify(policy),
    ]);
  return id;
}
async function task(goalId: string, options: any = {}) {
  const id = randomUUID();
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,stage,criteria,resources,execution)
   VALUES($1::uuid,$2,$1::text,'test','test','qa.backend',$3,$4,$5,'[]',$6,$7)`,
    [
      id,
      goalId,
      options.accountId ?? 'qwen',
      options.model ?? 'deepseek-v4-flash-0731',
      options.stage ?? 'implement',
      JSON.stringify(
        options.resources ?? [
          'remote-slot',
          `workspace:isolated/${id}`,
          'browser',
          'workstream:game',
        ],
      ),
      options.execution === null
        ? null
        : JSON.stringify(
            options.execution ?? {
              kind: 'harness',
              workspace: `isolated/${id}`,
              baseWorkspace: 'game',
            },
          ),
    ],
  );
  return id;
}
async function run(taskId: string, options: any = {}, client = pool) {
  const id = randomUUID();
  await client.query(
    `INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state)
   SELECT $1,id,1,$2,COALESCE($3,account_id),$4,$5,1,$6 FROM tasks WHERE id=$7`,
    [
      id,
      randomUUID(),
      options.accountId ?? null,
      options.model ?? null,
      options.runtime ?? 'deepseek-harness',
      options.state ?? 'queued',
      taskId,
    ],
  );
  return id;
}

it('removes only workstream locks for enabled goals and adds coordinator resource with null execution', async () => {
  const enabled = await goal(),
    disabled = await goal(false);
  await pool.query(
    "INSERT INTO settings(id,value) VALUES('exclusive-workstreams:workforce-test','{\"enabled\":true}') ON CONFLICT(id) DO UPDATE SET value=excluded.value",
  );
  const worker = await task(enabled),
    legacy = await task(disabled);
  const coordinator = await task(enabled, {
    stage: 'coordinate',
    accountId: 'codex',
    model: 'gpt-6-sol',
    execution: null,
    resources: [],
  });
  expect(
    (await pool.query('SELECT resources FROM tasks WHERE id=$1', [worker])).rows[0].resources,
  ).toEqual(['remote-slot', `workspace:isolated/${worker}`, 'browser']);
  expect(
    (await pool.query('SELECT resources FROM tasks WHERE id=$1', [legacy])).rows[0].resources,
  ).toContain('workstream:game');
  expect(
    (await pool.query('SELECT resources FROM tasks WHERE id=$1', [coordinator])).rows[0].resources,
  ).toEqual([`coordination:${enabled}`]);
  await run(worker);
  await run(await task(enabled)); // separate isolated copy of same logical game is allowed
  await expect(run(legacy)).rejects.toThrow('WORKSTREAM_BUSY');
});

it('blocks worker Sol including legacy null model inserts, without changing unrelated goals', async () => {
  const enabled = await goal();
  const worker = await task(enabled, { accountId: 'codex', model: 'gpt-6-sol' });
  await expect(run(worker)).rejects.toThrow('WORKFORCE_SOL_RESERVED');
  const explicit = await task(enabled);
  await expect(run(explicit, { accountId: 'codex', model: 'gpt-6-sol' })).rejects.toThrow(
    'WORKFORCE_SOL_RESERVED',
  );
  const isolated = await goal(false);
  const legacy = await task(isolated, { accountId: 'codex', model: 'gpt-6-sol', execution: null });
  await expect(run(legacy)).resolves.toBeTruthy();
});

it('serializes concurrent coordinator admissions and permits the next only after termination', async () => {
  const goalId = await goal();
  const tasks = await Promise.all(
    [0, 1].map(() =>
      task(goalId, {
        stage: 'coordinate',
        accountId: 'codex',
        model: 'gpt-6-sol',
        execution: null,
      }),
    ),
  );
  const results = await Promise.allSettled(tasks.map((id) => run(id)));
  expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
  const rejected = results.findIndex((result) => result.status === 'rejected');
  expect(String((results[rejected] as PromiseRejectedResult).reason)).toContain(
    'WORKFORCE_COORDINATOR_BUSY',
  );
  await pool.query("UPDATE runs SET state='submitted' WHERE task_id=ANY($1::uuid[])", [tasks]);
  await expect(run(tasks[rejected])).resolves.toBeTruthy();
});

it('routes native outbox delivery exactly once to OpenCode and keeps ordinary Harness delivery', async () => {
  const goalId = await goal();
  const native = await task(goalId, {
    accountId: 'glm',
    model: 'glm-5.3-flash',
    execution: { kind: 'opencode', workspace: 'isolated/native' },
  });
  const nativeRun = await run(native);
  await pool.query('INSERT INTO outbox(run_id) VALUES($1) ON CONFLICT DO NOTHING', [nativeRun]);
  await pool.query('INSERT INTO outbox(run_id) VALUES($1) ON CONFLICT DO NOTHING', [nativeRun]);
  expect(
    (await pool.query('SELECT * FROM opencode_jobs WHERE run_id=$1', [nativeRun])).rowCount,
  ).toBe(1);
  expect((await pool.query('SELECT * FROM outbox WHERE run_id=$1', [nativeRun])).rowCount).toBe(0);
  expect(
    (await pool.query('SELECT runtime,model FROM runs WHERE id=$1', [nativeRun])).rows[0],
  ).toMatchObject({ runtime: 'opencode', model: 'glm-5.3-flash' });
  const harnessRun = await run(await task(goalId));
  await pool.query('INSERT INTO outbox(run_id) VALUES($1)', [harnessRun]);
  expect((await pool.query('SELECT * FROM outbox WHERE run_id=$1', [harnessRun])).rowCount).toBe(1);
});

it('caps native concurrency at the configured target even during retries or mixed runtime labels', async () => {
  const goalId = await goal();
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{glmTarget}','2') WHERE id=$1", [
    `workforce:${goalId}`,
  ]);
  const ids = await Promise.all(
    [0, 1, 2].map((n) =>
      task(goalId, {
        accountId: 'glm',
        model: 'glm-5.3-flash',
        execution: { kind: 'opencode', workspace: `isolated/native-${n}` },
      }),
    ),
  );
  const results = await Promise.allSettled(ids.map((id) => run(id)));
  expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(2);
  expect(
    String(
      (results.find((result) => result.status === 'rejected') as PromiseRejectedResult).reason,
    ),
  ).toContain('WORKFORCE_GLM_CAPACITY');
  const completed = results.find(
    (result) => result.status === 'fulfilled',
  ) as PromiseFulfilledResult<string>;
  await pool.query("UPDATE runs SET state='submitted' WHERE id=$1", [completed.value]);
  const rejectedIndex = results.findIndex((result) => result.status === 'rejected');
  await expect(run(ids[rejectedIndex])).resolves.toBeTruthy();
});

it('serializes mixed Harness and OpenCode GLM admissions under one session target without native dispatching Harness', async () => {
  const goalId = await goal();
  await pool.query('UPDATE settings SET value=$2 WHERE id=$1',[
    `workforce:${goalId}`,JSON.stringify({...policy,glmTarget:2,glmHarnessEnabled:true})]);
  const ids = await Promise.all(['opencode','harness','harness'].map((kind,index)=>task(goalId,{
    accountId:'glm',model:'glm-5.3-flash',execution:{kind,workforceLane:'glm',workspace:`isolated/mixed-${index}`}})));
  const nativeRun = await run(ids[0]);
  const results = await Promise.allSettled(ids.slice(1).map(id=>run(id)));
  expect(results.filter(r=>r.status==='fulfilled')).toHaveLength(1);
  const rejectedIndex=results.findIndex(r=>r.status==='rejected');
  expect(String((results[rejectedIndex] as PromiseRejectedResult).reason)).toContain('WORKFORCE_GLM_CAPACITY');
  await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[nativeRun]);
  const admitted=await run(ids[rejectedIndex+1]);
  await pool.query('INSERT INTO outbox(run_id) VALUES($1)',[admitted]);
  expect((await pool.query('SELECT runtime FROM runs WHERE id=$1',[admitted])).rows[0].runtime).toBe('deepseek-harness');
  expect((await pool.query('SELECT 1 FROM opencode_jobs WHERE run_id=$1',[admitted])).rowCount).toBe(0);
  expect(workforceCapacityReason({goal_id:goalId,model:'glm-5.3-flash',execution:{kind:'harness',workforceLane:'glm'}},
    [{goal_id:goalId,state:'running',model:'glm-5.3-flash',runtime:'harness'},
      {goal_id:goalId,state:'reconciling',model:'glm-5.3-flash',runtime:'opencode'}],{...policy,glmTarget:2})).toBe('WORKFORCE_GLM_CAPACITY');
});

it('admits only two Luna workers concurrently and reserves their accounts without affecting old policies', async () => {
  const goalId = await goal();
  await pool.query('UPDATE settings SET value=$2 WHERE id=$1', [
    `workforce:${goalId}`,
    JSON.stringify({ ...policy, lunaTarget: 2, lunaAccounts: ['codex'] }),
  ]);
  const taskIds = await Promise.all(
    [0, 1, 2].map(() =>
      task(goalId, {
        accountId: 'codex',
        model: 'gpt-6-luna',
        execution: { kind: 'harness', workforceLane: 'luna' },
      }),
    ),
  );
  const results = await Promise.allSettled(taskIds.map((id) => run(id)));
  expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(2);
  const rejectedIndex = results.findIndex((result) => result.status === 'rejected');
  expect(String((results[rejectedIndex] as PromiseRejectedResult).reason)).toContain(
    'WORKFORCE_LUNA_CAPACITY',
  );
  const admitted = results.filter((result) => result.status === 'fulfilled');
  await pool.query("UPDATE runs SET state='running' WHERE id=$1", [admitted[0].value]);
  await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1", [admitted[1].value]);
  await expect(run(taskIds[rejectedIndex])).rejects.toThrow('WORKFORCE_LUNA_CAPACITY');
  await pool.query("UPDATE runs SET state='submitted' WHERE id=$1", [admitted[0].value]);
  await expect(run(taskIds[rejectedIndex])).resolves.toBeTruthy();
  const unauthorized = await task(goalId, { accountId: 'qwen', model: 'gpt-6-luna' });
  await expect(run(unauthorized)).rejects.toThrow('WORKFORCE_LUNA_ACCOUNT');
  const legacy = await goal();
  const legacyTask = await task(legacy, { accountId: 'codex', model: 'gpt-6-luna' });
  await expect(run(legacyTask)).resolves.toBeTruthy();
});

it('schedules two Luna and another provider while a third Luna waits without transaction rollback', async () => {
  const previous = { bridgeHost: config.bridgeHost, remoteExecution: config.remoteExecution };
  const capacity = (await pool.query("SELECT value FROM settings WHERE id='capacity'")).rows[0]
    .value;
  config.bridgeHost = 'test-only.invalid';
  config.remoteExecution = true;
  try {
    const goalId = await goal();
    await pool.query('UPDATE settings SET value=$2 WHERE id=$1', [
      `workforce:${goalId}`,
      JSON.stringify({ ...policy, lunaTarget: 2, lunaAccounts: ['codex'] }),
    ]);
    await pool.query(
      "UPDATE settings SET value='{" + '"maxRuns":8,"browserSlots":8' + "}' WHERE id='capacity'",
    );
    await pool.query('UPDATE accounts SET max_concurrency=8');
    await pool.query(
      'UPDATE accounts SET models=\'["gpt-6-sol","gpt-6-luna"]\' WHERE id=\'codex\'',
    );
    const lunaTasks: string[] = [];
    for (let index = 0; index < 3; index++)
      lunaTasks.push(
        await task(goalId, {
          accountId: 'codex',
          model: 'gpt-6-luna',
          resources: [],
          execution: { kind: 'harness', workforceLane: 'luna' },
        }),
      );
    await pool.query('UPDATE tasks SET priority=100 WHERE id=ANY($1::uuid[])', [lunaTasks]);
    const qwenTask = await task(goalId, { resources: [] });
    expect(await schedule()).toHaveLength(3);
    const all = (await pool.query('SELECT id,state,reason FROM tasks ORDER BY created_at')).rows;
    expect(
      all.filter((item) => lunaTasks.includes(item.id) && item.state === 'queued'),
    ).toHaveLength(2);
    const waiting = all.find(
      (item) => lunaTasks.includes(item.id) && item.state === 'waiting_resource',
    );
    expect(waiting?.reason).toContain('Luna');
    expect(all.find((item) => item.id === qwenTask)?.state).toBe('queued');
    expect((await pool.query('SELECT count(*)::int AS n FROM outbox')).rows[0].n).toBe(3);
    expect((await pool.query('SELECT count(*)::int AS n FROM runs')).rows[0].n).toBe(3);
    expect(await schedule()).toHaveLength(0);
    const runningLuna = (await pool.query("SELECT id FROM runs WHERE model='gpt-6-luna' LIMIT 1"))
      .rows[0];
    await pool.query("UPDATE runs SET state='submitted' WHERE id=$1", [runningLuna.id]);
    expect(await schedule()).toHaveLength(1);
    expect(
      (await pool.query('SELECT state FROM tasks WHERE id=$1', [waiting.id])).rows[0].state,
    ).toBe('queued');
  } finally {
    Object.assign(config, previous);
    await pool.query("UPDATE settings SET value=$1 WHERE id='capacity'", [
      JSON.stringify(capacity),
    ]);
  }
});
