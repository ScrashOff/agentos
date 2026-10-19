import { afterAll, beforeAll, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate, event } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { installBacklogPlanner } from '../server/backlog.js';
import { installSupervision, wakeSupervisors } from '../server/supervision.js';
import { installManagement } from '../server/management.js';
import { readManagementRequestPages } from '../server/management-retrieval.js';

beforeAll(async () => {
  await migrate();
  await seedRegistry();
  await installBacklogPlanner();
  await installSupervision();
  await installManagement();
  config.bridgeHost = 'test-only';
  config.remoteExecution = true;
});

afterAll(async () => {
  config.bridgeHost = '';
  config.remoteExecution = false;
  await pool.end();
});

it('freezes the focal live-failure contract and exact guide identity through the real supervisor wake path', async () => {
  const goal = randomUUID();
  const liveTask = randomUUID();
  const priorTask = randomUUID();
  const priorRun = randomUUID();
  const priorCase = randomUUID();
  const coordinator = randomUUID();
  const runId = randomUUID();
  const runFence = randomUUID();
  const contextVersion = randomUUID();
  const sol = 'sol_' + randomUUID();
  const muse = 'muse_' + randomUUID();
  const glm = 'glm_' + randomUUID();
  const criteria = [{ id: 'bounded', label: 'Preserve the worker contract', type: 'artifact', name: 'result.json' }];

  for (const [id, provider, model] of [
    [sol, 'codex', 'gpt-6.1-sol'],
    [muse, 'muse', 'muse-spark-1.3'],
    [glm, 'openai', 'glm-5.3-flash'],
  ]) {
    await pool.query(
      'INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id) VALUES($1,$1,$2,$3,$4,true,true,12,$1)',
      [id, provider, model, JSON.stringify([model])],
    );
  }

  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Focused live guidance','Preserve one live actor and make a bounded advisory decision.','manager-guidance-contract',$2::text)",
    [goal, goal],
  );
  const workforce = {
    enabled: true,
    glmAccountId: glm,
    glmTarget: 12,
    museTarget: 6,
    workerAccounts: [muse],
    coordinatorAccountId: sol,
    coordinatorModel: 'gpt-6.1-sol',
  };
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2),($3,$4),($5,$6)', [
    'workforce:' + goal,
    JSON.stringify(workforce),
    'supervision:' + goal,
    JSON.stringify({ enabled: true }),
    'management:' + goal,
    JSON.stringify({ enabled: true, intervalSeconds: 120, enabledAt: new Date(0).toISOString() }),
  ]);

  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count)
     VALUES($1,$2,'live-focus','Live worker','Continue the original bounded task after inspecting its concrete tool failure.\nCONSTRAINTS:\nWAIT: retain the current process until its durable receipt is observed. This is a frozen worker requirement.','qa.backend','running','work',$3,'muse-spark-1.3',$4,$5,1),
           ($6,$2,'manager','Coordinator','Review only the current manager focus.','qa.backend','accepted','coordinate',$7,'gpt-6.1-sol',$8,NULL,0),
           ($9,$2,'prior-case','Prior stopped task','Keep this older stopped case available for its own review.','qa.backend','failed','work',$3,'muse-spark-1.3',$4,$5,1)`,
    [
      liveTask,
      goal,
      muse,
      JSON.stringify(criteria),
      JSON.stringify({ kind: 'harness', profile: 'code', workspace: 'manager-guidance/live', baseWorkspace: 'manager-guidance' }),
      coordinator,
      sol,
      JSON.stringify([{ id: 'coordination', label: 'Review', type: 'artifact', name: 'coordination.json' }]),
      priorTask,
    ],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,summary,finished_at) VALUES($1,$2,1,$3,'harness',1,'failed','MODEL_TIMEOUT','Prior stopped case evidence',now())",
    [priorRun, priorTask, randomUUID()],
  );
  await pool.query(
    "INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,request_key) VALUES($1,$2,$3,$4,'open','Separate prior stopped case.','[]','')",
    [priorCase, goal, priorTask, priorRun],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,started_at) VALUES($1,$2,1,$3,'harness',1,'running',now())",
    [runId, liveTask, runFence],
  );
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','{}')", [contextVersion, runId]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [runId, contextVersion]);
  const identity = { goalId: goal, taskId: liveTask, runId, runFence, contextVersion };
  await event(pool, 'RUNTIME_GUIDANCE_AVAILABLE', liveTask, identity);
  const failureId = (
    await pool.query(
      "INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_TOOL_FAILURE',$1,$2) RETURNING id::text",
      [liveTask, JSON.stringify({ ...identity, callId: 'call_test_bounded', toolName: 'read', category: 'missing_resource', semanticCauseDigest: 'a'.repeat(64) })],
    )
  ).rows[0].id;

  await wakeSupervisors();

  const manager = (await pool.query('SELECT state,description FROM tasks WHERE id=$1', [coordinator])).rows[0];
  expect(manager.state).toBe('ready');
  expect(manager.description).toContain('LIVE_FAILURE_REVIEW_V1');
  expect(manager.description).toContain('RUNTIME_GUIDANCE_V1');
  expect(manager.description).toContain('consumo exige claim tipado canônico do SDK ligado à identidade completa');
  expect(manager.description).toContain('Recibos proposed/queued não comprovam consumo nem efeito');
  expect(manager.description).not.toContain('inbox-splice comprova consumo');
  expect(manager.description).toContain('kind:"guide"');
  expect(manager.description).toContain('liveFailureDisposition');
  expect(manager.description).toContain('Nonfocused cases and older work remain persisted');
  expect(manager.description).toContain('"kind":"live_failure"');
  expect(manager.description).toContain('"guidanceAvailable"');
  expect(manager.description).toContain(`"taskId":"${liveTask}"`);
  expect(manager.description).toContain(`"runId":"${runId}"`);
  expect(manager.description).toContain(`"runFence":"${runFence}"`);
  expect(manager.description).toContain(`"contextVersion":"${contextVersion}"`);
  expect(manager.description).toContain(`"eventId":"${failureId}"`);
  const snapshotStart = manager.description.lastIndexOf('\n{"cases":');
  expect(snapshotStart).toBeGreaterThanOrEqual(0);
  const snapshot = JSON.parse(manager.description.slice(snapshotStart + 1));
  expect(snapshot.operation.focus).toMatchObject({ kind: 'live_failure', taskId: liveTask, runId, runFence, contextVersion });
  expect(snapshot.cases).toHaveLength(1);
  expect(snapshot.cases[0]).toMatchObject({ id: priorCase, taskId: priorTask });
  expect(snapshot.operation.requiredDecision ?? null).toBeNull();
  const focal = snapshot.operation.tasks.find((task: any) => task.id === liveTask);
  expect(focal.intent.liveInstructionReference.request).toMatchObject({
    kind: 'live_task_instruction', goalId: goal, taskId: liveTask, expectedAttempt: 1, runId, runFence, contextVersion, offset: 0,
  });

  // A mutable task description can change after wake; the reader must still
  // return only the run's frozen snapshot and must not consult workspace data.
  await pool.query("UPDATE tasks SET description='FUTURE DESCRIPTION MUST NOT LEAK' WHERE id=$1", [liveTask]);
  const artifactReads: string[] = [];
  const client = await pool.connect();
  let read: any;
  try {
    await client.query('BEGIN READ ONLY');
    read = await readManagementRequestPages(client as any, goal, async (id: string) => {
      artifactReads.push(id); throw new Error('artifact reader must not be called');
    }, [focal.intent.liveInstructionReference.request]);
    await client.query('ROLLBACK');
  } finally { client.release(); }
  expect(read.results[0]).toMatchObject({ status: 'OK' });
  expect(read.results[0].text).toContain('WAIT: retain the current process');
  expect(read.results[0].text).not.toContain('FUTURE DESCRIPTION');
  expect(artifactReads).toEqual([]);

  const request = focal.intent.liveInstructionReference.request;
  const stale = await readManagementRequestPages(pool as any, goal, async () => { throw new Error('unexpected artifact read'); }, [
    { ...request, expectedAttempt: 2 },
    { ...request, runFence: randomUUID() },
    { ...request, contextVersion: randomUUID() },
    { ...request, goalId: randomUUID() },
  ]);
  const liveSource = await readManagementRequestPages(pool as any, goal, async () => { throw new Error('unexpected artifact read'); }, [
    { kind: 'task', taskId: liveTask, expectedAttempt: 1, offset: 0 },
    { kind: 'task_metadata', taskId: liveTask, expectedAttempt: 1, offset: 0 },
    { kind: 'artifacts', taskId: liveTask, expectedAttempt: 1, offset: 0 },
    { kind: 'artifact', taskId: liveTask, expectedAttempt: 1, name: 'result.json', offset: 0 },
  ]);
  expect(stale.results.map((row: any) => row.status)).toEqual(['STALE_ATTEMPT', 'STALE_BINDING', 'STALE_BINDING', 'GOAL_MISMATCH']);
  expect(liveSource.results.map((row: any) => row.status)).toEqual(['LIVE_SOURCE', 'LIVE_SOURCE', 'LIVE_SOURCE', 'LIVE_SOURCE']);
  const page2 = await readManagementRequestPages(pool as any, goal, async () => { throw new Error('unexpected artifact read'); }, [
    { ...request, offset: 10, snapshotDigest: read.results[0].snapshotDigest },
  ]);
  expect(page2.results[0].status).toBe('OK');
  expect(page2.results[0].snapshotDigest).toBe(read.results[0].snapshotDigest);
  const badDigest = await readManagementRequestPages(pool as any, goal, async () => { throw new Error('unexpected artifact read'); }, [
    { ...request, offset: 10, snapshotDigest: '0'.repeat(64) },
  ]);
  expect(badDigest.results[0]).toMatchObject({ status: 'STALE_CONTEXT', nextOffset: 0 });
});
