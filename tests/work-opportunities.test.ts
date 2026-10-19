import { afterAll, afterEach, beforeAll, beforeEach, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installBacklogPlanner, applyBacklog } from '../server/backlog.js';
import { installManagement, managementSnapshot } from '../server/management.js';
import { accountBridge } from '../server/bridge.js';
import { completeRun, validateTask, writeArtifact } from '../server/service.js';
import {
  projectWorkOpportunities,
  workOpportunityCharacterLimit,
} from '../server/work-opportunities.js';

it('rotates omitted accepted sources between reviews without advertising live or unaccepted input',()=>{
  const consumer={id:'consumer',stage:'verify',state:'blocked_dependencies',dependencies:[],
    execution:{baseWorkspace:'research/source',workspace:'research/review'}};
  const sources=Array.from({length:20},(_,i)=>({id:'source-'+i,state:'accepted',stage:'work',
    attempt_count:1,last_run_attempt:1,last_run_state:'submitted',last_run_id:'run-'+i,
    acceptance_passed:true,execution:{baseWorkspace:'research/source',workspace:'research/s'+i}}));
  const first=projectWorkOpportunities([consumer,...sources],[],[],workOpportunityCharacterLimit,0);
  const next=projectWorkOpportunities([consumer,...sources,{...sources[0],id:'live-source',has_live_run:true}],[],[],workOpportunityCharacterLimit,6);
  expect(first.acceptedSources.map(s=>s.taskId)).not.toEqual(next.acceptedSources.map(s=>s.taskId));
  expect(next.acceptedSources.some(s=>s.taskId==='live-source')).toBe(false);
  expect(JSON.stringify(next).length).toBeLessThanOrEqual(workOpportunityCharacterLimit);
  expect(next.selection.total.acceptedSources).toBe(20);
});

beforeAll(async () => {
  await migrate();
  await installBacklogPlanner();
  await installManagement();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
  vi.spyOn(accountBridge, 'request').mockResolvedValue({});
});
afterEach(() => vi.restoreAllMocks());
afterAll(async () => {
  await pool.end();
});

async function fixture() {
  const goal = randomUUID(),
    writer = randomUUID(),
    consumer = randomUUID(),
    coordinator = randomUUID();
  const writerRun = randomUUID(),
    writerContext = randomUUID(),
    run = randomUUID();
  const base = 'opportunities/game';
  const criteria = [
    {
      id: 'passed',
      label: 'Complete independent acceptance',
      type: 'json_field',
      name: 'verification.json',
      field: 'passed',
      equals: true,
    },
    {
      id: 'package',
      label: 'Reproducible package',
      type: 'artifact',
      name: 'evidence-manifest.json',
    },
  ];
  const policy = {
    enabled: true,
    glmTarget: 12,
    glmAccountId: 'glm',
    glmRequestLimit: 6,
    workerAccounts: [],
    coordinatorAccountId: 'sol',
    coordinatorModel: 'gpt-6-sol',
  };
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Five-game pilot','Acceptance remains mandatory','opportunities',$2)",
    [goal, goal],
  );
  await pool.query('INSERT INTO settings VALUES($1,$2)', [
    'workforce:' + goal,
    JSON.stringify(policy),
  ]);
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency) VALUES('glm','GLM','openai-compatible','glm-5.3-flash','[\"glm-5.3-flash\"]',true,12)",
  );
  for (const [id, key, stage, state, attempt] of [
    [writer, 'live-integrator', 'integrate', 'running', 1],
    [consumer, 'future-qa', 'verify', 'blocked_dependencies', 0],
    [coordinator, 'coordinate', 'coordinate', 'accepted', 1],
  ] as const)
    await pool.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,stage,state,criteria,covers,execution,attempt_count)
      VALUES($1,$2,$3,$3,'Preserved instructions','qa.backend',$4,$5,$6,'["game-acceptance"]',$7,$8)`,
      [
        id,
        goal,
        key,
        stage,
        state,
        JSON.stringify(criteria),
        JSON.stringify({
          kind: 'harness',
          baseWorkspace: base,
          workspace: 'opportunities/' + key,
          readOnlySource: stage === 'verify',
        }),
        attempt,
      ],
    );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'running','harness')",
    [writerRun, writer, randomUUID()],
  );
  await pool.query(
    "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{\"preserved\":true}','Immutable live context')",
    [writerContext, writerRun],
  );
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [writerRun, writerContext]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'submitted','harness')",
    [run, coordinator, randomUUID()],
  );
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [consumer, writer]);
  const source = await acceptedSource(goal, 'accepted-native', base);
  return {
    goal,
    writer,
    consumer,
    coordinator,
    writerRun,
    writerContext,
    run,
    base,
    source,
    criteria,
    policy,
  };
}

async function acceptedSource(goal: string, key: string, base: string, report?: any) {
  const id = randomUUID(),
    run = randomUUID(),
    fence = randomUUID();
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,covers,execution,attempt_count)
    VALUES($1,$2,$3,$3,'Accepted source fixture','qa.backend','running','work',$4,'["game-acceptance"]',$5,1)`,
    [
      id,
      goal,
      key,
      JSON.stringify([
        {
          id: 'passed',
          label: 'Focal test passed',
          type: 'json_field',
          name: 'verification.json',
          field: 'passed',
          equals: true,
        },
      ]),
      JSON.stringify({
        kind: 'harness',
        baseWorkspace: base,
        workspace: 'opportunities/' + key,
        ...(report ? { workKind: 'diagnostic' } : {}),
      }),
    ],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'running','harness')",
    [run, id, fence],
  );
  const input = await writeArtifact(
    run,
    fence,
    'candidate.json',
    JSON.stringify({ source: key, files: [] }),
  );
  await writeArtifact(run, fence, 'verification.json', JSON.stringify({ passed: true }));
  const planningReport = report
    ? await writeArtifact(run, fence, 'protocol-report.json', JSON.stringify(report))
    : undefined;
  await completeRun(run, fence, 'Accepted focal source; final game gate remains pending', {});
  await validateTask(id);
  return { id, run, input, planningReport };
}

async function writerState(f: Awaited<ReturnType<typeof fixture>>) {
  return {
    task: (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.writer])).rows,
    run: (await pool.query('SELECT * FROM runs WHERE id=$1', [f.writerRun])).rows,
    context: (await pool.query('SELECT * FROM contexts WHERE id=$1', [f.writerContext])).rows,
    dependencies: (
      await pool.query('SELECT * FROM dependencies WHERE task_id=$1 ORDER BY depends_on', [
        f.writer,
      ])
    ).rows,
  };
}

it('exposes three distinct investigations for future QA while its integrator stays alive and unchanged', async () => {
  const f = await fixture();
  const before = await writerState(f);
  const snapshot = await managementSnapshot(pool, f.goal, f.policy);
  const consumer = snapshot.tasks.find((task: any) => task.id === f.consumer);
  expect(consumer.criteria).toEqual(f.criteria);
  expect(consumer.covers).toEqual(['game-acceptance']);
  expect(snapshot.workOpportunities.consumers).toContainEqual({
    taskId: f.consumer,
    workKinds: ['diagnostic'],
    sourceTaskIds: [f.source.id],
    sourceCount: 1,
  });
  expect(snapshot.workOpportunities.liveActors).toContainEqual(
    expect.objectContaining({ taskId: f.writer, runId: f.writerRun }),
  );
  expect(snapshot.workOpportunities.acceptedSources).toContainEqual(
    expect.objectContaining({
      taskId: f.source.id,
      runId: f.source.run,
      expectedAttempt: 1,
      acceptancePassed: true,
      inputs: expect.arrayContaining([
        expect.objectContaining({ artifactId: f.source.input.id, digest: f.source.input.digest }),
      ]),
    }),
  );
  const created = await applyBacklog(f.goal, f.run, {
    reason: 'Independent focal investigations can feed future QA during integration.',
    tasks: ['durable-replay', 'player-isolation', 'bonus-resume'].map((coverageKey) => ({
      coverageKey,
      title: 'Investigate ' + coverageKey,
      description:
        'Execute a distinct ' +
        coverageKey +
        ' control on the exact accepted source and preserve the evidence for future QA.',
      baseWorkspace: f.base,
      deliverToTaskId: f.consumer,
      sourceTaskId: f.source.id,
      workKind: 'diagnostic',
    })),
  });
  expect(created).toHaveLength(3);
  expect(await writerState(f)).toEqual(before);
  expect(
    (await pool.query('SELECT count(*)::int count FROM acceptances WHERE task_id=$1', [f.consumer]))
      .rows[0].count,
  ).toBe(0);
  const refreshed = await managementSnapshot(pool, f.goal, f.policy);
  expect(
    refreshed.workOpportunities.existingCoverage.map((entry: any) => entry.coverageKey).sort(),
  ).toEqual(['bonus-resume', 'durable-replay', 'player-isolation']);
  expect(
    refreshed.workOpportunities.consumers.every((entry: any) => entry.taskId !== f.writer),
  ).toBe(true);
  expect(accountBridge.request).toHaveBeenCalledTimes(3);
});

it('rejects live and queued consumers, cross-game or cyclic sources and stale or rejected acceptance inputs', async () => {
  const f = await fixture();
  const stale = await acceptedSource(f.goal, 'stale-source', f.base);
  const rejected = await acceptedSource(f.goal, 'rejected-source', f.base);
  const cyclic = await acceptedSource(f.goal, 'cyclic-source', f.base);
  const indirectCycle = await acceptedSource(f.goal, 'indirect-cycle-source', f.base);
  const foreign = await acceptedSource(f.goal, 'foreign-source', 'opportunities/another-game');
  const liveSource = await acceptedSource(f.goal, 'live-source', f.base);
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [stale.id]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,2,$3,1,'submitted','harness')",
    [randomUUID(), stale.id, randomUUID()],
  );
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at) VALUES($1,$2,$3,false,'[]','test',now()+interval '1 second')",
    [randomUUID(), rejected.id, rejected.run],
  );
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [cyclic.id, f.consumer]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [indirectCycle.id, f.coordinator]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.coordinator, f.consumer]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,2,$3,1,'running','harness')",
    [randomUUID(), liveSource.id, randomUUID()],
  );
  let snapshot = await managementSnapshot(pool, f.goal, f.policy);
  const opportunity = snapshot.workOpportunities.consumers.find(
    (entry: any) => entry.taskId === f.consumer,
  )!;
  expect(opportunity.sourceTaskIds).toEqual([f.source.id]);
  expect(opportunity.sourceTaskIds).not.toEqual(
    expect.arrayContaining([stale.id, rejected.id, cyclic.id, foreign.id, liveSource.id]),
  );
  expect(
    snapshot.workOpportunities.acceptedSources.some((entry: any) =>
      [stale.id, rejected.id, foreign.id, liveSource.id].includes(entry.taskId),
    ),
  ).toBe(false);
  await pool.query("UPDATE tasks SET state='queued' WHERE id=$1", [f.consumer]);
  snapshot = await managementSnapshot(pool, f.goal, f.policy);
  expect(
    snapshot.workOpportunities.consumers.some((entry: any) => entry.taskId === f.consumer),
  ).toBe(false);
  await pool.query(
    "UPDATE tasks SET state='blocked_dependencies',stage='integrate',execution=jsonb_set(execution,'{readOnlySource}','false') WHERE id=$1",
    [f.consumer],
  );
  snapshot = await managementSnapshot(pool, f.goal, f.policy);
  expect(
    snapshot.workOpportunities.consumers.find((entry: any) => entry.taskId === f.consumer)
      ?.workKinds,
  ).toEqual(['diagnostic', 'repair']);
});

it('bounds planning metadata without putting source descriptions, inputs or historical outputs into the prompt', () => {
  const consumer = {
    id: 'consumer',
    stage: 'verify',
    state: 'blocked_dependencies',
    execution: { baseWorkspace: 'test/game', workspace: 'test/qa' },
    dependencies: [],
  };
  const sources = Array.from({ length: 120 }, (_, index) => ({
    id: 'source-' + index,
    title: 'Accepted source',
    stage: 'work',
    state: 'accepted',
    attempt_count: 1,
    last_run_id: 'run-' + index,
    last_run_attempt: 1,
    last_run_state: 'submitted',
    acceptance_passed: true,
    execution: {
      baseWorkspace: 'test/game',
      workspace: 'test/source-' + index,
      inputs: [{ base64: 'PRIVATE_FULL_INPUT' }],
    },
    description: 'FULL_SOURCE_DESCRIPTION'.repeat(1000),
    summary: 'FULL_HISTORICAL_OUTPUT'.repeat(1000),
  }));
  const catalog = projectWorkOpportunities([consumer, ...sources], [], []);
  const serialized = JSON.stringify(catalog);
  expect(serialized.length).toBeLessThanOrEqual(workOpportunityCharacterLimit);
  expect(catalog.selection.total.acceptedSources).toBe(120);
  expect(catalog.selection.omitted.acceptedSources).toBe(114);
  expect(catalog.selection.truncated).toBe(true);
  expect(catalog.consumers[0].sourceCount).toBe(120);
  expect(serialized).not.toMatch(
    /FULL_SOURCE_DESCRIPTION|FULL_HISTORICAL_OUTPUT|PRIVATE_FULL_INPUT/,
  );
});

it('delivers real consumer intent and one causal accepted report per scope without changing live actors or accepting games', async () => {
  const f = await fixture();
  const before = await writerState(f);
  const expected: any[] = [];
  for (let index = 0; index < 5; index++) {
    const base = index === 0 ? f.base : 'opportunities/game-' + index;
    const consumer = index === 0 ? f.consumer : randomUUID();
    const description =
      'Independently validate native controls, wallet and bonus resume.\n\nREQUIREMENTS: ' +
      JSON.stringify([
        {
          id: 'game-acceptance',
          description:
            'Real20-cycle desktop/mobile, complete bonus then next spin, isolated sessions and original rules. Scope' +
            index,
        },
      ]) +
      '\n\nCONSTRAINTS: Preserve original rules; current resource ceilings are supplied separately.';
    if (index === 0)
      await pool.query('UPDATE tasks SET description=$2 WHERE id=$1', [consumer, description]);
    else
      await pool.query(
        `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,stage,state,criteria,covers,execution)
      VALUES($1,$2,$3,$3,$4,'qa.backend','verify','blocked_dependencies',$5,'["game-acceptance"]',$6)`,
        [
          consumer,
          f.goal,
          'qa-' + index,
          description,
          JSON.stringify(f.criteria),
          JSON.stringify({
            kind: 'harness',
            baseWorkspace: base,
            workspace: base + '-qa',
            readOnlySource: true,
          }),
        ],
      );
    const source = await acceptedSource(f.goal, 'causal-source-' + index, base, {
      suiteExecuted: true,
      exitCode: 1,
      passed: false,
      limitations: ['Accepted diagnostic; full game remains unapproved. '.repeat(60)],
      cases: [
        ...Array.from({ length: 12 }, (_, i) => ({
          name: 'setup-' + i,
          status: 'pass',
          details: 'setup '.repeat(100),
        })),
        {
          name: 'delivery-consumption-frontier',
          status: 'failed',
          actual: 'Current native obligation' + index + ' remains unresolved.',
        },
      ],
      finding: {
        nativeAliasUnderPt:
          'The source configuration exists, but locale consumption' + index + ' never occurred.',
      },
      headers: { authorization: 'PRIVATE_FIXTURE' },
    });
    await pool.query('INSERT INTO dependencies VALUES($1,$2)', [consumer, source.id]);
    expected.push({ consumer, source, base });
  }
  const snapshot = await managementSnapshot(pool, f.goal, f.policy);
  for (const { consumer, source, base } of expected) {
    const intent = snapshot.tasks.find((task: any) => task.id === consumer).intent;
    expect(JSON.parse(intent.text).requirements[0].description).toContain(
      'complete bonus then next spin',
    );
    const evidence = snapshot.evidence.find((entry: any) => entry.taskId === source.id);
    expect(evidence.artifactId).toBe(source.planningReport.id);
    expect(evidence.digest).toBe(source.planningReport.digest);
    const report = JSON.parse(evidence.text);
    expect(report.passed).toBe(false);
    expect(report.finding.nativeAliasUnderPt).toContain('never occurred');
    expect(report.cases[0].actual).toContain('remains unresolved');
    expect(evidence.role).toBe('accepted_diagnostic');
    expect(evidence.projection.arrayIndices['/cases'][0]).toBe(12);
    expect(
      snapshot.workOpportunities.acceptedSources.some(
        (entry: any) => entry.taskId === source.id && entry.baseWorkspace === base,
      ),
    ).toBe(true);
    expect(
      (await pool.query('SELECT count(*)::int count FROM acceptances WHERE task_id=$1', [consumer]))
        .rows[0].count,
    ).toBe(0);
  }
  expect(
    snapshot.evidence
      .map(
        (entry: any) =>
          JSON.stringify({
            text: entry.text,
            truncated: entry.truncated,
            projection: entry.projection,
          }).length,
      )
      .reduce((a: number, b: number) => a + b, 0),
  ).toBeLessThanOrEqual(18000);
  expect(
    snapshot.tasks
      .filter((task: any) => task.intent)
      .map((task: any) => JSON.stringify(task.intent).length)
      .reduce((a: number, b: number) => a + b, 0),
  ).toBeLessThanOrEqual(12000);
  expect(JSON.stringify(snapshot.evidence)).not.toContain('PRIVATE_FIXTURE');
  expect(await writerState(f)).toEqual(before);
});

it('keeps a current source and live actor visible per game before a large coverage history consumes its budget', () => {
  const tasks: any[] = [],
    coverage: any[] = [],
    inputs: any[] = [];
  for (let game = 0; game < 3; game++) {
    const base = 'test/game-' + game;
    tasks.push({
      id: 'qa-' + game,
      stage: 'verify',
      state: 'blocked_dependencies',
      dependencies: [],
      execution: { baseWorkspace: base, workspace: base + '-qa', readOnlySource: true },
    });
    tasks.push({
      id: 'writer-' + game,
      stage: 'integrate',
      state: 'running',
      has_live_run: true,
      last_run_id: 'writer-run-' + game,
      execution: { baseWorkspace: base, workspace: base + '-live' },
    });
    for (let index = 0; index < 40; index++) {
      const id = 'diagnostic-' + game + '-' + index,
        run = 'run-' + id;
      tasks.push({
        id,
        title: 'Distinct preserved diagnostic ' + index,
        state: 'accepted',
        stage: 'work',
        attempt_count: 1,
        last_run_id: run,
        last_run_attempt: 1,
        last_run_state: 'submitted',
        acceptance_passed: true,
        covers: ['game-' + game],
        execution: {
          workKind: 'diagnostic',
          baseWorkspace: base,
          workspace: base + '-diagnostic-' + index,
        },
      });
      coverage.push({ task_id: id, coverage_key: 'coverage-' + game + '-' + index });
      inputs.push({
        id: 'artifact-' + id,
        run_id: run,
        name: 'protocol-report.json',
        digest: 'a'.repeat(64),
        size: 2000,
      });
    }
  }
  const catalog = projectWorkOpportunities(tasks, coverage, inputs);
  for (let game = 0; game < 3; game++) {
    const consumer = catalog.consumers.find((entry) => entry.taskId === 'qa-' + game)!;
    expect(consumer.sourceCount).toBe(40);
    expect(consumer.sourceTaskIds.length).toBeGreaterThanOrEqual(1);
    expect(
      catalog.acceptedSources.some(
        (source) =>
          source.taskId === consumer.sourceTaskIds[0] &&
          source.baseWorkspace === 'test/game-' + game &&
          source.inputs[0].digest === 'a'.repeat(64),
      ),
    ).toBe(true);
    expect(catalog.liveActors.some((actor) => actor.taskId === 'writer-' + game)).toBe(true);
  }
  expect(catalog.selection.total.existingCoverage).toBe(120);
  expect(catalog.selection.omitted.existingCoverage).toBeGreaterThan(0);
  expect(catalog.selection.truncated).toBe(true);
  expect(JSON.stringify(catalog).length).toBeLessThanOrEqual(workOpportunityCharacterLimit);
});
