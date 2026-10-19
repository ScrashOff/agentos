import { beforeAll, beforeEach, afterAll, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { writeArtifact, completeRun, validateTask } from '../server/service.js';
import { recoverOpenCodeRun } from '../server/opencode-recovery.js';
import { resumeFailedHarness, ContinuationSupervisor } from '../server/continuation.js';
import {
  installSupervision,
  recordSupervisionDecisions,
  applySupervisionDecisions,
} from '../server/supervision.js';
import {
  requestSemanticReplan,
  semanticRetryCode,
  semanticEvidenceCode,
} from '../server/semantic-retry.js';
import { hash } from '../server/memory.js';

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await installSupervision();
  await pool.query(
    "CREATE TABLE IF NOT EXISTS opencode_jobs(run_id uuid PRIMARY KEY REFERENCES runs(id),state text DEFAULT 'queued',created_at timestamptz DEFAULT now(),started_at timestamptz,finished_at timestamptz)",
  );
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await seedRegistry();
  await pool.query(
    "UPDATE settings SET value='{\"maxRuns\":5,\"browserSlots\":2}' WHERE id IN('capacity','remote-capacity')",
  );
  await pool.query(
    "INSERT INTO settings VALUES('autonomy:semantic-test','{\"enabled\":true}') ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",
  );
});
afterAll(async () => {
  await pool.end();
});

async function fixture(
  runtime = 'opencode',
  report: string | null = JSON.stringify({
    fixVerified: false,
    diagnosis: 'The proposed hypothesis was refuted; no behavioral repair was made.',
  }),
) {
  const goal = randomUUID(),
    task = randomUUID(),
    run = randomUUID(),
    fence = randomUUID(),
    coordinator = randomUUID();
  const workspace = 'semantic-test/' + task;
  const criteria = [
    {
      id: 'repair_verified',
      label: 'Actual repair',
      type: 'json_field',
      name: 'repair-report.json',
      field: 'fixVerified',
      equals: true,
    },
    {
      id: 'protocol',
      label: 'Executed protocol',
      type: 'json_field',
      name: 'verification.json',
      field: 'passed',
      equals: true,
    },
  ];
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown) VALUES('semantic-glm','test','openai','glm','[\"glm\"]',true,5,true)",
  );
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Semantic retry','Preserve repair and final QA contracts','semantic-test',$2)",
    [goal, goal],
  );
  await pool.query('INSERT INTO settings VALUES($1,\'{"enabled":true}\')', ['supervision:' + goal]);
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,criteria,execution,resources,attempt_count,max_attempts,recovery)
    VALUES($1,$2,'repair','Repair','Exact authorized requirements and original permissions','qa.backend','semantic-glm','glm','running','integrate',$3,$4,$5,1,1,'{"mode":"manual"}')`,
    [
      task,
      goal,
      JSON.stringify(criteria),
      JSON.stringify({
        kind: runtime,
        workspace,
        baseWorkspace: 'semantic-test/game',
        workKind: 'repair',
      }),
      JSON.stringify(['remote-slot', 'workspace:' + workspace]),
    ],
  );
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,criteria,attempt_count)
    VALUES($1,$2,'coordinate','Supervisor','Authorized automatic supervisor','qa.backend','semantic-glm','glm','pending','coordinate',$3,0)`,
    [
      coordinator,
      goal,
      JSON.stringify([{ id: 'coord', label: 'Plan', type: 'artifact', name: 'coordination.json' }]),
    ],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state) VALUES($1,$2,1,$3,'semantic-glm','glm',$4,1,'running')",
    [run, task, fence, runtime],
  );
  const context = randomUUID();
  await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)', [
    context,
    run,
    JSON.stringify({ version: 1, taskId: task, immutableProbe: true }),
    'Original immutable requirements and contextual evidence.',
  ]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [run, context]);
  const artifact =
    report === null ? null : await writeArtifact(run, fence, 'repair-report.json', report);
  await writeArtifact(run, fence, 'verification.json', JSON.stringify({ passed: true }));
  await completeRun(run, fence, 'Negative diagnostic delivery, not a verified repair');
  await validateTask(task);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [goal]);
  const proof = () => ({
    runId: run,
    runtime,
    workspace,
    checkedAt: new Date().toISOString(),
    state: {},
    candidate: { patch: '', files: [] },
    process: {
      unit: 'agentos-run-' + run,
      activeState: 'inactive',
      mainPid: 0,
      populated: false,
      stopped: true,
    },
  });
  return { goal, task, run, fence, coordinator, criteria, artifact, proof, workspace };
}
async function state(f: Awaited<ReturnType<typeof fixture>>) {
  return {
    run: (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0],
    context: (await pool.query('SELECT * FROM contexts WHERE run_id=$1', [f.run])).rows,
    acceptances: (
      await pool.query('SELECT * FROM acceptances WHERE run_id=$1 ORDER BY created_at', [f.run])
    ).rows,
    artifacts: (await pool.query('SELECT * FROM artifacts WHERE run_id=$1 ORDER BY id', [f.run]))
      .rows,
    leases: (await pool.query('SELECT * FROM leases WHERE run_id=$1 ORDER BY resource', [f.run]))
      .rows,
  };
}
async function appliedDecision(f: Awaited<ReturnType<typeof fixture>>, sourceTaskId?: string) {
  await requestSemanticReplan(f.task, f.run);
  const c = (
    await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1 AND source_run_id=$2', [
      f.task,
      f.run,
    ])
  ).rows[0];
  const action = {
    instruction:
      'Investigate a different causal boundary with a discriminating test; preserve the negative report and final acceptance gates.',
    diagnosis: 'The previous proposed hypothesis is refuted by the actual negative repair report.',
    kind: 'revise',
    caseId: c.id,
    ...(sourceTaskId ? { sourceTaskId } : {}),
  };
  const run = randomUUID(),
    fence = randomUUID();
  const attempt = (
    await pool.query('SELECT COALESCE(max(attempt),0)::int+1 n FROM runs WHERE task_id=$1', [
      f.coordinator,
    ])
  ).rows[0].n;
  await pool.query("UPDATE tasks SET state='running',attempt_count=$2 WHERE id=$1", [
    f.coordinator,
    attempt,
  ]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state) VALUES($1,$2,$3,$4,'semantic-glm','glm','harness',1,'running')",
    [run, f.coordinator, attempt, fence],
  );
  const artifact = await writeArtifact(
    run,
    fence,
    'coordination.json',
    JSON.stringify({ supervision: { actions: [action] } }),
  );
  await completeRun(run, fence, 'New real supervisor decision');
  await validateTask(f.coordinator);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  await recordSupervisionDecisions();
  await applySupervisionDecisions(async () => f.proof());
  expect(
    (await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [c.id])).rows[0].state,
  ).toBe('applied');
  return { caseId: c.id, run, artifact };
}

it.each(['opencode', 'harness'])(
  'a completed negative %s repair cannot become a second attempt without a new supervisor decision',
  async (runtime) => {
    const f = await fixture(runtime),
      original = await state(f);
    const recover = runtime === 'opencode' ? recoverOpenCodeRun : resumeFailedHarness;
    await expect(recover(f.run, f.fence, f.proof())).rejects.toThrow(semanticRetryCode);
    expect(await state(f)).toEqual(original);
    expect(
      (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
    ).toBe(1);
    expect(
      (
        await pool.query(
          "SELECT 1 FROM events WHERE kind='EXECUTION_RETRY_ADMITTED' AND entity_id=$1",
          [f.task],
        )
      ).rowCount,
    ).toBe(0);
  },
);

it('the scanner opens one immediate causal case after rollback instead of repeating checking or waiting for three failures', async () => {
  const f = await fixture(),
    original = await state(f);
  const scanner = new ContinuationSupervisor(async () => f.proof());
  await scanner.scan();
  await Promise.all([requestSemanticReplan(f.task, f.run), requestSemanticReplan(f.task, f.run)]);
  const cases = (await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1', [f.task]))
    .rows;
  expect(cases).toHaveLength(1);
  expect(cases[0]).toMatchObject({ state: 'open', source_run_id: f.run, applied_attempt: null,
    request_key:'semantic:semantic_repair_rejected:'+f.artifact!.digest,parent_case_id:null });
  expect(cases[0].evidence).toMatchObject([
    {
      kind: 'semantic_repair_rejected',
      sourceRunId: f.run,
      sourceAttempt: 1,
      report: { id: f.artifact!.id, digest: f.artifact!.digest },
      failedCriterion: { expected: true, actual: false },
    },
  ]);
  expect(
    (await pool.query('SELECT state,execution FROM tasks WHERE id=$1', [f.task])).rows[0],
  ).toMatchObject({
    state: 'recovery_blocked',
    execution: { continuation: { state: 'supervising', caseId: cases[0].id } },
  });
  expect(
    (
      await pool.query(
        "SELECT 1 FROM events WHERE kind='SEMANTIC_RETRY_SUPERVISION_REQUESTED' AND entity_id=$1",
        [f.task],
      )
    ).rowCount,
  ).toBe(1);
  expect(await state(f)).toEqual(original);
});

it('an actual accepted revise action survives JSONB key ordering and permits only one supervised admission', async () => {
  const f = await fixture(),
    original = await state(f),
    decision = await appliedDecision(f);
  const applied = (
    await pool.query('SELECT * FROM supervision_cases WHERE id=$1', [decision.caseId])
  ).rows[0];
  expect(applied.decision.actions[0].caseId).toBe(decision.caseId);
  expect(await requestSemanticReplan(f.task, f.run)).toBe(true);
  expect(
    (await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [decision.caseId])).rows[0]
      .state,
  ).toBe('applied');
  const results = await Promise.allSettled([
    recoverOpenCodeRun(f.run, f.fence, { ...f.proof(), supervisionResumeCaseId: decision.caseId }),
    recoverOpenCodeRun(f.run, f.fence, { ...f.proof(), supervisionResumeCaseId: decision.caseId }),
  ]);
  expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
  const created = (results.find((r) => r.status === 'fulfilled') as PromiseFulfilledResult<any>)
    .value;
  expect(created.attempt).toBe(2);
  expect(
    (await pool.query('SELECT state,instruction_snapshot FROM runs WHERE id=$1', [created.runId]))
      .rows[0],
  ).toMatchObject({ state: 'queued', instruction_snapshot: { task: { criteria: f.criteria } } });
  const after = await state(f);
  expect(after.run.instruction_snapshot).toEqual(original.run.instruction_snapshot);
  expect(after.context).toEqual(original.context);
  expect(after.acceptances).toEqual(original.acceptances);
  expect(after.acceptances[0].passed).toBe(false);
});

it.each([
  'text-only',
  'wrong-proof',
  'old-attempt',
  'wrong-source',
  'foreign-goal',
  'newer-case',
  'rejected-coordinate',
  'coordinate-live',
  'changed-action',
  'corrupt-coordinate',
])('a %s supervisor reference cannot release a semantic retry', async (invalid) => {
  const f = await fixture();
  const decision = await appliedDecision(f);
  let proofCase = decision.caseId;
  if (invalid === 'text-only') {
    proofCase = '';
    await pool.query('UPDATE tasks SET description=description||$2 WHERE id=$1', [
      f.task,
      ' supervisionResumeCaseId=' + decision.caseId,
    ]);
  }
  if (invalid === 'wrong-proof') proofCase = randomUUID();
  if (invalid === 'old-attempt')
    await pool.query('UPDATE supervision_cases SET applied_attempt=0 WHERE id=$1', [
      decision.caseId,
    ]);
  if (invalid === 'wrong-source')
    await pool.query(
      "UPDATE tasks SET execution=jsonb_set(execution,'{supervision,sourceRunId}',$2::jsonb) WHERE id=$1",
      [f.task, JSON.stringify(randomUUID())],
    );
  if (invalid === 'foreign-goal') {
    const other = randomUUID();
    await pool.query(
      "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Other','Other scope','other',$2)",
      [other, other],
    );
    await pool.query('UPDATE supervision_cases SET goal_id=$2 WHERE id=$1', [
      decision.caseId,
      other,
    ]);
  }
  if (invalid === 'newer-case')
    await pool.query(
      "INSERT INTO supervision_cases(id,goal_id,task_id,reason,evidence,state,created_at) VALUES($1,$2,$3,'New current case','[]','closed',now()+interval '1 second')",
      [randomUUID(), f.goal, f.task],
    );
  if (invalid === 'rejected-coordinate')
    await pool.query(
      "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at) VALUES($1,$2,$3,false,'[]','later-validation',now()+interval '1 second')",
      [randomUUID(), f.coordinator, decision.run],
    );
  if (invalid === 'coordinate-live')
    await pool.query("UPDATE runs SET state='running' WHERE id=$1", [decision.run]);
  if (invalid === 'changed-action')
    await pool.query(
      "UPDATE supervision_cases SET decision=jsonb_set(decision,'{actions,0,instruction}','\"Different unaccepted model instruction that was not approved by any coordinator.\"') WHERE id=$1",
      [decision.caseId],
    );
  if (invalid === 'corrupt-coordinate')
    await writeFile(path.join(config.dataDir, 'artifacts', decision.artifact.id), 'corrupt');
  const original = await state(f);
  await expect(
    recoverOpenCodeRun(f.run, f.fence, { ...f.proof(), supervisionResumeCaseId: proofCase }),
  ).rejects.toThrow(semanticRetryCode);
  expect(await state(f)).toEqual(original);
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1', [f.task])).rowCount).toBe(1);
});

it('thirteen new causal inputs and mutable criteria do not bypass the frozen negative repair contract', async () => {
  const f = await fixture();
  const inputs = Array.from({ length: 13 }, (_, i) => ({
    id: randomUUID(),
    name: 'new-evidence-' + i + '.json',
    digest: hash('{}'),
    base64: Buffer.from('{}').toString('base64'),
  }));
  await pool.query(
    "UPDATE tasks SET criteria='[]',execution=jsonb_set(execution,'{inputs}',$2::jsonb) WHERE id=$1",
    [f.task, JSON.stringify(inputs)],
  );
  await expect(
    recoverOpenCodeRun(f.run, f.fence, {
      ...f.proof(),
      candidate: { patch: '', files: [], candidateDigest: 'a'.repeat(64) },
    }),
  ).rejects.toThrow(semanticRetryCode);
  expect(
    (
      await pool.query(
        "SELECT 1 FROM events WHERE kind='EXECUTION_RETRY_ADMITTED' AND entity_id=$1",
        [f.task],
      )
    ).rowCount,
  ).toBe(0);
  expect(
    (await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [f.run])).rows[0]
      .instruction_snapshot.task.criteria,
  ).toEqual(f.criteria);
});

it('a stale applied decision remains immutable instead of reopening the same semantic generation', async () => {
  const f = await fixture(),
    decision = await appliedDecision(f);
  await pool.query('UPDATE supervision_cases SET applied_attempt=0 WHERE id=$1', [decision.caseId]);
  const before = (
    await pool.query('SELECT * FROM supervision_cases WHERE id=$1', [
      decision.caseId,
    ])
  ).rows[0];
  expect(await requestSemanticReplan(f.task, f.run)).toBe(false);
  expect(
    (
      await pool.query('SELECT * FROM supervision_cases WHERE id=$1', [
        decision.caseId,
      ])
    ).rows[0],
  ).toEqual(before);
  expect(
    (await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1', [f.task])).rowCount,
  ).toBe(1);
});

it('a new integrity cause receives one new semantic generation while the applied decision remains unchanged',async()=>{
  const f=await fixture(),decision=await appliedDecision(f);
  const before=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[decision.caseId])).rows[0];
  // The underlying immutable artifact is now demonstrably corrupt. This is a
  // new diagnostic cause, never permission to reinterpret fixVerified=false.
  await writeFile(path.join(config.dataDir,'artifacts',f.artifact!.id),'corrupt bytes');
  const history=await state(f);
  expect(await Promise.all([requestSemanticReplan(f.task,f.run),requestSemanticReplan(f.task,f.run)]))
    .toEqual([true,true]);
  const rows=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1 ORDER BY created_at,id',[f.task])).rows;
  expect(rows).toHaveLength(2);
  expect(rows.find(c=>c.id===decision.caseId)).toEqual(before);
  expect(rows.find(c=>c.id!==decision.caseId)).toMatchObject({state:'open',source_run_id:f.run,
    request_key:'semantic:semantic_repair_evidence_invalid:'+f.artifact!.digest,parent_case_id:decision.caseId,
    evidence:[{kind:'semantic_repair_evidence_invalid',failedCriterion:{actual:null}}]});
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SEMANTIC_RETRY_SUPERVISION_REQUESTED' AND entity_id=$1",[f.task])).rowCount).toBe(2);
  expect(await state(f)).toEqual(history);
});

it.each(['open','decided','delegated','waiting_evidence'])('preserves a different pending %s owner and retains the canonical rejection',async(status)=>{
  const f=await fixture();
  const prior=randomUUID();
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,request_key,reason,evidence)
    VALUES($1,$2,$3,$4,$5,'delivery:existing-cause','Current supervisor work','[{"kind":"delivery"}]')`,
    [prior,f.goal,f.task,f.run,status]);
  const before=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows;
  const taskBefore=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  expect(await requestSemanticReplan(f.task,f.run)).toBe(true);
  const after=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows;
  if(status==='open'){
    expect(after).toHaveLength(1);
    expect(after[0]).toMatchObject({id:before[0].id,state:'open',request_key:before[0].request_key,
      decision:before[0].decision,decision_run_id:before[0].decision_run_id});
    expect(after[0].evidence).toEqual([before[0].evidence[0],expect.objectContaining({kind:'semantic_repair_rejected',
      sourceRunId:f.run,report:expect.objectContaining({id:f.artifact!.id})})]);
  }else expect(after).toEqual(before);
  const fact=(await pool.query('SELECT value FROM settings WHERE id=$1',['canonical-repair-rejection:'+f.run])).rows[0].value;
  expect(fact).toMatchObject({ownerCaseId:prior,disposition:status==='open'?'routed':'deferred_existing_decision',
    routed:status==='open',acceptance:false});
  expect((await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual(taskBefore);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SEMANTIC_RETRY_SUPERVISION_REQUESTED'",[])).rowCount).toBe(0);
});

it('does not reopen legacy semantic history with the same report under a new request key',async()=>{
  const f=await fixture();expect(await requestSemanticReplan(f.task,f.run)).toBe(true);
  await pool.query("UPDATE supervision_cases SET state='closed',request_key='',evidence=jsonb_build_array((evidence->0)-'requestKey') WHERE task_id=$1",[f.task]);
  const before=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows;
  expect(await requestSemanticReplan(f.task,f.run)).toBe(false);
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows).toEqual(before);
});

it('defers a new namespace until old controllers can no longer require unique task/run pairs',async()=>{
  const f=await fixture();
  await pool.query('ALTER TABLE supervision_cases ADD CONSTRAINT semantic_test_legacy_pair UNIQUE(task_id,source_run_id)');
  const before=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  try{
    expect(await requestSemanticReplan(f.task,f.run)).toBe(false);
    expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.task])).rowCount).toBe(0);
    expect((await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual(before);
  }finally{await pool.query('ALTER TABLE supervision_cases DROP CONSTRAINT semantic_test_legacy_pair');}
  expect(await requestSemanticReplan(f.task,f.run)).toBe(true);
});

it.each(['new-live-attempt', 'cancel', 'goal-paused', 'supervision-disabled', 'autonomy-disabled'])(
  'the replan request preserves a %s race and does not create a case',
  async (race) => {
    const f = await fixture();
    await expect(recoverOpenCodeRun(f.run, f.fence, f.proof())).rejects.toThrow(semanticRetryCode);
    if (race === 'new-live-attempt') {
      await pool.query(
        "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'opencode',1,'running')",
        [randomUUID(), f.task, randomUUID()],
      );
      await pool.query("UPDATE tasks SET state='running',attempt_count=2 WHERE id=$1", [f.task]);
    }
    if (race === 'cancel')
      await pool.query(
        "INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",
        [f.task, JSON.stringify({ runId: f.run })],
      );
    if (race === 'goal-paused')
      await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
    if (race === 'supervision-disabled')
      await pool.query('UPDATE settings SET value=\'{"enabled":false}\' WHERE id=$1', [
        'supervision:' + f.goal,
      ]);
    if (race === 'autonomy-disabled')
      await pool.query(
        "UPDATE settings SET value='{\"enabled\":false}' WHERE id='autonomy:semantic-test'",
      );
    const before = (
      await pool.query(
        'SELECT t.*,r.instruction_snapshot FROM tasks t JOIN runs r ON r.task_id=t.id WHERE t.id=$1 ORDER BY r.attempt',
        [f.task],
      )
    ).rows;
    expect(await requestSemanticReplan(f.task, f.run)).toBe(false);
    expect(
      (await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1', [f.task])).rowCount,
    ).toBe(0);
    expect(
      (
        await pool.query(
          'SELECT t.*,r.instruction_snapshot FROM tasks t JOIN runs r ON r.task_id=t.id WHERE t.id=$1 ORDER BY r.attempt',
          [f.task],
        )
      ).rows,
    ).toEqual(before);
  },
);

it.each([null, '{malformed', JSON.stringify({ missingFixVerified: true })])(
  'keeps missing or malformed repair packaging on the existing native recovery path (%s)',
  async (report) => {
    const f = await fixture('opencode', report);
    const original = await state(f);
    const result = await recoverOpenCodeRun(f.run, f.fence, f.proof());
    expect(result.attempt).toBe(2);
    expect((await state(f)).acceptances).toEqual(original.acceptances);
    expect(
      (await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1', [f.task])).rowCount,
    ).toBe(0);
  },
);

it('keeps a real OOM interruption resumable without a semantic case', async () => {
  const f = await fixture();
  await pool.query("UPDATE runs SET state='failed',error_code='OPENCODE_SANDBOX_OOM' WHERE id=$1", [
    f.run,
  ]);
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1", [f.task]);
  const result = await recoverOpenCodeRun(f.run, f.fence, {
    ...f.proof(),
    state: { originalCode: 'OPENCODE_SANDBOX_OOM' },
  });
  expect(result.attempt).toBe(2);
  expect(
    (await pool.query('SELECT error_code FROM runs WHERE id=$1', [f.run])).rows[0].error_code,
  ).toBe('OPENCODE_SANDBOX_OOM');
  expect(
    (await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1', [f.task])).rowCount,
  ).toBe(0);
});

it('corrupt repair bytes remain integrity-blocked and automatically request diagnosis without inventing fixVerified=false', async () => {
  const f = await fixture();
  await writeFile(path.join(config.dataDir, 'artifacts', f.artifact!.id), '{"fixVerified":true}');
  const original = await state(f);
  await expect(recoverOpenCodeRun(f.run, f.fence, f.proof())).rejects.toThrow(semanticEvidenceCode);
  await new ContinuationSupervisor(async () => f.proof()).scan();
  const c = (await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1', [f.task]))
    .rows[0];
  expect(c.state).toBe('open');
  expect(c.evidence[0]).toMatchObject({
    kind: 'semantic_repair_evidence_invalid',
    report: { digest: f.artifact!.digest },
    failedCriterion: { actual: null },
  });
  expect(c.reason).not.toContain('fixVerified=false');
  expect(await state(f)).toEqual(original);
});

it('a fresh supervisor delivery binds an accepted causal repair without turning the prior negative into acceptance', async () => {
  const f = await fixture(),
    source = randomUUID(),
    sourceRun = randomUUID(),
    fence = randomUUID();
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1,$2,'source','Causal repair','Distinct accepted repair input','qa.backend','running','work',$3,$4,1)`,
    [
      source,
      f.goal,
      JSON.stringify(f.criteria),
      JSON.stringify({
        kind: 'opencode',
        workKind: 'repair',
        workspace: 'semantic-test/source',
        baseWorkspace: 'semantic-test/game',
      }),
    ],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",
    [sourceRun, source, fence],
  );
  const patch = await writeArtifact(
    sourceRun,
    fence,
    'implementation.patch',
    'Verified distinct implementation delta',
  );
  await writeArtifact(
    sourceRun,
    fence,
    'repair-report.json',
    JSON.stringify({ fixVerified: true }),
  );
  await writeArtifact(sourceRun, fence, 'verification.json', JSON.stringify({ passed: true }));
  await completeRun(sourceRun, fence, 'Verified causal repair');
  await validateTask(source);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  const priorAcceptance = (await state(f)).acceptances;
  const decision = await appliedDecision(f, source);
  const result = await recoverOpenCodeRun(f.run, f.fence, {
    ...f.proof(),
    supervisionResumeCaseId: decision.caseId,
  });
  const task = (await pool.query('SELECT execution FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(
    task.execution.inputs.some(
      (input: any) => input.id === patch.id && input.digest === patch.digest,
    ),
  ).toBe(true);
  expect(
    (
      await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2', [
        f.task,
        source,
      ])
    ).rowCount,
  ).toBe(1);
  expect((await state(f)).acceptances).toEqual(priorAcceptance);
  expect(
    (
      await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [result.runId])
    ).rows[0].instruction_snapshot.task.execution.inputs.some(
      (input: any) => input.id === patch.id,
    ),
  ).toBe(true);
});
