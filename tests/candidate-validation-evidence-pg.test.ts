import {
  installCandidateValidationEvidence,
  reconcileCandidateValidationEvidence,
  readCandidateValidationEvidence,
  prepareCandidateValidationEvidence,
  bindCandidateValidationEvidence,
} from '../server/candidate-validation-evidence.js';
import { runtimeInputs } from '../server/execution-inputs.js';
import { stageArtifactInputBundle } from '../server/artifact-input-bundle.js';
import { config } from '../server/config.js';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { installBacklogPlanner } from '../server/backlog.js';
import { installSupervision, applySupervisionDecisions } from '../server/supervision.js';
import { afterAll, beforeAll, beforeEach, expect, it, vi } from 'vitest';
import { createHash, randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import {
  installManagement,
  applyManagementAction,
  managementContract,
} from '../server/management.js';
import { writeArtifact, readArtifact } from '../server/service.js';
import {
  proposeCandidateValidation,
  reconcileCandidateValidations,
  readCandidateValidations,
} from '../server/candidate-validation.js';

let f: any;
const sha = (text: string) => createHash('sha256').update(text).digest('hex');
beforeAll(async () => {
  await migrate();
  await installBacklogPlanner();
  await installSupervision();
  await installManagement();
  await installCandidateValidationEvidence();
});
beforeEach(async () => {
  await pool.query("DELETE FROM settings WHERE id LIKE 'candidate-validation:%'");
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
  const goal = randomUUID(),
    managerTask = randomUUID(),
    managerRun = randomUUID(),
    managerFence = randomUUID(),
    taskId = randomUUID(),
    runId = randomUUID(),
    runFence = randomUUID(),
    contextVersion = randomUUID(),
    sol = 'test-sol-' + randomUUID();
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency) VALUES($1,$1,'codex','gpt-6.1-sol','[\"gpt-6.1-sol\"]',true,true,4)",
    [sol],
  );
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Validation','Frozen candidate validation','agentos',$1::text)",
    [goal],
  );
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2),($3,$2)', [
    'management:' + goal,
    '{"enabled":true}',
    'workforce:' + goal,
  ]);
  for (const [id, key, state, stage, execution] of [
    [managerTask, 'manager', 'accepted', 'coordinate', null],
    [
      taskId,
      'target',
      'failed',
      'integrate',
      JSON.stringify({
        kind: 'harness',
        workspace: 'agentos/otest',
        baseWorkspace: 'agentos/test',
      }),
    ],
  ]) {
    await pool.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
   VALUES($1,$2,$3,$3,'Exact immutable contract','qa.backend',$4,$5,'[]',$6,1)`,
      [id, goal, key, state, stage, execution],
    );
  }
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id,model) VALUES($1,$2,1,$3,'harness',1,'running',$4,'gpt-6.1-sol')",
    [managerRun, managerTask, managerFence, sol],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id) VALUES($1,$2,1,$3,'harness',1,'running',$4)",
    [runId, taskId, runFence, contextVersion],
  );
  await pool.query(
    "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Immutable context')",
    [contextVersion, runId],
  );
  const candidateDigest = sha('[]'),
    candidate = JSON.stringify({ baseDigest: candidateDigest, candidateDigest, files: [] });
  const artifact = await writeArtifact(runId, runFence, 'candidate.json', candidate);
  await pool.query("UPDATE runs SET state='failed',finished_at=now() WHERE id=$1", [runId]);
  f = {
    goal,
    managerRun,
    managerTask,
    managerFence,
    sol,
    taskId,
    runId,
    runFence,
    contextVersion,
    candidateDigest,
    candidateArtifactId: artifact.id,
    candidateArtifactDigest: artifact.digest,
  };
  await writeArtifact(
    managerRun,
    managerFence,
    'coordination.json',
    JSON.stringify({
      management: {
        summary: 'Approve exact frozen candidate validation',
        capacityReason: 'Existing isolated CI owns the deterministic checks',
        actions: Array.from({ length: 7 }, () => proposal()),
      },
    }),
  );
  await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1", [managerRun]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')",
    [randomUUID(), managerTask, managerRun],
  );
});
afterAll(async () => {
  await pool.end();
});
function proposal(extra: any = {}) {
  return {
    kind: 'request_validation',
    expectedAttempt: f.expectedAttempt ?? 1,
    taskId: f.taskId,
    runId: f.runId,
    runFence: f.runFence,
    contextVersion: f.contextVersion,
    candidateArtifactId: f.candidateArtifactId,
    candidateArtifactDigest: f.candidateArtifactDigest,
    candidateDigest: f.candidateDigest,
    reason: 'Validar publicação e critérios com ação correta no validador isolado. 🚀',
    ...extra,
  };
}
async function approveActions(actions: unknown[]) {
  const attempt = (
    await pool.query('SELECT COALESCE(max(attempt),0)+1 attempt FROM runs WHERE task_id=$1', [
      f.managerTask,
    ])
  ).rows[0].attempt;
  f.managerRun = randomUUID();
  f.managerFence = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id,model) VALUES($1,$2,$3,$4,'harness',1,'running',$5,'gpt-6.1-sol')",
    [f.managerRun, f.managerTask, attempt, f.managerFence, f.sol],
  );
  await writeArtifact(
    f.managerRun,
    f.managerFence,
    'coordination.json',
    JSON.stringify({
      management: {
        summary: 'Approve exact scoped validation',
        capacityReason: 'Existing deterministic isolated CI',
        actions,
      },
    }),
  );
  await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1", [
    f.managerRun,
  ]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')",
    [randomUUID(), f.managerTask, f.managerRun],
  );
}
async function inspect() {
  return {
    runId: f.runId,
    workspace: (await pool.query('SELECT execution FROM tasks WHERE id=$1', [f.taskId])).rows[0]
      .execution.workspace,
    checkedAt: new Date().toISOString(),
    candidate: { candidateDigest: f.candidateDigest },
    process: {
      unit: 'agentos-run-' + f.runId,
      stopped: true,
      mainPid: 0,
      populated: false,
      activeState: 'inactive',
    },
  };
}
async function action() {
  return proposeCandidateValidation(f.goal, f.managerRun, 0, proposal(), inspect);
}
async function stored(id: string) {
  return (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['candidate-validation:' + id])
  ).rows[0]?.value;
}
const receipt = (request: any, state: string, extra: any = {}) => ({
  actionId: request.actionId,
  candidateDigest: request.candidateDigest,
  requestDigest: sha(
    JSON.stringify(request, (_key, value) =>
      value && !Array.isArray(value) && typeof value === 'object'
        ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
        : value,
    ),
  ),
  state,
  ...extra,
});

const canonical = (v: any): any =>
  Array.isArray(v)
    ? v.map(canonical)
    : v && typeof v === 'object'
      ? Object.fromEntries(
          Object.keys(v)
            .sort()
            .map((k) => [k, canonical(v[k])]),
        )
      : v;
const digest = (v: any) => sha(JSON.stringify(canonical(v)));
let completed: any, exported: any;
async function completeValidation() {
  const proposed = await action();
  const dispatch = async (mode: string, req: any) =>
    receipt(
      req,
      mode === 'freeze' ? 'frozen' : 'completed',
      mode === 'freeze'
        ? { frozenWorkspace: 'agentos/ocvfixture' }
        : {
            passed: true,
            code: 'VALIDATION_PASSED',
            resultDigest: 'e'.repeat(64),
            publishedBaselineDigest: 'f'.repeat(64),
            registeredReceiptSha: 'f'.repeat(64),
          },
    );
  await reconcileCandidateValidations(dispatch, inspect);
  await reconcileCandidateValidations(dispatch, inspect);
  completed = await stored(proposed.actionId!);
  const authority = {
    version: 1,
    kind: 'registered_candidate_validation',
    actionId: completed.actionId,
    requestDigest: digest(completed.request),
    resultDigest: 'e'.repeat(64),
    receiptDigest: 'd'.repeat(64),
    producer: {
      ...Object.fromEntries(
        [
          'goalId',
          'taskId',
          'runId',
          'runFence',
          'contextVersion',
          'expectedAttempt',
          'candidateDigest',
        ].map((k) => [k, completed[k]]),
      ),
      contractDigest: completed.request.contractDigest,
    },
    outcome: { passed: true, code: 'VALIDATION_PASSED', state: 'completed' },
    registeredActionSha: '8df13ff892b6ceaf0f7cb50e2d718069c5aeb29ac193235a65ace1100607b7d6',
    registeredCommand: [
      '/usr/bin/python3',
      '/opt/agentos-control/agentos-release-action.py',
      'validate',
    ],
    publishedBaselineDigest: 'f'.repeat(64),
    publicationProofDigest: 'f'.repeat(64),
    validationProofDigest: 'f'.repeat(64),
    registeredReceiptSha: 'f'.repeat(64),
    validatedReleaseDigest: 'a'.repeat(64),
    log: {
      rawDigest: sha('Real registered CI build/test output.\n'),
      rawBytes: 38,
      digest: sha('Real registered CI build/test output.\n'),
      bytes: 38,
      sanitized: true,
      completeRegisteredArtifact: true,
      source: 'registered_action_validation.log',
      commandStdoutMayBeTruncatedByRegisteredAction: true,
    },
    acceptance: false,
  };
  const log = Buffer.from('Real registered CI build/test output.\n');
  authority.log.bytes = log.length;
  authority.log.rawBytes = log.length;
  const proof = Buffer.from(JSON.stringify(canonical(authority)));
  exported = {
    version: 1,
    actionId: completed.actionId,
    requestDigest: digest(completed.request),
    resultDigest: 'e'.repeat(64),
    receiptDigest: 'd'.repeat(64),
    authority,
    inputs: [
      {
        name: 'ci-receipt.json',
        mime: 'application/json',
        digest: sha(proof.toString()),
        size: proof.length,
        base64: proof.toString('base64'),
      },
      {
        name: 'validation.log',
        mime: 'text/plain',
        digest: sha(log.toString()),
        size: log.length,
        base64: log.toString('base64'),
      },
    ],
  };
  return completed;
}
async function job() {
  return (
    await pool.query('SELECT value FROM settings WHERE id=$1', [
      'candidate-validation-evidence:' + completed.actionId,
    ])
  ).rows[0]?.value;
}
async function allowRetry() {
  await pool.query(
    "UPDATE settings SET value=jsonb_set(value,'{nextProbeAt}',to_jsonb('1970-01-01T00:00:00.000Z'::text)) WHERE id=$1",
    ['candidate-validation-evidence:' + completed.actionId],
  );
}
async function revision(runId = f.runId) {
  const caseId = randomUUID(),
    managerRun = randomUUID(),
    fence = randomUUID();
  const attempt = (
    await pool.query('SELECT max(attempt)+1 n FROM runs WHERE task_id=$1', [f.managerTask])
  ).rows[0].n;
  const action = {
    caseId,
    kind: 'revise',
    diagnosis: 'The current exact candidate has a real registered external CI receipt.',
    instruction:
      'Read the immutable CI receipt and actual sanitized log; preserve source bytes and publish your own factual report, without claiming a local test or acceptance.',
    validationActionId: completed.actionId,
  };
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id,model) VALUES($1,$2,$3,$4,'harness',1,'running',$5,'gpt-6.1-sol')",
    [managerRun, f.managerTask, attempt, fence, f.sol],
  );
  await writeArtifact(
    managerRun,
    fence,
    'coordination.json',
    JSON.stringify({ supervision: { actions: [action] } }),
  );
  await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1", [managerRun]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')",
    [randomUUID(), f.managerTask, managerRun],
  );
  await pool.query(
    "INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision,decision_run_id) VALUES($1,$2,$3,$4,'decided','External CI report finalization','[]',$5,$6)",
    [caseId, f.goal, f.taskId, runId, JSON.stringify({ actions: [action] }), managerRun],
  );
  return {
    c: (await pool.query('SELECT * FROM supervision_cases WHERE id=$1', [caseId])).rows[0],
    a: action,
    t: (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.taskId])).rows[0],
    r: (await pool.query('SELECT * FROM runs WHERE id=$1', [runId])).rows[0],
  };
}
it('publishes genuine bound external inputs once, freezes them in the next real run and leaves historical artifacts/criteria/acceptance unchanged', async () => {
  await completeValidation();
  const before = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.taskId])).rows[0];
  const oldCount = (
    await pool.query('SELECT count(*)::int n FROM artifacts WHERE run_id=$1', [f.runId])
  ).rows[0].n;
  let calls = 0;
  const dispatch = async () => {
    calls++;
    return exported;
  };
  await reconcileCandidateValidationEvidence(dispatch);
  await reconcileCandidateValidationEvidence(dispatch);
  expect(calls).toBe(1);
  expect((await job()).state).toBe('published');
  expect((await pool.query('SELECT * FROM tasks WHERE id=$1', [f.taskId])).rows[0]).toEqual(before);
  expect(
    (await pool.query('SELECT count(*)::int n FROM artifacts WHERE run_id=$1', [f.runId])).rows[0]
      .n,
  ).toBe(oldCount);
  const rev = await revision(),
    proof = await inspect();
  const prepared = await prepareCandidateValidationEvidence(
    pool,
    rev.c,
    rev.a,
    rev.t,
    rev.r,
    proof,
  );
  const bound = await bindCandidateValidationEvidence(
    pool,
    rev.c,
    rev.a,
    rev.t,
    rev.r,
    proof,
    prepared,
  );
  expect(bound.producer.runId).toBe(f.runId);
  expect(bound.acceptance).toBe(false);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)', [
    'supervision:' + f.goal,
    JSON.stringify({ enabled: true }),
  ]);
  await applySupervisionDecisions(inspect);
  const appliedTask = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.taskId])).rows[0];
  expect(appliedTask.execution.inputs).toEqual(bound.inputs);
  expect(appliedTask.execution.candidateValidationEvidence.producer.runId).toBe(f.runId);
  expect(
    (await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [rev.c.id])).rows[0].state,
  ).toBe('applied');
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [f.taskId]);
  const runId = randomUUID(),
    fence = randomUUID(),
    context = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id) VALUES($1,$2,2,$3,'harness',1,'running',$4)",
    [runId, f.taskId, fence, context],
  );
  const run = (await pool.query('SELECT * FROM runs WHERE id=$1', [runId])).rows[0];
  const task = run.instruction_snapshot.task,
    manifest = { executionInputs: run.instruction_snapshot.executionInputs };
  await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)', [
    context,
    runId,
    JSON.stringify(manifest),
    'CI inputs are external byte evidence.',
  ]);
  const inputs = await runtimeInputs(task, manifest, readArtifact, { transport: 'referenced-v1' });
  expect(inputs.map((i) => i.name)).toEqual(['ci-receipt.json', 'validation.log']);
  const bundleRoot = path.resolve(config.dataDir, 'ci-bundle-' + randomUUID());
  const bundle = await stageArtifactInputBundle(run, task, { id: context, manifest }, inputs, {
    bundleRoot,
  });
  expect(bundle?.runId).toBe(runId);
  const materialized = JSON.parse(
    (await readFile(path.join(bundleRoot, runId + '-' + fence, 'manifest.json'))).toString(),
  );
  expect(materialized.inputs).toHaveLength(2);
  expect(materialized.inputs.every((i: any) => i.kind === 'inline')).toBe(true);
  expect(JSON.stringify(await readCandidateValidationEvidence(pool, f.goal))).not.toContain(
    'base64',
  );
  expect(
    (await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1', [f.taskId]))
      .rows[0].n,
  ).toBe(0);
  expect(task.criteria).toEqual(before.criteria);
  const report = await writeArtifact(
    runId,
    fence,
    'ci-consumer-report.json',
    JSON.stringify({
      externalCandidateDigest: bound.producer.candidateDigest,
      externalActionId: bound.actionId,
      readyForValidation: false,
    }),
  );
  expect((await readArtifact(report.id)).run_id).toBe(runId);
  await expect(
    writeArtifact(f.runId, f.runFence, 'ci-verification.json', '{}'),
  ).rejects.toMatchObject({ code: 'STALE_RUN' });
});
it('refuses tampered output and persists reconciliation ownership without changing task data', async () => {
  await completeValidation();
  const invalid = structuredClone(exported);
  invalid.inputs[1].base64 = Buffer.from('invented log').toString('base64');
  await reconcileCandidateValidationEvidence(async () => invalid);
  const pending = await job();
  expect(pending.state).toBe('reconciling');
  expect(pending.body).toBeUndefined();
  expect(pending.code).toBe('CI_EVIDENCE_BYTES_CHANGED');
  await allowRetry();
  await reconcileCandidateValidationEvidence(async () => exported);
  expect((await job()).inputIds).toEqual(pending.inputIds);
  expect((await job()).state).toBe('published');
});
it('lost read-only RPC replies retain IDs and never replay validation or duplicate publication records', async () => {
  await completeValidation();
  let calls = 0;
  await reconcileCandidateValidationEvidence(async () => {
    calls++;
    throw Error('lost reply');
  });
  const pending = await job();
  expect(pending.state).toBe('reconciling');
  await allowRetry();
  await reconcileCandidateValidationEvidence(async () => {
    calls++;
    return exported;
  });
  await reconcileCandidateValidationEvidence(async () => {
    calls++;
    return exported;
  });
  expect(calls).toBe(2);
  expect((await job()).inputIds).toEqual(pending.inputIds);
  expect(
    (
      await pool.query(
        "SELECT count(*)::int n FROM events WHERE kind='CANDIDATE_VALIDATION_EVIDENCE_PUBLISHED' AND payload->>'actionId'=$1",
        [completed.actionId],
      )
    ).rows[0].n,
  ).toBe(1);
});
it('a committed publication with a lost DB reply remains immutable and is not emitted again', async () => {
  await completeValidation();
  let lost = false;
  const originalConnect = pool.connect.bind(pool) as any;
  const spy = vi.spyOn(pool, 'connect').mockImplementation(((...args: any[]) => {
    if (args.length) return originalConnect(...args);
    return originalConnect().then((client: any) => {
      const query = client.query,
        release = client.release;
      let publishing = false;
      client.query = async (...queryArgs: any[]) => {
        if (
          typeof queryArgs[0] === 'string' &&
          queryArgs[0].startsWith('UPDATE settings SET value=$2') &&
          queryArgs[1]?.[1]
        )
          try {
            publishing = JSON.parse(queryArgs[1][1]).state === 'published';
          } catch {}
        const value = await query.apply(client, queryArgs);
        if (queryArgs[0] === 'COMMIT' && publishing && !lost) {
          lost = true;
          throw Error('lost committed DB reply');
        }
        return value;
      };
      client.release = (...releaseArgs: any[]) => {
        client.query = query;
        client.release = release;
        return release.apply(client, releaseArgs);
      };
      return client;
    });
  }) as any);
  try {
    await reconcileCandidateValidationEvidence(async () => exported);
  } finally {
    spy.mockRestore();
  }
  expect(lost).toBe(true);
  expect((await job()).state).toBe('published');
  const saved = await job();
  await reconcileCandidateValidationEvidence(async () => {
    throw Error('must not fetch published evidence');
  });
  expect(await job()).toEqual(saved);
  await expect(
    pool.query("UPDATE settings SET value=jsonb_set(value,'{state}','\"reading\"') WHERE id=$1", [
      'candidate-validation-evidence:' + completed.actionId,
    ]),
  ).rejects.toThrow('CI_EVIDENCE_IMMUTABLE');
  await expect(
    pool.query('DELETE FROM settings WHERE id=$1', [
      'candidate-validation-evidence:' + completed.actionId,
    ]),
  ).rejects.toThrow('CI_EVIDENCE_IMMUTABLE');
});
it('refuses fabricated accepted action, cross-goal input, changed candidate/criteria and stale consumer fence before injection', async () => {
  await completeValidation();
  await reconcileCandidateValidationEvidence(async () => exported);
  const rev = await revision(),
    proof = await inspect();
  await expect(
    prepareCandidateValidationEvidence(
      pool,
      rev.c,
      {
        ...rev.a,
        instruction: 'A fabricated instruction not present in the accepted coordination bytes.',
      },
      rev.t,
      rev.r,
      proof,
    ),
  ).rejects.toThrow('CI_EVIDENCE_DECISION_UNBOUND');
  await expect(
    prepareCandidateValidationEvidence(
      pool,
      { ...rev.c, goal_id: randomUUID() },
      rev.a,
      rev.t,
      rev.r,
      proof,
    ),
  ).rejects.toThrow();
  await expect(
    prepareCandidateValidationEvidence(pool, rev.c, rev.a, rev.t, rev.r, {
      ...proof,
      candidate: { candidateDigest: '0'.repeat(64) },
    }),
  ).rejects.toThrow('CI_EVIDENCE_SOURCE_CHANGED');
  await expect(
    prepareCandidateValidationEvidence(
      pool,
      rev.c,
      rev.a,
      { ...rev.t, criteria: [{ id: 'weakened' }] },
      rev.r,
      proof,
    ),
  ).rejects.toThrow('CI_EVIDENCE_CONSUMER_STALE');
  await pool.query('UPDATE tasks SET criteria=$2 WHERE id=$1', [
    f.taskId,
    JSON.stringify([{ id: 'weakened' }]),
  ]);
  const changedCriteria = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.taskId])).rows[0];
  await expect(
    prepareCandidateValidationEvidence(pool, rev.c, rev.a, changedCriteria, rev.r, proof),
  ).rejects.toThrow('CI_EVIDENCE_CONTRACT_CHANGED');
  await pool.query('UPDATE tasks SET criteria=$2 WHERE id=$1', [
    f.taskId,
    JSON.stringify(rev.t.criteria),
  ]);
  const prepared = await prepareCandidateValidationEvidence(
    pool,
    rev.c,
    rev.a,
    rev.t,
    rev.r,
    proof,
  );
  await expect(
    bindCandidateValidationEvidence(
      pool,
      rev.c,
      rev.a,
      { ...rev.t, attempt_count: 2 },
      rev.r,
      proof,
      prepared,
    ),
  ).rejects.toThrow('CI_EVIDENCE_CONSUMER_STALE');
  await pool.query("UPDATE runs SET state='running' WHERE id=$1", [f.runId]);
  await expect(
    prepareCandidateValidationEvidence(
      pool,
      rev.c,
      rev.a,
      rev.t,
      { ...rev.r, state: 'running' },
      proof,
    ),
  ).rejects.toThrow();
});
it('reuses identical tested source across epochs while preserving the original producer and refusing unrelated model artifacts', async () => {
  await completeValidation();
  await reconcileCandidateValidationEvidence(async () => exported);
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [f.taskId]);
  const next = randomUUID(),
    fence = randomUUID(),
    context = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id,finished_at) VALUES($1,$2,2,$3,'harness',1,'failed',$4,now())",
    [next, f.taskId, fence, context],
  );
  await pool.query(
    "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Same source current epoch')",
    [context, next],
  );
  const rev = await revision(next),
    proof = {
      ...(await inspect()),
      runId: next,
      process: { ...(await inspect()).process, unit: 'agentos-run-' + next },
    };
  const prepared = await prepareCandidateValidationEvidence(
    pool,
    rev.c,
    rev.a,
    rev.t,
    rev.r,
    proof,
  );
  expect(prepared.producer.runId).toBe(f.runId);
  expect(prepared.consumer.runId).toBe(next);
  expect(prepared.consumer.runFence).toBe(fence);
  await expect(
    prepareCandidateValidationEvidence(
      pool,
      rev.c,
      { ...rev.a, validationActionId: randomUUID() },
      rev.t,
      rev.r,
      proof,
    ),
  ).rejects.toThrow('CI_EVIDENCE_DECISION_UNBOUND');
  expect(
    (
      await pool.query('SELECT count(*)::int n FROM artifacts WHERE run_id=$1 AND name LIKE $2', [
        f.runId,
        'ci-%',
      ])
    ).rows[0].n,
  ).toBe(0);
});
