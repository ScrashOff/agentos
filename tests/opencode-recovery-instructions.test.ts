import { beforeAll, beforeEach, afterAll, afterEach, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { hash } from '../server/memory.js';
import { seedRegistry } from '../server/registry.js';
import { installSupervision } from '../server/supervision.js';
import { recoverOpenCodeRun } from '../server/opencode-recovery.js';
import { resumeFailedHarness } from '../server/continuation.js';
import { readAcceptedAuxiliaryProof } from '../server/accepted-auxiliary.js';
import { startRun, writeArtifact, completeRun, validateTask } from '../server/service.js';
import * as instructions from '../server/task-instructions.js';

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await installSupervision();
  await pool.query(`CREATE TABLE IF NOT EXISTS opencode_jobs(run_id uuid PRIMARY KEY REFERENCES runs(id),
    state text DEFAULT 'queued',created_at timestamptz DEFAULT now(),started_at timestamptz,finished_at timestamptz)`);
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await seedRegistry();
  await pool.query(`UPDATE settings SET value='{"maxRuns":8,"browserSlots":2}' WHERE id IN('capacity','remote-capacity')`);
  await pool.query("DELETE FROM settings WHERE id LIKE 'autonomy:%' OR id LIKE 'exclusive-workstreams:%'");
});
afterEach(() => vi.restoreAllMocks());
afterAll(async () => { await pool.end(); });

const stopped = (runId: string, workspace: string) => ({
  runId, runtime: 'opencode', workspace, checkedAt: new Date().toISOString(),
  state: { originalCode: 'OPENCODE_SANDBOX_OOM' }, candidate: { patch: '', files: [] },
  process: { unit: 'agentos-run-' + runId, activeState: 'inactive', mainPid: 0, populated: false, stopped: true },
});

async function fixture() {
  const goal = randomUUID(), source = randomUUID(), sourceRun = randomUUID(), sourceFence = randomUUID();
  const task = randomUUID(), run = randomUUID(), fence = randomUUID(), workspace = 'snapshot/aux-' + task;
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('native-test','Native','openai','glm','["glm"]',true,8,true,NULL),
      ('repair-account','Repair','codex','gpt-6-luna','["gpt-6-luna"]',true,8,true,'test-route')`);
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Snapshot','Snapshot','snapshot-test',$2)", [goal, 'goal-' + goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,allowed_accounts)
    VALUES($1,$2,'source','Source','Frozen source','qa.backend','running','integrate',$3,$4,1,'["repair-account"]')`,
    [source, goal, JSON.stringify([{ id: 'candidate', type: 'artifact', name: 'candidate.json', label: 'Candidate' }]),
      JSON.stringify({ kind: 'harness', profile: 'code', workspace: 'snapshot/source', baseWorkspace: 'snapshot/game' })]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,1,$3,'harness',1,'running')`, [sourceRun, source, sourceFence]);
  const sourceBytes = JSON.stringify({ baseDigest: hash('base'), candidateDigest: hash('candidate') });
  const input = await writeArtifact(sourceRun, sourceFence, 'candidate.json', sourceBytes);
  await completeRun(sourceRun, sourceFence, 'Source candidate');
  await validateTask(source);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [goal]);
  const execution = { kind: 'opencode', managed: true, workKind: 'diagnostic', workspace,
    sourceWorkspace: 'snapshot/source', baseWorkspace: 'snapshot/game', writePaths: ['evidence/'],
    inputs: [{ ...input, base64: Buffer.from(sourceBytes).toString('base64') }],
    recoveryContext: { instruction: 'Previous instruction', artifacts: [] },
    continuation: { state: 'failed', runId: run } };
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,
    criteria,execution,resources,attempt_count,max_attempts)
    VALUES($1,$2,'aux','Auxiliary','Check the frozen source','qa.backend','native-test','glm','reconciling','work',$3,$4,$5,1,1)`,
    [task, goal, JSON.stringify([{ id: 'proof', type: 'artifact', name: 'proof.json', label: 'Actual proof' }]),
      JSON.stringify(execution), JSON.stringify(['remote-slot', 'workspace:' + workspace])]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [task, source]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state,error_code)
    VALUES($1,$2,1,$3,'native-test','glm','opencode',1,'reconciling','STOP_UNCONFIRMED')`, [run, task, fence]);
  await pool.query("INSERT INTO leases(resource,run_id) VALUES('remote-slot',$1),($2,$1)", [run, 'workspace:' + workspace]);
  return { goal, source, sourceRun, task, run, fence, workspace, proof: stopped(run, workspace) };
}

async function acceptedRecovery(f: Awaited<ReturnType<typeof fixture>>) {
  const next = await recoverOpenCodeRun(f.run, f.fence, f.proof);
  const run = await startRun(next.runId, 'fixture-worker');
  expect(run).not.toBeNull();
  const snapshot = run!.instruction_snapshot, contextId = randomUUID();
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'Frozen input')",
    [contextId, run!.id, JSON.stringify({ executionInputs: snapshot.executionInputs, dependencyArtifacts: [],
      taskInstructions: { taskId: f.task, goalId: f.goal, revisionId: snapshot.revisionId,
        descriptionHash: hash(snapshot.task.description) } })]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [run!.id, contextId]);
  await writeArtifact(run!.id, run!.fence, 'proof.json', '{"result":"verified"}');
  await completeRun(run!.id, run!.fence, 'Verified frozen source');
  await validateTask(f.task);
  return run!;
}

async function reopenAncestor(f: Awaited<ReturnType<typeof fixture>>) {
  const failed = randomUUID(), fence = randomUUID(), qa = randomUUID();
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  await pool.query("UPDATE tasks SET state='failed',attempt_count=2 WHERE id=$1", [f.source]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state,error_code,finished_at)
    VALUES($1,$2,2,$3,'old-account','old-model','harness',1,'failed','AUTH_REQUIRED',now())`, [failed, f.source, fence]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
    VALUES($1,$2,'qa','QA','Recheck candidate','qa.backend','accepted','verify','[]','{"kind":"harness","readOnlySource":true}')`, [qa, f.goal]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [qa, f.source]);
  await resumeFailedHarness(failed, fence, stopped(failed, 'snapshot/source'));
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [qa])).rows[0].state).toBe('blocked_dependencies');
}

it('freezes native recovery inputs before execution and preserves its accepted proof on an ancestor retry', async () => {
  const f = await fixture();
  const before = (await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [f.run])).rows[0];
  const recovered = await acceptedRecovery(f);
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(recovered.instruction_snapshot.task.execution).toEqual(task.execution);
  expect(task.execution.recoveryContext.previousRunId).toBe(f.run);
  expect(task.execution.continuation.runId).toBe(recovered.id);
  expect(await readAcceptedAuxiliaryProof(pool, f.task)).toMatchObject({ eligible: true, runId: recovered.id });
  const accepted = (await pool.query('SELECT * FROM acceptances WHERE run_id=$1', [recovered.id])).rows;
  await reopenAncestor(f);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe('accepted');
  expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1', [recovered.id])).rows).toEqual(accepted);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual(before);
});

it.each(['instruction', 'artifacts', 'workspace'])('still invalidates accepted auxiliary work after a substantive %s change', async field => {
  const f = await fixture();
  const recovered = await acceptedRecovery(f);
  expect((await readAcceptedAuxiliaryProof(pool, f.task)).eligible).toBe(true);
  const path = field === 'workspace' ? ['workspace'] : ['recoveryContext', field];
  const value = field === 'artifacts' ? [{ id: randomUUID(), digest: hash('new evidence') }] : 'Changed semantic input';
  await pool.query('UPDATE tasks SET execution=jsonb_set(execution,$2::text[],$3::jsonb) WHERE id=$1', [f.task, path, JSON.stringify(value)]);
  expect(await readAcceptedAuxiliaryProof(pool, f.task)).toMatchObject({ eligible: false, reason: 'CONTRACT_CHANGED' });
  await reopenAncestor(f);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe('blocked_dependencies');
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [recovered.id])).rows[0].instruction_snapshot)
    .toEqual(recovered.instruction_snapshot);
});

async function ledger(taskId: string) {
  return (await pool.query(`SELECT to_jsonb(t) task,
    (SELECT jsonb_agg(to_jsonb(r) ORDER BY attempt) FROM runs r WHERE r.task_id=t.id) runs,
    (SELECT jsonb_agg(to_jsonb(a) ORDER BY a.id) FROM artifacts a JOIN runs r ON r.id=a.run_id WHERE r.task_id=t.id) artifacts,
    (SELECT jsonb_agg(to_jsonb(l) ORDER BY l.resource) FROM leases l JOIN runs r ON r.id=l.run_id WHERE r.task_id=t.id) leases,
    (SELECT jsonb_agg(to_jsonb(j) ORDER BY j.run_id) FROM opencode_jobs j JOIN runs r ON r.id=j.run_id WHERE r.task_id=t.id) jobs,
    (SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM events e WHERE e.entity_id=t.id::text) events
    FROM tasks t WHERE t.id=$1`, [taskId])).rows[0];
}

it('rolls back the pre-snapshot context, history and leases if instruction preparation fails', async () => {
  const f = await fixture(), before = await ledger(f.task);
  const prepare = vi.spyOn(instructions, 'prepareTaskInstructions').mockImplementationOnce(async (db, id) => {
    const current = (await db.query('SELECT execution FROM tasks WHERE id=$1', [id])).rows[0].execution;
    expect(current.recoveryContext.previousRunId).toBe(f.run);
    expect(current.continuation.runId).not.toBe(f.run);
    expect((await db.query('SELECT 1 FROM runs WHERE id=$1', [current.continuation.runId])).rowCount).toBe(0);
    throw Error('FIXTURE_INSTRUCTION_PREPARATION_FAILED');
  });
  await expect(recoverOpenCodeRun(f.run, f.fence, f.proof)).rejects.toThrow('FIXTURE_INSTRUCTION_PREPARATION_FAILED');
  expect(prepare).toHaveBeenCalledOnce();
  expect(await ledger(f.task)).toEqual(before);
  // Files collected before the transaction fails may remain unreferenced, as before.
});

it('rolls back even after inserting the next snapshot when a pending supervisor holds admission', async () => {
  const f = await fixture();
  await pool.query("INSERT INTO settings(id,value) VALUES($1,'{\"enabled\":true}')", ['supervision:' + f.goal]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
    VALUES($1,$2,$3,$4,'open','Waiting for a decision','[]')`, [randomUUID(), f.goal, f.task, f.run]);
  const before = await ledger(f.task);
  await expect(recoverOpenCodeRun(f.run, f.fence, f.proof)).rejects.toThrow('SUPERVISION_PENDING');
  expect(await ledger(f.task)).toEqual(before);
});

it('admits only one recovery under concurrent calls and freezes that exact context', async () => {
  const f = await fixture();
  const results = await Promise.allSettled([recoverOpenCodeRun(f.run, f.fence, f.proof), recoverOpenCodeRun(f.run, f.fence, f.proof)]);
  expect(results.filter(r => r.status === 'fulfilled')).toHaveLength(1);
  expect(results.filter(r => r.status === 'rejected')).toHaveLength(1);
  const next = (await pool.query('SELECT instruction_snapshot FROM runs WHERE task_id=$1 AND attempt=2', [f.task])).rows;
  expect(next).toHaveLength(1);
  expect(next[0].instruction_snapshot.task.execution).toEqual((await pool.query('SELECT execution FROM tasks WHERE id=$1', [f.task])).rows[0].execution);
  expect((await pool.query('SELECT 1 FROM opencode_jobs j JOIN runs r ON r.id=j.run_id WHERE r.task_id=$1', [f.task])).rowCount).toBe(1);
});
