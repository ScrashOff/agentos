import * as deliveryArtifactService from '../server/service.js';
import * as deliverySelection from '../server/delivery-consumption-selection.js';
import { beforeAll, beforeEach, afterAll, afterEach, it, expect, vi } from 'vitest';
import { randomUUID, createHash } from 'node:crypto';
import { pool, migrate, tx, event } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installSupervision, applySupervisionDecisions, resumeSupervisedWork,recordSupervisionDecisions,detectSupervisionCases } from '../server/supervision.js';
import { installDeferredHandoffs, stageDeferredHandoff, applyDeferredHandoff } from '../server/deferred-handoffs.js';
import { reconcileDeliveryConsumptions, readDeliveryConsumptions } from '../server/delivery-consumption.js';
import { requestDeliverySupervision } from '../server/delivery-supervision.js';
import { completeRun, failRun, validateTask, writeArtifact, readArtifact, schedule } from '../server/service.js';
import { glmRepairCriteria } from '../server/glm-repair.js';
import { config } from '../server/config.js';
import { installTaskExecutionMetadata } from '../server/task-execution-metadata.js';
import { ContinuationSupervisor, setContinuation } from '../server/continuation.js';
import { requestRecoveryReview } from '../server/recovery-supervision.js';
import { scheduledSupervisionAdmission } from '../server/supervised-admission.js';
import { runtimeProgressCode } from '../server/runtime-progress-contract.js';
import {deliverySourceConflict,recordDeliverySourceResolution,deliverySourceFollowthrough,rejectDeliverySourceDecision} from '../server/delivery-source-resolution.js';

const sha = (value: string) => createHash('sha256').update(value).digest('hex');
const originalRuntimeConfig = { bridgeHost: config.bridgeHost, remoteExecution: config.remoteExecution, dshCommand: config.dshCommand };
beforeAll(async () => { await migrate(); await installTaskExecutionMetadata(); await installDeferredHandoffs(); await installSupervision(); });
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE'); await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id='autonomy:consumption-test'");
});
afterEach(() => { Object.assign(config, originalRuntimeConfig); vi.restoreAllMocks(); });
afterAll(async () => { await pool.end(); });
const task = async (id: string) => (await pool.query('SELECT * FROM tasks WHERE id=$1', [id])).rows[0];
async function run(taskId: string) {
  const t = await task(taskId), id = randomUUID(), fence = randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=attempt_count+1 WHERE id=$1", [taskId]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,$3,$4,'harness',1,'running')`, [id, taskId, t.attempt_count + 1, fence]);
  return { id, fence, task_id: taskId, attempt: t.attempt_count + 1, runtime: 'harness' };
}
async function publish(r: any, values: Record<string, unknown>) {
  for (const [name, value] of Object.entries(values)) await writeArtifact(r.id, r.fence, name, typeof value === 'string' ? value : JSON.stringify(value));
  await completeRun(r.id, r.fence, 'Actual fixture artifacts', {});
  const result = await validateTask(r.task_id);
  await pool.query("UPDATE goals SET state='active' WHERE id=(SELECT goal_id FROM tasks WHERE id=$1)", [r.task_id]);
  return result;
}
async function manifest(r: any, refs: any[]) {
  const id = randomUUID();
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'Pinned dependency artifacts')",
    [id, r.id, JSON.stringify({ dependencyArtifacts: refs })]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [r.id, id]);
  return id;
}
const candidate = (bytes = 'fixed\n') => {
  const files = [{ path: 'src/app.js', sha256: sha(bytes) }], digest = sha(JSON.stringify(files.map(f => [f.path, f.sha256])));
  return { files, baseDigest: digest, candidateDigest: digest };
};
async function stop(id: string) {
  const r = (await pool.query('SELECT task_id FROM runs WHERE id=$1', [id])).rows[0], t = await task(r.task_id);
  return { runId: id, workspace: t.execution.workspace, checkedAt: new Date().toISOString(),
    process: { unit: 'agentos-run-' + id, stopped: true, mainPid: 0, populated: false, activeState: 'inactive' } };
}
async function fixture(failed = false, generic = false) {
  const goal = randomUUID(), producer = randomUUID(), consumer = randomUUID(), qa = randomUUID(), scope = 'test/consumption';
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Consume accepted repairs','Real candidate then independent QA','consumption-test',$2)", [goal, goal]);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)', ['supervision:' + goal, JSON.stringify({ enabled: true })]);
  for (const [id, stage, criteria, extra] of [
    [producer, 'work', glmRepairCriteria, { workKind: 'repair' }],
    [consumer, generic ? 'work' : 'integrate', [{ id: 'pass', label: 'Original validation', type: 'json_field', name: 'verification.json', field: 'passed', equals: true }], {}],
    [qa, 'verify', [{ id: 'qa', label: 'Independent QA', type: 'json_field', name: 'verification.json', field: 'passed', equals: true }], { readOnlySource: true, sourceWorkspace: 'test/o-' + consumer }],
  ] as const) await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
    VALUES($1,$2,$3,'Original task','Preserve all original criteria','engineering.integration','ready',$4,$5,$6)`,
    [id, goal, 'task-' + id, stage, JSON.stringify(criteria), JSON.stringify({ kind: 'harness', workspace: 'test/o-' + id,
      sourceWorkspace: 'test/source', baseWorkspace: scope, ...extra })]);
  if (!generic) await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)', [qa, consumer]);
  const pr = await run(producer);
  expect((await publish(pr, { 'implementation.patch': '--- a/src/app.js\n+++ b/src/app.js\n@@ -1 +1 @@\n-original\n+fixed\n',
    'repair-report.json': { fixVerified: true, sourceHashes: [{ path: 'src/app.js', before: sha('original\n'), after: sha('fixed\n') }] },
    'repair-tests.log': 'before exit 1; after exit 0' })).passed).toBe(true);
  let cr: any;
  if (failed) { cr = await run(consumer); await failRun(cr.id, cr.fence, 'RESULT_FORMAT', 'No final output'); }
  await tx(async db => stageDeferredHandoff(db, goal, producer, await task(producer), await task(consumer),
    { taskId: producer, plannerRunId: pr.id, coverageKey: 'one-real-repair', lane: 'glm', workKind: 'repair' }));
  const h = (await pool.query('SELECT * FROM deferred_handoffs WHERE producer_task_id=$1', [producer])).rows[0];
  expect(await applyDeferredHandoff(h.id, cr ? await stop(cr.id) : null, await stop(pr.id))).toBe(true);
  const refs = (await pool.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id', [pr.id])).rows;
  return { goal, producer, consumer, qa, scope, pr, cr, h, refs };
}
async function integrated(f: Awaited<ReturnType<typeof fixture>>, passed = true, eventBound = true, complete=true) {
  const cr = await run(f.consumer); await manifest(cr, f.refs.map(a => ({ ...a, task_id: f.producer })));
  const patch = f.refs.find(a => a.name === 'implementation.patch'), report = f.refs.find(a => a.name === 'repair-report.json');
  const composition = { version: 1, workspace: 'test/o-' + f.consumer, dryRun: false, complete,
    beforeDigest: candidate('original\n').candidateDigest, afterDigest: candidate().candidateDigest,
    receipts: [{ producerTaskId: f.producer, status: 'applied', patchDigest: patch.digest, reportDigest: report.digest,
      inputs: { patch: patch.id, report: report.id } }], acceptance: false, independentValidationRequired: true };
  // Same envelope as harness-supervision/worker production callbacks.
  if (eventBound) await event(pool, 'PATCH_COMPOSITION_RECORDED', f.consumer, {runId:cr.id,...composition});
  expect((await publish(cr, { 'candidate.json': candidate(), 'runtime-composition.json': composition, 'verification.json': { passed } })).passed).toBe(passed);
  return { cr, composition, candidateRef: (await pool.query("SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 AND name='candidate.json'", [cr.id])).rows[0] };
}
const row = async (goal: string) => (await readDeliveryConsumptions(pool, goal))[0];

// Exercise the actual authorized revision and resume, not a fabricated queued
// flag. No runtime is launched: the scheduler stops at the occupied account.
async function scheduledRevision(options: { apply?: boolean; code?: string; deferCase?: boolean } = {}) {
  const f = await fixture(true), account = randomUUID(), caseId = randomUUID(), coordinator = randomUUID();
  if (options.code) await pool.query('UPDATE runs SET error_code=$2 WHERE id=$1', [f.cr.id, options.code]);
  Object.assign(config, { bridgeHost: 'test.invalid', remoteExecution: true, dshCommand: 'test-only' });
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES($1,'Fixture account','codex','gpt-6-luna','["gpt-6-luna"]',true,1,true,'test-only')`, [account]);
  await pool.query('UPDATE tasks SET account_id=$2,model=$3,allowed_accounts=$4 WHERE id=$1',
    [f.consumer, account, 'gpt-6-luna', JSON.stringify([account])]);
  const action = { caseId, kind: 'revise', diagnosis: 'Existing evidence needs a revised packaging step.',
    instruction: 'Consume the accepted repair already attached. Preserve every criterion and record actual failed tests.' };
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria)
    VALUES($1,$2,$3,'Supervisor','Decide the next approach','qa.backend','ready','coordinate',
      '[{"id":"decision","type":"artifact","name":"coordination.json"}]')`, [coordinator, f.goal, 'manager-' + coordinator]);
  const decisionRun = await run(coordinator);
  expect((await publish(decisionRun, { 'coordination.json': { supervision: { actions: [action] } } })).passed).toBe(true);
  const insertDecision = async () => pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision,decision_run_id)
    VALUES($1,$2,$3,$4,'decided','Consume the existing repair','[]',$5,$6)`,
    [caseId, f.goal, f.consumer, f.cr.id, JSON.stringify({ actions: [action] }), decisionRun.id]);
  if (!options.deferCase) await insertDecision();
  if (options.apply === false) return { ...f, account, caseId, insertDecision };
  await applySupervisionDecisions(stop);
  expect((await pool.query('SELECT state,applied_attempt FROM supervision_cases WHERE id=$1', [caseId])).rows[0])
    .toEqual({ state: 'applied', applied_attempt: 1 });
  expect(await resumeSupervisedWork(stop)).toEqual([f.consumer]);
  expect((await task(f.consumer)).state).toBe('retry_scheduled');
  return { ...f, account, caseId, insertDecision };
}

async function occupyAccount(f: Awaited<ReturnType<typeof scheduledRevision>>) {
  const busy = randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria)
    VALUES($1,$2,$3,'Busy account','A different live task','qa.backend','ready','[]')`, [busy, f.goal, 'busy-' + busy]);
  const busyRun = await run(busy);
  await pool.query('UPDATE runs SET account_id=$2 WHERE id=$1', [busyRun.id, f.account]);
  expect(await schedule()).toEqual([]);
  expect(await task(f.consumer)).toMatchObject({ state: 'waiting_quota', reason: 'Vagas desta conta ocupadas.', attempt_count: 1 });
  return { busy, busyRun };
}

it('keeps an applied resume owned while the real scheduler waits for a slot, without reopening the old failure', async () => {
  const f = await scheduledRevision(), { busy, busyRun } = await occupyAccount(f);
  const before = await task(f.consumer), oldCase = (await pool.query('SELECT * FROM supervision_cases WHERE id=$1', [f.caseId])).rows[0];
  const oldRun = (await pool.query('SELECT instruction_snapshot,context_id FROM runs WHERE id=$1', [f.cr.id])).rows[0];
  const inspect = vi.fn(stop);
  await reconcileDeliveryConsumptions(inspect); await reconcileDeliveryConsumptions(inspect);
  expect(inspect).not.toHaveBeenCalled();
  expect(await row(f.goal)).toMatchObject({ state: 'attached', receipt: { code: 'SUPERVISION_ADMISSION_PENDING', owner: 'scheduler', caseId: f.caseId, sourceRunId: f.cr.id } });
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1', [f.consumer])).rows).toEqual([oldCase]);
  expect(await task(f.consumer)).toEqual(before);
  expect((await pool.query('SELECT instruction_snapshot,context_id FROM runs WHERE id=$1', [f.cr.id])).rows[0]).toEqual(oldRun);
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1', [f.consumer])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1', [f.consumer])).rowCount).toBe(0);
  await completeRun(busyRun.id, busyRun.fence, 'The separate account slot is now free.', {});
  await validateTask(busy);
  const dispatched = await schedule();
  expect(dispatched).toHaveLength(1);
  expect((await pool.query('SELECT task_id,attempt,state FROM runs WHERE id=$1', [dispatched[0]])).rows[0])
    .toEqual({ task_id: f.consumer, attempt: 2, state: 'queued' });
  expect(await schedule()).toEqual([]);
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1', [f.consumer])).rows).toEqual([oldCase]);
});

it('does not erase queued ownership with a stale continuation status or reopen its old stalled run', async () => {
  const f = await scheduledRevision({ code: runtimeProgressCode }); await occupyAccount(f);
  const before = await task(f.consumer), cases = (await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1', [f.consumer])).rows;
  expect(await scheduledSupervisionAdmission(pool, f.consumer, f.cr.id)).toMatchObject({ case_id: f.caseId });
  expect(await setContinuation(f.consumer, f.cr.id, 'checking', 'Old scan checking again.')).toBe(false);
  expect(await requestRecoveryReview(f.consumer, f.cr.id, runtimeProgressCode, 'Old source polling failure.', { stopProof: await stop(f.cr.id) })).toBeNull();
  expect(await task(f.consumer)).toEqual(before);
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1', [f.consumer])).rows).toEqual(cases);
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='CONTINUATION_STATUS'", [f.consumer])).rowCount).toBe(0);
});

it.each([false, true])('revalidates an applied decision during stop inspection with resume=%s', async resume => {
  const f = await scheduledRevision({ apply: false, code: runtimeProgressCode, deferCase: true });
  let authoritativeTask: any, authoritativeCases: any;
  const inspect = vi.fn(async (id: string) => {
    expect(id).toBe(f.cr.id);
    // A preexisting decided case already owns recovery and now correctly
    // prevents inspection. Exercise the actual race: it arrives during I/O.
    await f.insertDecision();
    await applySupervisionDecisions(stop);
    if (resume) { expect(await resumeSupervisedWork(stop)).toEqual([f.consumer]); await occupyAccount(f); }
    authoritativeTask = await task(f.consumer);
    authoritativeCases = (await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1', [f.consumer])).rows;
    return stop(id);
  });
  await new ContinuationSupervisor(inspect).scan();
  expect(inspect).toHaveBeenCalledTimes(1);
  expect(await task(f.consumer)).toEqual(authoritativeTask);
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1', [f.consumer])).rows).toEqual(authoritativeCases);
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1', [f.consumer])).rowCount).toBe(1);
  if (resume) expect(authoritativeTask.execution.continuation.state).toBe('queued');
  else expect(authoritativeCases[0]).toMatchObject({ state: 'applied', applied_attempt: 1 });
});

it('still requests the first stopped-progress review without an applied decision', async () => {
  const f = await fixture(true);
  await pool.query('UPDATE runs SET error_code=$2 WHERE id=$1', [f.cr.id, runtimeProgressCode]);
  await new ContinuationSupervisor(stop).scan();
  expect((await pool.query('SELECT source_run_id,state FROM supervision_cases WHERE task_id=$1', [f.consumer])).rows)
    .toEqual([{ source_run_id: f.cr.id, state: 'open' }]);
  expect((await task(f.consumer)).execution.continuation.state).toBe('supervising');
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1', [f.consumer])).rowCount).toBe(1);
});

it.each(['retry_scheduled', 'ready', 'waiting_resource', 'waiting_configuration'])('preserves the same admission ownership in %s', async state => {
  const f = await scheduledRevision();
  await pool.query('UPDATE tasks SET state=$2 WHERE id=$1', [f.consumer, state]);
  const inspect = vi.fn(stop); await reconcileDeliveryConsumptions(inspect);
  expect(inspect).not.toHaveBeenCalled();
  expect((await row(f.goal)).receipt.code).toBe('SUPERVISION_ADMISSION_PENDING');
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1', [f.consumer])).rowCount).toBe(1);
});

it.each(['source-run', 'case', 'applied-attempt', 'checkpoint', 'workspace', 'continuation', 'integrity'])
('does not treat a mismatched %s as an authorized pending admission', async mode => {
  const f = await scheduledRevision();
  if (mode === 'source-run') await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision,sourceRunId}',to_jsonb($2::text)) WHERE id=$1", [f.consumer, randomUUID()]);
  if (mode === 'case') await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision,caseId}',to_jsonb($2::text)) WHERE id=$1", [f.consumer, randomUUID()]);
  if (mode === 'applied-attempt') await pool.query('UPDATE supervision_cases SET applied_attempt=0 WHERE id=$1', [f.caseId]);
  if (mode === 'checkpoint') await pool.query("UPDATE runs SET checkpoint=jsonb_set(checkpoint,'{sourceRunId}',to_jsonb($2::text)) WHERE id=$1", [f.cr.id, randomUUID()]);
  if (mode === 'workspace') await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}','\"test/different\"') WHERE id=$1", [f.consumer]);
  if (mode === 'continuation') await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{continuation,runId}',to_jsonb($2::text)) WHERE id=$1", [f.consumer, randomUUID()]);
  if (mode === 'integrity') await pool.query("UPDATE runs SET error_code='ARTIFACT_CORRUPT' WHERE id=$1", [f.cr.id]);
  expect(await scheduledSupervisionAdmission(pool, f.consumer, f.cr.id)).toBeNull();
  await reconcileDeliveryConsumptions(stop);
  expect((await row(f.goal)).receipt.code).not.toBe('SUPERVISION_ADMISSION_PENDING');
});

it.each(['cancel', 'pause', 'autonomy', 'manual', 'supervision-disabled', 'pending'])
('does not claim pending admission after %s', async mode => {
  const f = await scheduledRevision();
  if (mode === 'cancel') await event(pool, 'CANCEL_REQUESTED', f.consumer, { runId: f.cr.id });
  if (mode === 'pause') await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
  if (mode === 'autonomy') await pool.query("INSERT INTO settings VALUES('autonomy:consumption-test','{\"enabled\":false}')");
  if (mode === 'manual') await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1", [f.consumer]);
  if (mode === 'supervision-disabled') await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1", ['supervision:' + f.goal]);
  if (mode === 'pending') await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,request_key,parent_case_id)
    VALUES($1,$2,$3,$4,'open','Distinct current evidence','[]','test-next',$5)`, [randomUUID(), f.goal, f.consumer, f.cr.id, f.caseId]);
  const before = await task(f.consumer), cases = (await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1 ORDER BY id', [f.consumer])).rows;
  expect(await scheduledSupervisionAdmission(pool, f.consumer, f.cr.id)).toBeNull();
  await reconcileDeliveryConsumptions(stop);
  expect((await row(f.goal)).receipt.code).not.toBe('SUPERVISION_ADMISSION_PENDING');
  expect(await task(f.consumer)).toEqual(before);
  const after = (await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1 ORDER BY id', [f.consumer])).rows;
  if (mode === 'pending') {
    // The existing open review may accumulate evidence; its predecessor and
    // decision identity remain intact, and no competing review is created.
    expect(after.map(c => ({ id: c.id, state: c.state, sourceRunId: c.source_run_id })))
      .toEqual(cases.map(c => ({ id: c.id, state: c.state, sourceRunId: c.source_run_id })));
    expect(after.find(c => c.id === f.caseId)).toEqual(cases.find(c => c.id === f.caseId));
  } else expect(after).toEqual(cases);
});

it('does not hide a newly failed semantic dependency behind queued supervision', async () => {
  const f = await scheduledRevision(), dependency = randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria)
    VALUES($1,$2,$3,'Changed prerequisite','New actual failure','qa.backend','recovery_blocked','[]')`, [dependency, f.goal, dependency]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.consumer, dependency]);
  await reconcileDeliveryConsumptions(stop);
  expect(await row(f.goal)).toMatchObject({ receipt: { code: 'SUPERVISION_REQUESTED', blockedTaskIds: [dependency] } });
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1', [f.consumer])).rowCount).toBe(2);
});

it('reviews an actual later failed run instead of inheriting the previous pending admission', async () => {
  const f = await scheduledRevision(), next = await run(f.consumer);
  await failRun(next.id, next.fence, 'RUNTIME_EXITED', 'A new execution genuinely failed.');
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  await reconcileDeliveryConsumptions(stop);
  expect((await row(f.goal)).receipt.code).toBe('SUPERVISION_REQUESTED');
  expect((await pool.query("SELECT source_run_id FROM supervision_cases WHERE task_id=$1 AND state='open'", [f.consumer])).rows)
    .toEqual([{ source_run_id: next.id }]);
});

it('does not suppress the recovery requester for a different failed attempt', async () => {
  const f = await scheduledRevision({ code: runtimeProgressCode }), next = await run(f.consumer);
  await failRun(next.id, next.fence, runtimeProgressCode, 'New execution repeated a failed poll.');
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  expect(await scheduledSupervisionAdmission(pool, f.consumer, next.id)).toBeNull();
  expect(await requestRecoveryReview(f.consumer, next.id, runtimeProgressCode, 'New failure', {})).toBeNull();
  const review = await requestRecoveryReview(f.consumer, next.id, runtimeProgressCode, 'New failure', { stopProof: await stop(next.id) });
  expect(review).toMatchObject({ state: 'open' });
  expect((await pool.query('SELECT source_run_id FROM supervision_cases WHERE id=$1', [review!.caseId])).rows[0])
    .toEqual({ source_run_id: next.id });
});

it('rechecks a resume admitted during artifact inspection before opening another case', async () => {
  const f = await scheduledRevision();
  // Remove only the queued marker to start an old-result inspection. Restore
  // the authoritative state while I/O is outside the transaction.
  const queued = (await task(f.consumer)).execution.continuation;
  await pool.query("UPDATE tasks SET execution=execution-'continuation' WHERE id=$1", [f.consumer]);
  let changed = false;
  await reconcileDeliveryConsumptions(stop, async id => {
    if (!changed) { changed = true; await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{continuation}',$2::jsonb) WHERE id=$1", [f.consumer, JSON.stringify(queued)]); }
    return readArtifact(id);
  });
  expect(changed).toBe(true);
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1', [f.consumer])).rowCount).toBe(1);
  await reconcileDeliveryConsumptions(stop);
  expect((await row(f.goal)).receipt.code).toBe('SUPERVISION_ADMISSION_PENDING');
});

it('tracks attached, actual candidate and independent QA without treating repair acceptance as final delivery', async () => {
  const f = await fixture();
  expect(await row(f.goal)).toMatchObject({ state: 'attached' });
  await reconcileDeliveryConsumptions(stop);
  expect((await row(f.goal)).state).toBe('attached');
  const i = await integrated(f); await reconcileDeliveryConsumptions(stop);
  expect(await row(f.goal)).toMatchObject({ state: 'qa_pending', candidate_run_id: i.cr.id, candidate_digest: candidate().candidateDigest });
  const qr = await run(f.qa); await manifest(qr, [{ ...i.candidateRef, task_id: f.consumer }]);
  expect((await publish(qr, { 'candidate.json': candidate(), 'verification.json': { passed: true } })).passed).toBe(true);
  await reconcileDeliveryConsumptions(stop);
  const completed = await row(f.goal);
  expect(completed).toMatchObject({ state: 'completed', qa_run_id: qr.id, receipt: { code: 'INDEPENDENT_QA_ACCEPTED', resultKind: 'candidate_and_independent_qa' } });
  const counts = (await pool.query('SELECT (SELECT count(*) FROM runs) runs,(SELECT count(*) FROM acceptances) acceptances,(SELECT count(*) FROM events) events')).rows[0];
  await reconcileDeliveryConsumptions(stop); await reconcileDeliveryConsumptions(stop);
  expect(await row(f.goal)).toEqual(completed);
  expect((await pool.query('SELECT (SELECT count(*) FROM runs) runs,(SELECT count(*) FROM acceptances) acceptances,(SELECT count(*) FROM events) events')).rows[0]).toEqual(counts);
});

it.each(['incomplete','extra-field','wrong-fence','duplicate'] as const)('keeps delivery uncomposed despite accepted outputs when runtime proof is %s',async change=>{
 const f=await fixture(),i=await integrated(f,true,true,change!=='incomplete');
 if(change==='extra-field'||change==='wrong-fence')await pool.query(`UPDATE events SET payload=payload||$2::jsonb
  WHERE kind='PATCH_COMPOSITION_RECORDED' AND entity_id=$1 AND payload->>'runId'=$3`,
  [f.consumer,JSON.stringify(change==='extra-field'?{unknownSemanticField:true}:{runFence:randomUUID()}),i.cr.id]);
 if(change==='duplicate')await event(pool,'PATCH_COMPOSITION_RECORDED',f.consumer,{runId:i.cr.id,...i.composition});
 const qr=await run(f.qa);await manifest(qr,[{...i.candidateRef,task_id:f.consumer}]);
 expect((await publish(qr,{'candidate.json':candidate(),'verification.json':{passed:true}})).passed).toBe(true);
 const accepts=(await pool.query('SELECT id,passed FROM acceptances ORDER BY id')).rows;
 await reconcileDeliveryConsumptions(stop);
 expect(await row(f.goal)).toMatchObject({state:'blocked',candidate_run_id:null,receipt:{code:'COMPOSITION_PROOF_REQUIRED'}});
 expect((await pool.query('SELECT id,passed FROM acceptances ORDER BY id')).rows).toEqual(accepts);
});

it('creates a single competent review for a stopped failed consumer, without retry or fake acceptance', async () => {
  const f = await fixture(true), before = await task(f.consumer);
  await Promise.all([reconcileDeliveryConsumptions(stop), reconcileDeliveryConsumptions(stop)]);
  await reconcileDeliveryConsumptions(stop);
  expect(await row(f.goal)).toMatchObject({ state: 'waiting_supervision', receipt: { code: 'SUPERVISION_REQUESTED' } });
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1', [f.consumer])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1', [f.consumer])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1', [f.consumer])).rowCount).toBe(0);
  expect(await task(f.consumer)).toEqual(before);
});

it('creates an evidence-bound successor after an applied case, preserving its immutable decision and deduplicating polls', async () => {
  const f = await fixture(true), id = randomUUID();
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision,applied_attempt)
    VALUES($1,$2,$3,$4,'applied','Earlier diagnosis','[]','{"actions":[]}',1)`, [id, f.goal, f.consumer, f.cr.id]);
  const old = (await pool.query('SELECT * FROM supervision_cases WHERE id=$1', [id])).rows[0];
  await reconcileDeliveryConsumptions(stop); await reconcileDeliveryConsumptions(stop);
  const cases = (await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1 ORDER BY created_at,id', [f.consumer])).rows;
  expect(cases).toHaveLength(2); expect(cases.find(c => c.id === id)).toEqual(old);
  expect(cases.find(c => c.id !== id)).toMatchObject({ source_run_id: f.cr.id, parent_case_id: id, state: 'open' });
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1', [f.consumer])).rowCount).toBe(1);
});

it.each(['pause', 'autonomy', 'cancel', 'live', 'stale-stop', 'manual'] as const)('preserves the consumer when %s prevents autonomous continuation', async change => {
  const f = await fixture(true);
  if (change === 'pause') await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
  if (change === 'autonomy') await pool.query("INSERT INTO settings VALUES('autonomy:consumption-test','{\"enabled\":false}')");
  if (change === 'cancel') await event(pool, 'CANCEL_REQUESTED', f.consumer, { runId: f.cr.id });
  if (change === 'live') await run(f.consumer);
  if (change === 'manual') await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1", [f.consumer]);
  const before = await task(f.consumer), inspect = vi.fn(async (id: string) => ({ ...await stop(id), ...(change === 'stale-stop' ? { checkedAt: '2000-01-01T00:00:00Z' } : {}) }));
  await reconcileDeliveryConsumptions(inspect);
  expect(await task(f.consumer)).toEqual(before);
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1', [f.consumer])).rowCount).toBe(0);
  expect((await row(f.goal)).state).not.toBe('completed');
});

it.each(['candidate', 'source-binding', 'composition-event', 'composition-hash', 'criteria', 'semantic-input', 'producer-criteria', 'producer-semantic-input'] as const)('does not certify final consumption with changed %s', async change => {
  const f = await fixture(), i = await integrated(f, true, change !== 'composition-event');
  if (change === 'composition-hash') await pool.query("UPDATE artifacts SET digest=$2 WHERE run_id=$1 AND name='runtime-composition.json'", [i.cr.id, sha('different')]);
  if (change === 'criteria') await pool.query("UPDATE tasks SET criteria=criteria||'[ {\"id\":\"extra\",\"type\":\"artifact\",\"name\":\"new.json\"} ]'::jsonb WHERE id=$1", [f.consumer]);
  if (change === 'semantic-input') await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{recoveryContext}','{\"instruction\":\"A different semantic task\"}') WHERE id=$1", [f.consumer]);
  if (change === 'producer-criteria') await pool.query("UPDATE tasks SET criteria=criteria||'[ {\"id\":\"extra\",\"type\":\"artifact\",\"name\":\"new.json\"} ]'::jsonb WHERE id=$1", [f.producer]);
  if (change === 'producer-semantic-input') await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{recoveryContext}','{\"instruction\":\"A different source contract\"}') WHERE id=$1", [f.producer]);
  const qr = await run(f.qa);
  await manifest(qr, change === 'source-binding' ? [] : [{ ...i.candidateRef, task_id: f.consumer }]);
  await publish(qr, { 'candidate.json': candidate(change === 'candidate' ? 'other source\n' : 'fixed\n'), 'verification.json': { passed: true } });
  await reconcileDeliveryConsumptions(stop);
  expect((await row(f.goal)).state).not.toBe('completed');
});

it('does not let artifact I/O hold the global lock and rejects an acceptance change while reading', async () => {
  const f = await fixture(), i = await integrated(f);
  let touched = false;
  await reconcileDeliveryConsumptions(stop, async id => {
    await tx(async db => { await db.query("SET LOCAL lock_timeout='250ms'"); await db.query('SELECT pg_advisory_xact_lock(4280001)'); });
    if (!touched) { touched = true; await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1', [i.cr.id]); }
    return readArtifact(id);
  });
  expect(touched).toBe(true); expect((await row(f.goal)).candidate_run_id).toBeNull();
});

it('can complete a generic consumer from its actual bound input and acceptance without labelling it QA', async () => {
  const f = await fixture(false, true), cr = await run(f.consumer);
  await manifest(cr, f.refs.map(a => ({ ...a, task_id: f.producer })));
  await publish(cr, { 'verification.json': { passed: true } }); await reconcileDeliveryConsumptions(stop);
  expect(await row(f.goal)).toMatchObject({ state: 'completed', candidate_digest: null, qa_run_id: null,
    receipt: { code: 'CONSUMER_ACCEPTED', resultKind: 'consumer_result' } });
});

it('keeps a decided case intact instead of injecting a second competing decision', async () => {
  const f = await fixture(true), id = randomUUID();
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision)
    VALUES($1,$2,$3,$4,'decided','Current decision','[]','{"actions":[]}')`, [id, f.goal, f.consumer, f.cr.id]);
  const before = (await pool.query('SELECT * FROM supervision_cases WHERE id=$1', [id])).rows[0];
  await reconcileDeliveryConsumptions(stop);
  expect((await pool.query('SELECT * FROM supervision_cases WHERE id=$1', [id])).rows[0]).toEqual(before);
  expect((await row(f.goal)).receipt.code).toBe('SUPERVISION_DECISION_PENDING');
});

it('does not reopen a consumed evidence generation or create another case from a renamed planner action', async () => {
  const f = await fixture(true), target = await task(f.consumer), latest = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.cr.id])).rows[0];
  const evidence = { actionKey: randomUUID(), action: { kind: 'prepare_integration', artifactIds: f.refs.map(r => r.id), reason: 'First wording' },
    sourceProof: { runId: f.pr.id, digest: f.refs[0].digest } };
  const first = await tx(db => requestDeliverySupervision(db, { id: f.goal }, target, latest, evidence));
  await pool.query("UPDATE supervision_cases SET state='applied',applied_attempt=1 WHERE id=$1", [first.caseId]);
  const prior = (await pool.query('SELECT * FROM supervision_cases WHERE id=$1', [first.caseId])).rows[0];
  const second = await tx(db => requestDeliverySupervision(db, { id: f.goal }, target, latest,
    { ...evidence, actionKey: randomUUID(), action: { ...evidence.action, reason: 'Different wording, same evidence' } }, { stopProof: undefined }));
  expect(second.code).toBe('SUPERVISION_GENERATION_RECORDED');
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1', [f.consumer])).rows).toEqual([prior]);
});

it('escalates a failed dependency to supervision instead of merely waiting for another scheduler tick', async () => {
  const f = await fixture(true), other = randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria)
    VALUES($1,$2,$3,'Needed dependency','An actual blocked prerequisite','qa.backend','recovery_blocked','work','[]')`, [other, f.goal, 'dependency-' + other]);
  await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)', [f.consumer, other]);
  await reconcileDeliveryConsumptions(stop);
  expect(await row(f.goal)).toMatchObject({ state: 'waiting_supervision', receipt: { code: 'SUPERVISION_REQUESTED', blockedTaskIds: [other] } });
  expect((await pool.query('SELECT evidence FROM supervision_cases WHERE task_id=$1', [f.consumer])).rows[0].evidence[0].blockedTaskIds).toEqual([other]);
  expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2', [f.consumer, other])).rowCount).toBe(1);
  expect((await task(other)).state).toBe('recovery_blocked');
});

async function replacementRepair(f:Awaited<ReturnType<typeof fixture>>,legacy=false,diagnostic=false){
 const id=randomUUID();
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
 SELECT $1::uuid,goal_id,$3,'Recertified repair',description,agent_id,'ready','work',criteria,
 jsonb_set(execution,'{workspace}',to_jsonb($4::text)) FROM tasks WHERE id=$2`,[id,f.producer,'replacement-'+id,'test/o-'+id]);
 if(legacy)await pool.query("UPDATE tasks SET criteria=(SELECT jsonb_agg(c) FROM jsonb_array_elements(criteria)c WHERE c->>'type'<>'repair_source_proof') WHERE id=$1",[id]);
 if(diagnostic)await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workKind}','\"diagnostic\"'::jsonb) WHERE id=$1",[id]);
 const r=await run(id);
 expect((await publish(r,{'implementation.patch':'--- a/src/app.js\n+++ b/src/app.js\n@@ -1 +1 @@\n-original\n+fixed\n',
 'repair-report.json':{fixVerified:true,sourceHashes:legacy?{'src/app.js@before':sha('original\n'),'src/app.js@after':sha('fixed\n')}:[{path:'src/app.js',before:sha('original\n'),after:sha('fixed\n')}]},
 'repair-tests.log':'before exit 1; after exit 0'})).passed).toBe(true);
 return {id,r,refs:(await pool.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[r.id])).rows};
}
async function conflictingSource(){
 const f=await fixture(true);
 await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{recoveryContext}','{"instruction":"Recertify changed recovery input"}') WHERE id=$1`,[f.producer]);
 await reconcileDeliveryConsumptions(stop);
 const d=await row(f.goal),c=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[d.receipt.caseId])).rows[0];
 expect(d).toMatchObject({state:'waiting_supervision',receipt:{sourceConflict:{code:'SOURCE_RECOVERY_CONTEXT_CHANGED'},code:'SUPERVISION_REQUESTED'}});
 return {...f,d,c};
}
async function sourceDecision(f:Awaited<ReturnType<typeof conflictingSource>>,replacement:Awaited<ReturnType<typeof replacementRepair>>,overrides:any={}){
 const action={caseId:f.c.id,kind:'revise',diagnosis:'Original source proof differs; this executed repair recertifies the required delta.',
 instruction:'Use the newly accepted frozen repair, compare its pre-image and integrate only the compatible delta. Keep independent QA mandatory.',
 sourceTaskId:replacement.id,deliveryResolution:{obligationId:f.d.id,generation:f.d.receipt.sourceConflict.generation,expectedSourceRunId:replacement.r.id},...overrides};
 const manager=randomUUID();
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria)
 VALUES($1,$2,$3,'Source decision','Resolve one provenance conflict','qa.backend','ready','coordinate','[{"id":"decision","type":"artifact","name":"coordination.json"}]')`,[manager,f.goal,'source-manager-'+manager]);
 const decision=await run(manager);await publish(decision,{'coordination.json':{supervision:{actions:[action]}}});
 await recordSupervisionDecisions();return {action,decision};
}
it('turns source contract drift into one actionable case without changing the accepted producer or original pins',async()=>{
 const f=await conflictingSource(),producer=await task(f.producer),pins=(await pool.query('SELECT binding,producer_run_id,producer_manifest_digest FROM delivery_consumptions WHERE id=$1',[f.d.id])).rows[0];
 await Promise.all([reconcileDeliveryConsumptions(stop),reconcileDeliveryConsumptions(stop)]);
 expect((await pool.query('SELECT count(*)::int n FROM supervision_cases WHERE task_id=$1',[f.consumer])).rows[0].n).toBe(1);
 expect(await task(f.producer)).toEqual(producer);
 expect((await pool.query('SELECT binding,producer_run_id,producer_manifest_digest FROM delivery_consumptions WHERE id=$1',[f.d.id])).rows[0]).toEqual(pins);
 expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=$1',[f.pr.id])).rows[0].passed).toBe(true);
});
it('classifies absent legacy snapshot separately from recovery context drift and a superseded pinned run',()=>{
 const id=randomUUID(),rid=randomUUID(),d={producer_task_id:id,producer_run_id:rid,producer_manifest_digest:'none'};
 const identity={id,run_id:rid,state:'accepted',run_state:'submitted',attempt:1,attempt_count:1,passed:true};
 const pinned={...d,producer_manifest_digest:sha('[]')};
 expect(deliverySourceConflict(pinned,{...identity,snapshot_schema:null},[])?.code).toBe('SOURCE_SNAPSHOT_MISSING');
 expect(deliverySourceConflict(pinned,{...identity,run_id:randomUUID()},[])?.code).toBe('SOURCE_RUN_CHANGED');
});
it('waits for the exact live consumer before requesting source recertification',async()=>{
 const f=await fixture(),cr=await run(f.consumer),before=await task(f.consumer);
 await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{recoveryContext}','{"instruction":"Changed source"}') WHERE id=$1`,[f.producer]);
 const inspector=vi.fn(stop);await reconcileDeliveryConsumptions(inspector);
 expect(inspector).not.toHaveBeenCalled();expect(await task(f.consumer)).toEqual(before);
 expect((await row(f.goal)).receipt).toMatchObject({code:'SOURCE_REVIEW_WAIT_CONSUMER',consumerRunId:cr.id});
 expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.consumer])).rowCount).toBe(0);
});
it('rejects a textual source revision before deciding the case, leaving an actionable reason',async()=>{
 const f=await conflictingSource(),replacement=await replacementRepair(f);
 await sourceDecision(f,replacement,{deliveryResolution:undefined});await applySupervisionDecisions(stop);
 expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0].state).toBe('open');
 expect((await pool.query("SELECT payload->>'code' code FROM events WHERE kind='SUPERVISION_PLAN_REJECTED' AND payload->>'caseId'=$1",[f.c.id])).rows[0].code).toBe('DELIVERY_RESOLUTION_REQUIRED');
});
it('applies an explicit immutable source substitution then requires real composition and independent candidate QA',async()=>{
 const f=await conflictingSource(),replacement=await replacementRepair(f),pins=(await pool.query('SELECT binding,producer_run_id,producer_manifest_digest FROM delivery_consumptions WHERE id=$1',[f.d.id])).rows[0];
 const {action}=await sourceDecision(f,replacement);await applySupervisionDecisions(stop);await applySupervisionDecisions(stop);
 expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0].state).toBe('applied');
 const receipt=(await pool.query("SELECT payload FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1",[f.d.id])).rows;
 expect(receipt).toHaveLength(1);expect(receipt[0].payload).toMatchObject({acceptance:false,compositionRequired:true,independentQARequired:true,replacement:{producerTaskId:replacement.id,producerRunId:replacement.r.id}});
 await tx(db=>recordDeliverySourceResolution(db,f.c.id,action,{taskId:replacement.id,runId:replacement.r.id},undefined));
 await reconcileDeliveryConsumptions(stop);expect((await row(f.goal)).state).not.toBe('completed');
 const i=await integrated({...f,producer:replacement.id,refs:replacement.refs});await reconcileDeliveryConsumptions(stop);
 expect((await row(f.goal)).state).toBe('qa_pending');
 const qr=await run(f.qa);await manifest(qr,[{...i.candidateRef,task_id:f.consumer}]);await publish(qr,{'candidate.json':candidate(),'verification.json':{passed:true}});
 await reconcileDeliveryConsumptions(stop);expect((await row(f.goal)).state).toBe('completed');
 expect((await pool.query('SELECT binding,producer_run_id,producer_manifest_digest FROM delivery_consumptions WHERE id=$1',[f.d.id])).rows[0]).toEqual(pins);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1",[f.d.id])).rowCount).toBe(1);
});
it.each(['source-drift','old-generation','cancel','stale-stop','consumer-live','pause'] as const)('refuses source substitution atomically when %s changes before apply',async(mode)=>{
 const f=await conflictingSource(),replacement=await replacementRepair(f);await sourceDecision(f,replacement);
 if(mode==='source-drift')await pool.query("UPDATE tasks SET description=description||' altered' WHERE id=$1",[replacement.id]);
 if(mode==='old-generation')await pool.query("UPDATE tasks SET description=description||' newer conflict' WHERE id=$1",[f.producer]);
 if(mode==='cancel')await event(pool,'CANCEL_REQUESTED',f.consumer,{runId:f.cr.id});
 if(mode==='consumer-live')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.cr.id]);
 if(mode==='pause'){
   // Pause precedes restoring objective eligibility, so the pending-case hold
   // does not replace the task pause while constructing this negative fixture.
   await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
   await pool.query("UPDATE tasks SET state='paused' WHERE id=$1",[f.consumer]);
   await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
 }
 await applySupervisionDecisions(async id=>({...await stop(id),...(mode==='stale-stop'?{checkedAt:'2000-01-01T00:00:00Z'}:{})}));
 expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1",[f.d.id])).rowCount).toBe(0);
 expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0].state).toBe(['source-drift','old-generation'].includes(mode)?'open':'decided');
 expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2',[f.consumer,replacement.id])).rowCount).toBe(0);
 if(['source-drift','old-generation'].includes(mode)){
   await recordSupervisionDecisions();
   expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0].state).toBe('open');
   expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_SOURCE_DECISION_REJECTED' AND payload->>'caseId'=$1",[f.c.id])).rowCount).toBe(1);
 }
});
it('routes new drift after an explicit resolution to a successor without rewriting that resolution',async()=>{
 const f=await conflictingSource(),replacement=await replacementRepair(f);await sourceDecision(f,replacement);await applySupervisionDecisions(stop);
 const original=(await pool.query("SELECT * FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1",[f.d.id])).rows;
 await pool.query("UPDATE tasks SET description=description||' changed later' WHERE id=$1",[replacement.id]);
 await reconcileDeliveryConsumptions(stop);
 expect((await row(f.goal)).receipt.sourceConflict.code).toBe('SOURCE_CONTRACT_CHANGED');
 expect((await pool.query('SELECT state,parent_case_id FROM supervision_cases WHERE task_id=$1 ORDER BY created_at DESC LIMIT 1',[f.consumer])).rows[0]).toEqual({state:'open',parent_case_id:f.c.id});
 expect((await pool.query("SELECT * FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1",[f.d.id])).rows).toEqual(original);
});
it('uses accepted recertification supports as new successor evidence instead of reopening an applied delegation',async()=>{
 const f=await conflictingSource(),replacement=await replacementRepair(f);
 await pool.query("UPDATE supervision_cases SET state='applied',applied_attempt=1,support_task_ids=$2 WHERE id=$1",[f.c.id,JSON.stringify([replacement.id])]);
 const previous=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0];
 const followthrough=await deliverySourceFollowthrough(pool,f.d,f.d.receipt.sourceConflict.generation);
 expect(followthrough.recertificationSources).toMatchObject([{taskId:replacement.id,runId:replacement.r.id}]);
 await reconcileDeliveryConsumptions(stop);await reconcileDeliveryConsumptions(stop);
 expect((await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0]).toEqual(previous);
 expect((await pool.query('SELECT state,parent_case_id FROM supervision_cases WHERE task_id=$1 AND id<>$2',[f.consumer,f.c.id])).rows).toEqual([{state:'open',parent_case_id:f.c.id}]);
});
it('does not erase a newer source decision when an older apply loses its CAS',async()=>{
 const f=await conflictingSource(),replacement=await replacementRepair(f),old=await sourceDecision(f,replacement);
 const next=await sourceDecision(f,replacement);
 await pool.query('UPDATE supervision_cases SET decision_run_id=$2,decision=$3 WHERE id=$1',[f.c.id,next.decision.id,JSON.stringify({actions:[next.action]})]);
 const current=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0];
 expect(await rejectDeliverySourceDecision(f.c.id,old.decision.id,'DELIVERY_RESOLUTION_SOURCE_UNPROVEN')).toBe(false);
 expect((await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0]).toEqual(current);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_SOURCE_DECISION_REJECTED' AND payload->>'caseId'=$1",[f.c.id])).rowCount).toBe(0);
});
it('keeps pending ownership while a real completed delegation awaits exact source selection',async()=>{
 const f=await conflictingSource(),replacement=await replacementRepair(f);
 await sourceDecision(f,replacement);
 await pool.query("UPDATE supervision_cases SET state='delegated',support_task_id=$2,support_task_ids=$3 WHERE id=$1",[f.c.id,replacement.id,JSON.stringify([replacement.id])]);
 await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[f.consumer,replacement.id]);
 await detectSupervisionCases(stop);
 const reopened=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0];
 expect(reopened).toMatchObject({state:'open',decision_run_id:null,support_task_id:null,support_task_ids:[]});
 expect(reopened.evidence).toContainEqual(expect.objectContaining({kind:'DELIVERY_RECERTIFICATION_READY',sources:[expect.objectContaining({taskId:replacement.id,runId:replacement.r.id,currentFrozen:true})]}));
 expect(await resumeSupervisedWork(stop)).toEqual([]);
 expect((await task(f.consumer)).state).toBe('recovery_blocked');
 expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.consumer])).rows[0].n).toBe(1);
 await sourceDecision(f,replacement);await applySupervisionDecisions(stop);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1",[f.d.id])).rowCount).toBe(1);
});
it.each(['next-attempt','autonomy'] as const)('does not reopen an unapplied resolution after %s changed',async(mode)=>{
 const f=await conflictingSource(),replacement=await replacementRepair(f),decision=await sourceDecision(f,replacement);
 if(mode==='autonomy')await pool.query("INSERT INTO settings VALUES('autonomy:consumption-test','{\"enabled\":false}')");
 else{const next=await run(f.consumer);await failRun(next.id,next.fence,'RESULT_FORMAT','A later attempt ended.');}
 const before=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0];
 expect(await rejectDeliverySourceDecision(f.c.id,decision.decision.id,'DELIVERY_RESOLUTION_SOURCE_UNPROVEN')).toBe(false);
 expect((await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0]).toEqual(before);
});
it('resolving one source does not resolve another obligation sharing its case and creates a bound successor',async()=>{
 const f=await conflictingSource(),second=await replacementRepair(f);
 await tx(async db=>stageDeferredHandoff(db,f.goal,second.id,await task(second.id),await task(f.consumer),
  {taskId:second.id,plannerRunId:second.r.id,coverageKey:'second-source',lane:'glm',workKind:'repair'}));
 const h=(await pool.query('SELECT * FROM deferred_handoffs WHERE producer_task_id=$1',[second.id])).rows[0];
 expect(await applyDeferredHandoff(h.id,await stop(f.cr.id),await stop(second.r.id))).toBe(true);
 await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{recoveryContext}','{"instruction":"Other changed source"}') WHERE id=$1`,[second.id]);
 await reconcileDeliveryConsumptions(stop);
 const secondRow=(await pool.query('SELECT * FROM delivery_consumptions WHERE producer_task_id=$1',[second.id])).rows[0];
 expect(secondRow.receipt.caseId).toBe(f.c.id);
 const replacement=await replacementRepair(f);await sourceDecision(f,replacement);await applySupervisionDecisions(stop);
 const applied=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0];
 expect(applied.state).toBe('applied');
 await reconcileDeliveryConsumptions(stop);
 const remaining=(await pool.query('SELECT * FROM delivery_consumptions WHERE id=$1',[secondRow.id])).rows[0];
 expect(remaining).toMatchObject({state:'waiting_supervision',producer_task_id:second.id,producer_run_id:second.r.id,binding:secondRow.binding});
 expect(remaining.receipt.caseId).not.toBe(f.c.id);
 expect((await pool.query('SELECT parent_case_id,state FROM supervision_cases WHERE id=$1',[remaining.receipt.caseId])).rows[0]).toEqual({parent_case_id:f.c.id,state:'open'});
 expect((await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0]).toEqual(applied);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1",[secondRow.id])).rowCount).toBe(0);
});

it('preserves a rejected legacy source proof cause through rollback and actionable supervision evidence without altering old acceptance',async()=>{
 const f=await conflictingSource(),replacement=await replacementRepair(f,true);
 const before=await task(f.consumer),accepted=(await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[replacement.r.id])).rows;
 const {decision}=await sourceDecision(f,replacement);await applySupervisionDecisions(stop);
 const current=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.c.id])).rows[0];
 expect(current.state).toBe('open');expect(current.decision_run_id).toBeNull();
 expect(current.evidence).toContainEqual(expect.objectContaining({kind:'DELIVERY_SOURCE_DECISION_REJECTED',
   code:'DELIVERY_RESOLUTION_PROOF_COVERAGE',replacementEligibleAtRejection:false,
   proofFailure:{version:1,code:'DELIVERY_RESOLUTION_PROOF_COVERAGE',sourceTaskId:replacement.id,sourceRunId:replacement.r.id}}));
 const receipt=(await pool.query("SELECT payload FROM events WHERE kind='DELIVERY_SOURCE_DECISION_REJECTED' AND payload->>'caseId'=$1",[f.c.id])).rows[0].payload;
 expect(receipt).toMatchObject({decisionRunId:decision.id,code:'DELIVERY_RESOLUTION_PROOF_COVERAGE',expectedSourceRunId:replacement.r.id});
 expect(receipt.nextAction).toContain('workKind:"repair"');expect(receipt.nextAction).toContain('sourceHashes:[{path,before,after}]');
 expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[replacement.r.id])).rows).toEqual(accepted);
 expect((await task(f.consumer)).criteria).toEqual(before.criteria);
 expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2',[f.consumer,replacement.id])).rowCount).toBe(0);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1",[f.d.id])).rowCount).toBe(0);
});
it('marks only a strictly proved recertification repair eligible while preserving legacy and diagnostic sources as useful but insufficient',async()=>{
 const f=await conflictingSource(),valid=await replacementRepair(f),legacy=await replacementRepair(f,true),diagnostic=await replacementRepair(f,false,true);
 await pool.query("UPDATE supervision_cases SET state='applied',applied_attempt=1,support_task_ids=$2 WHERE id=$1",[f.c.id,JSON.stringify([valid.id,legacy.id,diagnostic.id])]);
 const io=vi.spyOn(deliveryArtifactService,'readArtifact').mockRejectedValue(Error('UNEXPECTED_ARTIFACT_IO_UNDER_LOCK'));
 const result=await deliverySourceFollowthrough(pool,f.d,f.d.receipt.sourceConflict.generation);
 expect(io).not.toHaveBeenCalled();
 expect(result.recertificationSources.find((s:any)=>s.taskId===valid.id)).toMatchObject({eligibleReplacement:true,replacementProof:{verified:true,liveAuthorization:false}});
 expect(result.recertificationSources.find((s:any)=>s.taskId===legacy.id)).toMatchObject({eligibleReplacement:false,replacementProof:{code:'DELIVERY_RESOLUTION_PROOF_NOT_ATTESTED',status:'unknown'}});
 expect(result.recertificationSources.find((s:any)=>s.taskId===diagnostic.id)).toMatchObject({eligibleReplacement:false,replacementProof:{code:'DELIVERY_RESOLUTION_REPAIR_REQUIRED'}});
});

async function endedExecutingConsumer(){
 const f=await fixture(false,true),cr=await run(f.consumer);
 await manifest(cr,f.refs.map(a=>({...a,task_id:f.producer})));
 await reconcileDeliveryConsumptions(stop);
 expect(await row(f.goal)).toMatchObject({state:'executing',receipt:{code:'CONSUMER_ACTIVE'}});
 expect((await publish(cr,{'verification.json':{passed:true}})).passed).toBe(true);
 return {...f,cr,obligation:await row(f.goal)};
}

it('observes an ended executing consumer through the unchanged bound-proof path and completes it exactly once',async()=>{
 const f=await endedExecutingConsumer();
 const acceptances=(await pool.query('SELECT * FROM acceptances WHERE task_id=$1 ORDER BY id',[f.consumer])).rows;
 expect((await deliverySelection.selectDeliveryConsumptionBatch(pool)).map(r=>r.id)).toContain(f.obligation.id);
 await Promise.all([reconcileDeliveryConsumptions(stop),reconcileDeliveryConsumptions(stop)]);
 expect(await row(f.goal)).toMatchObject({state:'completed',receipt:{code:'CONSUMER_ACCEPTED',
  consumerRunId:f.cr.id,acceptanceId:acceptances[0].id,resultKind:'consumer_result'}});
 expect((await pool.query(`SELECT 1 FROM events WHERE kind='DELIVERY_CONSUMPTION_CHANGED'
  AND payload->>'obligationId'=$1 AND payload->>'state'='completed'`,[f.obligation.id])).rowCount).toBe(1);
 expect((await pool.query('SELECT * FROM acceptances WHERE task_id=$1 ORDER BY id',[f.consumer])).rows).toEqual(acceptances);
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.consumer])).rowCount).toBe(1);
});

it.each(['new_live_attempt','lease'] as const)('reobserves %s arriving after priority selection, without consumption or stop authority',async change=>{
 const f=await endedExecutingConsumer(),select=deliverySelection.selectDeliveryConsumptionBatch;
 const acceptances=(await pool.query('SELECT * FROM acceptances WHERE task_id=$1 ORDER BY id',[f.consumer])).rows;
 let changed=false,newRun:any;
 vi.spyOn(deliverySelection,'selectDeliveryConsumptionBatch').mockImplementationOnce(async db=>{
  const selected=await select(db);expect(selected.map(r=>r.id)).toContain(f.obligation.id);
  if(change==='new_live_attempt')newRun=await run(f.consumer);
  else await pool.query("INSERT INTO leases(resource,run_id) VALUES('selection-race',$1)",[f.cr.id]);
  changed=true;return selected;
 });
 const inspector=vi.fn(stop);await reconcileDeliveryConsumptions(inspector);
 expect(changed).toBe(true);expect(inspector).not.toHaveBeenCalled();
 expect(await row(f.goal)).toMatchObject({state:'executing',receipt:{code:change==='lease'?'CONSUMER_STOP_UNCONFIRMED':'CONSUMER_ACTIVE'}});
 expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.consumer])).rowCount).toBe(0);
 expect((await pool.query('SELECT * FROM acceptances WHERE task_id=$1 ORDER BY id',[f.consumer])).rows).toEqual(acceptances);
 if(newRun)expect((await pool.query('SELECT state FROM runs WHERE id=$1',[newRun.id])).rows[0].state).toBe('running');
 else expect((await pool.query("SELECT 1 FROM leases WHERE resource='selection-race' AND run_id=$1",[f.cr.id])).rowCount).toBe(1);
});

it('keeps the existing observation stamp guard when a lease arrives during artifact verification after priority selection',async()=>{
 const f=await endedExecutingConsumer();let changed=false;
 const before=await row(f.goal),events=(await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_CONSUMPTION_CHANGED' AND payload->>'obligationId'=$1",[f.obligation.id])).rowCount;
 await reconcileDeliveryConsumptions(stop,async artifactId=>{
  if(!changed){changed=true;await pool.query("INSERT INTO leases(resource,run_id) VALUES('verification-race',$1)",[f.cr.id]);}
  return readArtifact(artifactId);
 });
 expect(changed).toBe(true);
 expect(await row(f.goal)).toMatchObject({state:before.state,receipt:before.receipt});
 expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_CONSUMPTION_CHANGED' AND payload->>'obligationId'=$1",[f.obligation.id])).rowCount).toBe(events);
 expect((await pool.query("SELECT 1 FROM leases WHERE resource='verification-race' AND run_id=$1",[f.cr.id])).rowCount).toBe(1);
});

it.each(['artifact_bytes','frozen_instruction'] as const)('does not infer consumption from terminal priority when%s is invalid',async invalid=>{
 const f=await endedExecutingConsumer();
 const accepted=(await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.cr.id])).rows;
 if(invalid==='frozen_instruction')await pool.query('UPDATE tasks SET description=description||$2 WHERE id=$1',[f.consumer,' Changed after acceptance.']);
 await reconcileDeliveryConsumptions(stop,async artifactId=>{
  const source=await readArtifact(artifactId);
  return invalid==='artifact_bytes'?{...source,bytes:Buffer.from('different unverified bytes')}:source;
 });
 expect(await row(f.goal)).toMatchObject({state:'blocked',receipt:{code:invalid==='artifact_bytes'?'DELIVERY_ARTIFACT_CHANGED':'CONSUMER_CONTRACT_CHANGED'}});
 expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.cr.id])).rows).toEqual(accepted);
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.consumer])).rowCount).toBe(1);
});
