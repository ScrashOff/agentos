import { beforeAll, beforeEach, afterAll, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { recoverOpenCodeRun } from '../server/opencode-recovery.js';
import { installWorkforcePolicy } from '../server/workforce-policy.js';
import { installSupervision, recordSupervisionDecisions, applySupervisionDecisions } from '../server/supervision.js';
import { requestRecoveryReview } from '../server/recovery-supervision.js';
import { writeArtifact, completeRun, validateTask } from '../server/service.js';

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
});
afterAll(async () => {
  await pool.end();
});
async function fixture() {
  const goal = randomUUID(),
    task = randomUUID(),
    run = randomUUID(),
    fence = randomUUID();
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown) VALUES('opencode-test','test','openai','glm','[\"glm\"]',true,5,true)",
  );
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'test','test','isolated',$2)",
    [goal, 'goal-' + goal],
  );
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,state,criteria,execution,resources,attempt_count,max_attempts,recovery) VALUES($1,$2,'test','test','test','qa.backend','opencode-test','reconciling','[]','{\"kind\":\"opencode\",\"workspace\":\"isolated/test\"}','[\"remote-slot\",\"workspace:isolated/test\"]',1,1,'{\"mode\":\"manual\"}')",
    [task, goal],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state,error_code) VALUES($1,$2,1,$3,'opencode-test','glm','opencode',1,'reconciling','STOP_UNCONFIRMED')",
    [run, task, fence],
  );
  await pool.query(
    "INSERT INTO leases(resource,run_id) VALUES('remote-slot',$1),('workspace:isolated/test',$1)",
    [run],
  );
  return {
    goal,
    task,
    run,
    fence,
    proof: {
      runId: run,
      runtime: 'opencode',
      workspace: 'isolated/test',
      checkedAt: new Date().toISOString(),
      state: { originalCode: 'MODEL_REQUEST_FAILED', usage: null },
      candidate: { patch: '', files: [] },
      process: {
        unit: 'agentos-run-' + run,
        activeState: 'inactive',
        mainPid: 0,
        populated: false,
        stopped: true,
      },
    },
  };
}

async function collectedFixture(){
 const f=await fixture(),collected=randomUUID(),fence=randomUUID();
 await pool.query("UPDATE runs SET state='failed',error_code='RESULT_FORMAT' WHERE id=$1",[f.run]);
 await pool.query('DELETE FROM leases WHERE run_id=$1',[f.run]);
 await pool.query("UPDATE tasks SET attempt_count=2,max_attempts=2,state='failed' WHERE id=$1",[f.task]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state,error_code,checkpoint)
  VALUES($1,$2,2,$3,NULL,NULL,'opencode',1,'failed','PLANNING_CONTRACT_INVALID',$4)`,
  [collected,f.task,fence,JSON.stringify({kind:'artifact_recovery',sourceRunId:f.run})]);
 return {...f,collected,collectedFence:fence,proof:{...f.proof,runId:collected,executionRunId:f.run}};
}

it('resumes a collected native attempt against the original stopped process and preserves both immutable attempts',async()=>{
 const f=await collectedFixture();
 const summary='Collected plan failed its contract, preserving the original evidence.';
 await pool.query('UPDATE runs SET summary=$2 WHERE id=$1',[f.collected,summary]);
 f.proof.state.usage={inputTokens:100,outputTokens:30} as any;
 const originals=(await pool.query('SELECT id,instruction_snapshot,account_id,model,agent_version FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows;
 const next=await recoverOpenCodeRun(f.collected,f.collectedFence,f.proof);
 expect(next.attempt).toBe(3);
 expect((await pool.query('SELECT runtime,state,account_id,model,agent_version FROM runs WHERE id=$1',[next.runId])).rows[0])
  .toMatchObject({runtime:'opencode',state:'queued',account_id:'opencode-test',model:'glm',agent_version:1});
 expect((await pool.query('SELECT id,instruction_snapshot,account_id,model,agent_version FROM runs WHERE task_id=$1 AND attempt<3 ORDER BY attempt',[f.task])).rows).toEqual(originals);
 expect((await pool.query('SELECT state,error_code FROM runs WHERE id=$1',[f.run])).rows[0])
  .toMatchObject({state:'failed',error_code:'RESULT_FORMAT'});
 expect((await pool.query('SELECT summary,error_code,usage FROM runs WHERE id=$1',[f.collected])).rows[0])
  .toMatchObject({summary,error_code:'PLANNING_CONTRACT_INVALID',usage:null});
 expect((await pool.query('SELECT count(*)::int n FROM opencode_jobs WHERE run_id=$1',[next.runId])).rows[0].n).toBe(1);
});

it.each(['disabled','unsupported_model','exhausted','reconnect','unknown_denied','missing_source_route'])(
 'collection recovery respects the original account when it is %s, without falling back to another route',async reason=>{
 const f=await collectedFixture();
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown)
  VALUES('alternative','alternative','openai','other','["other"]',true,5,true)`);
 await pool.query("UPDATE tasks SET account_id='alternative',model='other' WHERE id=$1",[f.task]);
 await pool.query("UPDATE runs SET account_id='alternative',model='other' WHERE id=$1",[f.collected]);
 if(reason==='disabled')await pool.query("UPDATE accounts SET enabled=false WHERE id='opencode-test'");
 if(reason==='unsupported_model')await pool.query("UPDATE accounts SET models='[]' WHERE id='opencode-test'");
 if(reason==='exhausted')await pool.query("UPDATE accounts SET quota='{\"ordinaryUsageAllowed\":false}' WHERE id='opencode-test'");
 if(reason==='reconnect')await pool.query("UPDATE accounts SET connection_status='reconnect_required' WHERE id='opencode-test'");
 if(reason==='unknown_denied')await pool.query("UPDATE accounts SET allow_unknown=false WHERE id='opencode-test'");
 if(reason==='missing_source_route')await pool.query('UPDATE runs SET account_id=NULL,model=NULL WHERE id=$1',[f.run]);
 const before=(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows;
 await expect(recoverOpenCodeRun(f.collected,f.collectedFence,f.proof)).rejects.toThrow('ACCOUNT_UNAVAILABLE');
 expect((await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows).toEqual(before);
 expect((await pool.query('SELECT 1 FROM artifacts WHERE run_id=$1',[f.collected])).rowCount).toBe(0);
});

it('collection recovery reserves capacity against the original account even without a collection account',async()=>{
 const f=await collectedFixture();
 await pool.query("UPDATE accounts SET max_concurrency=1 WHERE id='opencode-test'");
 const otherTask=randomUUID();
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria)
  VALUES($1,$2,'occupied','Occupied','Real reserved work','qa.backend','queued','[]')`,[otherTask,f.goal]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state)
  VALUES($1,$2,1,$3,'opencode-test','glm','opencode',1,'queued')`,[randomUUID(),otherTask,randomUUID()]);
 await expect(recoverOpenCodeRun(f.collected,f.collectedFence,f.proof)).rejects.toThrow('CAPACITY_UNAVAILABLE');
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt>2',[f.task])).rowCount).toBe(0);
 expect((await pool.query('SELECT 1 FROM artifacts WHERE run_id=$1',[f.collected])).rowCount).toBe(0);
});

it('retains the scoped planner delivery contract instead of replacing it with a protocol suite after collection fails',async()=>{
 const f=await collectedFixture();
 await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workKind}','\"planning\"') WHERE id=$1",[f.task]);
 const next=await recoverOpenCodeRun(f.collected,f.collectedFence,f.proof);
 const checkpoint=(await pool.query('SELECT checkpoint FROM runs WHERE id=$1',[next.runId])).rows[0].checkpoint;
 expect(checkpoint.instruction).toContain('evidence/scope-plan.json');
 expect(checkpoint.instruction).toContain('contrato original');
 expect(checkpoint.instruction).not.toContain('Confira protocol-tests.log');
 expect((await pool.query('SELECT error_code FROM runs WHERE id=$1',[f.collected])).rows[0].error_code)
  .toBe('PLANNING_CONTRACT_INVALID');
});

it.each(['claimed_source','unit','live_source','workspace','foreign_task','self_reference'])(
 'rejects %s in an artifact recovery proof without admitting a new execution',async fault=>{
 const f=await collectedFixture();
 if(fault==='claimed_source')f.proof.executionRunId=randomUUID();
 if(fault==='unit')f.proof.process.unit='agentos-run-'+randomUUID();
 if(fault==='live_source')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
 if(fault==='workspace'){
  f.proof.workspace='isolated/another';
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}','\"isolated/another\"') WHERE id=$1",[f.task]);
 }
 if(fault==='foreign_task'){
  const task=randomUUID(),source=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count)
   VALUES($1,$2,'other','other','other','qa.backend','failed','[]','{"kind":"opencode","workspace":"isolated/test"}',1)`,[task,f.goal]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
   VALUES($1,$2,1,$3,'opencode',1,'failed')`,[source,task,randomUUID()]);
  await pool.query('UPDATE runs SET checkpoint=$2 WHERE id=$1',[f.collected,JSON.stringify({kind:'artifact_recovery',sourceRunId:source})]);
  f.proof.executionRunId=source;f.proof.process.unit='agentos-run-'+source;
 }
 if(fault==='self_reference'){
  await pool.query('UPDATE runs SET checkpoint=$2 WHERE id=$1',[f.collected,JSON.stringify({kind:'artifact_recovery',sourceRunId:f.collected})]);
  f.proof.executionRunId=f.collected;
 }
 await expect(recoverOpenCodeRun(f.collected,f.collectedFence,f.proof)).rejects.toThrow('INVALID_STOP_PROOF');
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt>2',[f.task])).rowCount).toBe(0);
 expect((await pool.query('SELECT 1 FROM artifacts WHERE run_id=$1',[f.collected])).rowCount).toBe(0);
});
it('preserves failed history and evidence, transfers leases atomically and queues only native OpenCode', async () => {
  const f = await fixture();
  const result = await recoverOpenCodeRun(f.run, f.fence, f.proof);
  expect(result.attempt).toBe(2);
  expect(
    (await pool.query('SELECT state,error_code FROM runs WHERE id=$1', [f.run])).rows[0],
  ).toMatchObject({ state: 'failed', error_code: 'MODEL_REQUEST_FAILED' });
  expect(
    (await pool.query('SELECT * FROM runs WHERE id=$1', [result.runId])).rows[0],
  ).toMatchObject({ state: 'queued', runtime: 'opencode', attempt: 2 });
  expect((await pool.query('SELECT * FROM artifacts WHERE run_id=$1', [f.run])).rowCount).toBe(2);
  expect((await pool.query('SELECT * FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(0);
  expect((await pool.query('SELECT * FROM leases WHERE run_id=$1', [result.runId])).rowCount).toBe(
    2,
  );
  expect((await pool.query('SELECT * FROM outbox WHERE run_id=$1', [result.runId])).rowCount).toBe(
    0,
  );
  expect(
    (await pool.query('SELECT * FROM opencode_jobs WHERE run_id=$1', [result.runId])).rowCount,
  ).toBe(1);
});
it('requires the durable native prelaunch receipt before recovering a never-started executor', async () => {
  const f = await fixture();
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1", [f.task]);
  await pool.query(
    "UPDATE runs SET state='failed',error_code='SUPERVISION_CONTEXT_CHANGED' WHERE id=$1",
    [f.run],
  );
  const proof = {
    ...f.proof,
    notStarted: true,
    state: { state: 'not_started', code: 'SUPERVISION_CONTEXT_CHANGED' },
  };
  await expect(recoverOpenCodeRun(f.run, f.fence, proof)).rejects.toThrow(
    'NOT_STARTED_RECEIPT_MISSING',
  );
  expect((await pool.query('SELECT 1 FROM artifacts WHERE run_id=$1', [f.run])).rowCount).toBe(0);
  await pool.query(
    "INSERT INTO events(kind,entity_id,payload) VALUES('OPENCODE_NOT_STARTED',$1,$2)",
    [f.task, JSON.stringify({ runId: f.run, confirmed: true })],
  );
  const result = await recoverOpenCodeRun(f.run, f.fence, proof);
  expect(result.attempt).toBe(2);
  expect(
    (await pool.query('SELECT runtime,state FROM runs WHERE id=$1', [result.runId])).rows[0],
  ).toMatchObject({ runtime: 'opencode', state: 'queued' });
  expect((await pool.query('SELECT 1 FROM outbox WHERE run_id=$1', [result.runId])).rowCount).toBe(
    0,
  );
});

it('retries an unaccepted native submission while preserving its submitted history', async () => {
  const f = await fixture();
  await pool.query(
    "UPDATE runs SET state='submitted',summary='Real report',finished_at=now() WHERE id=$1",
    [f.run],
  );
  await pool.query("UPDATE tasks SET state='submitted' WHERE id=$1", [f.task]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,'test')",
    [randomUUID(), f.task, f.run, JSON.stringify([{ type: 'artifact', passed: false }])],
  );
  const result = await recoverOpenCodeRun(f.run, f.fence, f.proof);
  expect(result.attempt).toBe(2);
  expect(
    (await pool.query('SELECT state,summary FROM runs WHERE id=$1', [f.run])).rows[0],
  ).toMatchObject({ state: 'submitted', summary: 'Real report' });
});

it('carries the exact assigned test file into a packaging retry without rewriting failed history', async () => {
  const f = await fixture();
  const testFile = 'qa-protocol/assigned-cookie-isolation.test.mjs';
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{testFile}',$2::jsonb) WHERE id=$1",
    [f.task, JSON.stringify(testFile)],
  );
  f.proof.state.originalCode = 'OPENCODE_DELIVERY_INVALID_REPORT_TEST_FILE';
  const result = await recoverOpenCodeRun(f.run, f.fence, f.proof);
  const checkpoint = (await pool.query('SELECT checkpoint FROM runs WHERE id=$1', [result.runId]))
    .rows[0].checkpoint;
  expect(checkpoint.instruction).toContain(testFile);
  expect(checkpoint.instruction).toContain('nunca altere o comando registrado para simular');
  expect(
    (await pool.query('SELECT state,error_code FROM runs WHERE id=$1', [f.run])).rows[0],
  ).toMatchObject({
    state: 'failed',
    error_code: 'OPENCODE_DELIVERY_INVALID_REPORT_TEST_FILE',
  });
  expect(
    (await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1', [f.task])).rows[0]
      .n,
  ).toBe(0);
});

it('does not rerun accepted native submissions or human-only review gates', async () => {
  const f = await fixture();
  await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1", [f.run]);
  await pool.query("UPDATE tasks SET state='submitted' WHERE id=$1", [f.task]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,'test')",
    [randomUUID(), f.task, f.run, JSON.stringify([{ type: 'review', passed: false }])],
  );
  await expect(recoverOpenCodeRun(f.run, f.fence, f.proof)).rejects.toThrow('NO_FAILED_CHECK');
});

async function invalidatedSubmission() {
  const f = await fixture();
  await pool.query(
    "UPDATE runs SET state='submitted',summary='Original positive auxiliary delivery',finished_at=now() WHERE id=$1",
    [f.run],
  );
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,$4,'test')",
    [randomUUID(), f.task, f.run, JSON.stringify([{ type: 'artifact', passed: true }])],
  );
  await pool.query(
    "INSERT INTO settings VALUES($1,'{\"enabled\":true}') ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",
    ['supervision:' + f.goal],
  );
  await pool.query(
    "INSERT INTO settings VALUES('autonomy:isolated','{\"enabled\":true}') ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",
  );
  await pool.query(
    "UPDATE tasks SET state='failed',execution=execution||$2::jsonb WHERE id=$1",
    [f.task, JSON.stringify({ repairFeedback: { reason: 'Independent QA rejected candidate' } })],
  );
  const review = await requestRecoveryReview(f.task,f.run,'NO_FAILED_CHECK','Later independent QA invalidated the candidate. Preserve prior evidence and repair the newly observed regression.');
  expect(review).not.toBeNull();
  const caseId = review!.caseId;
  const coordinator = randomUUID(), decisionRun = randomUUID(), decisionFence = randomUUID();
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,criteria,attempt_count)
     VALUES($1,$2,'coordinator','Supervisor','Authorized automatic supervisor','qa.backend','opencode-test','glm','running','coordinate',$3,1)`,
    [coordinator,f.goal,JSON.stringify([{id:'decision',label:'Supervisor decision',type:'artifact',name:'coordination.json'}])],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode-test','glm','harness',1,'running')",
    [decisionRun,coordinator,decisionFence],
  );
  await writeArtifact(decisionRun,decisionFence,'coordination.json',JSON.stringify({supervision:{actions:[{
    kind:'revise',caseId,diagnosis:'Independent QA rejected the candidate after the earlier auxiliary acceptance.',
    instruction:'Repair the newly observed regression with a discriminating test; preserve the original evidence and final acceptance gates.',
  }]}}));
  await completeRun(decisionRun,decisionFence,'Fresh supervisor decision');
  await validateTask(coordinator);
  await recordSupervisionDecisions();
  await applySupervisionDecisions(async()=>f.proof);
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[caseId])).rows[0].state).toBe('applied');
  const original = {
    run: (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0],
    acceptances: (await pool.query('SELECT * FROM acceptances WHERE run_id=$1', [f.run])).rows,
  };
  return { ...f, caseId, original, proof: { ...f.proof, supervisionResumeCaseId: caseId } };
}

it('a current applied supervisor decision admits one correction without rewriting a prior positive acceptance', async () => {
  const f = await invalidatedSubmission();
  const attempts = await Promise.allSettled([
    recoverOpenCodeRun(f.run, f.fence, f.proof),
    recoverOpenCodeRun(f.run, f.fence, f.proof),
  ]);
  expect(attempts.filter(a => a.status === 'fulfilled'), attempts.map(a=>a.status==='rejected'?a.reason.message:'queued').join('; ')).toHaveLength(1);
  const result = (attempts.find(a => a.status === 'fulfilled') as PromiseFulfilledResult<any>).value;
  expect(result.attempt).toBe(2);
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual(f.original.run);
  expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1', [f.run])).rows).toEqual(f.original.acceptances);
  expect((await pool.query('SELECT state,attempt,runtime FROM runs WHERE id=$1', [result.runId])).rows[0])
    .toMatchObject({ state: 'queued', attempt: 2, runtime: 'opencode' });
  expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id=$1', [result.runId])).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM opencode_jobs WHERE run_id=$1', [result.runId])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(0);
});

it.each(['forged_case', 'wrong_attempt', 'wrong_source', 'disabled', 'paused', 'cancelled', 'pending_case', 'dependency', 'live_run'])(
  'an invalidated positive submission cannot resume when its supervision proof is %s', async reason => {
    const f = await invalidatedSubmission();
    if (reason === 'forged_case') f.proof.supervisionResumeCaseId = randomUUID();
    if (reason === 'wrong_attempt') await pool.query('UPDATE supervision_cases SET applied_attempt=2 WHERE id=$1', [f.caseId]);
    if (reason === 'wrong_source') await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision,sourceRunId}',$2::jsonb) WHERE id=$1", [f.task, JSON.stringify(randomUUID())]);
    if (reason === 'disabled') await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1", ['supervision:' + f.goal]);
    if (reason === 'paused') await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
    if (reason === 'cancelled') await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [f.task, JSON.stringify({runId:f.run})]);
    if (reason === 'pending_case') await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,state,reason,evidence) VALUES($1,$2,$3,'open','Newer unresolved review','[]')", [randomUUID(), f.goal, f.task]);
    if (reason === 'dependency') {
      const parent = randomUUID();
      await pool.query("INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria) VALUES($1,$2,'pending-parent','Required current input','Still pending','qa.backend','ready','[]')", [parent,f.goal]);
      await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.task,parent]);
    }
    if (reason === 'live_run') await pool.query("INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state) VALUES($1,$2,2,$3,'opencode-test','glm','opencode',1,'queued')", [randomUUID(),f.task,randomUUID()]);
    const runsBefore = (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n;
    await expect(recoverOpenCodeRun(f.run,f.fence,f.proof)).rejects.toThrow(
      ['disabled','paused'].includes(reason) ? 'SUPERVISION_DISABLED' : 'SUPERVISION_HANDOFF_CHANGED',
    );
    expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n).toBe(runsBefore);
    expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual(f.original.run);
    expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1', [f.run])).rows).toEqual(f.original.acceptances);
    expect((await pool.query('SELECT 1 FROM artifacts WHERE run_id=$1', [f.run])).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
  },
);
it('concurrent recovery commands admit only one new attempt', async () => {
  const f = await fixture();
  const results = await Promise.allSettled([
    recoverOpenCodeRun(f.run, f.fence, f.proof),
    recoverOpenCodeRun(f.run, f.fence, f.proof),
  ]);
  expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
  expect((await pool.query('SELECT * FROM runs WHERE task_id=$1', [f.task])).rowCount).toBe(2);
});

it.each(['running', 'submitted', 'failed'])(
  'native retry waits for an added dependency that is %s and preserves its history and leases',
  async (state) => {
    const f = await fixture();
    const parent = randomUUID();
    await pool.query(
      "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria) VALUES($1,$2,'new-parent','Original asset discovery','Required current input','qa.backend',$3,'[]')",
      [parent, f.goal, state],
    );
    await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.task, parent]);
    await expect(recoverOpenCodeRun(f.run, f.fence, f.proof)).rejects.toThrow(
      'DEPENDENCIES_PENDING',
    );
    expect(
      (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
    ).toBe(1);
    expect((await pool.query('SELECT * FROM artifacts WHERE run_id=$1', [f.run])).rowCount).toBe(0);
    expect((await pool.query('SELECT * FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
    expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
      'reconciling',
    );
    await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1", [parent]);
    f.proof.checkedAt = new Date().toISOString();
    const result = await recoverOpenCodeRun(f.run, f.fence, f.proof);
    expect(result.attempt).toBe(2);
    expect(
      (await pool.query('SELECT state FROM runs WHERE id=$1', [result.runId])).rows[0].state,
    ).toBe('queued');
  },
);

it('persisted admission also fences older retry consumers without interrupting a live run', async () => {
  const f = await fixture();
  await pool.query('INSERT INTO settings VALUES($1,$2)', [
    'workforce:' + f.goal,
    JSON.stringify({ enabled: true, glmTarget: 12 }),
  ]);
  await installWorkforcePolicy(pool);
  const parent = randomUUID();
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria) VALUES($1,$2,'parent','Required atlas','Discovery still running','qa.backend','running','[]')",
    [parent, f.goal],
  );
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.task, parent]);
  const enqueue = () =>
    pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state) VALUES($1,$2,2,$3,'opencode-test','glm','opencode',1,'queued')",
      [randomUUID(), f.task, randomUUID()],
    );
  await expect(enqueue()).rejects.toThrow('DEPENDENCIES_PENDING');
  expect(
    (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
  ).toBe(1);
  // Existing real work can still heartbeat/reconcile even while a new parent is pending.
  await pool.query('UPDATE runs SET heartbeat_at=now() WHERE id=$1', [f.run]);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
    'reconciling',
  );
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1", [parent]);
  await expect(enqueue()).rejects.toThrow('one_live_run');
  await pool.query("UPDATE runs SET state='failed',finished_at=now() WHERE id=$1", [f.run]);
  await expect(enqueue()).resolves.toMatchObject({ rowCount: 1 });
});
it('rejects stale, mismatched or still-populated process proof', async () => {
  const f = await fixture();
  for (const proof of [
    { ...f.proof, runId: randomUUID() },
    { ...f.proof, checkedAt: '2020-01-01T00:00:00Z' },
    { ...f.proof, process: { ...f.proof.process, mainPid: 123 } },
    { ...f.proof, process: { ...f.proof.process, populated: true } },
  ])
    await expect(recoverOpenCodeRun(f.run, f.fence, proof)).rejects.toThrow('INVALID_STOP_PROOF');
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
    'reconciling',
  );
});
it('does not override pause, exhausted quota or a stale fence', async () => {
  const f = await fixture();
  await expect(recoverOpenCodeRun(f.run, randomUUID(), f.proof)).rejects.toThrow('STALE_RUN');
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
  await expect(recoverOpenCodeRun(f.run, f.fence, f.proof)).rejects.toThrow('GOAL_NOT_ACTIVE');
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  await pool.query(
    "UPDATE accounts SET quota='{\"ordinaryUsageAllowed\":false}' WHERE id='opencode-test'",
  );
  await expect(recoverOpenCodeRun(f.run, f.fence, f.proof)).rejects.toThrow('ACCOUNT_UNAVAILABLE');
  expect((await pool.query('SELECT * FROM leases WHERE run_id=$1', [f.run])).rowCount).toBe(2);
});

function waitProof(f: Awaited<ReturnType<typeof fixture>>, seconds = 3600) {
  const observedAt = new Date(Date.now() - 1000).toISOString();
  const notBefore = new Date(Date.now() + seconds * 1000).toISOString();
  return { ...f.proof, state: { ...f.proof.state, runId:f.run, workspace:f.proof.workspace,model:'glm',
    providerBackoff: { version:1,source:'http_retry_after',runId:f.run,workspace:f.proof.workspace,model:'glm',
      status:429,category:'rate_limit',observedAt,notBefore } } };
}

it('a stopped native retry retains the provider minimum wait without a new attempt, evidence or quota mutation', async () => {
  const f=await fixture(),proof=waitProof(f);
  await pool.query("UPDATE runs SET state='failed',error_code='RATE_LIMIT' WHERE id=$1",[f.run]);
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1",[f.task]);
  const before=(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows;
  const account=(await pool.query("SELECT * FROM accounts WHERE id='opencode-test'")).rows[0];
  await expect(recoverOpenCodeRun(f.run,f.fence,proof)).rejects.toMatchObject({message:'PROVIDER_RETRY_PENDING',notBefore:proof.state.providerBackoff.notBefore});
  expect((await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows).toEqual(before);
  expect((await pool.query("SELECT * FROM accounts WHERE id='opencode-test'")).rows[0]).toEqual(account);
  expect((await pool.query('SELECT 1 FROM artifacts WHERE run_id=$1',[f.run])).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM opencode_jobs WHERE run_id=$1',[f.run])).rowCount).toBe(0);
});

it('expired provider wait admits only one concurrent successor and preserves the immutable failure', async () => {
  const f=await fixture(),proof=waitProof(f,-10);
  proof.state.providerBackoff.observedAt=new Date(Date.now()-20000).toISOString();
  const outcomes=await Promise.allSettled([recoverOpenCodeRun(f.run,f.fence,proof),recoverOpenCodeRun(f.run,f.fence,proof)]);
  expect(outcomes.filter(x=>x.status==='fulfilled')).toHaveLength(1);
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt=2',[f.task])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM opencode_jobs j JOIN runs r ON r.id=j.run_id WHERE r.task_id=$1',[f.task])).rowCount).toBe(1);
});

it.each(['runId','workspace','model','observedAt','notBefore'])('refuses invalid provider wait %s without mutating admission', async field => {
  const f=await fixture(),proof=waitProof(f);
  (proof.state.providerBackoff as any)[field]=field==='observedAt'?new Date(Date.now()+60000).toISOString():'foreign';
  await expect(recoverOpenCodeRun(f.run,f.fence,proof)).rejects.toThrow('INVALID_PROVIDER_BACKOFF_PROOF');
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt>1',[f.task])).rowCount).toBe(0);
});

it('provider wait never substitutes for confirmed physical stop',async()=>{
  const f=await fixture(),proof=waitProof(f,-10);
  proof.process.populated=true; proof.process.stopped=false;
  await expect(recoverOpenCodeRun(f.run,f.fence,proof)).rejects.toThrow('INVALID_STOP_PROOF');
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt>1',[f.task])).rowCount).toBe(0);
});

it('collection retry remains bound to the original execution provider wait',async()=>{
  const f=await collectedFixture();
  const proof={...waitProof(f),runId:f.collected,executionRunId:f.run};
  await expect(recoverOpenCodeRun(f.collected,f.collectedFence,proof)).rejects.toThrow('PROVIDER_RETRY_PENDING');
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt>2',[f.task])).rowCount).toBe(0);
});
