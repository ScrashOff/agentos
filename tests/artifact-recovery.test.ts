import { beforeAll, beforeEach, afterAll, it, expect, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { hash } from '../server/memory.js';
import { adoptRecoveredArtifacts,prepareCompletedNativeArtifactRecovery } from '../server/artifact-recovery.js';
import { installNativeSupervision,nativeSupervisorUnit,nativeSupervisorLockKey,nativeSupervisorLockNamespace } from '../server/native-supervision.js';
import { ContinuationSupervisor } from '../server/continuation.js';
import { recoverStoppedTasks } from '../server/recovery.js';
import { recoverMissingOpenCodeArtifacts } from '../server/missing-artifacts.js';
import { applySupervisionDecisions, readSupervisedResumeCandidates, resumeSupervisedWork } from '../server/supervision.js';
import { writeArtifact, completeRun, validateTask } from '../server/service.js';
import { appliedSupervisionOwnsContinuation } from '../server/recovery-supervision.js';
import { readPreparedNativeAdmission } from '../server/supervised-opencode-admission.js';
import { installArtifactRecoveryPolicy } from '../server/artifact-recovery-policy.js';
import {reconcileCompletedNativeArtifacts} from '../server/native-completed-recovery.js';
import {
  recoverAnalyticalDeliveries,
  recoverCoordinationArtifact,
} from '../server/analytical-delivery.js';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await installNativeSupervision(pool);
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id='artifact-recovery'");
});
afterAll(async () => {
  await pool.query("DELETE FROM settings WHERE id='artifact-recovery'");
  await pool.end();
});
async function fixture() {
  const goal = randomUUID(),
    task = randomUUID(),
    run = randomUUID(),
    fence = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Test','Test','artifact-test',$2)",
    [goal, goal],
  );
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count)
    VALUES($1,$2,'fix','Test','Test','qa.backend','failed',$3,$4,15)`,
    [
      task,
      goal,
      JSON.stringify([
        {
          id: 'test',
          label: 'Test',
          type: 'json_field',
          name: 'verification.json',
          field: 'passed',
          equals: true,
        },
      ]),
      JSON.stringify({ kind: 'harness', workspace: 'artifact-test/fix' }),
    ],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,finished_at) VALUES($1,$2,15,$3,'harness',1,'failed','ARTIFACT_PATH',now())",
    [run, task, fence],
  );
  const artifacts = Object.entries({
    'verification.json': '{"passed":true}',
    'candidate.json': '{"candidateDigest":"test"}',
    'changes.patch': 'patch',
  }).map(([name, text]) => ({
    name,
    mime: 'text/plain',
    base64: Buffer.from(text).toString('base64'),
    digest: hash(Buffer.from(text)),
  }));
  const delivery = {
    runId: run,
    workspace: 'artifact-test/fix',
    checkedAt: new Date().toISOString(),
    process: {
      unit: 'agentos-run-' + run,
      stopped: true,
      mainPid: 0,
      populated: false,
      activeState: 'inactive',
    },
    candidate: { candidateDigest: 'test' },
    summary: 'Existing output',
    artifacts,
  };
  return { goal, task, run, fence, delivery };
}
it('recovers the delivery without a model job and preserves the failed execution', async () => {
  const f = await fixture();
  const result = await adoptRecoveredArtifacts(f.run, f.fence, f.delivery);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'accepted',
  );
  expect(
    (await pool.query('SELECT state,error_code FROM runs WHERE id=$1', [f.run])).rows[0],
  ).toEqual({ state: 'failed', error_code: 'ARTIFACT_PATH' });
  expect(
    (await pool.query('SELECT usage,checkpoint,worker_id FROM runs WHERE id=$1', [result.runId]))
      .rows[0],
  ).toMatchObject({
    worker_id: 'artifact-collector',
    usage: { cost: 0, totalTokens: 0 },
    checkpoint: { sourceRunId: f.run, modelInvoked: false },
  });
  expect((await pool.query('SELECT * FROM outbox')).rowCount).toBe(0);
});
it('hands reserved-name failures to the collector before a legacy worker retries the model', async () => {
  const f = await fixture();
  await installArtifactRecoveryPolicy(pool);
  await pool.query("INSERT INTO settings VALUES('artifact-recovery','{\"enabled\":true}')");
  await pool.query("UPDATE runs SET error_code='RESERVED_ARTIFACT_NAME' WHERE id=$1", [f.run]);
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1", [f.task]);
  await recoverStoppedTasks();
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'recovering_artifacts',
  );
});
it('recovers reserved-name packaging with canonical evidence and preserves the failed attempt', async () => {
  const f = await fixture();
  await pool.query("UPDATE runs SET error_code='RESERVED_ARTIFACT_NAME' WHERE id=$1", [f.run]);
  const claim = {
    name: 'worker-changes.patch',
    mime: 'text/plain',
    digest: hash(Buffer.from('worker claim')),
    base64: Buffer.from('worker claim').toString('base64'),
  };
  const delivery = {
    ...f.delivery,
    artifacts: [...f.delivery.artifacts, claim],
    reservedClaims: [{ originalName: 'changes.patch', name: claim.name, digest: claim.digest }],
  };
  const result = await adoptRecoveredArtifacts(f.run, f.fence, delivery);
  expect(
    (await pool.query('SELECT error_code FROM runs WHERE id=$1', [f.run])).rows[0].error_code,
  ).toBe('RESERVED_ARTIFACT_NAME');
  const recovered = (
    await pool.query('SELECT checkpoint,usage FROM runs WHERE id=$1', [result.runId])
  ).rows[0];
  expect(recovered.checkpoint.reservedClaims).toEqual(delivery.reservedClaims);
  expect(recovered.checkpoint.modelInvoked).toBe(false);
  expect(recovered.usage.totalTokens).toBe(0);
  expect(
    (
      await pool.query('SELECT name FROM artifacts WHERE run_id=$1 ORDER BY name', [result.runId])
    ).rows.map((a) => a.name),
  ).toEqual(['candidate.json', 'changes.patch', 'verification.json', 'worker-changes.patch']);
});

it('leaves failed criteria failed when only collection succeeds', async () => {
  const f = await fixture(),
    a = f.delivery.artifacts[0];
  a.base64 = Buffer.from('{"passed":false}').toString('base64');
  a.digest = hash(Buffer.from('{"passed":false}'));
  await adoptRecoveredArtifacts(f.run, f.fence, f.delivery);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'submitted',
  );
  expect((await pool.query('SELECT passed FROM acceptances')).rows[0].passed).toBe(false);
});
it('fences concurrent collection, rejects stale stop proof and corrupt bytes', async () => {
  const f = await fixture();
  await expect(
    adoptRecoveredArtifacts(f.run, f.fence, { ...f.delivery, checkedAt: '2020-01-01' }),
  ).rejects.toThrow('STOP_UNCONFIRMED');
  await expect(
    adoptRecoveredArtifacts(f.run, f.fence, {
      ...f.delivery,
      artifacts: [{ ...f.delivery.artifacts[0], digest: 'bad' }],
    }),
  ).rejects.toThrow('ARTIFACT_CORRUPT');
  const attempts = await Promise.allSettled([
    adoptRecoveredArtifacts(f.run, f.fence, f.delivery),
    adoptRecoveredArtifacts(f.run, f.fence, f.delivery),
  ]);
  expect(attempts.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
  expect((await pool.query('SELECT * FROM runs')).rowCount).toBe(2);
});
it('hands packaging errors to collection before a running legacy retry loop can reschedule', async () => {
  const f = await fixture();
  await pool.query("INSERT INTO settings VALUES('artifact-recovery','{\"enabled\":true}')");
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1", [f.task]);
  await recoverStoppedTasks();
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'recovering_artifacts',
  );
  let modelRetries = 0,
    collections = 0;
  await new ContinuationSupervisor(
    async () => {
      modelRetries++;
      throw Error('unexpected');
    },
    async () => {
      collections++;
      return f.delivery;
    },
  ).scan();
  expect(modelRetries).toBe(0);
  expect(collections).toBe(1);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'accepted',
  );
});
it('does not spin new model attempts when packaging keeps failing even if files change', async () => {
  const f = await fixture();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code) VALUES($1,$2,14,$3,'harness',1,'failed','ARTIFACT_PATH')",
    [randomUUID(), f.task, randomUUID()],
  );
  let retries = 0;
  await new ContinuationSupervisor(
    async () => {
      retries++;
      throw Error('unexpected');
    },
    async () => {
      throw Error('ARTIFACT_MISSING');
    },
  ).scan();
  expect(retries).toBe(0);
  expect(
    (await pool.query('SELECT execution FROM tasks WHERE id=$1', [f.task])).rows[0].execution
      .continuation.state,
  ).toBe('needs_action');
  expect((await pool.query('SELECT * FROM outbox')).rowCount).toBe(0);
});
it('will not collect a cancelled task or one with a newer execution', async () => {
  const f = await fixture();
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [
    f.task,
    JSON.stringify({ runId: f.run }),
  ]);
  await expect(adoptRecoveredArtifacts(f.run, f.fence, f.delivery)).rejects.toThrow('TASK_CHANGED');
  await pool.query('DELETE FROM events');
  await pool.query('UPDATE tasks SET attempt_count=16 WHERE id=$1', [f.task]);
  await expect(adoptRecoveredArtifacts(f.run, f.fence, f.delivery)).rejects.toThrow('TASK_CHANGED');
});

async function nativeFixture() {
  const f = await fixture();
  const testFile = 'qa-protocol/neko.test.mjs';
  const criteria = [
    {
      id: 'executed',
      label: 'Executed',
      type: 'json_field',
      name: 'protocol-report.json',
      field: 'suiteExecuted',
      equals: true,
    },
    { id: 'report', label: 'Cases and limitations', type: 'artifact', name: 'protocol-report.md' },
  ];
  const candidate = {
    baseDigest: hash('base'),
    candidateDigest: hash('candidate'),
    files: [{ path: testFile, sha256: hash('test') }],
  };
  const content = {
    'candidate.json': JSON.stringify(candidate),
    'changes.patch': `--- /dev/null\n+++ b/${testFile}\n@@ -0,0 +1 @@\n+test('failure');\n`,
    'protocol-report.json': JSON.stringify({
      suiteExecuted: true,
      command: `node --test ${testFile}`,
      exitCode: 1,
      passed: false,
    }),
    'protocol-tests.log': 'not ok 1 - native boot failed',
    'protocol-report.md': 'Native boot failed. Game acceptance remains pending.',
  };
  f.delivery.candidate = candidate;
  f.delivery.artifacts = Object.entries(content).map(([name, value]) => ({
    name,
    mime: 'text/plain',
    base64: Buffer.from(value).toString('base64'),
    digest: hash(value),
  }));
  for (const a of f.delivery.artifacts.filter((a) => a.name !== 'protocol-report.md'))
    await pool.query(
      'INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
      [randomUUID(), f.run, a.name, a.mime, a.digest, Buffer.from(a.base64, 'base64').length],
    );
  await pool.query(
    "UPDATE runs SET state='submitted',runtime='opencode',error_code=NULL WHERE id=$1",
    [f.run],
  );
  await pool.query("UPDATE tasks SET state='submitted',criteria=$2,execution=$3 WHERE id=$1", [
    f.task,
    JSON.stringify(criteria),
    JSON.stringify({
      kind: 'opencode',
      workspace: f.delivery.workspace,
      testFile,
      continuation: { runId: f.run, state: 'needs_action' },
    }),
  ]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,'test')",
    [
      randomUUID(),
      f.task,
      f.run,
      JSON.stringify(criteria.map((c, i) => ({ ...c, passed: i === 0 }))),
    ],
  );
  return f;
}

async function completedOrphanFixture(){
 const f=await nativeFixture();
 await pool.query("UPDATE runs SET state='reconciling',error_code='SUPERVISOR_LOST',finished_at=NULL,checkpoint='{\"original\":\"retained\"}' WHERE id=$1",[f.run]);
 await pool.query("UPDATE tasks SET state='reconciling' WHERE id=$1",[f.task]);
 const reservation={version:1,mode:'detached',runId:f.run,runFence:f.fence,launchToken:randomUUID(),
  unit:nativeSupervisorUnit(f.run),candidate:'/opt/agentos/releases/managed-'+randomUUID(),digest:'a'.repeat(64),
  launchAttempt:1,reservedAt:new Date().toISOString()};
 await pool.query("INSERT INTO opencode_jobs(run_id,state,supervision) VALUES($1,'orphaned',$2)",[f.run,JSON.stringify(reservation)]);
 const prepared=await prepareCompletedNativeArtifactRecovery(f.run,f.fence);
 const proof={...prepared,supervisorObservation:{unit:reservation.unit,checkedAt:new Date().toISOString(),activeState:'inactive',
  loadState:'loaded',mainPid:0,pendingJob:false,cgroupEmpty:true},
  inspection:{runId:f.run,workspace:f.delivery.workspace,state:{state:'completed',runId:f.run,runtime:'opencode',stopped:true,exitCode:0,
    artifacts:f.delivery.artifacts.map(artifact=>({name:artifact.name,digest:artifact.digest}))},checkedAt:new Date().toISOString(),
    process:{...f.delivery.process},candidate:{...f.delivery.candidate}}};
 return {...f,proof};
}

it('automatically collects a completed orphan and refreshes both proofs without starting another model',async()=>{
 const f=await completedOrphanFixture();let reads=0,collections=0,observations=0;
 const recovered=await reconcileCompletedNativeArtifacts({
  collect:async(id,readonly,testFile,exact)=>{expect(id).toBe(f.run);expect(exact).toBe(true);collections++;return f.delivery;},
  inspect:async(id)=>{expect(id).toBe(f.run);reads++;return f.proof.inspection;},
  observe:async(reservation)=>{expect(reservation.runId).toBe(f.run);observations++;return f.proof.supervisorObservation;},
 });
 expect(recovered).toHaveLength(1);expect({reads,collections,observations}).toEqual({reads:2,collections:1,observations:1});
 expect((await pool.query('SELECT 1 FROM outbox')).rowCount).toBe(0);
});

it('does not inspect or collect a paused native orphan',async()=>{
 const f=await completedOrphanFixture();await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 const unexpected=async():Promise<any>=>{throw Error('REMOTE_CALL_NOT_ALLOWED');};
 expect(await reconcileCompletedNativeArtifacts({collect:unexpected,inspect:unexpected,observe:unexpected})).toEqual([]);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='NATIVE_COLLECTION_DEFERRED'")).rowCount).toBe(0);
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('reconciling');
});

it('rotates native orphan probes past unavailable or incomplete older runs and respects cooldown',async()=>{
 const fixtures:Awaited<ReturnType<typeof completedOrphanFixture>>[]=[];
 for(let i=0;i<4;i++){
  const f=await completedOrphanFixture();fixtures.push(f);
  await pool.query("UPDATE runs SET created_at=now()-($2::int*interval '1 minute') WHERE id=$1",[f.run,4-i]);
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}',to_jsonb($2::text)) WHERE id=$1",[f.task,'artifact-test/fix-'+i]);
 }
 const inspected:string[]=[],collected:string[]=[];
 const deps={
  inspect:async(id:string):Promise<any>=>{
   inspected.push(id);
   if(id===fixtures[0].run)throw Error('SOURCE_BUSY');
   if(id===fixtures[1].run)return {state:{state:'failed'},process:{stopped:true}};
   if(id===fixtures[2].run)return {state:{state:'completed'},process:{stopped:false}};
   return {state:{state:'completed'},process:{stopped:true}};
  },
  collect:async(id:string):Promise<any>=>{collected.push(id);throw Error('COLLECTION_UNAVAILABLE');},
  observe:async():Promise<any>=>{throw Error('UNEXPECTED_OBSERVATION');},
 };
 expect(await reconcileCompletedNativeArtifacts(deps)).toEqual([]);
 expect(inspected).toEqual(fixtures.slice(0,3).map(f=>f.run));expect(collected).toEqual([]);
 expect(await reconcileCompletedNativeArtifacts(deps)).toEqual([]);
 expect(inspected).toEqual(fixtures.map(f=>f.run));expect(collected).toEqual([fixtures[3].run]);
 expect(await reconcileCompletedNativeArtifacts(deps)).toEqual([]);
 expect(inspected).toHaveLength(4);
 await pool.query("UPDATE events SET created_at=now()-interval '61 seconds' WHERE kind='NATIVE_COLLECTION_CHECKED' AND entity_id=$1",[fixtures[0].run]);
 expect(await reconcileCompletedNativeArtifacts(deps)).toEqual([]);
 expect(inspected).toEqual([...fixtures.map(f=>f.run),fixtures[0].run]);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='NATIVE_COLLECTION_DEFERRED' AND payload->>'code'='SOURCE_BUSY'")).rowCount).toBe(1);
 expect((await pool.query("SELECT 1 FROM runs WHERE state='reconciling'")).rowCount).toBe(4);
 expect((await pool.query('SELECT 1 FROM outbox')).rowCount).toBe(0);
});

it('adopts completed native orphan outputs once after both stops without invoking or replacing the model attempt',async()=>{
 const f=await completedOrphanFixture();
 const original=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
 const oldArtifacts=(await pool.query('SELECT * FROM artifacts WHERE run_id=$1 ORDER BY id',[f.run])).rows;
 const results=await Promise.all([adoptRecoveredArtifacts(f.run,f.fence,f.delivery,{nativeCompleted:f.proof}),
  adoptRecoveredArtifacts(f.run,f.fence,f.delivery,{nativeCompleted:f.proof})]);
 expect(results[0]).toEqual(results[1]);
 const stopped=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
 expect(stopped).toMatchObject({...original,state:'failed',finished_at:expect.any(Date)});
 expect(stopped.error_code).toBe('SUPERVISOR_LOST');
 expect((await pool.query('SELECT * FROM artifacts WHERE run_id=$1 ORDER BY id',[f.run])).rows).toEqual(oldArtifacts);
 const collection=(await pool.query('SELECT * FROM runs WHERE id=$1',[results[0].runId])).rows[0];
 expect(collection).toMatchObject({attempt:16,worker_id:'artifact-collector',state:'submitted',
  usage:{input:0,output:0,totalTokens:0,cost:0},checkpoint:{originalError:'SUPERVISOR_LOST',sourceRunId:f.run,modelInvoked:false,
    nativeCompletedRecovery:{taskContractDigest:f.proof.taskContractDigest,reservation:f.proof.reservation}}});
 expect((await pool.query('SELECT state FROM opencode_jobs WHERE run_id=$1',[f.run])).rows[0].state).toBe('finished');
 expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.task])).rows[0].n).toBe(2);
 expect((await pool.query('SELECT 1 FROM outbox')).rowCount).toBe(0);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='NATIVE_COMPLETED_OUTPUTS_RECONCILED'")).rows[0].n).toBe(1);
});

it('keeps the ordinary collector unable to consume SUPERVISOR_LOST without the explicit completed-native proofs',async()=>{
 const f=await completedOrphanFixture();
 await expect(adoptRecoveredArtifacts(f.run,f.fence,f.delivery)).rejects.toThrow('STALE_RUN');
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('reconciling');
});

it.each(['supervisor-live','supervisor-stale','supervisor-job','sandbox-live','sandbox-stale','not-completed','foreign-reservation',
  'foreign-completion','wrong-runtime','failed-exit','manifest-mismatch','legacy-status'] as const)
 ('refuses a native completed collection with %s evidence',async(mode)=>{
 const f=await completedOrphanFixture(),proof=structuredClone(f.proof);
 if(mode==='supervisor-live')proof.supervisorObservation.mainPid=42;
 if(mode==='supervisor-stale')proof.supervisorObservation.checkedAt=new Date(Date.now()-31000).toISOString();
 if(mode==='supervisor-job')proof.supervisorObservation.pendingJob=true;
 if(mode==='sandbox-live')proof.inspection.process.populated=true;
 if(mode==='sandbox-stale')proof.inspection.checkedAt=new Date(Date.now()-61000).toISOString();
 if(mode==='not-completed')proof.inspection.state.state='failed';
 if(mode==='foreign-reservation')proof.reservation={...proof.reservation,launchToken:randomUUID()};
 if(mode==='foreign-completion')proof.inspection.state.runId=randomUUID();
 if(mode==='wrong-runtime')proof.inspection.state.runtime='harness';
 if(mode==='failed-exit')proof.inspection.state.exitCode=1;
 if(mode==='manifest-mismatch')proof.inspection.state.artifacts[0].digest=hash('other-runtime-artifact');
 if(mode==='legacy-status')(proof.inspection as any).state='completed';
 await expect(adoptRecoveredArtifacts(f.run,f.fence,f.delivery,{nativeCompleted:proof})).rejects.toThrow();
 expect((await pool.query('SELECT state,error_code FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual({state:'reconciling',error_code:'SUPERVISOR_LOST'});
 expect((await pool.query('SELECT state FROM opencode_jobs WHERE run_id=$1',[f.run])).rows[0].state).toBe('orphaned');
 expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.task])).rows[0].n).toBe(1);
});

it('cannot collect while a native owner holds the real PostgreSQL per-run lock',async()=>{
 const f=await completedOrphanFixture(),owner=await pool.connect();
 try{
  await owner.query('SELECT pg_advisory_lock($1::int,$2::int)',[nativeSupervisorLockNamespace,nativeSupervisorLockKey(f.run)]);
  await expect(adoptRecoveredArtifacts(f.run,f.fence,f.delivery,{nativeCompleted:f.proof})).rejects.toThrow('NATIVE_SUPERVISOR_ACTIVE');
 }finally{
  await owner.query('SELECT pg_advisory_unlock($1::int,$2::int)',[nativeSupervisorLockNamespace,nativeSupervisorLockKey(f.run)]);owner.release();
 }
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('reconciling');
});

it.each(['cancel','paused','manual','criterion','context','workspace-busy'] as const)
 ('retains the orphan reservation when %s changes during native collection',async(mode)=>{
 const f=await completedOrphanFixture();
 if(mode==='cancel')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.task,JSON.stringify({runId:f.run})]);
 if(mode==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 if(mode==='manual')await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.task]);
 if(mode==='criterion')await pool.query("UPDATE tasks SET criteria=criteria||'[{\"id\":\"extra\",\"type\":\"artifact\",\"name\":\"new.json\",\"label\":\"New required evidence\"}]'::jsonb WHERE id=$1",[f.task]);
 if(mode==='context')await pool.query("UPDATE runs SET checkpoint='{\"original\":\"changed-context\"}' WHERE id=$1",[f.run]);
 if(mode==='workspace-busy'){
  const task=randomUUID(),run=randomUUID();
  await pool.query("INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count) VALUES($1,$2,'competing','Other task','Other authorized task','qa.backend','running','[]',$3,1)",
   [task,f.goal,JSON.stringify({kind:'opencode',workspace:f.delivery.workspace})]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,task,randomUUID()]);
 }
 await expect(adoptRecoveredArtifacts(f.run,f.fence,f.delivery,{nativeCompleted:f.proof})).rejects.toThrow();
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('reconciling');
 expect((await pool.query('SELECT state FROM opencode_jobs WHERE run_id=$1',[f.run])).rows[0].state).toBe('orphaned');
});

it.each(['inspection-source','artifact-conflict','invalid-protocol'] as const)
 ('rejects completed native outputs with %s while preserving the source attempt',async(mode)=>{
 const f=await completedOrphanFixture();
 if(mode==='inspection-source')f.proof.inspection.candidate.candidateDigest=hash('other-source');
 if(mode==='artifact-conflict')await pool.query("UPDATE artifacts SET digest=repeat('f',64) WHERE run_id=$1 AND name='changes.patch'",[f.run]);
 if(mode==='invalid-protocol'){
  const item=f.delivery.artifacts.find(artifact=>artifact.name==='protocol-report.json')!;
  const bytes=JSON.stringify({suiteExecuted:false,command:'node test.mjs'});item.base64=Buffer.from(bytes).toString('base64');item.digest=hash(bytes);
  await pool.query("DELETE FROM artifacts WHERE run_id=$1 AND name='protocol-report.json'",[f.run]);
  f.proof.inspection.state.artifacts=f.delivery.artifacts.map(artifact=>({name:artifact.name,digest:artifact.digest}));
  Object.assign(f.proof,await prepareCompletedNativeArtifactRecovery(f.run,f.fence));
 }
 await expect(adoptRecoveredArtifacts(f.run,f.fence,f.delivery,{nativeCompleted:f.proof})).rejects.toThrow();
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('reconciling');
});

it('does not turn successful orphan collection into acceptance of failed original criteria',async()=>{
 const f=await completedOrphanFixture();
 await pool.query("UPDATE tasks SET criteria=criteria||'[{\"id\":\"game_passed\",\"type\":\"json_field\",\"name\":\"protocol-report.json\",\"field\":\"passed\",\"equals\":true,\"label\":\"Functional requirement\"}]'::jsonb WHERE id=$1",[f.task]);
 Object.assign(f.proof,await prepareCompletedNativeArtifactRecovery(f.run,f.fence));
 const result=await adoptRecoveredArtifacts(f.run,f.fence,f.delivery,{nativeCompleted:f.proof});
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).not.toBe('accepted');
 expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=$1',[result.runId])).rows[0].passed).toBe(false);
});

it('recovers the omitted native companion report exactly once without starting another model', async () => {
  const f = await nativeFixture();
  let collections = 0;
  const collect = async () => {
    collections++;
    return f.delivery;
  };
  await Promise.all([
    recoverMissingOpenCodeArtifacts(collect),
    recoverMissingOpenCodeArtifacts(collect),
  ]);
  expect(collections).toBe(1);
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(task.state).toBe('accepted');
  expect(task.execution.continuation).toBeUndefined();
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
    'submitted',
  );
  expect(
    (await pool.query('SELECT passed FROM acceptances WHERE run_id=$1', [f.run])).rows[0].passed,
  ).toBe(false);
  const run = (
    await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [f.task])
  ).rows[0];
  expect(run).toMatchObject({
    runtime: 'opencode',
    worker_id: 'artifact-collector',
    usage: { totalTokens: 0 },
  });
  expect((await pool.query('SELECT * FROM opencode_jobs')).rowCount).toBe(0);
});

it('does not recover a changed published snapshot or manufacture a missing companion', async () => {
  const f = await nativeFixture();
  await expect(
    adoptRecoveredArtifacts(f.run, f.fence, {
      ...f.delivery,
      artifacts: f.delivery.artifacts.filter((a) => a.name !== 'protocol-report.md'),
    }),
  ).rejects.toThrow('ARTIFACT_MISSING');
  const changed = f.delivery.artifacts.find((a) => a.name === 'protocol-tests.log')!;
  changed.base64 = Buffer.from('ok, false replacement').toString('base64');
  changed.digest = hash('ok, false replacement');
  await expect(adoptRecoveredArtifacts(f.run, f.fence, f.delivery)).rejects.toThrow(
    'SOURCE_CHANGED_DURING_COLLECTION',
  );
  expect(
    (await pool.query('SELECT count(*) n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
  ).toBe('1');
});

it('does not collect real failed criteria, paused goals or manual recovery', async () => {
  const f = await nativeFixture();
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
  const collect = async () => {
    throw Error('should not collect');
  };
  expect(await recoverMissingOpenCodeArtifacts(collect)).toEqual([]);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  await pool.query('UPDATE tasks SET recovery=\'{"mode":"manual"}\' WHERE id=$1', [f.task]);
  expect(await recoverMissingOpenCodeArtifacts(collect)).toEqual([]);
  await pool.query('UPDATE tasks SET recovery=\'{"mode":"automatic"}\' WHERE id=$1', [f.task]);
  await pool.query(
    'UPDATE acceptances SET checks=\'[{"id":"executed","type":"json_field","passed":false}]\' WHERE run_id=$1',
    [f.run],
  );
  expect(await recoverMissingOpenCodeArtifacts(collect)).toEqual([]);
});

it('collects native packaging failures once before another model can repeat finished work', async () => {
  const f = await nativeFixture();
  await pool.query('DELETE FROM artifacts WHERE run_id=$1', [f.run]);
  await pool.query('DELETE FROM acceptances WHERE run_id=$1', [f.run]);
  await pool.query("UPDATE runs SET state='failed',error_code='ARTIFACT_NAME' WHERE id=$1", [
    f.run,
  ]);
  await pool.query("UPDATE tasks SET state='recovering_artifacts' WHERE id=$1", [f.task]);
  let calls = 0;
  const collect = async () => {
    calls++;
    return { ...f.delivery, checkedAt: new Date().toISOString() };
  };
  const result = await Promise.all([
    recoverMissingOpenCodeArtifacts(collect),
    recoverMissingOpenCodeArtifacts(collect),
  ]);
  expect(calls).toBe(1);
  expect(result.flat()).toHaveLength(1);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'accepted',
  );
  expect(
    (await pool.query('SELECT state,error_code FROM runs WHERE id=$1', [f.run])).rows[0],
  ).toEqual({ state: 'failed', error_code: 'ARTIFACT_NAME' });
  expect((await pool.query('SELECT 1 FROM opencode_jobs')).rowCount).toBe(0);
  expect(
    (
      await pool.query('SELECT usage FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [
        f.task,
      ])
    ).rows[0].usage.totalTokens,
  ).toBe(0);
});

for (const stage of ['integrate', 'verify']) it(`collects a generic ${stage} manifest without replacing its substantive criteria or starting a model`, async () => {
  const f = await fixture();
  await pool.query("UPDATE tasks SET stage=$2,state='recovering_artifacts',execution=$3 WHERE id=$1", [f.task, stage,
    JSON.stringify({ kind: 'opencode', workspace: 'artifact-test/fix', managed: true })]);
  await pool.query("UPDATE runs SET runtime='opencode',error_code='RESULT_FORMAT' WHERE id=$1", [f.run]);
  const rejectedBytes = Buffer.from('{"passed":false,"reason":"real business requirement missing"}');
  const candidate = { baseDigest: hash('base'), candidateDigest: hash('candidate'), files: [] };
  const result = { ...f.delivery, collectorVersion: 'artifact-manifest-v1',
    candidate,
    manifest: { version: 1, runId: f.run, digest: hash('current-attempt-manifest') },
    artifacts: f.delivery.artifacts.map(a => {
      const bytes = a.name === 'verification.json' ? rejectedBytes :
        Buffer.from(a.name === 'candidate.json' ? JSON.stringify(candidate) : '');
      return { ...a, base64: bytes.toString('base64'), digest: hash(bytes) };
    }) };
  let collections = 0;
  const collected = await recoverMissingOpenCodeArtifacts(async (runId, _readOnly, testFile) => {
    expect(runId).toBe(f.run); expect(testFile).toBeUndefined(); collections++;
    return { ...result, checkedAt: new Date().toISOString() };
  });
  expect(collections).toBe(1); expect(collected).toHaveLength(1);
  const latest = (await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [f.task])).rows[0];
  expect(latest.worker_id).toBe('artifact-collector');
  expect(latest.usage.totalTokens).toBe(0);
  expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=$1', [latest.id])).rows[0].passed).toBe(false);
  expect((await pool.query('SELECT criteria,state FROM tasks WHERE id=$1', [f.task])).rows[0]).toMatchObject({
    criteria: [{ id: 'test', name: 'verification.json', field: 'passed', equals: true }], state: 'submitted' });
  expect((await pool.query('SELECT 1 FROM opencode_jobs WHERE run_id=$1', [latest.id])).rowCount).toBe(0);
  expect((await pool.query('SELECT state,error_code FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual({ state: 'failed', error_code: 'RESULT_FORMAT' });
});

it('does not admit generic native recovery with an absent, foreign or stale manifest receipt', async () => {
  const f = await fixture();
  await pool.query("UPDATE tasks SET stage='integrate',state='recovering_artifacts',execution=$2 WHERE id=$1", [f.task,
    JSON.stringify({ kind: 'opencode', workspace: 'artifact-test/fix', managed: true })]);
  await pool.query("UPDATE runs SET runtime='opencode',error_code='RESULT_FORMAT' WHERE id=$1", [f.run]);
  for (const extra of [{}, { collectorVersion: 'artifact-manifest-v1', manifest: { version: 1, runId: randomUUID(), digest: hash('manifest') } }])
    await expect(adoptRecoveredArtifacts(f.run, f.fence, { ...f.delivery, ...extra })).rejects.toThrow('GENERIC_DELIVERY_MANIFEST_REQUIRED');
  await expect(adoptRecoveredArtifacts(f.run, f.fence, { ...f.delivery, collectorVersion: 'artifact-manifest-v1',
    manifest: { version: 1, runId: f.run, digest: hash('manifest') }, checkedAt: new Date(Date.now() - 120000).toISOString() })).rejects.toThrow('STOP_UNCONFIRMED');
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1', [f.task])).rowCount).toBe(1);
});

it('falls back to native delivery repair only after a definitive collection failure and fresh stop', async () => {
  const f = await nativeFixture();
  await pool.query('DELETE FROM artifacts WHERE run_id=$1', [f.run]);
  await pool.query('DELETE FROM acceptances WHERE run_id=$1', [f.run]);
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency) VALUES('native-package','test','openai','glm','[\"glm\"]',true,true,12)",
  );
  await pool.query(
    "UPDATE runs SET state='failed',error_code='ARTIFACT_NAME',account_id='native-package',model='glm' WHERE id=$1",
    [f.run],
  );
  await pool.query(
    "UPDATE tasks SET state='recovering_artifacts',account_id='native-package',model='glm' WHERE id=$1",
    [f.task],
  );
  let inspections = 0;
  await recoverMissingOpenCodeArtifacts(
    async () => {
      throw Error('ARTIFACT_MISSING');
    },
    async () => {
      inspections++;
      return {
        ...f.delivery,
        checkedAt: new Date().toISOString(),
        runtime: 'opencode',
        state: { code: 'ARTIFACT_NAME' },
      };
    },
  );
  expect(inspections).toBe(1);
  expect(
    (await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1', [f.task])).rows[0],
  ).toEqual({ state: 'queued', attempt_count: 16 });
  expect(
    (
      await pool.query(
        'SELECT checkpoint FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',
        [f.task],
      )
    ).rows[0].checkpoint.collectionFailure,
  ).toBe('ARTIFACT_MISSING');
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='ARTIFACT_COLLECTION_FALLBACK'")).rowCount,
  ).toBe(1);
  expect((await pool.query('SELECT 1 FROM opencode_jobs')).rowCount).toBe(1);
});

it('does not infer terminal state from unavailable collection observation', async () => {
  const f = await nativeFixture();
  await pool.query('DELETE FROM artifacts WHERE run_id=$1', [f.run]);
  await pool.query('DELETE FROM acceptances WHERE run_id=$1', [f.run]);
  await pool.query("UPDATE runs SET state='failed',error_code='ARTIFACT_NAME' WHERE id=$1", [
    f.run,
  ]);
  await pool.query("UPDATE tasks SET state='recovering_artifacts' WHERE id=$1", [f.task]);
  let inspections = 0;
  expect(
    await recoverMissingOpenCodeArtifacts(
      async () => {
        throw Error('COLLECTION_UNAVAILABLE');
      },
      async () => {
        inspections++;
        return f.delivery;
      },
    ),
  ).toEqual([]);
  expect(inspections).toBe(0);
  expect(
    (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
  ).toBe(1);
});

it.each([
  'cancel',
  'live',
  'manual-during-collection',
  'pause-during-collection',
  'autonomy-during-collection',
])('does not adopt native package files after %s', async (mode) => {
  const f = await nativeFixture();
  await pool.query('DELETE FROM artifacts WHERE run_id=$1', [f.run]);
  await pool.query('DELETE FROM acceptances WHERE run_id=$1', [f.run]);
  await pool.query("UPDATE runs SET state='failed',error_code='ARTIFACT_NAME' WHERE id=$1", [
    f.run,
  ]);
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1", [f.task]);
  if (mode === 'cancel')
    await pool.query(
      "INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",
      [f.task, JSON.stringify({ runId: f.run })],
    );
  if (mode === 'live')
    await pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,16,$3,'opencode',1,'running')",
      [randomUUID(), f.task, randomUUID()],
    );
  let calls = 0;
  const collect = async () => {
    calls++;
    if (mode === 'manual-during-collection')
      await pool.query('UPDATE tasks SET recovery=\'{"mode":"manual"}\' WHERE id=$1', [f.task]);
    if (mode === 'pause-during-collection')
      await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
    if (mode === 'autonomy-during-collection')
      await pool.query(
        "INSERT INTO settings VALUES('autonomy:artifact-test','{\"enabled\":false}') ON CONFLICT(id) DO UPDATE SET value=excluded.value",
      );
    return f.delivery;
  };
  try {
    expect(await recoverMissingOpenCodeArtifacts(collect)).toEqual([]);
    expect(calls).toBe(['cancel', 'live'].includes(mode) ? 0 : 1);
    expect(
      (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
    ).toBe(mode === 'live' ? 2 : 1);
  } finally {
    await pool.query("DELETE FROM settings WHERE id='autonomy:artifact-test'");
  }
});

it('recovers the explicitly named coordination JSON once and leaves the original report unchanged', async () => {
  const f = await fixture();
  const value = JSON.stringify({
    summary: 'Still blocked',
    priorities: [],
    recommendations: [],
    backlog: { reason: 'Pending integration', tasks: [] },
  });
  const content = '# coordination.json\n\n```json\n' + value + '\n```\nOriginal limitations.';
  await pool.query(
    "UPDATE tasks SET state='submitted',stage='coordinate',execution=NULL,criteria=$2 WHERE id=$1",
    [
      f.task,
      JSON.stringify([
        { id: 'coordination', type: 'artifact', name: 'coordination.json', label: 'Plan' },
      ]),
    ],
  );
  await pool.query("UPDATE runs SET state='submitted' WHERE id=$1", [f.run]);
  const id = randomUUID();
  await mkdir(path.join(config.dataDir, 'artifacts'), { recursive: true });
  await writeFile(path.join(config.dataDir, 'artifacts', id), content);
  await pool.query(
    'INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
    [id, f.run, 'report.md', 'text/markdown', hash(content), Buffer.byteLength(content)],
  );
  await Promise.all([recoverAnalyticalDeliveries(), recoverAnalyticalDeliveries()]);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'accepted',
  );
  expect(
    (await pool.query('SELECT count(*) n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
  ).toBe('2');
  expect(
    (await pool.query('SELECT name,digest FROM artifacts WHERE run_id=$1', [f.run])).rows,
  ).toEqual([{ name: 'report.md', digest: hash(content) }]);
  for (const text of [
    value,
    '```json\n' + value + '\n```',
    content + '\n```json\n' + value + '\n```',
    '# coordination.json\n```json\n{}\n```',
  ]) {
    const source = [{ name: 'report.md', content: text }];
    expect(recoverCoordinationArtifact(source)).toBe(source);
  }
});


// The collector holds no admission transaction while awaiting the remote host.
// Apply the real accepted supervision decision in that exact interval.
async function nativeCollectionDecision(f: Awaited<ReturnType<typeof nativeFixture>>) {
  const caseId = randomUUID(), coordinator = randomUUID(), decisionRun = randomUUID(), decisionFence = randomUUID();
  await pool.query("INSERT INTO settings VALUES($1,'{\"enabled\":true}')", ['supervision:' + f.goal]);
  const action = { caseId, kind: 'revise', diagnosis: 'The existing output needs the supervised packaging approach.',
    instruction: 'Inspect the preserved output and keep the original criteria. Follow this packaging decision before another attempt.' };
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision)
    VALUES($1,$2,$3,$4,'decided','Packaging requires a decision','[]',$5)`,
    [caseId,f.goal,f.task,f.run,JSON.stringify({actions:[action]})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,'collector-supervisor','Supervisor','Decide the next approach','qa.backend','running','coordinate',
      '[{"id":"decision","type":"artifact","name":"coordination.json"}]',1)`,[coordinator,f.goal]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,1,$3,'harness',1,'running')`,[decisionRun,coordinator,decisionFence]);
  await writeArtifact(decisionRun,decisionFence,'coordination.json',JSON.stringify({supervision:{actions:[action]}}),'application/json');
  await completeRun(decisionRun,decisionFence,'Accepted fixture decision',{});
  await validateTask(coordinator);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
  await pool.query('UPDATE supervision_cases SET decision_run_id=$2 WHERE id=$1',[caseId,decisionRun]);
  return caseId;
}
async function nativeCollectionState(f: Awaited<ReturnType<typeof nativeFixture>>) {
  return {
    task:(await pool.query(`SELECT state,attempt_count,description,criteria,execution,instruction_revision_id,account_id,model
      FROM tasks WHERE id=$1`,[f.task])).rows[0],
    runs:(await pool.query('SELECT id,attempt,fence,state,error_code,checkpoint FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows,
    cases:(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1 ORDER BY id',[f.task])).rows,
    artifacts:(await pool.query('SELECT id,run_id,name,digest FROM artifacts WHERE run_id=$1 ORDER BY id',[f.run])).rows,
    leases:(await pool.query('SELECT * FROM leases WHERE run_id=$1',[f.run])).rows,
    jobs:(await pool.query('SELECT * FROM opencode_jobs WHERE run_id IN(SELECT id FROM runs WHERE task_id=$1) ORDER BY run_id',[f.task])).rows,
  };
}
it.each([['submitted','delivery'],['failed','delivery'],['recovering_artifacts','delivery'],
  ['failed','ARTIFACT_MISSING'],['recovering_artifacts','ARTIFACT_MISSING']] as const)
('keeps the applied native owner after collection in %s ends with %s',async(state,result)=>{
  const f=await nativeFixture();
  if(state!=='submitted'){
    await pool.query('DELETE FROM artifacts WHERE run_id=$1',[f.run]);
    await pool.query('DELETE FROM acceptances WHERE run_id=$1',[f.run]);
    await pool.query("UPDATE runs SET state='failed',error_code='ARTIFACT_NAME' WHERE id=$1",[f.run]);
    if(state==='recovering_artifacts'){
      await installArtifactRecoveryPolicy(pool);
      await pool.query("INSERT INTO settings VALUES('artifact-recovery','{\"enabled\":true}')");
    }
    // Use the canonical trigger, not a fabricated recovering_artifacts row.
    await pool.query("UPDATE tasks SET state='failed' WHERE id=$1",[f.task]);
  }
  if(result==='ARTIFACT_MISSING'||state==='recovering_artifacts'){
    await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency)
      VALUES('native-package','test','openai','glm','["glm"]',true,true,12)`);
    await pool.query("UPDATE runs SET account_id='native-package',model='glm' WHERE id=$1",[f.run]);
    await pool.query("UPDATE tasks SET account_id='native-package',model='glm' WHERE id=$1",[f.task]);
  }
  const caseId=await nativeCollectionDecision(f);
  let applied: Awaited<ReturnType<typeof nativeCollectionState>> | undefined;
  const inspect=vi.fn(async()=>({...f.delivery,runtime:'opencode',checkedAt:new Date().toISOString(),state:{code:'ARTIFACT_NAME'}}));
  const collect=vi.fn(async()=>{
    await applySupervisionDecisions(async()=>({...f.delivery,checkedAt:new Date().toISOString()}));
    applied=await nativeCollectionState(f);
    expect(applied.task.state).toBe(state);
    expect(applied.cases[0]).toMatchObject({id:caseId,state:'applied',applied_attempt:15});
    expect(applied.task.execution.supervision).toMatchObject({caseId,sourceRunId:f.run});
    if(result==='ARTIFACT_MISSING')throw Error(result);
    return {...f.delivery,checkedAt:new Date().toISOString()};
  });
  expect(await recoverMissingOpenCodeArtifacts(collect,inspect)).toEqual([]);
  expect(collect).toHaveBeenCalledTimes(1);
  expect(inspect).toHaveBeenCalledTimes(result==='ARTIFACT_MISSING'?1:0);
  expect(applied).toBeDefined();
  expect(await nativeCollectionState(f)).toEqual(applied);
  expect(await appliedSupervisionOwnsContinuation(pool,f.task,f.run)).toBe(true);
  expect((await pool.query(`SELECT kind FROM events WHERE entity_id=$1 AND kind IN
    ('ARTIFACTS_RECOVERED','ARTIFACT_COLLECTION_FALLBACK')`,[f.task])).rows).toEqual([]);
  expect((await pool.query("SELECT payload->>'code' code FROM events WHERE entity_id=$1 AND kind='ARTIFACT_COLLECTION_DEFERRED'",[f.task])).rows)
    .toEqual(result==='ARTIFACT_MISSING'?[{code:'TASK_CHANGED'}]:[]);
  if(state==='recovering_artifacts'){
    expect((await readSupervisedResumeCandidates()).map(row=>row.id)).toContain(f.task);
    expect(await resumeSupervisedWork(async()=>({...f.delivery,runtime:'opencode',state:{code:'ARTIFACT_NAME',usage:null},checkedAt:new Date().toISOString()}))).toEqual([f.task]);
    const prepared=await readPreparedNativeAdmission(pool,f.task);
    expect(prepared).not.toBeNull();
    expect(prepared!.receipt).toMatchObject({caseId,sourceRunId:f.run,sourceFence:f.fence,attempt:15});
    expect(prepared!.task).toMatchObject({state:'retry_scheduled',attempt_count:15,
      description:applied!.task.description,criteria:applied!.task.criteria,instruction_revision_id:applied!.task.instruction_revision_id});
    expect(prepared!.task.execution.supervision).toEqual(applied!.task.execution.supervision);
    expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.task])).rows[0].n).toBe(1);
    expect((await pool.query('SELECT 1 FROM opencode_jobs WHERE run_id IN(SELECT id FROM runs WHERE task_id=$1)',[f.task])).rowCount).toBe(0);
  }
});

it('leaves an advanced native attempt untouched when an earlier collector returns',async()=>{
  const f=await nativeFixture();
  let advanced: Awaited<ReturnType<typeof nativeCollectionState>> | undefined;
  const inspect=vi.fn(async()=>f.delivery);
  const collect=vi.fn(async()=>{
    await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code)
      VALUES($1,$2,16,$3,'opencode',1,'failed','RESULT_FORMAT')`,[randomUUID(),f.task,randomUUID()]);
    await pool.query("UPDATE tasks SET state='failed',attempt_count=16 WHERE id=$1",[f.task]);
    advanced=await nativeCollectionState(f);
    return {...f.delivery,checkedAt:new Date().toISOString()};
  });
  expect(await recoverMissingOpenCodeArtifacts(collect,inspect)).toEqual([]);
  expect(collect).toHaveBeenCalledTimes(1);
  expect(inspect).not.toHaveBeenCalled();
  expect(advanced).toBeDefined();
  expect(await nativeCollectionState(f)).toEqual(advanced);
});
