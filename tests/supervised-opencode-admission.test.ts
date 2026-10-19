import { beforeAll, beforeEach, afterAll, afterEach, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { recoverOpenCodeRun } from '../server/opencode-recovery.js';
import { installWorkforcePolicy } from '../server/workforce-policy.js';
import { installSupervision, recordSupervisionDecisions, applySupervisionDecisions } from '../server/supervision.js';
import { requestRecoveryReview } from '../server/recovery-supervision.js';
import { readDeliverySourceIdentity, sourceIdentityFrozen } from '../server/delivery-source-resolution.js';
import { scheduledSupervisionAdmission } from '../server/supervised-admission.js';
import { writeArtifact, completeRun, validateTask, schedule, readScheduleCandidates } from '../server/service.js';

beforeAll(async()=>{
 expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
 await migrate(); await installSupervision();
 await pool.query(`CREATE TABLE IF NOT EXISTS opencode_jobs(run_id uuid PRIMARY KEY REFERENCES runs(id),state text DEFAULT 'queued',created_at timestamptz DEFAULT now(),started_at timestamptz,finished_at timestamptz)`);
 await installWorkforcePolicy(pool);
});
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE'); await seedRegistry();
 await pool.query(`UPDATE settings SET value='{"maxRuns":28,"browserSlots":8}' WHERE id IN('capacity','remote-capacity')`);
 config.bridgeHost='test-only';config.remoteExecution=true;
});
afterEach(()=>{config.bridgeHost='';config.remoteExecution=false;vi.restoreAllMocks();});
afterAll(async()=>{await pool.end();});
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

async function invalidatedSubmission(collected=false) {
  const f:any = await fixture();
  if(collected){
    const originalRun=f.run,collectedRun=randomUUID(),collectedFence=randomUUID();
    await pool.query(`UPDATE runs SET state='failed',agent_version=3 WHERE id=$1`,[originalRun]);
    await pool.query('DELETE FROM leases WHERE run_id=$1',[originalRun]);
    await pool.query(`UPDATE tasks SET state='failed',attempt_count=2,max_attempts=2 WHERE id=$1`,[f.task]);
    await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,checkpoint)
      VALUES($1,$2,2,$3,'opencode',9,'failed','RESULT_FORMAT',$4)`,[collectedRun,f.task,collectedFence,
      JSON.stringify({kind:'artifact_recovery',sourceRunId:originalRun})]);
    f.run=collectedRun;f.fence=collectedFence;f.executionRunId=originalRun;
    f.proof={...f.proof,runId:collectedRun,executionRunId:originalRun};
  }
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


async function preparedFixture(collected=false){
 const f=await invalidatedSubmission(collected);
 await pool.query(`UPDATE accounts SET bridge_id=id,max_concurrency=1 WHERE id='opencode-test'`);
 await pool.query(`UPDATE tasks SET stage='integrate' WHERE id=$1`,[f.task]);
 await pool.query(`INSERT INTO settings(id,value) VALUES($1,$2)`,['workforce:'+f.goal,JSON.stringify({enabled:true,
  glmAccountId:'opencode-test',glmTarget:12,deliveryFirstEnabled:true,workerAccounts:[],
  coordinatorAccountId:'opencode-test',coordinatorModel:'glm'})]);
 await pool.query(`UPDATE goals SET state='active' WHERE id=$1`,[f.goal]);
 return {...f,proof:{...f.proof,checkedAt:new Date().toISOString(),candidate:{...f.proof.candidate,candidateDigest:'a'.repeat(64)}}};
}
async function ready(f:any,account='opencode-test',resources?:string[]){
 const id=randomUUID(),workspace='isolated/'+id;
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,criteria,execution,resources)
  VALUES($1,$2,$3,'Auxiliary diagnosis','Bounded diagnostic','qa.backend',$4,'glm','ready','[]',$5,$6)`,
  [id,f.goal,'ready-'+id,account,JSON.stringify({kind:'opencode',workspace,workKind:'diagnostic'}),JSON.stringify(resources??['remote-slot','workspace:'+workspace])]);
 return id;
}
const stage=(f:any)=>recoverOpenCodeRun(f.run,f.fence,f.proof,{prepareSupervised:true});
async function fresh(f:any){return {...f.proof,checkedAt:new Date().toISOString()};}
async function releaseQueued(runId:string){
 await pool.query(`UPDATE runs SET state='failed',finished_at=now() WHERE id=$1`,[runId]);
 await pool.query(`UPDATE tasks SET state='cancelled' WHERE id=(SELECT task_id FROM runs WHERE id=$1)`,[runId]);
 await pool.query('DELETE FROM leases WHERE run_id=$1',[runId]);
}
it('prepares with a full account and admits the consumer ahead of later diagnoses in the common queue',async()=>{
 const f=await preparedFixture(),occupied=await ready(f);
 const [occupant]=await schedule();expect((await pool.query('SELECT task_id FROM runs WHERE id=$1',[occupant])).rows[0].task_id).toBe(occupied);
 const before=(await pool.query(`SELECT count(*)::int n FROM events WHERE kind='EXECUTION_RETRY_ADMITTED'`)).rows[0].n;
 const result=await stage(f);expect(result).toMatchObject({prepared:true,runId:null,attempt:1});
 expect((await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual({state:'retry_scheduled',attempt_count:1});
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt>1',[f.task])).rowCount).toBe(0);
 expect((await pool.query(`SELECT count(*)::int n FROM events WHERE kind='EXECUTION_RETRY_ADMITTED'`)).rows[0].n).toBe(before);
 expect(await scheduledSupervisionAdmission(pool,f.task,f.run)).toMatchObject({case_id:f.caseId,source_run_id:f.run});
 const inspector=vi.fn(()=>fresh(f));expect(await schedule('agentos-opencode',inspector)).toEqual([]);expect(inspector).not.toHaveBeenCalled();
 await ready(f);await ready(f);await releaseQueued(occupant);
 const queued=await schedule('agentos-opencode',inspector);expect(queued).toHaveLength(1);
 const next=(await pool.query('SELECT * FROM runs WHERE id=$1',[queued[0]])).rows[0];
 expect(next).toMatchObject({task_id:f.task,attempt:2,runtime:'opencode',account_id:'opencode-test',model:'glm'});
 expect(next.checkpoint.kind).toBe('opencode_recovery');expect(next.checkpoint.previousRunId).toBe(f.run);
 expect(next.instruction_snapshot.task.execution.recoveryContext).toEqual(next.checkpoint);
 expect(next.instruction_snapshot.task.execution.supervisedAdmission).toBeUndefined();
 expect((await pool.query('SELECT execution FROM tasks WHERE id=$1',[f.task])).rows[0].execution.supervisedAdmission).toBeUndefined();
 expect(sourceIdentityFrozen(await readDeliverySourceIdentity(pool,f.task))).toBe(true);
 expect((await pool.query('SELECT 1 FROM opencode_jobs WHERE run_id=$1',[next.id])).rowCount).toBe(1);
 expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows).toEqual(f.original.acceptances);
 expect((await pool.query(`SELECT count(*)::int n FROM events WHERE kind='EXECUTION_RETRY_ADMITTED'`)).rows[0].n).toBe(before+1);
 expect((await pool.query(`SELECT 1 FROM events WHERE kind='SUPERVISION_RESUMED' AND entity_id=$1`,[f.task])).rowCount).toBe(1);
});
it('two independent schedulers inspect outside the mutex and queue exactly one next attempt',async()=>{
 const f=await preparedFixture();await stage(f);await ready(f);
 let count=0,release!:()=>void;const both=new Promise<void>(r=>release=r);
 const inspect=async()=>{if(++count===2)release();await both;return fresh(f);};
 const result=(await Promise.all([schedule(undefined,inspect),schedule('agentos-opencode',inspect)])).flat();
 expect(count).toBe(2);expect(result).toHaveLength(1);
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt=2',[f.task])).rowCount).toBe(1);
 expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1',[result[0]])).rowCount).toBe(2);
});
it.each(['quota','resource'])('a prepared consumer blocked by %s does not inspect or reserve an unrelated slot',async mode=>{
 const f=await preparedFixture();await stage(f);
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
  VALUES('other','other','openai','glm','["glm"]',true,1,true,'other')`);
 const other=await ready(f,'other');
 if(mode==='quota')await pool.query(`UPDATE accounts SET quota='{"ordinaryUsageAllowed":false}' WHERE id='opencode-test'`);
 else {const lockTask=await ready(f,'other',[]),r=randomUUID();await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'builtin',1,'running')`,[r,lockTask,randomUUID()]);await pool.query(`INSERT INTO leases(resource,run_id) VALUES('workspace:isolated/test',$1)`,[r]);}
 const inspect=vi.fn(()=>fresh(f));const [id]=await schedule(undefined,inspect);
 expect((await pool.query('SELECT task_id FROM runs WHERE id=$1',[id])).rows[0].task_id).toBe(other);expect(inspect).not.toHaveBeenCalled();
 expect((await pool.query('SELECT attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0].attempt_count).toBe(1);
});
it.each(['failure','expired','active','wrong-unit','wrong-workspace'])('failed physical inspection %s cannot launch or hold a slot',async mode=>{
 const f=await preparedFixture();await stage(f);const other=await ready(f);
 const inspect=async()=>{if(mode==='failure')throw Error('private transport error');const p:any=await fresh(f);
  if(mode==='expired')p.checkedAt=new Date(Date.now()-61000).toISOString();
  if(mode==='active')p.process={...p.process,populated:true,mainPid:7,stopped:false};
  if(mode==='wrong-unit')p.process={...p.process,unit:'agentos-run-'+randomUUID()};
  if(mode==='wrong-workspace')p.workspace='isolated/other';return p;};
 const [id]=await schedule(undefined,inspect);expect((await pool.query('SELECT task_id FROM runs WHERE id=$1',[id])).rows[0].task_id).toBe(other);
 expect((await pool.query('SELECT state,attempt_count,next_attempt_at FROM tasks WHERE id=$1',[f.task])).rows[0]).toMatchObject({state:'waiting_resource',attempt_count:1,next_attempt_at:expect.any(Date)});
 expect((await pool.query(`SELECT 1 FROM events WHERE kind='EXECUTION_RETRY_ADMITTED' AND entity_id=$1`,[f.task])).rowCount).toBe(0);
});
it.each(['cancel','decision','description','fence','route','pause','dependencies','marker-removed','marker-null'])('rechecks %s changed while inspection is outside the lock',async mode=>{
 const f=await preparedFixture();await stage(f);
 const inspect=async()=>{
  if(mode==='cancel')await pool.query(`INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)`,[f.task,JSON.stringify({runId:f.run})]);
  if(mode==='decision')await pool.query(`UPDATE supervision_cases SET decision=decision||'{"changed":true}' WHERE id=$1`,[f.caseId]);
  if(mode==='description')await pool.query(`UPDATE tasks SET description=description||' changed' WHERE id=$1`,[f.task]);
  if(mode==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.run,randomUUID()]);
  if(mode==='route')await pool.query(`UPDATE tasks SET model='other' WHERE id=$1`,[f.task]);
  if(mode==='pause')await pool.query(`UPDATE goals SET state='paused' WHERE id=$1`,[f.goal]);
  if(mode==='marker-removed')await pool.query(`UPDATE tasks SET execution=execution-'supervisedAdmission' WHERE id=$1`,[f.task]);
  if(mode==='marker-null')await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{supervisedAdmission}','null'::jsonb) WHERE id=$1`,[f.task]);
  if(mode==='dependencies'){const dep=await ready(f);await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[f.task,dep]);}
  return fresh(f);
 };
 await schedule(undefined,inspect);
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt>1',[f.task])).rowCount).toBe(0);
 expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id<>$1 AND task_id=$2',[f.run,f.task])).rowCount).toBe(0);
});
it('keeps the prepared contract after transient stop failure and resumes after the backoff with fresh proof',async()=>{
 const f=await preparedFixture();await stage(f);
 const original=(await pool.query(`SELECT execution->'supervisedAdmission' receipt FROM tasks WHERE id=$1`,[f.task])).rows[0].receipt;
 expect(await schedule(undefined,async()=>{throw Error('no observation');})).toEqual([]);
 const inspect=vi.fn(()=>fresh(f));expect(await schedule(undefined,inspect)).toEqual([]);expect(inspect).not.toHaveBeenCalled();
 expect((await pool.query(`SELECT execution->'supervisedAdmission' receipt FROM tasks WHERE id=$1`,[f.task])).rows[0].receipt).toEqual(original);
 await pool.query(`UPDATE tasks SET next_attempt_at=now()-interval '1 second' WHERE id=$1`,[f.task]);
 expect(await schedule(undefined,inspect)).toHaveLength(1);expect(inspect).toHaveBeenCalledTimes(1);
});

it('does not hold the admission mutex while inspecting a physical process',async()=>{
 const f=await preparedFixture();await stage(f);
 const result=await schedule(undefined,async()=>{
  const client=await pool.connect();try{await client.query('BEGIN');expect((await client.query('SELECT pg_try_advisory_xact_lock(4280001) ok')).rows[0].ok).toBe(true);}
  finally{await client.query('ROLLBACK');client.release();}return fresh(f);
 });expect(result).toHaveLength(1);
});

it('uses the native queue even for a supervised goal without a workforce policy',async()=>{
 const f=await preparedFixture();await pool.query('DELETE FROM settings WHERE id=$1',['workforce:'+f.goal]);
 await stage(f);const [id]=await schedule(undefined,()=>fresh(f));
 expect((await pool.query('SELECT 1 FROM opencode_jobs WHERE run_id=$1',[id])).rowCount).toBe(1);
 expect((await pool.query('SELECT 1 FROM outbox WHERE run_id=$1',[id])).rowCount).toBe(0);
});

it('propagates an unexpected database failure with rollback instead of fabricating a recovery refusal',async()=>{
 const f=await preparedFixture();await stage(f);
 const before=(await pool.query('SELECT state,execution,attempt_count,reason FROM tasks WHERE id=$1',[f.task])).rows[0];
 const name='test_native_fault_'+randomUUID().replaceAll('-','');
 await pool.query(`CREATE FUNCTION ${name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  IF NEW.task_id='${f.task}'::uuid THEN RAISE EXCEPTION USING ERRCODE='XX000',MESSAGE='test database failure'; END IF; RETURN NEW; END $$;
  CREATE TRIGGER ${name} BEFORE INSERT ON runs FOR EACH ROW EXECUTE FUNCTION ${name}()`);
 try {
  await expect(schedule(undefined,()=>fresh(f))).rejects.toMatchObject({code:'XX000'});
  expect((await pool.query('SELECT state,execution,attempt_count,reason FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual(before);
  expect((await pool.query(`SELECT 1 FROM events WHERE kind='SUPERVISED_NATIVE_ADMISSION_DEFERRED' AND entity_id=$1`,[f.task])).rowCount).toBe(0);
 }finally{await pool.query(`DROP TRIGGER ${name} ON runs; DROP FUNCTION ${name}()`);}
});

it('prepares artifact collection against the original physical execution and route without retargeting its unit',async()=>{
 const f=await preparedFixture(true);await stage(f);
 const receipt=(await pool.query(`SELECT execution->'supervisedAdmission' receipt FROM tasks WHERE id=$1`,[f.task])).rows[0].receipt;
 expect(receipt).toMatchObject({sourceRunId:f.run,executionRunId:f.executionRunId,attempt:2,route:{accountId:'opencode-test',model:'glm',agentVersion:3}});
 const wrong=await fresh(f);wrong.executionRunId=f.run;
 expect(await schedule(undefined,async()=>wrong)).toEqual([]);
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt=3',[f.task])).rowCount).toBe(0);
 await pool.query(`UPDATE tasks SET next_attempt_at=now()-interval '1 second' WHERE id=$1`,[f.task]);
 const [id]=await schedule(undefined,()=>fresh(f));
 expect((await pool.query('SELECT task_id,attempt,account_id,model,agent_version FROM runs WHERE id=$1',[id])).rows[0])
  .toMatchObject({task_id:f.task,attempt:3,account_id:'opencode-test',model:'glm',agent_version:3});
 expect((await pool.query('SELECT checkpoint FROM runs WHERE id=$1',[f.run])).rows[0].checkpoint)
  .toEqual({kind:'artifact_recovery',sourceRunId:f.executionRunId});
});
it('preserves a fresh provider Retry-After without reserving capacity or counting an admission',async()=>{
 const f=await preparedFixture();await stage(f);const other=await ready(f);
 const observedAt=new Date().toISOString(),notBefore=new Date(Date.now()+120000).toISOString();
 const proof=await fresh(f);proof.state={runId:f.run,workspace:'isolated/test',model:'glm',
  providerBackoff:{version:1,source:'http_retry_after',runId:f.run,workspace:'isolated/test',model:'glm',
  status:429,category:'rate_limit',observedAt,notBefore}};
 const [id]=await schedule(undefined,async()=>proof);
 expect((await pool.query('SELECT task_id FROM runs WHERE id=$1',[id])).rows[0].task_id).toBe(other);
 const t=(await pool.query('SELECT attempt_count,next_attempt_at FROM tasks WHERE id=$1',[f.task])).rows[0];
 expect(t.attempt_count).toBe(1);expect(t.next_attempt_at.toISOString()).toBe(notBefore);
 expect((await pool.query(`SELECT 1 FROM events WHERE kind='EXECUTION_RETRY_ADMITTED' AND entity_id=$1`,[f.task])).rowCount).toBe(0);
});

it('does not treat a prepared event from an older attempt as authority for a later ordinary admission',async()=>{
 const f=await preparedFixture();await stage(f);const [first]=await schedule(undefined,()=>fresh(f));
 await pool.query(`UPDATE runs SET state='failed',finished_at=now() WHERE id=$1`,[first]);
 await pool.query('DELETE FROM leases WHERE run_id=$1',[first]);
 await pool.query(`UPDATE tasks SET state='ready' WHERE id=$1`,[f.task]);
 const candidate=(await readScheduleCandidates()).find(t=>t.id===f.task);
 expect(candidate.supervised_native_prepared).toBe(false);
 const inspect=vi.fn(()=>fresh(f)),[second]=await schedule(undefined,inspect);
 expect(inspect).not.toHaveBeenCalled();
 expect((await pool.query('SELECT attempt FROM runs WHERE id=$1',[second])).rows[0].attempt).toBe(3);
});
