import {afterAll,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,type DB} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {observeInputFailure,observeInputFailureBounded} from '../server/input-failure-observation.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(()=>pool.end());
const failure=()=>Object.assign(Error('PRIVATE_PROMPT_AND_TOKEN'),{
  code:'INPUT_MANIFEST_CHANGED',stderr:'PRIVATE_STDERR',inputFailure:{phase:'effective_source_manifest',
    subcode:'EFFECTIVE_INPUT_MISSING',inputCount:4,totalBytes:96,limitBytes:100,token:'PRIVATE_TOKEN',command:'PRIVATE_COMMAND'}});
async function fixture(state='failed'){
  const goal=randomUUID(),task=randomUUID(),id=randomUUID(),fence=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Diagnosis','No agents','input-diagnosis',$1::text)",[goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count,execution)
    VALUES($1::uuid,$2::uuid,$1::text,'Task','Original contract','qa.backend',$3,'[]',1,
      '{"kind":"harness","workspace":"test/input-diagnosis"}')`,[task,goal,state]);
  const run=(await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,checkpoint)
    VALUES($1,$2,1,$3,'harness',1,$4,'INPUT_MANIFEST_CHANGED','{"priorEvidence":true}') RETURNING *`,[id,task,fence,state])).rows[0];
  await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)',['test/input-diagnosis',id]);
  return run;
}
async function authority(run:any){return {
  run:(await pool.query('SELECT * FROM runs WHERE id=$1',[run.id])).rows,
  task:(await pool.query('SELECT * FROM tasks WHERE id=$1',[run.task_id])).rows,
  leases:(await pool.query('SELECT * FROM leases WHERE run_id=$1 ORDER BY resource',[run.id])).rows,
  acceptances:(await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[run.id])).rows,
};}

it.each(['running','reconciling','failed','cancelled'])('records sanitized current diagnostics without changing %s authority or leases',async state=>{
  const run=await fixture(state),before=await authority(run);
  expect(await observeInputFailure(pool,run,failure())).toBe(true);
  const rows=(await pool.query("SELECT entity_id,payload FROM events WHERE kind='RUN_INPUT_FAILURE'")).rows;
  expect(rows).toEqual([{entity_id:run.task_id,payload:{runId:run.id,runFence:run.fence,attempt:1,runtime:'harness',contextVersion:null,
    code:'INPUT_MANIFEST_CHANGED',phase:'effective_source_manifest',subcode:'EFFECTIVE_INPUT_MISSING',
    inputCount:4,totalBytes:96,limitBytes:100}}]);
  expect(JSON.stringify(rows)).not.toContain('PRIVATE');
  expect(await authority(run)).toEqual(before);
});

it.each(['fence','task','attempt','newer-attempt'])('does not attribute an input failure to changed %s ownership',async change=>{
  const run=await fixture(),supplied={...run};
  if(change==='fence')supplied.fence=randomUUID();
  if(change==='task')supplied.task_id=randomUUID();
  if(change==='attempt')supplied.attempt=2;
  if(change==='newer-attempt'){
    await pool.query("UPDATE tasks SET attempt_count=2,state='running' WHERE id=$1",[run.task_id]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'running')",[randomUUID(),run.task_id,randomUUID()]);
  }
  const before=await authority(run);
  expect(await observeInputFailure(pool,supplied,failure())).toBe(false);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='RUN_INPUT_FAILURE'")).rowCount).toBe(0);
  expect(await authority(run)).toEqual(before);
});

it('absorbs diagnostic storage errors without replacing the original exception',async()=>{
  const run=await fixture(),error=failure(),before=await authority(run);
  const query=vi.fn(async()=>{throw Error('PRIVATE_DATABASE_ERROR');});
  expect(await observeInputFailure({query:query as DB['query']},run,error)).toBe(false);
  expect(query).toHaveBeenCalledTimes(1);
  expect(error.message).toBe('PRIVATE_PROMPT_AND_TOKEN');
  expect(await authority(run)).toEqual(before);
});

it('also absorbs malformed error access before opening its optional diagnostic connection',async()=>{
  const run=await fixture(),malformed=Object.defineProperty({},'code',{get(){throw Error('PRIVATE_GETTER');}});
  expect(await observeInputFailureBounded(run,malformed)).toBe(false);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='RUN_INPUT_FAILURE'")).rowCount).toBe(0);
});

it('ignores unrelated exceptions and strips invalid structural diagnostics',async()=>{
  const run=await fixture(),query=vi.fn(async()=>{throw Error('must not query');});
  expect(await observeInputFailure({query:query as DB['query']},run,{code:'STOP_UNCONFIRMED'})).toBe(false);
  expect(query).not.toHaveBeenCalled();
  expect(await observeInputFailure(pool,run,{code:'SUPERVISION_INPUTS_TOO_LARGE',inputFailure:{
    phase:'private https://host',subcode:'PRIVATE/token',totalBytes:-1,inputCount:1.5,limitBytes:24*1024*1024}})).toBe(true);
  expect((await pool.query("SELECT payload FROM events WHERE kind='RUN_INPUT_FAILURE'")).rows[0].payload)
    .toEqual({runId:run.id,runFence:run.fence,attempt:1,runtime:'harness',contextVersion:null,code:'SUPERVISION_INPUTS_TOO_LARGE',limitBytes:24*1024*1024});
});

it('bounds a blocked diagnostic statement on a private connection without a late event or authority change',async()=>{
  const run=await fixture(),before=await authority(run),owner=await pool.connect();
  let elapsed=0;
  try{
    await owner.query('BEGIN');await owner.query('LOCK TABLE events IN ACCESS EXCLUSIVE MODE');
    const started=performance.now();
    expect(await observeInputFailureBounded(run,failure())).toBe(false);
    elapsed=performance.now()-started;
    expect(elapsed).toBeLessThan(3000);
  }finally{await owner.query('ROLLBACK');owner.release();}
  expect((await pool.query("SELECT 1 FROM events WHERE kind='RUN_INPUT_FAILURE'")).rowCount).toBe(0);
  expect((await pool.query("SELECT 1 FROM pg_stat_activity WHERE application_name='agentos-input-failure-observation'")).rowCount).toBe(0);
  expect(await authority(run)).toEqual(before);
});
