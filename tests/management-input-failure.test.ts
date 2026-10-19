import {afterAll,beforeAll,beforeEach,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,type DB} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {readManagementRetrievalRequest} from '../server/management-retrieval.js';
import {observeInputFailure} from '../server/input-failure-observation.js';
import {readArtifact} from '../server/service.js';
import {hash} from '../server/memory.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installTaskExecutionMetadata();
});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(()=>pool.end());

const failure=(subcode='EFFECTIVE_INPUT_MISSING')=>Object.assign(Error('PRIVATE_PROMPT'),{
  code:'INPUT_MANIFEST_CHANGED',stderr:'PRIVATE_STDERR',
  inputFailure:{phase:'effective_source_manifest',subcode,inputCount:4,totalBytes:96,limitBytes:100,
    secret:'PRIVATE_TOKEN',command:'PRIVATE_COMMAND'},
});
async function fixture(options:{context?:boolean;runtime?:'harness'|'opencode';summary?:string}={}){
  const goal=randomUUID(),task=randomUUID(),id=randomUUID(),fence=randomUUID(),runtime=options.runtime??'harness';
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Input diagnosis','No agents','manager-input-diagnosis',$1::text)",[goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count,execution)
    VALUES($1::uuid,$2::uuid,$1::text,'Consumer','Preserve original contract','engineering.integration','failed','work',
      '[{"id":"report","type":"artifact","name":"result.json"}]',1,$3::jsonb)`,
    [task,goal,JSON.stringify({kind:runtime,workspace:'test/manager-input-diagnosis',baseWorkspace:'test/manager-input-diagnosis',workKind:'diagnostic'})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,summary,finished_at)
    VALUES($1::uuid,$2::uuid,1,$3::uuid,$4::text,1,'failed','INPUT_MANIFEST_CHANGED',$5::text,clock_timestamp())`,
    [id,task,fence,runtime,options.summary??'The original run failure remains unchanged.']);
  let context:string|null=null;
  if(options.context!==false){
    context=randomUUID();
    await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','No private body')",[context,id]);
    await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[id,context]);
  }
  await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)',['test/manager-input-diagnosis:'+id,id]);
  const run=(await pool.query('SELECT * FROM runs WHERE id=$1',[id])).rows[0];
  return {goal,task,run,context};
}
type Fixture=Awaited<ReturnType<typeof fixture>>;
function read(f:Fixture,extra:Record<string,unknown>={},db:DB=pool){
  return readManagementRetrievalRequest(db,f.goal,readArtifact,
    {kind:'task_metadata',taskId:f.task,expectedAttempt:f.run.attempt,offset:0,...extra} as any);
}
async function document(f:Fixture,part:'decision'|'outcome'|'criteria'|'dependencies'='decision',db:DB=pool){
  const page=await read(f,{part},db);
  expect(page.status).toBe('OK');expect(page.nextOffset).toBeNull();
  return {page,value:JSON.parse(page.text)};
}
async function recorded(f:Fixture,error=failure()){
  expect(await observeInputFailure(pool,f.run,error)).toBe(true);
  return (await pool.query("SELECT id::text,entity_id,payload FROM events WHERE kind='RUN_INPUT_FAILURE' AND entity_id=$1 ORDER BY id DESC LIMIT 1",[f.task])).rows[0];
}
async function authorities(f:Fixture){return {
  task:(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows,
  runs:(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows,
  contexts:(await pool.query('SELECT * FROM contexts WHERE run_id=$1',[f.run.id])).rows,
  leases:(await pool.query('SELECT * FROM leases WHERE run_id=$1 ORDER BY resource',[f.run.id])).rows,
  acceptances:(await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run.id])).rows,
  events:(await pool.query('SELECT * FROM events WHERE entity_id=$1 ORDER BY id',[f.task])).rows,
};}

it.each(['harness','opencode'] as const)('exposes the real stopped %s diagnostic in decision/outcome without changing authority',async runtime=>{
  const f=await fixture({runtime}),event=await recorded(f),before=await authorities(f);
  for(const part of ['decision','outcome'] as const){
    const {value}=await document(f,part);
    expect(value.inputFailureDiagnosis).toMatchObject({version:1,authority:'diagnostic_only',
      eventId:event.id,runId:f.run.id,runFence:f.run.fence,attempt:1,contextVersion:f.context,
      code:'INPUT_MANIFEST_CHANGED',phase:'effective_source_manifest',subcode:'EFFECTIVE_INPUT_MISSING',
      inputCount:4,totalBytes:96,limitBytes:100});
    expect(JSON.stringify(value)).not.toContain('PRIVATE');
    expect(value.errorCode).toBe('INPUT_MANIFEST_CHANGED');
  }
  for(const part of ['criteria','dependencies'] as const)
    expect((await document(f,part)).value).not.toHaveProperty('inputFailureDiagnosis');
  expect(await authorities(f)).toEqual(before);
});

it('preserves a real pre-context failure with explicit null context and distinguishes absent diagnostics',async()=>{
  const f=await fixture({context:false});
  expect((await document(f)).value.inputFailureDiagnosis).toBeNull();
  await recorded(f);
  expect((await document(f)).value.inputFailureDiagnosis).toMatchObject({contextVersion:null,runId:f.run.id});
});

it('never exposes another goal task or imports another source event',async()=>{
  const f=await fixture(),other=await fixture();await recorded(other);
  expect((await document(f)).value.inputFailureDiagnosis).toBeNull();
  const result=await readManagementRetrievalRequest(pool,other.goal,readArtifact,
    {kind:'task_metadata',taskId:f.task,expectedAttempt:1,offset:0});
  expect(result.status).toBe('NOT_FOUND');expect(result.text).toBeUndefined();
});

it.each(['entity_id','runId','runFence','contextVersion','runtime','attempt'] as const)
('refuses a diagnostic whose %s does not bind the current stopped source',async field=>{
  const f=await fixture(),event=await recorded(f);
  if(field==='entity_id')await pool.query('UPDATE events SET entity_id=$2 WHERE id=$1',[event.id,randomUUID()]);
  else {
    const payload={...event.payload,[field]:field==='attempt'?2:field==='runtime'?'builtin':randomUUID()};
    await pool.query('UPDATE events SET payload=$2::jsonb WHERE id=$1',[event.id,JSON.stringify(payload)]);
  }
  expect((await document(f)).value.inputFailureDiagnosis).toBeNull();
});

it.each(['queued','running','cancelling','reconciling'])('keeps the existing LIVE_SOURCE refusal for %s actors',async state=>{
  const f=await fixture();await recorded(f);
  await pool.query('UPDATE runs SET state=$2 WHERE id=$1',[f.run.id,state]);
  const result=await read(f);
  expect(result.status).toBe('LIVE_SOURCE');expect(result.text).toBeUndefined();
});

it('preserves a legacy event without inventing its missing fence and context binding',async()=>{
  const f=await fixture(),event=await recorded(f);
  const legacy={runId:f.run.id,code:'INPUT_MANIFEST_CHANGED',phase:'effective_source_manifest',subcode:'EFFECTIVE_INPUT_MISSING'};
  await pool.query('UPDATE events SET payload=$2::jsonb WHERE id=$1',[event.id,JSON.stringify(legacy)]);
  expect((await document(f)).value.inputFailureDiagnosis).toBeNull();
  expect((await pool.query('SELECT payload FROM events WHERE id=$1',[event.id])).rows[0].payload).toEqual(legacy);
});

it.each(['MODEL_TIMEOUT',null])('avoids diagnostic event I/O for unrelated current error %s',async code=>{
  const f=await fixture();await recorded(f);
  await pool.query('UPDATE runs SET error_code=$2 WHERE id=$1',[f.run.id,code]);
  let diagnosticQueries=0;
  const db:DB={query:(async(sql:any,args:any)=>{
    if(String(sql).includes('SELECT e.id::text event_id,e.payload'))diagnosticQueries++;
    return pool.query(sql,args);
  }) as DB['query']};
  expect((await document(f,'outcome',db)).value.inputFailureDiagnosis).toBeNull();
  expect(diagnosticQueries).toBe(0);
});

it('requires the new attempt to have its own diagnostic rather than inheriting history',async()=>{
  const f=await fixture();await recorded(f);
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.task]);
  const next=(await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,finished_at)
    VALUES($1,$2,2,$3,'harness',1,'failed','INPUT_MANIFEST_CHANGED',clock_timestamp()) RETURNING *`,
    [randomUUID(),f.task,randomUUID()])).rows[0];
  const stale=await read(f);expect(stale.status).toBe('STALE_ATTEMPT');expect(stale.text).toBeUndefined();
  const current={...f,run:next,context:null};
  expect((await document(current)).value.inputFailureDiagnosis).toBeNull();
  const event=await recorded(current,failure('RUN_CHANGED'));
  expect((await document(current)).value.inputFailureDiagnosis).toMatchObject({eventId:event.id,runId:next.id,attempt:2,subcode:'RUN_CHANGED'});
});

it('invalidates a diagnostic after an actual context revision without changing historical evidence',async()=>{
  const f=await fixture(),event=await recorded(f),context=randomUUID();
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','New context')",[context,f.run.id]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.run.id,context]);
  expect((await document(f)).value.inputFailureDiagnosis).toBeNull();
  expect((await pool.query('SELECT payload FROM events WHERE id=$1',[event.id])).rows[0].payload).toEqual(event.payload);
});

it('keeps repeated pages stable and rejects an old digest after a newly persisted diagnosis',async()=>{
  const f=await fixture({summary:'Observed failure detail without secret material. '.repeat(700)});
  const original=await read(f,{part:'outcome'});
  expect(original.status).toBe('OK');expect(original.nextOffset).not.toBeNull();
  const event=await recorded(f);
  const first=await read(f,{part:'outcome'});
  expect(first.snapshotDigest).not.toBe(original.snapshotDigest);
  const stale=await read(f,{part:'outcome',offset:original.nextOffset,snapshotDigest:original.snapshotDigest});
  expect(stale.status).toBe('STALE_CONTEXT');expect(stale.text).toBeUndefined();
  let offset=0,text='',pages=0;
  do{
    const request={part:'outcome',offset,snapshotDigest:first.snapshotDigest};
    const a=await read(f,request),b=await read(f,request);
    expect(a.status).toBe('OK');expect(b).toEqual(a);
    expect(a.snapshotDigest).toBe(first.snapshotDigest);
    text+=a.text;offset=a.nextOffset;pages++;
    expect(pages).toBeLessThan(10);
  }while(offset!==null);
  expect(pages).toBeGreaterThan(1);expect(hash(text)).toBe(first.digest);
  expect(JSON.parse(text).inputFailureDiagnosis).toMatchObject({eventId:event.id,authority:'diagnostic_only'});
});

it.each(['fence','context','live'] as const)('revalidates %s churn between task projection and diagnostic lookup',async mode=>{
  const f=await fixture();await recorded(f);let changed=false;
  const db:DB={query:(async(sql:any,args:any)=>{
    const result=await pool.query(sql,args);
    if(!changed&&String(sql).includes('SELECT t.id,t.description,t.instruction_revision_id')){
      changed=true;
      if(mode==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.run.id,randomUUID()]);
      else if(mode==='live')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run.id]);
      else{
        const context=randomUUID();
        await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Changed while reading')",[context,f.run.id]);
        await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.run.id,context]);
      }
    }
    return result;
  }) as DB['query']};
  const result=await read(f,{},db);
  expect(changed).toBe(true);
  if(result.status==='OK')expect(JSON.parse(result.text).inputFailureDiagnosis).toBeNull();
  else {expect(['STALE_CONTEXT','LIVE_SOURCE']).toContain(result.status);expect(result.text).toBeUndefined();}
});
