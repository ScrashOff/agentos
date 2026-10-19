import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { migrate, pool } from '../server/db.js';
import type {DB} from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installBacklogPlanner } from '../server/backlog.js';
import { installManagement, recordManagementReviews } from '../server/management.js';
import { completeRun, validateTask, writeArtifact, readArtifact } from '../server/service.js';
import { hash } from '../server/memory.js';
import { readManagementRetrieval, readManagementRetrievalRequest, managementRetrievalCharacterLimit } from '../server/management-retrieval.js';
import {retainManagementEvidence,readManagementEvidenceLedger} from '../server/management-evidence-ledger.js';

beforeAll(async () => { await migrate(); await installBacklogPlanner(); await installManagement(); });
beforeEach(async () => { await pool.query('TRUNCATE goals,agents,accounts,events CASCADE'); await seedRegistry(); });
afterAll(async () => { await pool.end(); });

it('returns exact latest stopped run fence/context without exposing stale, live or cross-goal authority',async()=>{
 const f=await fixture(),context=randomUUID();
 await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','{}')",[context,f.run]);
 await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.run,context]);
 const input={kind:'task_metadata' as const,taskId:f.task,expectedAttempt:1,offset:0};
 const read=(goal=f.goal,request=input)=>readManagementRetrievalRequest(pool,goal,readArtifact,request);
 const first=await read();
 expect(first.status).toBe('OK');
 expect(JSON.parse(first.text)).toMatchObject({runId:f.run,runFence:f.fence,contextVersion:context,expectedAttempt:1});
 expect((await read(randomUUID())).status).toBe('NOT_FOUND');
 const latest=randomUUID(),fence=randomUUID(),newContext=randomUUID();
 await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.task]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,finished_at) VALUES($1,$2,2,$3,1,'submitted','harness',now())",[latest,f.task,fence]);
 await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','{}')",[newContext,latest]);
 await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[latest,newContext]);
 const stale=await read();
 expect(stale.status).toBe('STALE_ATTEMPT');
 expect(stale.text).toBeUndefined();
 expect(stale.runFence).toBeUndefined();
 expect(stale.contextVersion).toBeUndefined();
 const currentInput={...input,expectedAttempt:2};
 const current=await read(f.goal,currentInput);
 expect(JSON.parse(current.text)).toMatchObject({runId:latest,runFence:fence,contextVersion:newContext});
 expect(current.snapshotDigest).not.toBe(first.snapshotDigest);
 const stalePage=await readManagementRetrievalRequest(pool,f.goal,readArtifact,{...currentInput,snapshotDigest:first.snapshotDigest,offset:1});
 expect(stalePage.status).toBe('STALE_CONTEXT');
 expect(stalePage.text).toBeUndefined();
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[latest]);
 const live=await read(f.goal,currentInput);
 expect(live.status).toBe('LIVE_SOURCE');
 expect(live.text).toBeUndefined();
});

it('recovers the exact stopped attempt summary and error with task metadata, refusing stale or live sources',async()=>{
 const f=await fixture(),summary='A discriminating observed failure with exact detail.';
 await pool.query('UPDATE runs SET summary=$2,error_code=$3 WHERE id=$1',[f.run,summary,'MODEL_TIMEOUT']);
 let offset:any=0,text='';
 do{await request(f.goal,[{kind:'task_metadata',taskId:f.task,expectedAttempt:1,offset}]);
  const recovered=await readManagementRetrieval(pool,f.goal,readArtifact);text+=recovered.results[0].text;
  offset=recovered.results[0].nextOffset;
 }while(offset!==null);
 expect(JSON.parse(text)).toMatchObject({summary,errorCode:'MODEL_TIMEOUT',expectedAttempt:1});
 await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.task]);
 await request(f.goal,[{kind:'task_metadata',taskId:f.task,expectedAttempt:1,offset:0}]);
 expect((await readManagementRetrieval(pool,f.goal,readArtifact)).results[0].status).toBe('STALE_ATTEMPT');
 await pool.query("UPDATE tasks SET attempt_count=1 WHERE id=$1",[f.task]);
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
 expect((await readManagementRetrieval(pool,f.goal,readArtifact)).results[0].status).toBe('LIVE_SOURCE');
});

it('retains exact evidence across completed manager sessions and refuses it after a new source attempt',async()=>{
 const f=await fixture();let offset=0;
 do {
  await request(f.goal,[{kind:'artifact',taskId:f.task,expectedAttempt:1,name:'findings.json',offset}]);
  const recovered=await readManagementRetrieval(pool,f.goal,readArtifact);
  await retainManagementEvidence(pool,f.goal,recovered);
  offset=recovered.results[0].nextOffset;
 }while(offset!==null);
 const retained=await readManagementEvidenceLedger(pool,f.goal,'research/source');
 expect(retained.entries).toHaveLength(1);
 expect(retained.entries[0]).toMatchObject({complete:true,digest:f.artifact.digest,runId:f.run});
 expect(retained.entries[0].pages).toEqual([{offset:0,text:f.text}]);
 expect((await readManagementEvidenceLedger(pool,randomUUID(),'research/source')).entries).toEqual([]);
 expect((await readManagementEvidenceLedger(pool,f.goal,'unrelated/source')).entries).toEqual([]);
 await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.task]);
 expect((await readManagementEvidenceLedger(pool,f.goal,'research/source')).entries).toEqual([]);
});

it('does not merge a conflicting page into retained evidence or mutate the source artifacts',async()=>{
 const f=await fixture();
 await request(f.goal,[{kind:'artifact',taskId:f.task,expectedAttempt:1,name:'findings.json',offset:0}]);
 const recovered=await readManagementRetrieval(pool,f.goal,readArtifact);
 await retainManagementEvidence(pool,f.goal,recovered);
 await expect(retainManagementEvidence(pool,f.goal,{results:[{...recovered.results[0],text:'conflicting'}]}))
  .rejects.toThrow('MANAGEMENT_EVIDENCE_CONFLICT');
 expect((await readManagementEvidenceLedger(pool,f.goal)).entries[0].pages[0].text).toBe(recovered.results[0].text);
 expect((await readArtifact(f.artifact.id)).bytes.toString()).toBe(f.text);
});

it('caches the verified full short source when only its last page was requested',async()=>{
 const f=await fixture();
 await request(f.goal,[{kind:'artifact',taskId:f.task,expectedAttempt:1,name:'findings.json',offset:f.text.length-200}]);
 const recovered=await readManagementRetrieval(pool,f.goal,readArtifact);
 expect(recovered.results[0].text.length).toBe(200);
 await retainManagementEvidence(pool,f.goal,recovered,readArtifact);
 const retained=await readManagementEvidenceLedger(pool,f.goal);
 expect(retained.entries[0]).toMatchObject({complete:true,digest:f.artifact.digest});
 expect(retained.entries[0].pages).toEqual([{offset:0,text:f.text}]);
});

async function fixture() {
  const goal = randomUUID(), task = randomUUID(), run = randomUUID(), fence = randomUUID();
  const description = 'Pesquisa geral: comparar fontes e entregar um relatório.\n' + 'Fonte verificável — ü 😀\n'.repeat(500);
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Research','A long task','retrieval',$2)", [goal, goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,covers,execution,attempt_count)
    VALUES($1,$2,'source','Pesquisa fonte bibliográfica',$3,'research.web','running','work',
    '[{"id":"report","label":"Report exists","type":"artifact","name":"findings.json"}]',
    '["bibliografia"]','{"baseWorkspace":"research/source","workspace":"research/copy","workKind":"diagnostic"}',1)`, [task, goal, description]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'running','harness')", [run, task, fence]);
  const text = JSON.stringify({ findings: [{ source: 'Original ü 😀', conclusion: 'Verifiable claim' }], limitations: ['line\\quote"\n'.repeat(900)] });
  const artifact = await writeArtifact(run, fence, 'findings.json', text, 'application/json');
  await completeRun(run, fence, 'Research evidence persisted', {});
  await validateTask(task);
  return { goal, task, run, fence, description, text, artifact };
}
async function request(goal: string, contextRequests: any[]) {
  await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    ['management-state:' + goal, JSON.stringify({ contextRequests })]);
}

it('keeps requested pages bound to their decision while preserving source data and explicitly exposing recovery after focus changes',async()=>{
 const f=await fixture(),previous={caseId:randomUUID(),scope:'research/source'},current={caseId:randomUUID(),scope:'research/source'};
 await request(f.goal,[{kind:'artifact',taskId:f.task,expectedAttempt:1,name:'findings.json',offset:0}]);
 await pool.query("UPDATE settings SET value=value||$2::jsonb WHERE id=$1",['management-state:'+f.goal,JSON.stringify({contextRequestFocus:previous})]);
 const before=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 let reads=0;const reader=async(id:string)=>{reads++;return readArtifact(id);};
 const moved=await readManagementRetrieval(pool,f.goal,reader,current);
 expect(moved.results).toEqual([]);expect(reads).toBe(0);
 expect(moved).toMatchObject({selection:{status:'FOCUS_CHANGED',omittedRequests:1,requestedFocus:previous,currentFocus:current,fullSourcesPreserved:true,
  recovery:{requests:before.contextRequests}}});
 expect((await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value).toEqual(before);
 expect((await readManagementRetrieval(pool,f.goal,reader,previous)).results[0]).toMatchObject({status:'OK',digest:f.artifact.digest});
 expect((await readArtifact(f.artifact.id)).bytes.toString()).toBe(f.text);
});

it('recovers a legacy request focus from its immutable review instead of the newer mutable manager focus',async()=>{
 const f=await fixture(),manager=randomUUID(),run=randomUUID(),previous={caseId:randomUUID(),scope:'research/source'},current={caseId:randomUUID(),scope:'research/source'};
 const description='Contract\n'+JSON.stringify({cases:[],operation:{focus:previous}});
 await pool.query("INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count) VALUES($1,$2,'manager','Manager',$3,'engineering.supervisor','accepted','coordinate','[]',1)",[manager,f.goal,description]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'submitted','harness')",[
  run,manager,randomUUID()]);
 expect((await pool.query("SELECT instruction_snapshot->'task'->>'description' description FROM runs WHERE id=$1",[run])).rows[0].description).toBe(description);
 await request(f.goal,[{kind:'task',taskId:f.task,expectedAttempt:1,offset:0}]);
 await pool.query("UPDATE settings SET value=value||$2::jsonb WHERE id=$1",['management-state:'+f.goal,JSON.stringify({runId:run,focus:current})]);
 expect(await readManagementRetrieval(pool,f.goal,readArtifact,current)).toMatchObject({results:[],selection:{status:'FOCUS_CHANGED',requestedFocus:previous}});
 expect((await readManagementRetrieval(pool,f.goal,readArtifact,previous)).results[0]).toMatchObject({status:'OK',digest:hash(f.description)});
});

it('recovers an omitted task graph and exact criteria through hash-bound metadata pages without reading private execution inputs',async()=>{
 const f=await fixture(),dependency=randomUUID();
 await pool.query("INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria) VALUES($1,$2,'dep','Source','Accepted source','research.web','accepted','[]')",[dependency,f.goal]);
 await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[f.task,dependency]);
 await pool.query("UPDATE tasks SET execution=execution||'{\"inputs\":[{\"base64\":\"PRIVATE_INPUT\"}]}'::jsonb WHERE id=$1",[f.task]);
 let offset=0,text='',digest:string|undefined;
 do{
  await request(f.goal,[{kind:'task_metadata',taskId:f.task,expectedAttempt:1,offset}]);
  const result=(await readManagementRetrieval(pool,f.goal,readArtifact)).results[0];
  expect(result.status).toBe('OK');expect(result.runId).toBe(f.run);
  if(digest)expect(result.digest).toBe(digest);digest=result.digest;text+=result.text;offset=result.nextOffset;
 }while(offset!==null);
 expect(hash(text)).toBe(digest);expect(text).not.toContain('PRIVATE_INPUT');
 expect(JSON.parse(text)).toMatchObject({taskId:f.task,expectedAttempt:1,dependencies:[dependency],
  instructionReference:{digest:hash(f.description),totalCharacters:f.description.length,
   request:{kind:'task',taskId:f.task,expectedAttempt:1,offset:0,snapshotDigest:hash(f.description)}},
  criteria:[{id:'report',name:'findings.json'}],representation:'observed_task_metadata_v2',part:'decision'});
 expect(JSON.parse(text)).not.toHaveProperty('instruction');
});

it('recovers omitted research text by immutable hash and exact pages without changing task/run/evidence', async () => {
  const f = await fixture();
  const before = (await pool.query('SELECT to_jsonb(t) task,to_jsonb(r) run FROM tasks t JOIN runs r ON r.task_id=t.id WHERE t.id=$1', [f.task])).rows;
  let text = '', offset = 0;
  do {
    await request(f.goal, [{ kind: 'artifact', taskId: f.task, expectedAttempt: 1, name: 'findings.json', offset }]);
    const recovered = await readManagementRetrieval(pool, f.goal, readArtifact);
    const page = recovered.results[0];
    expect(page.status).toBe('OK'); expect(page.digest).toBe(f.artifact.digest);
    expect(page.artifactId).toBe(f.artifact.id); expect(page.runId).toBe(f.run);
    expect(JSON.stringify(recovered).length).toBeLessThanOrEqual(managementRetrievalCharacterLimit);
    text += page.text; offset = page.nextOffset;
  } while (offset !== null);
  expect(text).toBe(f.text); expect(hash(text)).toBe(f.artifact.digest);
  expect((await pool.query('SELECT to_jsonb(t) task,to_jsonb(r) run FROM tasks t JOIN runs r ON r.task_id=t.id WHERE t.id=$1', [f.task])).rows).toEqual(before);
});

it('exposes original instructions, discoverable artifacts and failed coverage with literal query and bounded envelopes', async () => {
  const f = await fixture();
  await pool.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1", [f.task]);
  await request(f.goal, [
    { kind: 'task', taskId: f.task, expectedAttempt: 1 },
    { kind: 'artifact', taskId: f.task, expectedAttempt: 1, name: 'findings.json' },
    { kind: 'artifacts', taskId: f.task, expectedAttempt: 1 },
    { kind: 'coverage', baseWorkspace: 'research/source', query: 'bibliográ' },
  ]);
  const recovered = await readManagementRetrieval(pool, f.goal, readArtifact);
  expect(JSON.stringify(recovered).length).toBeLessThanOrEqual(managementRetrievalCharacterLimit);
  expect(recovered.results).toHaveLength(4);
  expect(recovered.results[0]).toMatchObject({ status: 'OK', digest: hash(f.description), totalCharacters: f.description.length });
  expect(recovered.results[0].text).toBe(f.description.slice(0, recovered.results[0].text.length));
  expect(recovered.results[2].rows[0]).toMatchObject({ id: f.artifact.id, name: 'findings.json', digest: f.artifact.digest });
  expect(recovered.results[3].rows[0]).toMatchObject({ task_id: f.task, state: 'recovery_blocked', expected_attempt: 1 });
  await request(f.goal, [{ kind: 'coverage', baseWorkspace: 'research/source', query: '%' }]);
  expect((await readManagementRetrieval(pool, f.goal, readArtifact)).results[0].totalRows).toBe(0);
});

it('refuses foreign goals, stale attempts, live sources, missing names and altered artifact bytes explicitly', async () => {
  const f = await fixture();
  const base = { kind: 'artifact', taskId: f.task, expectedAttempt: 1, name: 'findings.json' };
  await request(f.goal, [{ ...base, expectedAttempt: 0 }, { ...base, name: 'missing.json' }]);
  expect((await readManagementRetrieval(pool, f.goal, readArtifact)).results.map(r => r.status)).toEqual(['STALE_ATTEMPT', 'NOT_FOUND']);
  await request(f.goal, [base]);
  expect((await readManagementRetrieval(pool, f.goal, async () => ({ bytes: Buffer.from('changed') }))).results[0].status).toBe('INTEGRITY_FAILED');
  const foreign = randomUUID(); await request(foreign, [base]);
  expect((await readManagementRetrieval(pool, foreign, readArtifact)).results[0].status).toBe('NOT_FOUND');
  await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1", [f.run]);
  expect((await readManagementRetrieval(pool, f.goal, readArtifact)).results[0].status).toBe('LIVE_SOURCE');
});

it('reserves a real page for every request so the first large result cannot starve later evidence', async () => {
  const f = await fixture();
  const requests = [
    { kind: 'coverage', baseWorkspace: 'research/source' },
    { kind: 'artifact', taskId: f.task, expectedAttempt: 1, name: 'findings.json' },
    { kind: 'task', taskId: f.task, expectedAttempt: 1 },
    { kind: 'artifact', taskId: f.task, expectedAttempt: 1, name: 'findings.json', offset: 1500 },
  ];
  await request(f.goal, requests);
  const recovered = await readManagementRetrieval(pool, f.goal, readArtifact);
  expect(JSON.stringify(recovered).length).toBeLessThanOrEqual(managementRetrievalCharacterLimit);
  expect(recovered.results.map(r => r.status)).toEqual(['OK', 'OK', 'OK', 'OK']);
  for (const index of [1, 2, 3]) {
    const page = recovered.results[index];
    expect(page.text.length).toBeGreaterThan(0);
    expect(page.nextOffset).toBeGreaterThan(page.request.offset);
    const original = index === 2 ? f.description : f.text;
    expect(page.text).toBe(original.slice(page.request.offset, page.nextOffset));
  }
});

it('persists accepted supervisor retrieval requests idempotently for the next real snapshot', async () => {
  const f = await fixture(), task = randomUUID(), run = randomUUID(), fence = randomUUID();
  await pool.query('INSERT INTO settings VALUES($1,$2)', ['management:' + f.goal, JSON.stringify({ enabled: true, enabledAt: '2026-01-01T00:00:00Z' })]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,'manager','Supervisor','MANAGEMENT_V1','engineering.supervisor','running','coordinate',
    '[{"id":"decision","label":"Decision exists","type":"artifact","name":"coordination.json"}]',1)`, [task, f.goal]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'running','harness')", [run, task, fence]);
  const contextRequests = [{ kind: 'artifact', taskId: f.task, expectedAttempt: 1, name: 'findings.json', offset: 0 }];
  await writeArtifact(run, fence, 'coordination.json', JSON.stringify({ management: {
    summary: 'Recover the missing research evidence', capacityReason: 'Evidence is outside the default projection', actions: [], contextRequests,
  } }), 'application/json');
  await completeRun(run, fence, 'Recover original artifact before planning', {}); await validateTask(task);
  await recordManagementReviews(); await recordManagementReviews();
  expect((await pool.query('SELECT value FROM settings WHERE id=$1', ['management-state:' + f.goal])).rows[0].value.contextRequests).toEqual(contextRequests);
  const reviews = (await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1', [run])).rows;
  expect(reviews).toHaveLength(1); expect(reviews[0].receipts).toContainEqual(expect.objectContaining({ code: 'CONTEXT_REQUESTED', count: 1 }));
  const recovered = await readManagementRetrieval(pool, f.goal, readArtifact);
  expect(recovered.results[0]).toMatchObject({ status: 'OK', artifactId: f.artifact.id, digest: f.artifact.digest });
});

it('returns only current identity for stale task_metadata and requires an explicit fresh request without mutating the source',async()=>{
 const f=await fixture(),run5=randomUUID();
 await pool.query("UPDATE tasks SET attempt_count=5,state='accepted' WHERE id=$1",[f.task]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,5,$3,1,'submitted','harness')",[run5,f.task,randomUUID()]);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','fixture')",[randomUUID(),f.task,run5]);
 await request(f.goal,[{kind:'task_metadata',taskId:f.task,expectedAttempt:1,offset:900}]);
 const before=(await pool.query('SELECT to_jsonb(t) task,to_jsonb(r) run FROM tasks t JOIN runs r ON r.task_id=t.id WHERE t.id=$1 ORDER BY r.attempt',[f.task])).rows;
 const settings=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 let reads=0;const neverRead=async(id:string)=>{reads++;throw Error('NO_ARTIFACT_READ:'+id);};
 const first=(await readManagementRetrieval(pool,f.goal,neverRead)).results[0];
 expect(first).toMatchObject({status:'STALE_ATTEMPT',request:{taskId:f.task,expectedAttempt:1,offset:900},
  currentAttempt:5,currentState:'accepted',currentRunId:run5,currentRunAttempt:5,currentRunState:'submitted',currentHasLiveRun:false});
 expect(first).not.toHaveProperty('text');expect(first).not.toHaveProperty('digest');expect(reads).toBe(0);
 expect((await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value).toEqual(settings);
 expect((await pool.query('SELECT to_jsonb(t) task,to_jsonb(r) run FROM tasks t JOIN runs r ON r.task_id=t.id WHERE t.id=$1 ORDER BY r.attempt',[f.task])).rows).toEqual(before);
 await request(f.goal,[{kind:'task_metadata',taskId:f.task,expectedAttempt:first.currentAttempt,offset:0}]);
 expect((await readManagementRetrieval(pool,f.goal,neverRead)).results[0]).toMatchObject({status:'OK',runId:run5});
 // A fresh observation is not a lease: a later attempt still refuses the old request.
 await pool.query('UPDATE tasks SET attempt_count=6 WHERE id=$1',[f.task]);
 expect((await readManagementRetrieval(pool,f.goal,neverRead)).results[0]).toMatchObject({status:'STALE_ATTEMPT',currentAttempt:6});
 expect(reads).toBe(0);
});

it('does not disclose current metadata across goals or treat an old request as permission to read a live source',async()=>{
 const f=await fixture(),foreign=randomUUID();
 await request(foreign,[{kind:'task_metadata',taskId:f.task,expectedAttempt:0}]);
 const other=(await readManagementRetrieval(pool,foreign,readArtifact)).results[0];
 expect(other).toMatchObject({status:'NOT_FOUND'});expect(other).not.toHaveProperty('currentAttempt');expect(other).not.toHaveProperty('currentRunId');
 await pool.query("UPDATE tasks SET state='running' WHERE id=$1",[f.task]);
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
 await request(f.goal,[{kind:'task_metadata',taskId:f.task,expectedAttempt:0}]);
 const stale=(await readManagementRetrieval(pool,f.goal,readArtifact)).results[0];
 expect(stale).toMatchObject({status:'STALE_ATTEMPT',currentAttempt:1,currentHasLiveRun:true,currentRunState:'running'});
 expect(stale).not.toHaveProperty('text');
 await request(f.goal,[{kind:'task_metadata',taskId:f.task,expectedAttempt:stale.currentAttempt}]);
 expect((await readManagementRetrieval(pool,f.goal,readArtifact)).results[0]).toMatchObject({status:'LIVE_SOURCE'});
});

it('delivers decision metadata in one of four slots while preserving a 103k instruction with unstructured requirements exactly',async()=>{
 const f=await fixture();
 const description='Current unstructured requirement: preserve the original settlement boundary.\n'+
  'Substantive task context. '.repeat(4400)+'\nCONSTRAINTS: This CURRENT non-JSON requirement also remains mandatory.';
 await pool.query('UPDATE tasks SET description=$2 WHERE id=$1',[f.task,description]);
 const before=(await pool.query('SELECT to_jsonb(t) task FROM tasks t WHERE id=$1',[f.task])).rows;
 await request(f.goal,[
  {kind:'task_metadata',taskId:f.task,expectedAttempt:1},
  {kind:'task_metadata',taskId:f.task,expectedAttempt:1,part:'criteria'},
  {kind:'task_metadata',taskId:f.task,expectedAttempt:1,part:'dependencies'},
  {kind:'task_metadata',taskId:f.task,expectedAttempt:1,part:'outcome'},
 ]);
 const recovered=await readManagementRetrieval(pool,f.goal,readArtifact);
 expect(JSON.stringify(recovered).length).toBeLessThanOrEqual(managementRetrievalCharacterLimit);
 expect(recovered.results.map(r=>r.status)).toEqual(['OK','OK','OK','OK']);
 for(const result of recovered.results){expect(result.nextOffset).toBeNull();expect(result.truncated).toBe(false);}
 const [decision,criteria,dependencies,outcome]=recovered.results.map(r=>JSON.parse(r.text));
 expect(decision).toMatchObject({criteria:[{id:'report',name:'findings.json'}],dependencies:[],
  instructionReference:{digest:hash(description),totalCharacters:description.length}});
 expect(criteria.criteria).toEqual(decision.criteria);expect(criteria).not.toHaveProperty('summary');
 expect(dependencies.dependencies).toEqual([]);expect(dependencies).not.toHaveProperty('criteria');
 expect(outcome.summary).toBe('Research evidence persisted');expect(outcome).not.toHaveProperty('dependencies');
 expect(recovered.results[0].totalCharacters).toBeLessThan(3000);
 expect(description.length).toBeGreaterThan(103000);
 // The reference does not classify any part of the instruction as obsolete.
 // Fetching it uses the same reader and proves every original byte survives.
 let offset:any=0,text='';
 do {
  const page=await readManagementRetrievalRequest(pool,f.goal,readArtifact,
   {...decision.instructionReference.request,offset});
  expect(page.status).toBe('OK');expect(page.digest).toBe(hash(description));
  expect(page.request.taskId).toBe(f.task);expect(page.text.length).toBeLessThanOrEqual(12000);
  text+=page.text;offset=page.nextOffset;
 }while(offset!==null);
 expect(text).toBe(description);
 expect((await pool.query('SELECT to_jsonb(t) task FROM tasks t WHERE id=$1',[f.task])).rows).toEqual(before);
});

it('does not resume a legacy large metadata offset as an empty successful page',async()=>{
 const f=await fixture();
 const result=await readManagementRetrievalRequest(pool,f.goal,readArtifact,
  {kind:'task_metadata',taskId:f.task,expectedAttempt:1,offset:83850});
 expect(result).toMatchObject({status:'PAGE_OUT_OF_RANGE',nextOffset:0});
 expect(result).not.toHaveProperty('text');
 const restarted=await readManagementRetrievalRequest(pool,f.goal,readArtifact,
  {...result.request,offset:0,snapshotDigest:result.snapshotDigest});
 expect(restarted.status).toBe('OK');expect(JSON.parse(restarted.text).part).toBe('decision');
});

it('rejects changed task bytes and metadata between pages without mixing versions or reading manager state',async()=>{
 const f=await fixture();
 const db:DB={query:(async(text:any,values?:any)=>{
  expect(String(text)).not.toContain('FROM settings');return pool.query(text,values);
 }) as DB['query']};
 const taskPage=await readManagementRetrievalRequest(db,f.goal,readArtifact,
  {kind:'task',taskId:f.task,expectedAttempt:1});
 const metadata=await readManagementRetrievalRequest(db,f.goal,readArtifact,
  {kind:'task_metadata',taskId:f.task,expectedAttempt:1});
 await pool.query('UPDATE tasks SET description=description||$2 WHERE id=$1',[f.task,'\nAn additional current requirement.']);
 for(const page of [taskPage,metadata]) {
  const changed=await readManagementRetrievalRequest(db,f.goal,readArtifact,
   {...page.request,offset:10,snapshotDigest:page.snapshotDigest});
  expect(changed.status).toBe('STALE_CONTEXT');expect(changed.nextOffset).toBe(0);
  expect(changed).not.toHaveProperty('text');expect(changed.digest).not.toBe(page.digest);
 }
 const foreign=await readManagementRetrievalRequest(db,randomUUID(),readArtifact,metadata.request);
 expect(foreign.status).toBe('NOT_FOUND');expect(foreign).not.toHaveProperty('digest');
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
 expect((await readManagementRetrievalRequest(db,f.goal,readArtifact,metadata.request)).status).toBe('LIVE_SOURCE');
 expect((await readManagementRetrievalRequest(db,f.goal,readArtifact,{...metadata.request,expectedAttempt:0})).status).toBe('STALE_ATTEMPT');
});
