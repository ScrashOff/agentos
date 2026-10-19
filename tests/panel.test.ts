import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {panelState,panelGoal,panelTask,panelCollection} from '../server/panel.js';
import {hash} from '../server/memory.js';
import {installTaskPanelMetadata} from '../server/task-panel-metadata.js';

beforeAll(async()=>{await migrate();await installTaskPanelMetadata();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});
async function fixture(){
  const goal=randomUUID(),task=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Long operation','Deliver results','panel',$2)",[goal,goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,state,execution,attempt_count)
    VALUES($1,$2,'first','Long task',repeat('HISTORICAL-INSTRUCTION-',5000),'research.web','[]','recovery_blocked',
    $3,200)`,[task,goal,JSON.stringify({kind:'harness',workspace:'panel/first',baseWorkspace:'panel/source',readOnlySource:true,inputs:[{id:randomUUID(),name:'private.txt',
      base64:Buffer.from('PRIVATE-INPUT-MARKER').toString('base64'),digest:hash('PRIVATE-INPUT-MARKER')}]})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,instruction_snapshot,checkpoint)
    SELECT gen_random_uuid(),$1,n,'failed',gen_random_uuid(),'harness',1,
    jsonb_build_object('description',repeat('HISTORICAL-CONTEXT-',2000)),
    jsonb_build_object('kind','recovery','long',repeat('CHECKPOINT-CONTENT-',200)) FROM generate_series(1,200)n`,[task]);
  await pool.query("INSERT INTO events(kind,payload) VALUES('BEFORE_BOOTSTRAP','{}')");
  return {goal,task};
}
it('keeps hundreds of immutable attempts out of repeated status reads and returns a replay watermark',async()=>{
  const f=await fixture();
  const before=(await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n;
  const run=(await pool.query('SELECT id FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[f.task])).rows[0].id;
  for (const version of [1,2]) await pool.query(`INSERT INTO events(kind,entity_id,payload)
    VALUES('EXECUTION_OBSERVED',$1,$2)`,[f.task,JSON.stringify({runId:run,observation:{version}})]);
  const state=await panelState(),goal=await panelGoal(f.goal);
  expect(state.runs).toHaveLength(1);expect(goal.runs).toHaveLength(1);
  expect(state.tasks[0].execution).toEqual({kind:'harness',workspace:'panel/first',baseWorkspace:'panel/source',readOnlySource:true});
  expect(state.events.filter(e=>e.kind==='EXECUTION_OBSERVED')).toHaveLength(1);
  expect(state.events.find(e=>e.kind==='EXECUTION_OBSERVED').payload.observation.version).toBe(2);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='EXECUTION_OBSERVED'")).rows[0].n).toBe(2);
  for(const result of [state,goal]){
    const text=JSON.stringify(result);
    expect(text).not.toContain('HISTORICAL-INSTRUCTION');
    expect(text).not.toContain('HISTORICAL-CONTEXT');
    expect(text).not.toContain(Buffer.from('PRIVATE-INPUT-MARKER').toString('base64'));
    expect(text).not.toContain('CHECKPOINT-CONTENT');
    expect(Buffer.byteLength(text)).toBeLessThan(200*1024);
    expect(Number(result.eventCursor)).toBeGreaterThan(0);
  }
  const event=(await pool.query("INSERT INTO events(kind,payload) VALUES('AFTER_BOOTSTRAP','{}') RETURNING id")).rows[0];
  expect(Number(event.id)).toBeGreaterThan(Number(state.eventCursor));
  expect((await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n).toBe(before);
});
it('pages exact historical attempts without repeats when a newer attempt appears and scopes collection history',async()=>{
  const f=await fixture();
  const first=await panelTask(f.task,undefined,20);
  expect(first.page).toEqual({total:200,nextBefore:181});
  expect(first.task.description).toContain('HISTORICAL-INSTRUCTION');
  expect(first.runs[0].checkpoint.long).toContain('CHECKPOINT-CONTENT');
  await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version)
    VALUES($1,$2,201,'submitted',$3,'harness',1)`,[randomUUID(),f.task,randomUUID()]);
  const second=await panelTask(f.task,first.page.nextBefore!,20);
  expect(second.runs[0].attempt).toBe(180);
  expect(second.runs.some(run=>first.runs.some(previous=>previous.id===run.id))).toBe(false);
  const all=await panelCollection('runs',undefined,50,f.goal);
  expect(all.page.total).toBe(201);expect(all.rows).toHaveLength(50);expect(all.page.nextBefore).toBeTruthy();
  expect((await panelCollection('runs',undefined,50,randomUUID())).rows).toEqual([]);
  await expect(panelTask(randomUUID())).rejects.toMatchObject({code:'NOT_FOUND'});
});
it('reports the persisted remote scheduler tick instead of inferring health from an API read',async()=>{
  await pool.query("DELETE FROM settings WHERE id='scheduler-health'");
  expect((await panelState()).engine).toBeNull();
  const remote={lastTick:new Date(Date.now()-60000).toISOString(),error:'QUEUE_UNAVAILABLE',workerId:'remote-worker',active:2};
  await pool.query("INSERT INTO settings VALUES('scheduler-health',$1)",[JSON.stringify(remote)]);
  expect((await panelState()).engine).toEqual(remote);
  expect((await pool.query("SELECT value FROM settings WHERE id='scheduler-health'")).rows[0].value).toEqual(remote);
});
