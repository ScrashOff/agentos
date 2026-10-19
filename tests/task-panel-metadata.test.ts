import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {config} from '../server/config.js';
import {installTaskExecutionMetadata,requireTaskExecutionMetadataReady} from '../server/task-execution-metadata.js';
import {installTaskPanelMetadata,requireTaskPanelMetadataReady,backfillTaskPanelMetadataBatch,
 taskPanelMetadataReceipt,panelExecutionColumns,panelExecutionJoin,checkedPanelExecution} from '../server/task-panel-metadata.js';
import {panelGoal,panelState,panelTask} from '../server/panel.js';
import {readManagementMateriality} from '../server/management-materiality.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();
 await installTaskExecutionMetadata();await installTaskPanelMetadata();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(()=>pool.end());
async function goal(){const id=randomUUID();await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
 VALUES($1::uuid,'Panel','Panel','panel-metadata',$1::text)`,[id]);return id;}
async function task(goalId:string,execution:any={},key:string=randomUUID()){
 const id=randomUUID();await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,state,stage,execution)
 VALUES($1,$2,$3,'Read model','Same semantic instruction','research.requirements','[]','ready','work',$4)`,
 [id,goalId,key,JSON.stringify(execution)]);return id;
}
const projection=async(id:string)=>(await pool.query('SELECT * FROM task_panel_metadata WHERE task_id=$1',[id])).rows[0];
const legacySQL=`SELECT t.id,jsonb_strip_nulls(to_jsonb(x)) execution FROM tasks t
 LEFT JOIN LATERAL jsonb_to_record(t.execution) AS x(kind text,"baseWorkspace" text,workspace text,"workKind" text,
 "workforceLane" text,"readOnlySource" boolean,continuation jsonb) ON true WHERE t.goal_id=$1 ORDER BY t.id`;
const projectedSQL=`SELECT t.id,${panelExecutionColumns} FROM tasks t ${panelExecutionJoin} WHERE t.goal_id=$1 ORDER BY t.id`;

it('preserves the exact legacy panel contract including casts, nested null stripping and SQL NULL',async()=>{
 const g=await goal();
 for(const execution of [{},{kind:'harness',workspace:'panel/work',baseWorkspace:'panel/base',workKind:'repair',
  workforceLane:'glm',readOnlySource:false,continuation:{runId:randomUUID(),state:'needs_action',owner:'user',
   humanRequest:{kind:'credentials',confirmed:true},message:'A required user action',nextAt:'2026-10-10T12:00:00Z',
   nested:{removed:null,preserved:[null,{removed:null,yes:1}]}},inputs:[{base64:'PRIVATE-INPUT-NOT-IN-PANEL'}]},
  {kind:12,workspace:false,baseWorkspace:{nested:'text-cast'},readOnlySource:'yes',continuation:[null,{gone:null,keep:2}]},
  {kind:null,readOnlySource:null,continuation:null}])await task(g,execution);
 const sqlNull=await task(g);await pool.query('UPDATE tasks SET execution=NULL WHERE id=$1',[sqlNull]);
 const expected=(await pool.query(legacySQL,[g])).rows;
 const actual=checkedPanelExecution((await pool.query(projectedSQL,[g])).rows);
 expect(actual).toEqual(expected);
 const detail=await panelGoal(g),summary=await panelState();
 for(const row of expected){expect(detail.tasks.find(t=>t.id===row.id)?.execution).toEqual(row.execution);
  expect(summary.tasks.find(t=>t.id===row.id)?.execution).toEqual(row.execution);
  expect((await panelTask(row.id)).task.execution).toEqual(row.execution);}
 expect(JSON.stringify(detail)).not.toContain('PRIVATE-INPUT-NOT-IN-PANEL');
});

it.each([[],42,'scalar',null,{readOnlySource:'not-a-boolean'}].map(execution=>({execution})))('records invalid legacy input without blocking writes, then refuses the panel read: $execution',async({execution})=>{
 const g=await goal(),id=await task(g,execution);
 expect((await projection(id)).projection_valid).toBe(false);
 await expect(pool.query(legacySQL,[g])).rejects.toBeDefined();
 await expect(panelGoal(g)).rejects.toMatchObject({code:'TASK_PANEL_METADATA_INVALID'});
 expect((await pool.query('SELECT execution FROM tasks WHERE id=$1',[id])).rows[0].execution).toEqual(execution);
});

it('prepares historical rows explicitly without changing semantic metadata, original task bytes or old readiness',async()=>{
 const beforeReceipt=await requireTaskExecutionMetadataReady();
 await pool.query('ALTER TABLE tasks DISABLE TRIGGER agentos_panel_execution_sync');
 const g=await goal();let id:string;
 try{id=await task(g,{kind:'harness',continuation:{state:'waiting_slot',runId:randomUUID()}});}
 finally{await pool.query('ALTER TABLE tasks ENABLE TRIGGER agentos_panel_execution_sync');}
 const before=(await pool.query(`SELECT t.xmin::text task_version,t.execution,m.control,m.input_digests,
  (SELECT pg_get_functiondef('agentos_execution_metadata_project(jsonb)'::regprocedure)) old_projector
  FROM tasks t JOIN task_execution_metadata m ON m.task_id=t.id WHERE t.id=$1`,[id!])).rows[0];
 await expect(requireTaskPanelMetadataReady()).rejects.toMatchObject({code:'TASK_PANEL_METADATA_INCOMPLETE'});
 await expect(panelGoal(g)).rejects.toMatchObject({code:'TASK_PANEL_METADATA_MISSING'});
 expect(await installTaskPanelMetadata()).toMatchObject({state:'complete'});
 expect((await projection(id!)).execution.continuation.state).toBe('waiting_slot');
 const after=(await pool.query(`SELECT t.xmin::text task_version,t.execution,m.control,m.input_digests,
  (SELECT pg_get_functiondef('agentos_execution_metadata_project(jsonb)'::regprocedure)) old_projector
  FROM tasks t JOIN task_execution_metadata m ON m.task_id=t.id WHERE t.id=$1`,[id!])).rows[0];
 expect(after).toEqual(before);expect(await requireTaskExecutionMetadataReady()).toEqual(beforeReceipt);
});

it('captures final BEFORE-trigger changes on a state-only update and rolls back with its task',async()=>{
 const g=await goal(),id=await task(g,{kind:'harness'},'panel-final-before'),db=await pool.connect();
 await pool.query(`CREATE OR REPLACE FUNCTION panel_test_before() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  IF NEW.key='panel-final-before' THEN NEW.execution:=NEW.execution||'{"continuation":{"state":"supervising","owner":"supervisor","message":"Preserved"}}'::jsonb;END IF;
  RETURN NEW;END $$;
  CREATE TRIGGER zz_panel_test_before BEFORE UPDATE OF state ON tasks FOR EACH ROW EXECUTE FUNCTION panel_test_before()`);
 try{
  await db.query('BEGIN');await db.query("UPDATE tasks SET state='waiting_resource' WHERE id=$1",[id]);
  expect((await db.query('SELECT execution FROM task_panel_metadata WHERE task_id=$1',[id])).rows[0].execution.continuation.state).toBe('supervising');
  await db.query('ROLLBACK');expect((await projection(id)).execution).toEqual({kind:'harness'});
  await pool.query("UPDATE tasks SET state='waiting_resource' WHERE id=$1",[id]);
  expect((await panelTask(id)).task.execution.continuation).toEqual({state:'supervising',owner:'supervisor',message:'Preserved'});
 }finally{await db.query('ROLLBACK');db.release();await pool.query('DROP TRIGGER zz_panel_test_before ON tasks;DROP FUNCTION panel_test_before()');}
});

it('keeps continuation observation churn out of manager materiality while updating the panel atomically',async()=>{
 const g=await goal(),id=await task(g,{kind:'harness',workspace:'panel/work',continuation:{state:'checking',updatedAt:'2026-10-10T01:00:00Z'}});
 const materiality=await readManagementMateriality(pool,g),control=(await pool.query('SELECT control FROM task_execution_metadata WHERE task_id=$1',[id])).rows[0].control;
 await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{continuation}',
  '{"state":"checking","updatedAt":"2026-10-10T01:02:00Z","nextAt":"2026-10-10T01:04:00Z"}') WHERE id=$1`,[id]);
 expect((await panelTask(id)).task.execution.continuation.updatedAt).toBe('2026-10-10T01:02:00Z');
 expect((await pool.query('SELECT control FROM task_execution_metadata WHERE task_id=$1',[id])).rows[0].control).toEqual(control);
 expect(await readManagementMateriality(pool,g)).toBe(materiality);
});

it.each(['missing','version','goal'])('fails closed for %s projection even if another task has valid metadata',async mode=>{
 const g=await goal(),other=await goal(),id=await task(g);await task(g);
 if(mode==='missing')await pool.query('DELETE FROM task_panel_metadata WHERE task_id=$1',[id]);
 else if(mode==='version')await pool.query('UPDATE task_panel_metadata SET version=0 WHERE task_id=$1',[id]);
 else await pool.query('UPDATE task_panel_metadata SET goal_id=$2 WHERE task_id=$1',[id,other]);
 await expect(requireTaskPanelMetadataReady()).rejects.toMatchObject({code:'TASK_PANEL_METADATA_INCOMPLETE'});
 for(const read of [()=>panelState(),()=>panelGoal(g),()=>panelTask(id)])
  await expect(read()).rejects.toMatchObject({code:'TASK_PANEL_METADATA_MISSING'});
 expect(await backfillTaskPanelMetadataBatch(pool)).toContain(id);
 await expect(requireTaskPanelMetadataReady()).resolves.toMatchObject({state:'complete'});
});

it('does not certify incomplete coverage while a legacy writer owns a missing row',async()=>{
 const g=await goal(),id=await task(g),writer=await pool.connect();
 await pool.query('DELETE FROM task_panel_metadata WHERE task_id=$1',[id]);
 try{
  await writer.query('BEGIN');await writer.query(`UPDATE tasks SET execution='{"kind":"harness","continuation":{"state":"waiting_slot"}}' WHERE id=$1`,[id]);
  expect(await backfillTaskPanelMetadataBatch(pool)).not.toContain(id);
  await expect(installTaskPanelMetadata()).rejects.toMatchObject({code:'55P03'});
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',[taskPanelMetadataReceipt])).rows[0].value.state).toBe('preparing');
  await writer.query('COMMIT');expect((await projection(id)).execution.continuation.state).toBe('waiting_slot');
  await expect(installTaskPanelMetadata()).resolves.toMatchObject({state:'complete'});
 }finally{await writer.query('ROLLBACK');writer.release();}
});

it('holds a backfilled row until commit so a following legacy update refreshes the projection',async()=>{
 const g=await goal(),id=await task(g),backfill=await pool.connect(),writer=await pool.connect();
 await pool.query('DELETE FROM task_panel_metadata WHERE task_id=$1',[id]);let pending:Promise<any>|undefined;
 try{
  const backfillPid=(await backfill.query('SELECT pg_backend_pid() pid')).rows[0].pid;
  const writerPid=(await writer.query('SELECT pg_backend_pid() pid')).rows[0].pid;
  await backfill.query('BEGIN');expect(await backfillTaskPanelMetadataBatch(backfill)).toContain(id);
  let finished=false;pending=writer.query(`UPDATE tasks SET execution='{"kind":"opencode","continuation":{"owner":"supervisor"}}' WHERE id=$1`,[id]).then(r=>{finished=true;return r;});
  let blocked=false;
  for(let i=0;i<100&&!blocked;i++){
   blocked=(await pool.query(`SELECT $1::int=ANY(pg_blocking_pids($2::int)) blocked`,[backfillPid,writerPid])).rows[0].blocked;
   if(!blocked)await new Promise(resolve=>setTimeout(resolve,20));
  }
  expect(blocked).toBe(true);expect(finished).toBe(false);
  await backfill.query('COMMIT');await pending;
  expect((await projection(id)).execution).toEqual({kind:'opencode',continuation:{owner:'supervisor'}});
 }finally{await backfill.query('ROLLBACK');await pending;backfill.release();writer.release();}
});

it('keeps startup and completed prepare read-only, refusing incomplete receipts and disabled synchronization',async()=>{
 const g=await goal(),id=await task(g),db=await pool.connect();
 const before=await projection(id),receipt=await installTaskPanelMetadata();
 try{await db.query('BEGIN READ ONLY');await expect(requireTaskPanelMetadataReady(db)).resolves.toEqual(receipt);await db.query('COMMIT');}
 finally{await db.query('ROLLBACK');db.release();}
 expect(await projection(id)).toEqual(before);
 await pool.query('ALTER TABLE tasks DISABLE TRIGGER agentos_panel_execution_sync');
 try{await expect(requireTaskPanelMetadataReady()).rejects.toMatchObject({code:'TASK_PANEL_METADATA_REQUIRED'});}
 finally{await pool.query('ALTER TABLE tasks ENABLE TRIGGER agentos_panel_execution_sync');}
 await pool.query(`UPDATE settings SET value=value||'{"state":"preparing"}' WHERE id=$1`,[taskPanelMetadataReceipt]);
 await expect(requireTaskPanelMetadataReady()).rejects.toMatchObject({code:'TASK_PANEL_METADATA_REQUIRED'});
 await installTaskPanelMetadata();await pool.query('DELETE FROM tasks WHERE id=$1',[id]);expect(await projection(id)).toBeUndefined();
});

it('keeps large embedded inputs out of the repeated read path and measures the independent write cost',async()=>{
 const g=await goal(),bytes=randomBytes(128*1024),input={base64:bytes.toString('base64'),digest:createHash('sha256').update(bytes).digest('hex')};
 const ids=[];for(let i=0;i<24;i++)ids.push(await task(g,{kind:'harness',workspace:'panel/'+i,continuation:{state:'checking',owner:'controller'},inputs:[input]}));
 const old=(await pool.query('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+legacySQL,[g])).rows[0]['QUERY PLAN'][0];
 const projected=(await pool.query('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+projectedSQL,[g])).rows[0]['QUERY PLAN'][0];
 const buffers=(plan:any)=>(plan.Plan['Shared Hit Blocks']??0)+(plan.Plan['Shared Read Blocks']??0);
 expect(buffers(old)).toBeGreaterThan(buffers(projected)*3);
 expect(JSON.stringify(projected.Plan)).not.toContain('jsonb_to_record');
 expect(checkedPanelExecution((await pool.query(projectedSQL,[g])).rows)).toEqual((await pool.query(legacySQL,[g])).rows);
 const writeStart=performance.now();await pool.query("UPDATE tasks SET state='waiting_resource' WHERE id=$1",[ids[0]]);
 console.info(JSON.stringify({probe:'task-panel-projection',tasks:ids.length,inputBytesPerTask:bytes.length,
  legacyMs:old['Execution Time'],projectedMs:projected['Execution Time'],legacyBuffers:buffers(old),projectedBuffers:buffers(projected),
  stateOnlyWriteMs:Math.round(performance.now()-writeStart)}));
});
