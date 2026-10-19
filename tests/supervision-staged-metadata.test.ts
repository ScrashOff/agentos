import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID,randomBytes} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installDeferredHandoffs} from '../server/deferred-handoffs.js';
import {installTaskExecutionMetadata,taskExecutionMetadataReceipt} from '../server/task-execution-metadata.js';
import {readStagedSupervisionRepairs} from '../server/supervision-context.js';

const legacy=`SELECT p.id,p.title,r.id run_id FROM deferred_handoffs h JOIN tasks p ON p.id=h.producer_task_id
      JOIN runs r ON r.task_id=p.id AND r.attempt=p.attempt_count
      WHERE h.consumer_task_id=$1 AND h.goal_id=$2 AND h.state IN('staged','replanning_required')
      AND p.id<>$3 AND p.state='accepted' AND p.execution->>'workKind'='repair'
      AND p.execution->>'baseWorkspace'=$4 AND r.state='submitted'
      AND (SELECT passed FROM acceptances v WHERE v.run_id=r.id ORDER BY created_at DESC LIMIT 1)=true
      AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=p.id AND live.state IN('queued','running','cancelling','reconciling'))
      ORDER BY p.created_at,p.id`;
const scope='staged-metadata/game';
let goalId:string,source:any,target:any,sequence=0;
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installTaskExecutionMetadata();await installDeferredHandoffs();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();sequence=0;
  goalId=await goal();source=await task({workKind:'diagnostic',baseWorkspace:scope});
  target=await task({kind:'opencode',baseWorkspace:scope});
});
afterAll(()=>pool.end());
async function goal(){const id=randomUUID();await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
  VALUES($1::uuid,'Staged context','Original objective','staged-metadata',$1::text)`,[id]);return id;}
async function task(execution:any={workKind:'repair',baseWorkspace:scope},options:any={}){
  const id=randomUUID(),run=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count,created_at)
    VALUES($1::uuid,$2,$1::text,$1::text,'Original criteria','qa.backend',$3,'[]',$4,1,$5)`,
    [id,options.goal??goalId,options.state??'accepted',execution===undefined?null:JSON.stringify(execution),
      new Date(Date.parse('2026-10-11T01:00:00Z')+sequence++*1000)]);
  if(options.run!==false){
    await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
      VALUES($1,$2,1,$3,'harness',1,$4)`,[run,id,randomUUID(),options.runState??'submitted']);
    if(options.acceptance!==false)await acceptance(id,run,options.passed??true);
  }
  return {id,run,goal_id:options.goal??goalId,attempt_count:1,execution};
}
async function acceptance(taskId:string,runId:string,passed:boolean,date='2026-10-11T02:00:00Z'){
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,$4,'[]','fixture',$5)`,[randomUUID(),taskId,runId,passed,date]);
}
async function staged(execution:any={workKind:'repair',baseWorkspace:scope},options:any={}){
  const producer=options.producer??await task(execution,options);
  await pool.query(`INSERT INTO deferred_handoffs(id,goal_id,producer_task_id,consumer_task_id,source_task_id,
    source_proof,consumer_proof,handoff,state) VALUES($1,$2,$3,$4,$5,'{}','{}','{}',$6)`,
    [randomUUID(),options.handoffGoal??goalId,producer.id,options.consumer??target.id,source.id,options.handoffState??'staged']);
  return producer;
}
async function equivalent(){
  const db=await pool.connect();try{
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const expected=(await db.query(legacy,[target.id,target.goal_id,source.id,target.execution.baseWorkspace])).rows;
    const actual=await readStagedSupervisionRepairs(db,source,target);expect(actual).toEqual(expected);
    return actual;
  }finally{await db.query('ROLLBACK');db.release();}
}
it('matches null, scalar and malformed raw execution without coercing legacy fields',async()=>{
  for(const value of [null,[],2,true,'scalar',{}, {workKind:null}, {workKind:['repair'],baseWorkspace:scope},
    {workKind:'repair',baseWorkspace:null},{workKind:'repair',baseWorkspace:[scope]},
    {workKind:'repair',baseWorkspace:{nested:scope}},{workKind:'other',baseWorkspace:scope}])await staged(value);
  const valid=await staged({workKind:'repair',baseWorkspace:scope,readOnlySource:'not-a-boolean'});
  expect((await equivalent()).map(r=>r.id)).toEqual([valid.id]);
});
it.each([true,1])('preserves ->> text extraction for legacy workspace %s',async(value)=>{
  target.execution.baseWorkspace=String(value);
  const valid=await staged({workKind:'repair',baseWorkspace:value});
  expect((await equivalent()).map(r=>r.id)).toEqual([valid.id]);
});
it('preserves consumer/goal/source/state predicates and the producer actual current attempt',async()=>{
  const valid=await staged(),replanning=await staged(undefined,{handoffState:'replanning_required'});
  await staged(undefined,{handoffState:'waiting_result'});await staged(undefined,{state:'failed'});
  await staged(undefined,{runState:'failed'});await staged(undefined,{run:false});
  await staged(undefined,{acceptance:false});await staged(undefined,{passed:false});
  await staged(undefined,{consumer:source.id});await staged(undefined,{producer:source});
  await staged(undefined,{handoffGoal:await goal()});
  const obsolete=await staged();await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[obsolete.id]);
  expect((await equivalent()).map(r=>r.id)).toEqual([valid.id,replanning.id]);
});
it('uses the latest acceptance, and rejects each live actor state',async()=>{
  const olderPass=await staged();await acceptance(olderPass.id,olderPass.run,false,'2026-10-11T03:00:00Z');
  const latestPass=await staged(undefined,{passed:false});await acceptance(latestPass.id,latestPass.run,true,'2026-10-11T03:00:00Z');
  for(const state of ['queued','running','cancelling','reconciling']){
    const row=await staged();await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
      VALUES($1,$2,2,$3,'harness',1,$4)`,[randomUUID(),row.id,randomUUID(),state]);
  }
  expect((await equivalent()).map(r=>r.id)).toEqual([latestPass.id]);
});
it('retains exact creation/id ordering and small results without decoding embedded inputs',async()=>{
  const a=await staged({workKind:'repair',baseWorkspace:scope,contextSnapshot:randomBytes(512*1024).toString('hex')}),b=await staged();
  await pool.query('UPDATE tasks SET created_at=$1 WHERE id=ANY($2::uuid[])',['2026-10-11T04:00:00Z',[a.id,b.id]]);
  const rows=await equivalent();expect(rows.map(r=>r.id)).toEqual([a.id,b.id].sort());
  expect(JSON.stringify(rows).length).toBeLessThan(400);
});
it.each(['missing','version','goal'])('refuses %s canonical metadata before projected scope filters',async(kind)=>{
  const row=await staged({workKind:'other',baseWorkspace:'other'});
  if(kind==='missing')await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1',[row.id]);
  if(kind==='version')await pool.query('UPDATE task_execution_metadata SET version=0 WHERE task_id=$1',[row.id]);
  if(kind==='goal')await pool.query('UPDATE task_execution_metadata SET goal_id=$2 WHERE task_id=$1',[row.id,await goal()]);
  await expect(readStagedSupervisionRepairs(pool,source,target)).rejects.toMatchObject({code:'TASK_EXECUTION_METADATA_INCOMPLETE'});
});
it('sentinel detects metadata disappearing after readiness, including a row outside projected filters',async()=>{
  const row=await staged({workKind:'other',baseWorkspace:'other'});let deleted=false;
  const db={query:async(sql:any,args?:any)=>{
    if(typeof sql==='string'&&sql.startsWith('WITH relational AS MATERIALIZED')){
      await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1',[row.id]);deleted=true;
    }
    return pool.query(sql,args);
  }};
  await expect(readStagedSupervisionRepairs(db as any,source,target)).rejects.toMatchObject({code:'TASK_EXECUTION_METADATA_MISSING'});
  expect(deleted).toBe(true);
});
it('fails closed on readiness provenance without performing preparation',async()=>{
  const original=(await pool.query('SELECT value FROM settings WHERE id=$1',[taskExecutionMetadataReceipt])).rows[0].value;
  try{
    await pool.query('UPDATE settings SET value=$2 WHERE id=$1',[taskExecutionMetadataReceipt,JSON.stringify({...original,sourceHash:'0'.repeat(64)})]);
    await expect(readStagedSupervisionRepairs(pool,source,target)).rejects.toMatchObject({code:'TASK_EXECUTION_METADATA_REQUIRED'});
  }finally{await pool.query('UPDATE settings SET value=$2 WHERE id=$1',[taskExecutionMetadataReceipt,JSON.stringify(original)]);}
});
it('keeps projection and actual run consistent under concurrent uncommitted source changes',async()=>{
  const row=await staged(),writer=await pool.connect();
  try{
    await writer.query('BEGIN');await writer.query("UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}','\"other\"') WHERE id=$1",[row.id]);
    expect((await equivalent()).map(r=>r.id)).toEqual([row.id]);
    await writer.query('COMMIT');expect(await equivalent()).toEqual([]);
  }finally{await writer.query('ROLLBACK');writer.release();}
});
