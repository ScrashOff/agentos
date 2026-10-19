import { beforeAll, beforeEach, afterAll, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installBacklogPlanner } from '../server/backlog.js';
import { installManagement, managementSnapshot } from '../server/management.js';
import { readManagementScopeTaskIds } from '../server/management-projections.js';
import { readManagementRetrieval } from '../server/management-retrieval.js';

beforeAll(async () => { await migrate(); await installBacklogPlanner(); await installManagement(); });
beforeEach(async () => { await pool.query('TRUNCATE goals,agents,accounts,events CASCADE'); await seedRegistry(); });
afterAll(async () => { await pool.end(); });

const base='management-projections/source';
async function goal() {
  const id=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Projection','Projection','management-projections',$1::text)",[id]);
  return id;
}
async function task(goalId:string, execution:any={baseWorkspace:base,workspace:base,workKind:'diagnostic'}, title='Coverage % literal') {
  const id=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,covers,execution)
    VALUES($1::uuid,$2,$1::text,$3,'Original source','research.web','ready','work','[]','["evidence"]',$4)`,
    [id,goalId,title,execution===undefined?null:JSON.stringify(execution)]);
  return id;
}
async function request(goalId:string,contextRequests:any[]) {
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    ['management-state:'+goalId,JSON.stringify({contextRequests})]);
}
const noArtifact=async()=>{throw Error('UNEXPECTED_ARTIFACT_READ');};
const oldCoverage=`SELECT t.id task_id,t.title,t.state,t.stage,t.attempt_count expected_attempt,
  t.covers,t.execution->>'workKind' work_kind,t.execution->>'baseWorkspace' base_workspace,b.coverage_key,
  (SELECT r.id FROM runs r WHERE r.task_id=t.id ORDER BY r.attempt DESC LIMIT 1) run_id,
  EXISTS(SELECT 1 FROM runs r WHERE r.task_id=t.id AND r.state IN('queued','running','cancelling','reconciling')) live
  FROM tasks t LEFT JOIN workforce_backlog_items b ON b.task_id=t.id AND b.goal_id=$1
  WHERE t.goal_id=$1 AND t.execution->>'baseWorkspace'=$2
  AND (b.task_id IS NOT NULL OR t.execution->>'workKind' IN('diagnostic','repair'))
  AND ($3='' OR strpos(lower(t.title||' '||COALESCE(b.coverage_key,'')||' '||t.covers::text),lower($3))>0)
  ORDER BY t.id`;

it('keeps literal filtering, complete counts and every page equal to the old coverage query',async()=>{
  const g=await goal();
  for(let i=0;i<24;i++)await task(g,{baseWorkspace:base,workspace:base,workKind:i%2?'diagnostic':'repair',
    inputs:i%3===0?'malformed-but-irrelevant':[{base64:'private'.repeat(12000)}]},i%3?'Coverage % literal':'Other coverage');
  const indexed=await task(g,{baseWorkspace:base,workspace:base,workKind:'analysis'},'Coverage % indexed');
  await pool.query('INSERT INTO workforce_backlog_items VALUES($1,$2,$3)',[g,'indexed-source',indexed]);
  await task(g,{baseWorkspace:'other/source',workKind:'diagnostic'});
  await task(await goal());
  for(const query of ['', '%', 'INDEXED', 'absent']) {
    const expected=(await pool.query(oldCoverage,[g,base,query])).rows;
    for(const offset of [0,10,20,100]) {
      await request(g,[{kind:'coverage',baseWorkspace:base,query,offset}]);
      const page=(await readManagementRetrieval(pool,g,noArtifact)).results[0];
      expect(page.status).toBe('OK');expect(page.totalRows).toBe(expected.length);
      expect(page.rows).toEqual(expected.slice(offset,offset+10));
      expect(JSON.stringify(page)).not.toContain('private');
    }
  }
});

it('preserves ->> scope semantics for SQL null, JSON null, scalars, arrays and malformed irrelevant inputs',async()=>{
  const g=await goal();
  for(const value of [null,3,'string',[],{}, {baseWorkspace:base,inputs:null},
    {baseWorkspace:base,inputs:[null,7,'text']},{baseWorkspace:12,workKind:false}])await task(g,value);
  const sqlNull=await task(g);await pool.query('UPDATE tasks SET execution=NULL WHERE id=$1',[sqlNull]);
  for(const scope of [base,'12','missing/source',null]) {
    const old=(await pool.query(`SELECT id FROM tasks WHERE goal_id=$1 AND ($2::text IS NULL OR execution->>'baseWorkspace'=$2) ORDER BY id`,[g,scope])).rows.map(row=>row.id);
    expect(await readManagementScopeTaskIds(pool,g,scope)).toEqual(old);
  }
});

it('fails closed before pagination even if a missing projection belongs to another scope',async()=>{
  const g=await goal();for(let i=0;i<12;i++)await task(g);
  const missing=await task(g,{baseWorkspace:'other/source',workKind:'diagnostic'});
  await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1',[missing]);
  await request(g,[{kind:'coverage',baseWorkspace:base,offset:100}]);
  await expect(readManagementRetrieval(pool,g,noArtifact)).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
  await expect(readManagementScopeTaskIds(pool,g,base)).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
});

it('rejects wrong goal/version projections and refreshes scope from the transactional trigger',async()=>{
  const g=await goal(),other=await goal(),id=await task(g);
  await pool.query('UPDATE task_execution_metadata SET goal_id=$2 WHERE task_id=$1',[id,other]);
  await expect(readManagementScopeTaskIds(pool,g,base)).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
  await pool.query('UPDATE tasks SET title=title WHERE id=$1',[id]);
  await pool.query('UPDATE task_execution_metadata SET version=0 WHERE task_id=$1',[id]);
  await expect(readManagementScopeTaskIds(pool,g,base)).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
  await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}','"moved/source"') WHERE id=$1`,[id]);
  expect(await readManagementScopeTaskIds(pool,g,base)).toEqual([]);
  expect(await readManagementScopeTaskIds(pool,g,'moved/source')).toEqual([id]);
});

it('keeps immutable read snapshots isolated while another transaction moves scope',async()=>{
  const g=await goal(),id=await task(g),db=await pool.connect();
  try {
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    expect(await readManagementScopeTaskIds(db,g,base)).toEqual([id]);
    await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}','"moved/source"') WHERE id=$1`,[id]);
    expect(await readManagementScopeTaskIds(db,g,base)).toEqual([id]);
    await db.query('COMMIT');
  }finally{await db.query('ROLLBACK');db.release();}
  expect(await readManagementScopeTaskIds(pool,g,base)).toEqual([]);
});

it('keeps focused manager coverage and composition ordering scoped to current attempts',async()=>{
  const g=await goal(),same=await task(g),other=await task(g,{baseWorkspace:'other/source',workspace:'other/source',workKind:'repair'});
  for(const [id,attempt] of [[same,1],[same,2],[other,1]] as const){
    await pool.query('UPDATE tasks SET attempt_count=$2 WHERE id=$1',[id,attempt]);
    const run=randomUUID();
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,$3,$4,1,'submitted','harness')",[run,id,attempt,randomUUID()]);
    await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('PATCH_COMPOSITION_RECORDED',$1,$2)",[id,JSON.stringify({
      runId:run,version:1,workspace:id===same?base:'other/source',dryRun:false,complete:false,
      receipts:[{producerTaskId:randomUUID(),status:'conflict',code:'PREIMAGE_MISMATCH'}],
      acceptance:false,independentValidationRequired:true})]);
  }
  const policy={workerAccounts:[],glmTarget:12,museTarget:0};
  const focused=await managementSnapshot(pool,g,policy,{scope:base});
  const expected=(await pool.query(`SELECT t.id,t.title,t.state,t.attempt_count expected_attempt,t.covers,b.coverage_key FROM tasks t
    LEFT JOIN workforce_backlog_items b ON b.task_id=t.id AND b.goal_id=$1 WHERE t.goal_id=$1
    AND t.execution->>'baseWorkspace'=$2 AND (b.task_id IS NOT NULL OR t.execution->>'workKind' IN('diagnostic','repair')) ORDER BY t.id LIMIT 501`,[g,base])).rows;
  expect(focused.coverageIndex).toMatchObject({scope:base,rows:expected,complete:true,nextOffset:null});
  expect(focused.compositions.map(row=>[row.taskId,row.expectedAttempt])).toEqual([[same,2]]);
  expect(focused.compositions[0].bindingStatus).toBe('BOUND');
  expect(JSON.stringify(focused.compositions)).toContain('PREIMAGE_MISMATCH');
  const all=await managementSnapshot(pool,g,policy);
  expect(all.compositions.map(row=>[row.taskId,row.expectedAttempt])).toEqual([[other,1],[same,2]]);
});
