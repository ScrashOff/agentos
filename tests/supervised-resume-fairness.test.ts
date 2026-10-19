import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {installSupervision,readSupervisedResumeCandidates,resumeSupervisedWork} from '../server/supervision.js';

const cursorKey='supervised-resume-cursor';
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installSupervision();await installTaskExecutionMetadata();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query('DELETE FROM settings WHERE id=$1',[cursorKey]);
});
afterAll(async()=>{await pool.query('DELETE FROM settings WHERE id=$1',[cursorKey]);await pool.end();});

async function fixture(index:number){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID(),account=randomUUID();
  const project='resume-fairness-'+goal,workspace=project+'/candidate';
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES($1,'Fixture only','codex','gpt-6-luna','["gpt-6-luna"]',true,12,true,'fixture-only')`,[account]);
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1,'Resume fairly','Preserve the applied decision',$2,$3,'active')`,[goal,project,goal]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,
    attempt_count,account_id,model,allowed_accounts,updated_at)
    VALUES($1,$2,'repair','Preserved task','Original discriminating test','qa.backend','failed','work',
      '[{"id":"evidence","type":"artifact","name":"repair-report.json"}]',$3,1,$4,'gpt-6-luna',$5,
      '2026-01-01'::timestamptz+$6*interval '1 second')`,[task,goal,JSON.stringify({kind:'harness',profile:'code',workspace,
        baseWorkspace:project+'/base',supervision:{caseId,sourceRunId:run},inputs:[]}),account,JSON.stringify([account]),index]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,account_id,model,finished_at)
    VALUES($1,$2,1,$3,'harness',1,'failed','MODEL_TIMEOUT',$4,'gpt-6-luna',now())`,[run,task,randomUUID(),account]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,applied_attempt)
    VALUES($1,$2,$3,$4,'applied','A bounded revised approach','[]',1)`,[caseId,goal,task,run]);
  return {goal,task,run,caseId,account,workspace};
}
async function fixtures(count=7){const rows=[];for(let i=0;i<count;i++)rows.push(await fixture(i));return rows;}
const refusal=()=>{throw Error('CAPACITY_UNAVAILABLE');};
const currentCursor=async()=>(await pool.query('SELECT value FROM settings WHERE id=$1',[cursorKey])).rows[0]?.value;
const originals=async(ids:string[])=>(await pool.query(`SELECT id,description,criteria,instruction_revision_id,
  account_id,model,allowed_accounts,attempt_count FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY id`,[ids])).rows;

it('examines and actually prepares the sixth recovery despite four older capacity refusals, without changing its contract or account limit',async()=>{
  const f=await fixtures(),before=await originals(f.map(x=>x.task));
  const accountIds=f.map(x=>x.account).sort();
  const beforeAccounts=(await pool.query('SELECT id,max_concurrency FROM accounts WHERE id=ANY($1::text[]) ORDER BY id',[accountIds])).rows;
  expect(beforeAccounts).toEqual(accountIds.map(id=>({id,max_concurrency:12})));
  const initialRuns=(await pool.query('SELECT id,task_id,attempt,fence,state,instruction_snapshot FROM runs ORDER BY id')).rows;
  const inspect=vi.fn(async(id:string)=>{
    const index=f.findIndex(x=>x.run===id);if(index<4)return refusal();
    const row=f[index];return {runId:id,workspace:row.workspace,checkedAt:new Date().toISOString(),candidate:{patch:''},
      process:{unit:'agentos-run-'+id,stopped:true,activeState:'inactive',mainPid:0,populated:false}};
  });
  expect(await resumeSupervisedWork(inspect)).toEqual([]);
  expect(inspect.mock.calls.map(x=>x[0]).sort()).toEqual(f.slice(0,4).map(x=>x.run).sort());
  expect((await currentCursor()).taskId).toBe(f[3].task);
  // The real backlog cycle is longer than this cooldown: all four remain
  // eligible, so a static oldest-first limit would select them forever.
  await pool.query("UPDATE events SET created_at=now()-interval '60 seconds' WHERE kind='SUPERVISION_RESUME_DEFERRED'");
  inspect.mockClear();
  expect((await resumeSupervisedWork(inspect)).sort()).toEqual(f.slice(4).map(x=>x.task).sort());
  expect(inspect.mock.calls.map(x=>x[0]).sort()).toEqual([f[4].run,f[5].run,f[6].run,f[0].run].sort());
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f[5].task])).rows[0].state).toBe('retry_scheduled');
  expect(await originals(f.map(x=>x.task))).toEqual(before);
  expect((await pool.query('SELECT id,task_id,attempt,fence,state,instruction_snapshot FROM runs ORDER BY id')).rows).toEqual(initialRuns);
  expect((await pool.query('SELECT id,max_concurrency FROM accounts WHERE id=ANY($1::text[]) ORDER BY id',[accountIds])).rows).toEqual(beforeAccounts);
  expect((await pool.query('SELECT state FROM supervision_cases')).rows.every(x=>x.state==='applied')).toBe(true);
});

it('advances past a microsecond timestamp tie after guarded early returns without inspecting integrity-blocked runs',async()=>{
  const f=(await fixtures()).sort((a,b)=>a.task.localeCompare(b.task));
  await pool.query("UPDATE tasks SET updated_at='2026-01-01T00:00:00.123456Z'");
  await pool.query("UPDATE runs SET error_code='ARTIFACT_CORRUPT' WHERE id=ANY($1::uuid[])",[f.slice(0,4).map(x=>x.run)]);
  const before=await originals(f.map(x=>x.task)),inspect=vi.fn(async()=>refusal());
  expect(await resumeSupervisedWork(inspect)).toEqual([]);expect(inspect).not.toHaveBeenCalled();
  expect((await currentCursor()).taskId).toBe(f[3].task);
  expect((await currentCursor()).updatedAt).toContain('.123456');
  await resumeSupervisedWork(inspect);
  expect(inspect.mock.calls).toHaveLength(3);
  expect(await originals(f.map(x=>x.task))).toEqual(before);
});

it('does not advance while any selected examination is still pending',async()=>{
  const f=await fixtures(4);
  let release!:()=>void,entered!:()=>void;
  const pending=new Promise<void>(resolve=>{release=resolve;}),started=new Promise<void>(resolve=>{entered=resolve;});
  const running=resumeSupervisedWork(async(id:string)=>{if(id===f[0].run){entered();await pending;}return refusal();});
  await started;expect(await currentCursor()).toBeUndefined();release();await running;
  expect((await currentCursor()).taskId).toBe(f[3].task);
});

it('keeps a concurrently committed cursor through compare-and-swap instead of overwriting another owner',async()=>{
  const f=await fixtures(4),competing={version:1,taskId:randomUUID(),updatedAt:'2026-01-02T00:00:00.000Z'};
  await resumeSupervisedWork(async(id:string)=>{
    if(id===f[0].run)await pool.query('INSERT INTO settings VALUES($1,$2)',[cursorKey,JSON.stringify(competing)]);
    return refusal();
  });
  expect(await currentCursor()).toEqual(competing);
});

it('does not recreate a cursor removed by another owner while the batch is examined',async()=>{
  const f=await fixtures(4),prior={version:1,taskId:randomUUID(),updatedAt:'2025-12-31T00:00:00.000Z'};
  await pool.query('INSERT INTO settings VALUES($1,$2)',[cursorKey,JSON.stringify(prior)]);
  await resumeSupervisedWork(async(id:string)=>{
    if(id===f[0].run)await pool.query('DELETE FROM settings WHERE id=$1',[cursorKey]);
    return refusal();
  });
  expect(await currentCursor()).toBeUndefined();
});

it('leaves the cursor unchanged if the selected batch cannot be read safely',async()=>{
  const f=await fixture(0),prior={version:1,taskId:randomUUID(),updatedAt:'2026-01-01T00:00:00.000Z'};
  await pool.query('INSERT INTO settings VALUES($1,$2)',[cursorKey,JSON.stringify(prior)]);
  await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1',[f.task]);
  const inspect=vi.fn(async()=>refusal());
  await expect(resumeSupervisedWork(inspect)).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
  expect(inspect).not.toHaveBeenCalled();expect(await currentCursor()).toEqual(prior);
});

it('keeps direct read-only selection compatible, adds deterministic ties, and wraps without dropping tasks',async()=>{
  const f=await fixtures();await pool.query("UPDATE tasks SET updated_at='2026-01-01T00:00:00Z'");
  const sorted=f.map(x=>x.task).sort(),db=await pool.connect();
  try{
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const first=await readSupervisedResumeCandidates(db);
    expect(first.map(x=>x.id)).toEqual(sorted.slice(0,4));
    const next=await readSupervisedResumeCandidates(db,{updatedAt:first[3].updated_at.toISOString(),taskId:first[3].id});
    expect(next.map(x=>x.id)).toEqual([...sorted.slice(4),sorted[0]]);
    await db.query('COMMIT');
  }finally{await db.query('ROLLBACK');db.release();}
  expect(await currentCursor()).toBeUndefined();
});

it('retains cancellation and changed-source guards after the cursor rotates',async()=>{
  const f=await fixtures();const inspect=vi.fn(async(_id:string)=>refusal());await resumeSupervisedWork(inspect);
  await event(pool,'CANCEL_REQUESTED',f[5].task,{runId:f[5].run});
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision,sourceRunId}',to_jsonb($2::text)) WHERE id=$1",[f[6].task,randomUUID()]);
  inspect.mockClear();await resumeSupervisedWork(inspect);
  expect(inspect.mock.calls.map(x=>x[0])).not.toContain(f[5].run);
  expect(inspect.mock.calls.map(x=>x[0])).not.toContain(f[6].run);
  expect((await pool.query('SELECT count(*)::int count FROM runs')).rows[0].count).toBe(7);
});

it('does not manufacture a cursor for an empty batch',async()=>{
  expect(await resumeSupervisedWork(async()=>refusal())).toEqual([]);expect(await currentCursor()).toBeUndefined();
});
