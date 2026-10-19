import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {recoverMissingOpenCodeArtifacts} from '../server/missing-artifacts.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id='autonomy:artifact-fairness-test'");
  await seedRegistry();
});
afterAll(async()=>{await pool.end();});

async function goal(){
  const id=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Recover outputs','Preserve acceptance','artifact-fairness-test',$1::text)",[id]);
  return id;
}
async function task(goalId:string,rank:number,mode:string){
  const id=randomUUID(),run=randomUUID();
  const submitted=mode==='submitted_semantic'||mode==='submitted_missing';
  const execution={kind:'opencode',workKind:'planning',workspace:'artifact-fairness-test/'+id,
    ...(mode==='cooldown'?{artifactCollectionAt:new Date().toISOString()}:{}),
  };
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count,updated_at)
    VALUES($1::uuid,$2,$1::text,'Recover plan','Preserve original contract','qa.backend',$3,$4,$5,1,now()-($6::int*interval '1 minute'))`,
    [id,goalId,submitted?'submitted':'recovering_artifacts',JSON.stringify([{id:'plan',type:'artifact',label:'Structured plan',name:'scope-plan.json'}]),JSON.stringify(execution),100-rank]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,finished_at)
    VALUES($1,$2,1,$3,'opencode',1,$4,$5,now())`,[run,id,randomUUID(),submitted?'submitted':'failed',submitted?null:mode==='nonpackaging'?'PLANNING_SCOPE_INVALID':'RESULT_FORMAT']);
  if(submitted){
    await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,'test')",
      [randomUUID(),id,run,JSON.stringify([{id:'plan',type:'artifact',passed:false}])]);
    if(mode==='submitted_semantic')await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'scope-plan.json','application/json',$3,2)",[randomUUID(),run,'a'.repeat(64)]);
  }
  if(mode==='cancelled')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[id,JSON.stringify({runId:run})]);
  return {id,run};
}

it.each(['failed_packaging','submitted_missing'])('reaches the 31st recoverable %s output beyond 30 older ineligible plans',async(mode)=>{
  const goalId=await goal();
  const older=[];
  for(let i=0;i<30;i++)older.push(await task(goalId,i,['nonpackaging','submitted_semantic','cooldown','cancelled'][i%4]));
  const wanted=await task(goalId,30,mode);
  // Exercise the real SQL selector and authoritative claim. The controlled
  // stale-run result ends collection before any bridge/model/artifact adoption.
  const collect=vi.fn(async()=>{throw Error('TASK_CHANGED');});
  const inspect=vi.fn(async()=>{throw Error('UNEXPECTED_REMOTE_INSPECTION');});
  expect(await recoverMissingOpenCodeArtifacts(collect,inspect)).toEqual([]);
  expect(collect).toHaveBeenCalledExactlyOnceWith(wanted.run,false,undefined);
  expect(inspect).not.toHaveBeenCalled();
  expect((await pool.query("SELECT execution->>'artifactCollectionAt' AS collected_at FROM tasks WHERE id=$1",[wanted.id])).rows[0].collected_at).toBeTruthy();
  expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE id=ANY($1::uuid[]) AND execution->'continuation' IS NOT NULL",[older.map(t=>t.id)])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n).toBe(31);
  expect((await pool.query('SELECT count(*)::int n FROM outbox')).rows[0].n).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE passed')).rows[0].n).toBe(0);
});

it('leaves a recoverable output untouched while another actor owns its workspace',async()=>{
  const goalId=await goal(),target=await task(goalId,0,'failed_packaging'),actor=await task(goalId,1,'failed_packaging');
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}',to_jsonb($2::text)),state='running' WHERE id=$1",[actor.id,'artifact-fairness-test/'+target.id]);
  await pool.query("UPDATE runs SET state='running',finished_at=NULL,error_code=NULL WHERE id=$1",[actor.run]);
  const collect=vi.fn(async()=>{throw Error('UNEXPECTED_COLLECTION');});
  expect(await recoverMissingOpenCodeArtifacts(collect)).toEqual([]);
  expect(collect).not.toHaveBeenCalled();
  expect((await pool.query("SELECT execution->>'artifactCollectionAt' AS collected_at FROM tasks WHERE id=$1",[target.id])).rows[0].collected_at).toBeNull();
});

it('rechecks a paused objective after candidate selection waits for the claim lock',async()=>{
  const goalId=await goal(),target=await task(goalId,0,'failed_packaging');
  const collect=vi.fn(async()=>{throw Error('UNEXPECTED_COLLECTION');});
  const owner=await pool.connect();
  let pending:ReturnType<typeof recoverMissingOpenCodeArtifacts>|undefined;
  try{
    await owner.query('BEGIN');
    await owner.query('SELECT pg_advisory_xact_lock(4280001)');
    pending=recoverMissingOpenCodeArtifacts(collect);
    let waiting=false;
    for(let i=0;i<200&&!waiting;i++){
      waiting=(await pool.query("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=4280001 AND NOT granted) waiting")).rows[0].waiting;
      if(!waiting)await new Promise(resolve=>setTimeout(resolve,10));
    }
    expect(waiting).toBe(true);
    await owner.query("UPDATE goals SET state='paused' WHERE id=$1",[goalId]);
    await owner.query('COMMIT');
    expect(await pending).toEqual([]);
    expect(collect).not.toHaveBeenCalled();
    expect((await pool.query("SELECT execution->>'artifactCollectionAt' AS collected_at FROM tasks WHERE id=$1",[target.id])).rows[0].collected_at).toBeNull();
  }finally{
    await owner.query('ROLLBACK');
    owner.release();
    if(pending)await pending;
  }
});
