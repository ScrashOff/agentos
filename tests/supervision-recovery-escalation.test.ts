import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,event} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision,detectSupervisionCases} from '../server/supervision.js';

beforeAll(async()=>{await migrate();await installSupervision();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(()=>pool.end());

async function fixture(){
  const goal=randomUUID(),task=randomUUID(),priorRun=randomUUID(),run=randomUUID(),priorCase=randomUUID();
  const context=randomUUID(),project='recovery-escalation-'+goal,workspace=project+'/candidate';
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1::uuid,'Recovery','Preserve real evidence',$2,$1::text,'active')`,[goal,project]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  const continuation={state:'needs_action',runId:run,owner:'supervisor',message:'Collection failed; another decision is required.',
    updatedAt:new Date().toISOString(),nextAt:new Date(Date.now()+120000).toISOString()};
  const execution={kind:'harness',workspace,baseWorkspace:project+'/base',continuation};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1,$2,'integrate','Integrate candidate','Preserve all original criteria','qa.backend','recovering_artifacts',
      'integrate',$3,$4,77)`,[task,goal,JSON.stringify([{id:'proof',type:'artifact',name:'protocol-report.json',label:'Real protocol proof'}]),JSON.stringify(execution)]);
  for(const [id,attempt] of [[priorRun,76],[run,77]] as const){
    await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,summary,context_id,checkpoint,instruction_snapshot)
      SELECT $1,t.id,$3,$4,'harness',1,'failed','RESULT_FORMAT','Preserved failed delivery',$5,$6,
        jsonb_build_object('schema',1,'revisionId',NULL,'task',to_jsonb(t)) FROM tasks t WHERE t.id=$2`,
      [id,task,attempt,randomUUID(),context,JSON.stringify({kind:'automatic_recovery',sourceRunId:priorRun})]);
  }
  await pool.query(`INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Frozen context')`,[context,run]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,applied_attempt)
    VALUES($1,$2,$3,$4,'applied','Previous valid decision','[]',76)`,[priorCase,goal,task,priorRun]);
  return {goal,task,priorRun,run,priorCase,context,project,workspace,continuation};
}
function stopped(f:Awaited<ReturnType<typeof fixture>>){
  return {runId:f.run,workspace:f.workspace,checkedAt:new Date().toISOString(),
    process:{unit:'agentos-run-'+f.run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};
}
async function historical(f:Awaited<ReturnType<typeof fixture>>){
  return (await pool.query(`SELECT id,state,fence,attempt,context_id,md5(instruction_snapshot::text) snapshot
    FROM runs WHERE task_id=$1 ORDER BY attempt`,[f.task])).rows;
}
async function openCases(f:Awaited<ReturnType<typeof fixture>>){
  return (await pool.query("SELECT * FROM supervision_cases WHERE task_id=$1 AND state='open'",[f.task])).rows;
}

it('opens exactly one decision for stopped attempt77 after applied76, without adding retries or mutating frozen evidence',async()=>{
  const f=await fixture(),before=await historical(f),inspect=vi.fn(async()=>{
    const probe=await pool.connect();
    try{
      const available=(await probe.query('SELECT pg_try_advisory_lock(4280001) owned')).rows[0].owned;
      if(available)await probe.query('SELECT pg_advisory_unlock(4280001)');
      expect(available).toBe(true);
    }finally{probe.release();}
    return stopped(f);
  });
  const added=await detectSupervisionCases(inspect);
  expect(added).toHaveLength(1);expect(inspect).toHaveBeenCalledWith(f.run,false);
  const cases=await openCases(f);expect(cases).toHaveLength(1);
  expect(cases[0]).toMatchObject({id:added[0],source_run_id:f.run,state:'open'});
  expect(cases[0].evidence.map((item:any)=>item.attempt)).toEqual([77,76]);
  expect(await historical(f)).toEqual(before);
  expect((await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0])
    .toEqual({state:'recovery_blocked',attempt_count:77});
  const receipt=(await pool.query("SELECT payload FROM events WHERE kind='SUPERVISION_REQUESTED' AND entity_id=$1",[f.task])).rows[0].payload;
  expect(receipt.recoveryEscalation).toMatchObject({previousCaseId:f.priorCase,previousAppliedAttempt:76,attempt:77,contextId:f.context,
    process:{unit:'agentos-run-'+f.run,stopped:true}});
  expect(await detectSupervisionCases(inspect)).toEqual([]);expect(inspect).toHaveBeenCalledTimes(1);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1',[f.run])).rows[0].n).toBe(0);
});

it.each(['ordinary','old-marker','same-attempt','quota','cancel','live','manual','paused','disabled','closed','user-owned'])
('preserves generic cooldown and authority guards for %s',async mode=>{
  const f=await fixture();
  if(mode==='ordinary')await pool.query("UPDATE tasks SET execution=execution-'continuation' WHERE id=$1",[f.task]);
  if(mode==='old-marker')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{continuation,runId}',to_jsonb($2::text)) WHERE id=$1",[f.task,f.priorRun]);
  if(mode==='same-attempt')await pool.query('UPDATE supervision_cases SET applied_attempt=77 WHERE id=$1',[f.priorCase]);
  if(mode==='quota')await pool.query("UPDATE runs SET error_code='QUOTA_EXCEEDED' WHERE id=$1",[f.run]);
  if(mode==='cancel')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
  if(mode==='live')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
  if(mode==='manual')await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.task]);
  if(mode==='paused')await pool.query('INSERT INTO settings VALUES($1,$2)',['autonomy:'+f.project,JSON.stringify({enabled:false})]);
  if(mode==='disabled')await pool.query('UPDATE settings SET value=$2 WHERE id=$1',['supervision:'+f.goal,JSON.stringify({enabled:false})]);
  if(mode==='closed')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(mode==='user-owned')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{continuation,owner}','\"user\"') WHERE id=$1",[f.task]);
  const inspect=vi.fn(async()=>stopped(f));
  expect(await detectSupervisionCases(inspect)).toEqual([]);
  expect(inspect).not.toHaveBeenCalled();expect(await openCases(f)).toEqual([]);
});

it.each(['expired','busy','wrong-unit','wrong-workspace','unavailable'])('refuses %s physical stop proof without replacing cooldown',async mode=>{
  const f=await fixture(),before=await historical(f);
  const inspect=vi.fn(async()=>{
    if(mode==='unavailable')throw Error('SSH_UNAVAILABLE');
    const proof=stopped(f);
    if(mode==='expired')proof.checkedAt=new Date(Date.now()-61000).toISOString();
    if(mode==='busy')proof.process.populated=true;
    if(mode==='wrong-unit')proof.process.unit='agentos-run-'+f.priorRun;
    if(mode==='wrong-workspace')proof.workspace+='-other';
    return proof;
  });
  expect(await detectSupervisionCases(inspect)).toEqual([]);
  expect(inspect).toHaveBeenCalledTimes(1);expect(await openCases(f)).toEqual([]);
  expect(await historical(f)).toEqual(before);
});

it.each(['cancel','context','workspace','continuation','fence','pending','new-actor'])
('revalidates %s changes made while remote inspection is in flight',async mode=>{
  const f=await fixture();
  const inspect=vi.fn(async()=>{
    if(mode==='cancel')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
    if(mode==='context')await pool.query('UPDATE runs SET context_id=NULL WHERE id=$1',[f.run]);
    if(mode==='workspace')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}',to_jsonb($2::text)) WHERE id=$1",[f.task,f.workspace+'-changed']);
    if(mode==='continuation')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{continuation,state}','\"waiting_slot\"') WHERE id=$1",[f.task]);
    if(mode==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.run,randomUUID()]);
    if(mode==='pending')await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
      VALUES($1,$2,$3,$4,'waiting_evidence','Authoritative evidence wait','[]')`,[randomUUID(),f.goal,f.task,f.run]);
    if(mode==='new-actor'){
      await pool.query('UPDATE tasks SET attempt_count=78 WHERE id=$1',[f.task]);
      await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
        VALUES($1,$2,78,$3,'harness',1,'running')`,[randomUUID(),f.task,randomUUID()]);
    }
    return stopped(f);
  });
  await detectSupervisionCases(inspect);
  expect(inspect).toHaveBeenCalledTimes(1);
  expect((await pool.query("SELECT payload FROM events WHERE kind='SUPERVISION_REQUESTED' AND entity_id=$1",[f.task])).rows).toEqual([]);
});

it('does not reopen an already resolved case for the same failed run',async()=>{
  const f=await fixture();
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
    VALUES($1,$2,$3,$4,'resolved','Already handled current run','[]')`,[randomUUID(),f.goal,f.task,f.run]);
  const inspect=vi.fn(async()=>stopped(f));
  expect(await detectSupervisionCases(inspect)).toEqual([]);expect(inspect).not.toHaveBeenCalled();
});
