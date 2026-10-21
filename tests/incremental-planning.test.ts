import { beforeAll,beforeEach,afterEach,afterAll,it,expect,vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool,migrate,tx,lock } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { config } from '../server/config.js';
import { accountBridge } from '../server/bridge.js';
import { objectiveSchema,validatePlan } from '../server/objective-contracts.js';
import { compileDelivery,incrementalEligible,verifiedBatch } from '../server/incremental-planning.js';
import { submitObjective,claimPlanning,applyPlannedResult,reconcilePlanning,PlanningEngine } from '../server/planner.js';
import { refreshGoal } from '../server/service.js';

const request = () => objectiveSchema.parse({message:'Corrigir dois defeitos independentes e verificar a candidata acumulada.',project:'test',
  workspaces:['test/base'],mode:'execute',planningStrategy:'incremental',requirements:['O endpoint deve persistir a alteração correta.','A interface deve exibir o progresso correto.'],idempotencyKey:randomUUID()});
const decision = (requirementId='req-1') => ({requirementId,title:'Corrigir persistência',action:'Reproduzir a falha de persistência e corrigir sua causa com teste discriminante.',
  verification:'Executar regressão que falha na base e passa na candidata, preservando os demais controles.',agentId:'engineering.backend.node'});
beforeAll(()=>migrate());
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'workforce:%'");
  await seedRegistry();
  await pool.query(`UPDATE accounts SET provider='codex',model='test-model',models='["test-model"]',bridge_id='test-account',max_concurrency=4,allow_unknown=true`);
  config.bridgeHost='fixture';config.remoteExecution=true;
});
afterEach(()=>{vi.restoreAllMocks();config.bridgeHost='';config.remoteExecution=false;});
afterAll(()=>pool.end());

// Explicitly synthetic acceptance fixtures: these tests validate controller
// transitions, never count toward the externally measured autonomy cohort.
async function acceptBatch(goalId:string,revision:number,candidate=true){
  for(const t of (await pool.query('SELECT id FROM tasks WHERE goal_id=$1 AND plan_revision=$2',[goalId,revision])).rows){
    const id=randomUUID();
    await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)`,[id,t.id,randomUUID()]);
    await pool.query("UPDATE tasks SET state='accepted',attempt_count=1 WHERE id=$1",[t.id]);
    await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test-fixture')",[randomUUID(),t.id,id]);
    if(candidate)await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'candidate.json','application/json',$3,2)",[randomUUID(),id,'a'.repeat(64)]);
  }
}
async function start(){
  const input=request();const {objectiveId}=await submitObjective(input);const run=await claimPlanning();
  const plan=compileDelivery(decision(),input);await applyPlannedResult(run,plan);return {input,objectiveId,plan,run};
}
it('compiles two concrete stages while preserving every pending requirement',async()=>{
  const input=request(),plan=compileDelivery(decision(),input);
  validatePlan(plan,input,(await pool.query('SELECT * FROM agents')).rows,[]);
  expect(plan.tasks.map(t=>t.stage)).toEqual(['work','verify']);
  expect(plan.incremental).toEqual({version:1,selectedRequirementId:'req-1',completedRequirementIds:[],pendingRequirementIds:['req-2']});
  expect(plan.requirements.map(r=>r.description)).toEqual(input.requirements);
  expect(()=>compileDelivery({...decision(),requirementId:'invented'},input)).toThrow();
  expect(()=>compileDelivery({...decision(),accountId:'other'},input)).toThrow();
  expect(incrementalEligible({...input,mode:'plan'},undefined,true)).toBe(false);
  expect(incrementalEligible({...input,workspaces:['test/base','test/other']},undefined,true)).toBe(false);
});
it('does not turn an accepted partial lot into a completed objective',async()=>{
  const {objectiveId}=await start();await acceptBatch(objectiveId,1);
  await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('active');
});
it('queues exactly one successor across repeated/concurrent reconciliation and carries its verified source',async()=>{
  const {objectiveId}=await start();await acceptBatch(objectiveId,1);
  await Promise.all(Array.from({length:4},()=>reconcilePlanning()));
  const queued=(await pool.query("SELECT * FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rows;
  expect(queued).toHaveLength(1);
  expect(queued[0].feedback.sourceOverrides['test/base']).toContain('r1-deliver');
  expect((await pool.query('SELECT count(*) FROM tasks WHERE goal_id=$1',[objectiveId])).rows[0].count).toBe('2');
});
it('will not advance from a model claim without acceptance or from missing candidate evidence',async()=>{
  const {objectiveId}=await start();
  await pool.query("UPDATE tasks SET state='accepted' WHERE goal_id=$1",[objectiveId]);
  await reconcilePlanning();expect(await verifiedBatch(pool,objectiveId,1)).toBeNull();
  await acceptBatch(objectiveId,1,false);await reconcilePlanning();
  expect((await pool.query("SELECT 1 FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rowCount).toBe(0);
});
it('rechecks acceptance before applying a successor and rejects forged completed coverage',async()=>{
  const {objectiveId,input,plan}=await start();await acceptBatch(objectiveId,1);await reconcilePlanning();
  const run=await claimPlanning();const next=compileDelivery(decision('req-2'),input,plan,true);
  await pool.query('UPDATE acceptances SET passed=false WHERE task_id IN(SELECT id FROM tasks WHERE goal_id=$1)',[objectiveId]);
  await expect(applyPlannedResult(run,next)).rejects.toMatchObject({code:'STALE_PLAN'});
  await pool.query('UPDATE acceptances SET passed=true WHERE task_id IN(SELECT id FROM tasks WHERE goal_id=$1)',[objectiveId]);
  await applyPlannedResult(run,next);
  const final=(await pool.query('SELECT covers FROM tasks WHERE goal_id=$1 AND plan_revision=2 AND stage=\'verify\'',[objectiveId])).rows[0];
  expect(final.covers).toEqual(['req-1','req-2']);
  await acceptBatch(objectiveId,2);
  await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('completed');
});
it('rejects an initial plan that claims previous deliveries without proof',async()=>{
  const input=request();await submitObjective(input);const run=await claimPlanning();
  const plan=compileDelivery(decision(),input);
  plan.incremental!.pendingRequirementIds=[];plan.incremental!.completedRequirementIds=['req-2'];plan.tasks[1].covers=['req-1','req-2'];
  await expect(applyPlannedResult(run,plan)).rejects.toMatchObject({code:'PLAN_INVALID'});
});
it('resumes a fenced planning claim after executor loss without applying its stale response',async()=>{
  const input=request();await submitObjective(input);const old=await claimPlanning();
  await pool.query("UPDATE planning_runs SET heartbeat_at=now()-interval '2 minutes' WHERE id=$1",[old.id]);
  await reconcilePlanning();const current=await claimPlanning();
  await expect(applyPlannedResult(old,compileDelivery(decision(),input))).rejects.toMatchObject({code:'STALE_PLAN'});
  await applyPlannedResult(current,compileDelivery(decision(),input));
  expect((await pool.query('SELECT count(*) FROM tasks')).rows[0].count).toBe('2');
});
it('recovers a confirmed timeout with the small contract and reuses persisted output during preparation',async()=>{
  let generated=0,prepared=0;
  vi.spyOn(accountBridge,'request').mockImplementation(async(method,p:any)=>{
    if(method==='memory.search')return {items:[]};
    if(method==='worker.workspaces')return [{id:'test/base'}];
    if(method==='generate'){
      expect(p.maxOutputTokens).toBe(2200);expect(JSON.parse(p.messages[0].content)).not.toHaveProperty('previousPlan');
      if(++generated===1)throw Object.assign(Error('confirmed'),{code:'PROVIDER_TIMEOUT'});
      return {message:{content:[{type:'text',text:JSON.stringify(decision())}]}};
    }
    if(method==='worker.prepare'){if(++prepared===1)throw Object.assign(Error('busy'),{code:'SOURCE_BUSY'});return {};}
    throw Error('ranking fallback');
  });
  await submitObjective(request());const engine=new PlanningEngine();
  for(let i=0;i<3;i++){await engine.execute(await claimPlanning(),new AbortController().signal);await pool.query('UPDATE planning_runs SET next_attempt_at=now()');}
  expect(generated).toBe(2);expect(prepared).toBe(2);expect((await pool.query('SELECT count(*) FROM tasks')).rows[0].count).toBe('2');
});
it('does not dispatch a truncated JSON fragment or retry an uncertain termination',async()=>{
  vi.spyOn(accountBridge,'request').mockImplementation(async(method)=>{
    if(method==='memory.search')return {items:[]};if(method==='worker.workspaces')return [{id:'test/base'}];
    if(method==='generate')return {message:{content:[{type:'text',text:'{"requirementId":"req-1"'}]}};
    throw Error('ranking fallback');
  });
  await submitObjective(request());const engine=new PlanningEngine();await engine.execute(await claimPlanning(),new AbortController().signal);
  expect((await pool.query('SELECT count(*) FROM tasks')).rows[0].count).toBe('0');
  await pool.query('UPDATE planning_runs SET next_attempt_at=now()');
  vi.mocked(accountBridge.request).mockRejectedValue(Object.assign(Error('uncertain'),{code:'STOP_UNCONFIRMED'}));
  await engine.execute(await claimPlanning(),new AbortController().signal);
  expect((await pool.query('SELECT state FROM planning_runs')).rows[0].state).toBe('failed');
});
it('honors paused goals between lots',async()=>{
  const {objectiveId}=await start();await acceptBatch(objectiveId,1);await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[objectiveId]);
  await reconcilePlanning();expect(await claimPlanning()).toBeNull();
});
