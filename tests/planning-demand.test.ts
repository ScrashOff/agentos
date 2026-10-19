import { beforeAll,beforeEach,afterAll,afterEach,it,expect,vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool,migrate,tx,lock,event } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installBacklogPlanner,applyBacklog } from '../server/backlog.js';
import { installManagement } from '../server/management.js';
import {plannerCatalogInputVersion} from '../scripts/planner-catalog-transport.mjs';
import { installPlanningDemand,createPlanningDemands,assignPlanningRequest,retryRejectedScopedPlan,refreshScopedPlanningFeedback,recoverScopedPlannerContexts,scopeSourceRevision,scopeSnapshot,planningSourceFence,rejectScopedPlanningResult,observeRunningScopedPlanner,readScopedPlannerCapacity } from '../server/planning-demand.js';
import { installSupervision } from '../server/supervision.js';
import { scopedPlannerContextVersion } from '../server/scoped-planner-context.js';
import { acceptedSnapshotProof,applyDeferredHandoff } from '../server/deferred-handoffs.js';
import { WorkspacePreparationWorker,requestWorkspacePreparation } from '../server/workspace-preparation.js';
import { scopedSourceRevision,validateScopedPlan,scopedPlanningRejection } from '../server/scoped-plan-contract.js';
import {applyPlanningDependencyChange} from '../server/planning-dependencies.js';
import { accountBridge } from '../server/bridge.js';
import {supervisionInputs} from '../server/supervision-context.js';
import {adoptRecoveredArtifacts} from '../server/artifact-recovery.js';
import {recoverMissingOpenCodeArtifacts} from '../server/missing-artifacts.js';
import {hash} from '../server/memory.js';
import { completeRun,writeArtifact,validateTask,schedule,failRun } from '../server/service.js';
import { config } from '../server/config.js';
import {recordDeliveryTransition,reconcileDeliveryTransitions} from '../server/delivery-transitions.js';
import {retireSupersededPlanningTasks} from '../server/planning-retirement.js';
import {readWorkforceHealth} from '../server/workforce-health.js';
import {noWorkDecisionUnchanged,planningDecisionMateriality,reconcileNoWorkBlockers,readLatestPlanningDecision} from '../server/planning-no-work.js';
import {recordScopedPlanningEffects} from '../server/planning-remainders.js';
import {createPlanningDemands as createRuntimePlanningDemands,reconcilePlanningDemand as reconcileRuntimePlanningDemand} from '../server/planning-demand-runtime.js';

beforeAll(async()=>{await migrate();await installBacklogPlanner();await installManagement();await installPlanningDemand();await installSupervision();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterEach(()=>vi.restoreAllMocks());afterAll(async()=>{await pool.end();});
async function fixture(scopes=3,policy:Record<string,unknown>={}){
  await pool.query("DELETE FROM settings WHERE id='autonomy:test'");
  const goal=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Generic delivery','Keep real criteria','test',$2)",[goal,'goal-'+goal]);
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('glm','GLM','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,12,true,'glm')`);
  await pool.query('INSERT INTO settings VALUES($1,$2)', ['workforce:'+goal,JSON.stringify({enabled:true,glmAccountId:'glm',glmTarget:12,
    glmRequestLimit:6,museTarget:6,workerAccounts:[],coordinatorAccountId:'sol',coordinatorModel:'gpt-6.1-sol',...policy})]);
  const entries=[];
  for(let i=0;i<scopes;i++){
    const source=randomUUID(),run=randomUUID(),fence=randomUUID(),consumer=randomUUID();
    const scope='test/unit-'+i;
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count,execution)
      VALUES($1,$2,$3,'Accepted snapshot','Source','qa.backend','running','work','[]',1,$4)`,
      [source,goal,'source-'+i,JSON.stringify({kind:'opencode',baseWorkspace:scope,workspace:'test/source-'+i,workKind:'diagnostic'})]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,source,fence]);
    await writeArtifact(run,fence,'protocol-report.json',JSON.stringify({suiteExecuted:true,exitCode:1,command:'node test.mjs',cases:['real fault'],limitations:[]}));
    await completeRun(run,fence,'Source evidence',{});await validateTask(source);
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
      VALUES($1,$2,$3,'Deliver unit','Implement missing requirement on the accepted immutable source','engineering.integration','recovery_blocked','integrate',$4,$5)`,
      [consumer,goal,'consumer-'+i,JSON.stringify([{id:'passed',label:'Actual acceptance',type:'json_field',name:'verification.json',field:'passed',equals:true}]),
        JSON.stringify({kind:'harness',baseWorkspace:scope,workspace:'test/current-'+i})]);
    entries.push({source,run,fence,consumer,scope});
  }
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  vi.spyOn(accountBridge,'request').mockResolvedValue({});return{goal,entries};
}
async function preparePending(){
  await pool.query("UPDATE workspace_preparations SET state='prepared',finished_at=now()");
}
async function acceptedManager(f:any){
  const id=randomUUID(),run=randomUUID(),fence=randomUUID();
  for(const kind of ['management','supervision'])await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    [kind+':'+f.goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Manager decision','MANAGEMENT_V1','research.requirements','running','coordinate','[]',1)`,[id,f.goal,'manager-'+id]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[run,id,fence]);
  await completeRun(run,fence,'Accepted strategy decision',{});await validateTask(id);
  return run;
}
const strategy=(scope:string,sourceRevision:string)=>({mode:'plan',scope,sourceRevision,strategyKey:'verify-patch-consumption',
  cause:'Compatible patches were accepted but their consumption is not yet proven.',
  hypothesis:'A version-bound composition receipt exposes only the actual conflicting preimages.',expectedArtifact:'runtime-composition.json'});

async function publishNoWork(f:Awaited<ReturnType<typeof fixture>>,blockedByTaskIds:string[]){
  await createRuntimePlanningDemands(f.goal);
  const request=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1',[f.goal])).rows[0];
  const plan={version:1 as const,requestId:request.id,sourceRevision:request.source_revision,
    reason:'Preserve the existing consumers while their exact prerequisites remain owned.',tasks:[],actions:[],dependencyChanges:[],
    blockers:[{taskId:f.entries[0].consumer,criterionId:'passed',blockedByTaskIds,
      artifactIds:[request.snapshot.artifacts[0].id],reason:'The existing delivery requires completion and consumption of the referenced evidence.',
      nextAction:'Observe the referenced prerequisites and reconcile their existing consumer without replacing its criteria.'}]};
  validateScopedPlan(plan,request);
  const task=randomUUID(),run=randomUUID(),fence=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count,execution)
    VALUES($1,$2,$3,'Scoped plan','Publish exact frozen no-work blockers','qa.backend','running','work',
      '[{"id":"plan","type":"artifact","name":"scope-plan.json","label":"Plan artifact"}]',1,$4)`,
    [task,f.goal,'plan-'+task,JSON.stringify({kind:'opencode',workKind:'planning',baseWorkspace:request.scope,
      workspace:'test/planner-'+task,planningRequestId:request.id})]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,task,fence]);
  await pool.query("UPDATE planning_requests SET task_id=$2,state='assigned' WHERE id=$1",[request.id,task]);
  const artifact=await writeArtifact(run,fence,'scope-plan.json',JSON.stringify(plan));
  await completeRun(run,fence,'Evidence-backed no work in the frozen catalog',{});
  expect((await validateTask(task)).passed).toBe(true);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
  await applyBacklog(f.goal,run,{reason:plan.reason,tasks:[]});
  const backlog=(await pool.query('SELECT state,receipts,task_ids FROM workforce_backlog_plans WHERE run_id=$1',[run])).rows[0];
  return {request:{...request,task_id:task,state:'assigned',run_id:run,fence},artifact,plan,
    receipt:{code:'EVIDENCED_NO_WORK',evaluatedRunId:run,blockers:plan.blockers,backlog,dependencyReceipts:[],deliveryActionReceipts:[]}};
}

it('reconciles diagnoses accepted during a frozen no-work plan without claiming consumption or hiding remaining dependencies',async()=>{
  const f=await fixture(1,{museTarget:0}),entry=f.entries[0];
  const diagnoses=[] as Array<{task:string;run:string;fence:string}>;
  for(let index=0;index<2;index++){
    const task=await planningCandidate(f.goal,'wild-diagnosis-'+index,'diagnostic'),run=randomUUID(),fence=randomUUID();
    await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}',to_jsonb($2::text)),state='running',attempt_count=1 WHERE id=$1",[task,entry.scope]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,task,fence]);
    diagnoses.push({task,run,fence});
  }
  const pending=await planningCandidate(f.goal,'still-missing-repair','repair');
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}',to_jsonb($2::text)) WHERE id=$1",[pending,entry.scope]);
  for(const task of [...diagnoses.map(d=>d.task),pending])await pool.query('INSERT INTO dependencies VALUES($1,$2)',[entry.consumer,task]);
  const p=await publishNoWork(f,[...diagnoses.map(d=>d.task),pending]);
  for(const diagnosis of diagnoses){
    await writeArtifact(diagnosis.run,diagnosis.fence,'diagnosis.json',JSON.stringify({source:diagnosis.task,finding:'Discriminating evidence'}));
    await completeRun(diagnosis.run,diagnosis.fence,'Accepted diagnostic findings',{});
    expect((await validateTask(diagnosis.task)).passed).toBe(true);
  }
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
  expect(await recordScopedPlanningEffects(p.request,p.artifact,p.plan,p.receipt,'blocked_external')).toBe(true);
  const recorded=(await pool.query('SELECT * FROM planning_requests WHERE id=$1',[p.request.id])).rows[0];
  expect(recorded.snapshot).toEqual(p.request.snapshot);
  expect(recorded.receipt.blockers).toEqual(p.plan.blockers);
  const reconciled=recorded.receipt.noWorkReconciliation.entries[0];
  expect(reconciled.acceptedTaskIds.sort()).toEqual(diagnoses.map(d=>d.task).sort());
  expect(reconciled.pendingTaskIds).toEqual([pending]);
  expect(reconciled.pendingDependencyTaskIds).toEqual([pending]);
  expect(reconciled).toMatchObject({code:'BLOCKER_PREREQUISITES_PENDING',consumerReadyClaimed:false,criterionAccepted:false});
  // The accepted bytes were not read by the old frozen plan: preserve one new
  // semantic decision, now carrying exact reconciled waits instead of hiding it.
  const created=(await Promise.all([createRuntimePlanningDemands(f.goal),createRuntimePlanningDemands(f.goal)])).flat();
  expect(created).toHaveLength(1);
  const next=(await pool.query('SELECT snapshot FROM planning_requests WHERE id=$1',[created[0]])).rows[0].snapshot;
  expect(next.previousNoWork).toMatchObject({requestId:p.request.id,reconciliation:{authority:'observation_only'}});
  expect(next.previousNoWork.reconciliation.entries[0].acceptedTaskIds.sort()).toEqual(diagnoses.map(d=>d.task).sort());
  expect(await createRuntimePlanningDemands(f.goal)).toEqual([]);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[entry.consumer])).rows[0].state).toBe('recovery_blocked');
});

it('coalesces staged handoff bookkeeping after accepted no-work while preserving the source fence and genuine conflicts',async()=>{
  const f=await fixture(1,{museTarget:0}),entry=f.entries[0],handoff=randomUUID();
  await pool.query(`INSERT INTO deferred_handoffs(id,goal_id,producer_task_id,consumer_task_id,source_task_id,source_proof,consumer_proof,handoff,state)
    VALUES($1,$2,$3,$4,$3,'{}','{}','{}','waiting_result')`,[handoff,f.goal,entry.source,entry.consumer]);
  const p=await publishNoWork(f,[entry.source]);
  expect(await recordScopedPlanningEffects(p.request,p.artifact,p.plan,p.receipt,'blocked_external')).toBe(true);
  expect((await readLatestPlanningDecision(pool,f.goal,entry.scope)).accepted_plan).toBe(true);
  const beforeFence=await planningSourceFence(pool,f.goal,entry.scope);
  await pool.query("UPDATE deferred_handoffs SET state='staged' WHERE id=$1",[handoff]);
  expect(await scopeSourceRevision(pool,f.goal,entry.scope)).not.toBe(p.request.source_revision);
  expect(await planningSourceFence(pool,f.goal,entry.scope)).not.toBe(beforeFence);
  expect(await createRuntimePlanningDemands(f.goal)).toEqual([]);
  const noCatalog=vi.fn(async()=>{throw Error('PARKED_DECISION_REREAD_CATALOG');});
  expect(await createRuntimePlanningDemands(f.goal,noCatalog)).toEqual([]);
  expect(noCatalog).not.toHaveBeenCalled();
  const phases:string[]=[];
  await reconcileRuntimePlanningDemand(phase=>phases.push(phase.name));
  expect(phases).toContain('create_demands');
  expect((await pool.query('SELECT id FROM planning_requests WHERE goal_id=$1',[f.goal])).rows.map(row=>row.id)).toEqual([p.request.id]);
  expect((await pool.query("SELECT id FROM events WHERE kind='SCOPED_NO_WORK_RECONCILED' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
  expect((await pool.query('SELECT receipt FROM planning_requests WHERE id=$1',[p.request.id])).rows[0].receipt.noWorkReconciliation.entries[0])
    .toMatchObject({code:'BLOCKER_COMPLETION_OBSERVED_CONSUMPTION_UNVERIFIED',consumerReadyClaimed:false,criterionAccepted:false});
  await pool.query(`UPDATE deferred_handoffs SET receipt='{"code":"SOURCE_CONFLICT"}' WHERE id=$1`,[handoff]);
  expect(await createRuntimePlanningDemands(f.goal)).toHaveLength(1);
});

it('never suppresses a new decision from a revoked no-work acceptance',async()=>{
  const f=await fixture(1,{museTarget:0}),entry=f.entries[0];
  const p=await publishNoWork(f,[entry.source]);
  expect(await recordScopedPlanningEffects(p.request,p.artifact,p.plan,p.receipt,'blocked_external')).toBe(true);
  await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1',[p.request.run_id]);
  const prior=await readLatestPlanningDecision(pool,f.goal,entry.scope);
  expect(prior.accepted_plan).toBe(false);
  expect(noWorkDecisionUnchanged(prior,p.request.snapshot,null)).toBe(false);
});

it('keeps revoked prerequisite acceptance and changed consumer criteria visible as review instead of completion',async()=>{
  const f=await fixture(1),entry=f.entries[0];
  const p=await publishNoWork(f,[entry.source]);
  await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1',[entry.run]);
  const revoked=await reconcileNoWorkBlockers(pool,f.goal,entry.scope,p.plan.blockers);
  expect(revoked.entries[0]).toMatchObject({code:'BLOCKER_SOURCE_REVIEW_REQUIRED',acceptedTaskIds:[],
    reviewTaskIds:[entry.source],consumerReadyClaimed:false,criterionAccepted:false});
  await pool.query("UPDATE tasks SET criteria='[]' WHERE id=$1",[entry.consumer]);
  const changed=await reconcileNoWorkBlockers(pool,f.goal,entry.scope,p.plan.blockers);
  expect(changed.entries[0].code).toBe('BLOCKER_BINDING_CHANGED');
});

it('keeps changed criteria, accepted evidence and new hypothesis owners material while coalescing only known handoff progress',()=>{
  const snapshot={tasks:[{id:'consumer',state:'recovery_blocked',stage:'verify',intent:'Keep the exact criterion',
    criteria:[{id:'passed',equals:true}],dependencies:['diagnosis'],execution:{workspace:'same',baseWorkspace:'test/unit'}},
    {id:'diagnosis',state:'accepted',attempt_count:1,last_run_id:'run',acceptance_passed:true,
      criteria:[],execution:{workKind:'diagnostic',workspace:'source'}}],
    artifacts:[{task_id:'diagnosis',name:'diagnosis.json',digest:'a'.repeat(64)}],
    deferredDeliveries:[{id:'delivery',producer_task_id:'diagnosis',consumer_task_id:'consumer',source_task_id:'source',state:'waiting_result',receipt:{}}]};
  const prior={accepted_plan:true,state:'blocked_external',snapshot,receipt:{code:'EVIDENCED_NO_WORK',blockers:[]}};
  const staged=structuredClone(snapshot);staged.deferredDeliveries[0].state='staged';
  expect(noWorkDecisionUnchanged(prior,staged,null)).toBe(true);
  for(const change of ['criterion','instruction','evidence','dependency','hypothesis','conflict','applied','signal']){
    const next:any=structuredClone(staged);
    if(change==='criterion')next.tasks[0].criteria[0].equals=false;
    if(change==='instruction')next.tasks[0].intent='A newly authorized different condition';
    if(change==='evidence')next.artifacts[0].digest='b'.repeat(64);
    if(change==='dependency')next.tasks[0].dependencies.push('new-source');
    if(change==='hypothesis')next.tasks.push({...next.tasks[1],id:'distinct-hypothesis'});
    if(change==='conflict')next.deferredDeliveries[0].receipt={code:'SOURCE_CONFLICT'};
    if(change==='applied')next.deferredDeliveries[0].state='applied';
    expect(noWorkDecisionUnchanged(prior,next,change==='signal'?'new-failure':null),change).toBe(false);
  }
  expect(planningDecisionMateriality(snapshot)).not.toBe(planningDecisionMateriality(snapshot,'new-failure'));
});

it('allows queue admission while a slow scoped catalog is read and coalesces concurrent demands',async()=>{
  const f=await fixture(1);let enter!:()=>void,release!:()=>void;
  const entered=new Promise<void>(r=>enter=r),held=new Promise<void>(r=>release=r);
  const reader:typeof scopeSnapshot=async(db,...args)=>{
    expect((await db.query(`SELECT count(*)::int n FROM pg_locks WHERE pid=pg_backend_pid()
      AND locktype='advisory' AND objid=4280001 AND granted`)).rows[0].n).toBe(0);
    enter();await held;return scopeSnapshot(db,...args);
  };
  const first=createPlanningDemands(f.goal,reader),second=createPlanningDemands(f.goal,reader);
  try{
    await Promise.race([entered,first.then(()=>{throw Error('CATALOG_NOT_REACHED');})]);
    // Exercise the same PostgreSQL admission lock used by schedule(), without
    // mutating this proposal's sources or launching an actual model.
    await tx(async db=>{await lock(db);await db.query('SELECT 1');});
  }finally{release();await Promise.all([first,second]);}
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1',[f.goal])).rowCount).toBe(1);
});

it.each(['pause','policy','criterion','dependency','artifact','acceptance','handoff'])('rejects a catalog after concurrent %s changes without changing original criteria',async(change)=>{
  const f=await fixture(1),entry=f.entries[0];let enter!:()=>void,release!:()=>void;
  const entered=new Promise<void>(r=>enter=r),held=new Promise<void>(r=>release=r);
  const preparation=createPlanningDemands(f.goal,async(db,...args)=>{enter();await held;return scopeSnapshot(db,...args);});
  try{
    await Promise.race([entered,preparation.then(()=>{throw Error('CATALOG_NOT_REACHED');})]);
    if(change==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
    if(change==='policy')await pool.query("UPDATE settings SET value=jsonb_set(value,'{glmTarget}','10') WHERE id=$1",['workforce:'+f.goal]);
    if(change==='criterion')await pool.query("UPDATE tasks SET criteria=criteria||'{\"id\":\"new\",\"type\":\"artifact\",\"name\":\"new.json\",\"label\":\"New obligation\"}'::jsonb WHERE id=$1",[entry.consumer]);
    if(change==='dependency')await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[entry.consumer,entry.source]);
    if(change==='artifact')await pool.query('UPDATE artifacts SET digest=$2 WHERE run_id=$1',[entry.run,'a'.repeat(64)]);
    if(change==='acceptance')await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1',[entry.run]);
    if(change==='handoff')await pool.query(`INSERT INTO deferred_handoffs(id,goal_id,producer_task_id,consumer_task_id,source_task_id,source_proof,consumer_proof,handoff,state)
      VALUES($1,$2,$3,$4,$3,'{}','{}','{}','staged')`,[randomUUID(),f.goal,entry.source,entry.consumer]);
  }finally{release();await preparation;}
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
});

it('does not invalidate source fences for heartbeat-only updates on a live run',async()=>{
  const f=await fixture(1),entry=f.entries[0],run=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",
    [run,entry.consumer,randomUUID()]);
  const before=await planningSourceFence(pool,f.goal,entry.scope);
  await pool.query('UPDATE runs SET heartbeat_at=clock_timestamp() WHERE id=$1',[run]);
  expect(await planningSourceFence(pool,f.goal,entry.scope)).toBe(before);
});

it('keeps scope revisions tied to current accepted artifacts without reading embedded inputs once per artifact',async()=>{
  const f=await fixture(2),entry=f.entries[0];
  const initial=await scopeSourceRevision(pool,f.goal,entry.scope);
  const base64='A'.repeat(512*1024);
  await pool.query(`UPDATE tasks SET execution=execution||jsonb_build_object('inputs',$2::jsonb) WHERE id=$1`,
    [entry.source,JSON.stringify([{id:randomUUID(),name:'historical-large-input',digest:hash(Buffer.from(base64,'base64')),base64}])]);
  const staleRun=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'opencode',1,'submitted')",
    [staleRun,entry.source,randomUUID()]);
  const first=await scopeSourceRevision(pool,f.goal,entry.scope);
  // A latest-run identity remains a material revision, but an artifact from a
  // noncurrent attempt or another scope must never enter accepted source hashes.
  expect(first).not.toBe(initial);
  for(const [run,name] of [[staleRun,'stale.json'],[f.entries[1].run,'other-scope.json']] as const)
    await pool.query('INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,\'application/json\',$4,1)',
      [randomUUID(),run,name,'f'.repeat(64)]);
  expect(await scopeSourceRevision(pool,f.goal,entry.scope)).toBe(first);
  await pool.query('INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,\'new-current.json\',\'application/json\',$3,1)',
    [randomUUID(),entry.run,'a'.repeat(64)]);
  expect(await scopeSourceRevision(pool,f.goal,entry.scope)).not.toBe(first);
  expect((await pool.query('SELECT execution->\'inputs\' inputs FROM tasks WHERE id=$1',[entry.source])).rows[0].inputs[0].base64.length).toBe(512*1024);
});

it('retains exact scope catalogs using preselected identities without rereading execution for each related artifact or handoff',async()=>{
  const f=await fixture(2),entry=f.entries[0],planner=randomUUID(),staleRun=randomUUID();
  const bytes=Buffer.from('Large immutable source input.'.repeat(30000));
  await pool.query(`UPDATE tasks SET execution=execution||jsonb_build_object('inputs',$2::jsonb,
    'backlogContext',jsonb_build_object('sourceTaskId',$3::text)) WHERE id=$1`,[entry.source,
    JSON.stringify([{id:randomUUID(),name:'source.txt',digest:hash(bytes),base64:bytes.toString('base64')}]),entry.consumer]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution)
    VALUES($1,$2,$3,'Planner history','Frozen planner contract','research.requirements','accepted','[]',$4)`,
    [planner,f.goal,'planner-'+planner,JSON.stringify({kind:'opencode',workKind:'planning',baseWorkspace:entry.scope,workspace:'test/planner'})]);
  await pool.query('INSERT INTO workforce_backlog_items VALUES($1,$2,$3),($1,$4,$5)',
    [f.goal,'covered-source',entry.source,'covered-planner',planner]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,2,$3,'opencode',1,'failed')`,[staleRun,entry.source,randomUUID()]);
  await pool.query(`INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'stale.json','application/json',$3,1)`,
    [randomUUID(),staleRun,'a'.repeat(64)]);
  const handoff=randomUUID();
  await pool.query(`INSERT INTO deferred_handoffs(id,goal_id,producer_task_id,consumer_task_id,source_task_id,source_proof,consumer_proof,handoff,state)
    VALUES($1,$2,$3,$4,$3,'{}','{}','{}','staged')`,[handoff,f.goal,entry.source,entry.consumer]);
  const queries:string[]=[];
  const db:any={query:async(sql:string,args:any[])=>{queries.push(sql);return pool.query(sql,args);}};
  const snapshot=await scopeSnapshot(db,f.goal,entry.scope);
  expect(snapshot.tasks.map(task=>task.id).sort()).toEqual([entry.source,entry.consumer].sort());
  expect(snapshot.tasks.find(task=>task.id===entry.source)).toMatchObject({last_run_id:staleRun,attempt_count:1});
  expect(snapshot.artifacts.every(artifact=>artifact.run_id===entry.run)).toBe(true);
  expect(snapshot.artifacts.map(artifact=>artifact.name)).not.toContain('stale.json');
  expect(snapshot.coverage).toEqual([
    {coverage_key:'covered-planner',id:planner,title:'Planner history',state:'accepted',work_kind:'planning',source_task_id:null},
    {coverage_key:'covered-source',id:entry.source,title:'Accepted snapshot',state:'accepted',work_kind:'diagnostic',source_task_id:entry.consumer},
  ]);
  expect(snapshot.deferredDeliveries.map(item=>item.id)).toEqual([handoff]);
  const related=queries.filter(sql=>sql.includes('FROM unnest(')||sql.includes('SELECT coverage_key,task_id FROM workforce_backlog_items')||sql.includes('h.consumer_proof FROM deferred_handoffs'));
  expect(related).toHaveLength(3);
  expect(related.every(sql=>!sql.includes('execution')&&!sql.includes('JOIN tasks'))).toBe(true);
  expect(snapshot.tasks.every(task=>!Object.hasOwn(task,'coverage_source_task_id'))).toBe(true);
  expect((await pool.query("SELECT execution->'inputs' inputs FROM tasks WHERE id=$1",[entry.source])).rows[0].inputs[0].digest).toBe(hash(bytes));
});
it('binds accepted strategies to one persisted planning demand under concurrent delivery reviews',async()=>{
  const f=await fixture(1),run=await acceptedManager(f),entry=f.entries[0],p=strategy(entry.scope,await scopeSourceRevision(pool,f.goal,entry.scope));
  const receipts=await Promise.all([0,1,2].map(()=>tx(db=>recordDeliveryTransition(db,f.goal,run,p))));
  expect(new Set(receipts.map(r=>r.transitionId)).size).toBe(1);
  expect(receipts[0].code).toBe('STRATEGY_PLANNING_REQUESTED');
  const request=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1',[f.goal])).rows;
  expect(request).toHaveLength(1);expect(request[0].snapshot.strategy).toMatchObject({key:p.strategyKey,reviewRunId:run});
  expect(request[0].source_revision).toBe(p.sourceRevision);
  await reconcileDeliveryTransitions();
  expect((await pool.query('SELECT state FROM delivery_transitions WHERE review_run_id=$1',[run])).rows[0].state).toBe('planning');
});
it('allows a different accepted approach on unchanged source without timer spins or duplicate strategies',async()=>{
  const f=await fixture(1),entry=f.entries[0];await createPlanningDemands(f.goal);
  await pool.query("UPDATE planning_requests SET state='blocked_external' WHERE goal_id=$1",[f.goal]);
  const original=await scopeSourceRevision(pool,f.goal,entry.scope),run=await acceptedManager(f),p=strategy(entry.scope,original);
  expect((await tx(db=>recordDeliveryTransition(db,f.goal,run,p))).code).toBe('STRATEGY_PLANNING_REQUESTED');
  await pool.query("UPDATE planning_requests SET state='blocked_external' WHERE goal_id=$1",[f.goal]);
  expect(await createPlanningDemands(f.goal)).toEqual([]);
  const duplicate=await acceptedManager(f);
  expect((await tx(db=>recordDeliveryTransition(db,f.goal,duplicate,p))).code).toBe('STRATEGY_ALREADY_PLANNED');
  expect((await pool.query('SELECT demand_epoch FROM planning_requests WHERE goal_id=$1 ORDER BY created_at',[f.goal])).rows.map(r=>r.demand_epoch)).toEqual([1,2]);
});
it('refuses stale sources, paused goals and disabled autonomy while preserving the original backlog and worker',async()=>{
  const f=await fixture(1),entry=f.entries[0];const run=await acceptedManager(f);
  const p=strategy(entry.scope,'f'.repeat(64));
  expect((await tx(db=>recordDeliveryTransition(db,f.goal,run,p))).code).toBe('SOURCE_CHANGED');
  const paused=await acceptedManager(f);await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  expect((await tx(db=>recordDeliveryTransition(db,f.goal,paused,p))).code).toBe('TRANSITION_AUTHORITY');
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);const manual=await acceptedManager(f);
  await pool.query("INSERT INTO settings VALUES('autonomy:test','{\"enabled\":false}') ON CONFLICT(id) DO UPDATE SET value=excluded.value");
  expect((await tx(db=>recordDeliveryTransition(db,f.goal,manual,p))).code).toBe('TRANSITION_AUTHORITY');
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[entry.consumer])).rows[0].state).toBe('recovery_blocked');
});
it('tracks a generic document task to a current accepted artifact, never to a heartbeat or rejected/stale evidence',async()=>{
  const f=await fixture(1),review=await acceptedManager(f),task=randomUUID(),run=randomUUID(),fence=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Write report','Provide substantive findings','research.documentation','running','work',$4,1)`,
    [task,f.goal,'doc-'+task,JSON.stringify([{id:'report',label:'Written report',type:'artifact',name:'report.json'}])]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[run,task,fence]);
  const p={mode:'task',taskId:task,expectedAttempt:1,expectedArtifact:'report.json',cause:'A report is required before the objective can advance.',hypothesis:'The isolated research task can provide the exact requested report.'};
  expect((await tx(db=>recordDeliveryTransition(db,f.goal,review,p))).code).toBe('TRANSITION_BOUND');
  await reconcileDeliveryTransitions();expect((await pool.query('SELECT state FROM delivery_transitions WHERE review_run_id=$1',[review])).rows[0].state).toBe('executing');
  await writeArtifact(run,fence,'report.json',JSON.stringify({findings:['Measured result']}));
  await completeRun(run,fence,'Report delivered',{});
  await reconcileDeliveryTransitions();expect((await pool.query('SELECT state FROM delivery_transitions WHERE review_run_id=$1',[review])).rows[0].state).not.toBe('fulfilled');
  await validateTask(task);await reconcileDeliveryTransitions();
  expect((await pool.query('SELECT state,receipt FROM delivery_transitions WHERE review_run_id=$1',[review])).rows[0]).toMatchObject({state:'fulfilled',receipt:{code:'EXPECTED_ARTIFACT_ACCEPTED',runId:run}});
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('active');
});
it('coalesces concurrent demand signals and plans three independent scopes without Sol',async()=>{
  const f=await fixture();
  await Promise.all([createPlanningDemands(f.goal),createPlanningDemands(f.goal),createPlanningDemands(f.goal)]);
  const rows=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1',[f.goal])).rows;
  expect(rows).toHaveLength(3);expect(new Set(rows.map(r=>r.scope)).size).toBe(3);
  expect(new Set(rows.map(r=>r.lane))).toEqual(new Set(['glm','muse']));
  expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE stage='coordinate'")).rows[0].n).toBe(0);
  await Promise.all(rows.map(r=>assignPlanningRequest(r.id)));await preparePending();
  const ids=await Promise.all(rows.map(r=>assignPlanningRequest(r.id)));
  expect(ids.filter(Boolean)).toHaveLength(3);
  expect((await pool.query("SELECT count(*)::int n FROM planning_requests WHERE state='assigned'")).rows[0].n).toBe(3);
  expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE execution->>'workKind'='planning'")).rows[0].n).toBe(3);
  await createPlanningDemands(f.goal);expect((await pool.query('SELECT count(*)::int n FROM planning_requests')).rows[0].n).toBe(3);
});
it('never advertises planning activity as a new source revision',()=>{
  const one={id:'one',state:'accepted',execution:{workKind:'repair'},criteria:[]};
  expect(scopedSourceRevision([one],[])).toBe(scopedSourceRevision([one,{id:'planner',state:'running',execution:{workKind:'planning'}}],[]));
  const retry={id:'worker',state:'failed',attempt_count:1,last_run_id:'one',execution:{workspace:'test/worker'}};
  expect(scopedSourceRevision([retry],[{id:'old',task_id:'worker',name:'report.json',digest:'same'}])).toBe(
    scopedSourceRevision([{...retry,state:'retry_scheduled',attempt_count:9,last_run_id:'nine'}],[{id:'new',task_id:'worker',name:'report.json',digest:'new-failure-log-timestamp'}]));
  const accepted={...retry,state:'accepted',acceptance_passed:true};
  expect(scopedSourceRevision([accepted],[{task_id:'worker',name:'repair-report.json',digest:'original-source'}])).not.toBe(
    scopedSourceRevision([accepted],[{task_id:'worker',name:'repair-report.json',digest:'changed-source'}]));
});

it('keeps metadata revision checks identical to the frozen catalogs and detects material source changes',async()=>{
 const f=await fixture(2);await createPlanningDemands(f.goal);
 const requests=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1 ORDER BY scope',[f.goal])).rows;
 for(const request of requests)expect(await scopeSourceRevision(pool,f.goal,request.scope))
  .toBe(scopedSourceRevision(request.snapshot.tasks,request.snapshot.artifacts,request.snapshot.deferredDeliveries));
 const original=requests[0].source_revision,entry=f.entries[0];
 await pool.query('UPDATE tasks SET description=$2 WHERE id=$1',[entry.consumer,'Long current observation '.repeat(10000)]);
 expect(await scopeSourceRevision(pool,f.goal,entry.scope)).toBe(original);
 await pool.query("UPDATE tasks SET covers='[\"new-requirement\"]' WHERE id=$1",[entry.consumer]);
 expect(await scopeSourceRevision(pool,f.goal,entry.scope)).not.toBe(original);
 await pool.query("UPDATE tasks SET covers='[]' WHERE id=$1",[entry.consumer]);
 expect(await scopeSourceRevision(pool,f.goal,entry.scope)).toBe(original);
 await pool.query("UPDATE artifacts SET digest=repeat('a',64) WHERE run_id=$1",[entry.run]);
 expect(await scopeSourceRevision(pool,f.goal,entry.scope)).not.toBe(original);
});

it('does not create another demand for observation-only changes but retains changed criteria as a new revision',async()=>{
 const f=await fixture(1);await createPlanningDemands(f.goal);
 await pool.query("UPDATE planning_requests SET state='blocked_external' WHERE goal_id=$1",[f.goal]);
 await pool.query('UPDATE tasks SET reason=$2,description=$2 WHERE id=$1',[f.entries[0].consumer,'Same source; new observed log text.']);
 expect(await createPlanningDemands(f.goal)).toEqual([]);
 await pool.query("UPDATE tasks SET criteria=criteria||'[ {\"id\":\"extra\",\"label\":\"New mandatory evidence\",\"type\":\"artifact\",\"name\":\"extra.json\"} ]'::jsonb WHERE id=$1",[f.entries[0].consumer]);
 expect(await createPlanningDemands(f.goal)).toHaveLength(1);
 const requests=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1 ORDER BY created_at',[f.goal])).rows;
 expect(requests).toHaveLength(2);expect(requests[1].source_revision).not.toBe(requests[0].source_revision);
 expect(requests[1].snapshot.tasks.find((task:any)=>task.id===f.entries[0].consumer).criteria).toHaveLength(2);
});

async function fullUsefulReserve(f:Awaited<ReturnType<typeof fixture>>){
 for(let i=0;i<6;i++){
  const id=await planningCandidate(f.goal,'ready-reserve-'+i,'diagnostic');
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}',to_jsonb($2::text)) WHERE id=$1",[id,f.entries[0].scope]);
 }
 const policy=(await pool.query('SELECT value FROM settings WHERE id=$1',['workforce:'+f.goal])).rows[0].value;
 expect((await readWorkforceHealth(pool,f.goal,policy)).glm.admittedReady).toBe(6);
}
async function canonicalRejection(f:Awaited<ReturnType<typeof fixture>>,attempt=1){
 const consumer=f.entries[0].consumer,run=randomUUID(),fence=randomUUID();
 await pool.query("UPDATE tasks SET state='running',attempt_count=$2 WHERE id=$1",[consumer,attempt]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,$3,$4,'harness',1,'running')",[run,consumer,attempt,fence]);
 await completeRun(run,fence,'Candidate still lacks mandatory verification evidence',{});
 expect((await validateTask(consumer)).passed).toBe(false);
 return run;
}

it('wakes one scope for canonical QA rejection with a full ready reserve and coalesces duplicate failure events',async()=>{
 const f=await fixture(1,{museTarget:0});await fullUsefulReserve(f);
 expect(await createPlanningDemands(f.goal)).toEqual([]);
 const originalRevision=await scopeSourceRevision(pool,f.goal,f.entries[0].scope);
 const failedRun=await canonicalRejection(f);
 expect(await scopeSourceRevision(pool,f.goal,f.entries[0].scope)).toBe(originalRevision);
 const created=await Promise.all([createPlanningDemands(f.goal),createPlanningDemands(f.goal)]);
 expect(created.flat()).toHaveLength(1);
 const request=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1',[f.goal])).rows[0];
 expect(request).toMatchObject({reason:'DELIVERY_EVIDENCE_CHANGED',priority:75});
 expect(request.snapshot.demandSignal.events).toContainEqual(expect.objectContaining({kind:'VALIDATION_PENDING',run_id:failedRun}));
 await pool.query("UPDATE planning_requests SET state='blocked_external' WHERE id=$1",[request.id]);
 await validateTask(f.entries[0].consumer);
 await event(pool,'HEARTBEAT',f.entries[0].consumer,{runId:failedRun});
 await pool.query('UPDATE runs SET heartbeat_at=clock_timestamp() WHERE id=$1',[failedRun]);
 expect(await createPlanningDemands(f.goal)).toEqual([]);
 await canonicalRejection(f,2);
 expect(await createPlanningDemands(f.goal)).toEqual([]);
 expect((await pool.query('SELECT count(*)::int n FROM planning_requests WHERE goal_id=$1',[f.goal])).rows[0].n).toBe(1);
});

it('new accepted patch wakes a delivery planner despite a full reserve and leaves original work intact',async()=>{
 const f=await fixture(1,{museTarget:0});await fullUsefulReserve(f);
 await canonicalRejection(f);await createPlanningDemands(f.goal);
 await pool.query("UPDATE planning_requests SET state='blocked_external' WHERE goal_id=$1",[f.goal]);
 const before=(await pool.query('SELECT id,criteria,state FROM tasks WHERE goal_id=$1 ORDER BY id',[f.goal])).rows;
 const patch=await planningCandidate(f.goal,'new-candidate-patch','repair'),run=randomUUID(),fence=randomUUID();
 await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}',to_jsonb($2::text)),state='running',attempt_count=1 WHERE id=$1",[patch,f.entries[0].scope]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,patch,fence]);
 await writeArtifact(run,fence,'implementation.patch','--- a/source.js\n+++ b/source.js\n@@ -1 +1 @@\n-old\n+corrected\n');
 await completeRun(run,fence,'Patch bytes ready for composition',{});await validateTask(patch);
 expect(await createPlanningDemands(f.goal)).toHaveLength(1);
 const latest=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1',[f.goal])).rows[0];
 expect(latest.reason).toBe('DELIVERY_EVIDENCE_CHANGED');
 expect(latest.snapshot.demandSignal.events).toContainEqual(expect.objectContaining({kind:'TASK_ACCEPTED',task_id:patch,run_id:run}));
 expect((await pool.query('SELECT id,criteria,state FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY id',[before.map(task=>task.id)])).rows).toEqual(before);
 await pool.query("UPDATE planning_requests SET state='blocked_external' WHERE id=$1",[latest.id]);
 expect(await createPlanningDemands(f.goal)).toEqual([]);
});

it('does not wake full-reserve planning from unverified events or planner acceptance',async()=>{
 const f=await fixture(1,{museTarget:0});await fullUsefulReserve(f);
 await event(pool,'VALIDATION_PENDING',f.entries[0].consumer,{runId:randomUUID(),checks:[{id:'passed',passed:false}]});
 const planner=await planningCandidate(f.goal,'accepted-planning','planning'),run=randomUUID(),fence=randomUUID();
 await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}',to_jsonb($2::text)),state='running',attempt_count=1 WHERE id=$1",[planner,f.entries[0].scope]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,planner,fence]);
 await completeRun(run,fence,'A plan is not candidate acceptance',{});await validateTask(planner);
 expect(await createPlanningDemands(f.goal)).toEqual([]);
});

it('leaves waiting demand untouched while planners are full and rechecks its source after a slot opens',async()=>{
 const f=await fixture(3,{plannerMaxActive:2});await createPlanningDemands(f.goal);
 const requests=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1 ORDER BY scope',[f.goal])).rows;
 await Promise.all(requests.map(r=>assignPlanningRequest(r.id)));await preparePending();
 const first=await assignPlanningRequest(requests[0].id),second=await assignPlanningRequest(requests[1].id);
 expect(first).toBeTruthy();expect(second).toBeTruthy();
 await pool.query("UPDATE artifacts SET digest=repeat('b',64) WHERE run_id=$1",[f.entries[2].run]);
 expect(await assignPlanningRequest(requests[2].id)).toBeNull();
 expect((await pool.query('SELECT state FROM planning_requests WHERE id=$1',[requests[2].id])).rows[0].state).toBe('pending');
 await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[first]);
 expect(await assignPlanningRequest(requests[2].id)).toBeNull();
 expect((await pool.query('SELECT state,receipt FROM planning_requests WHERE id=$1',[requests[2].id])).rows[0])
   .toMatchObject({state:'superseded',receipt:{code:'SOURCE_CHANGED_BEFORE_PREPARATION'}});
});
it('carries bounded rejection feedback across a changed source without granting old plan references',async()=>{
 const f=await fixture(1),prior=randomUUID(),secret='sk-untrusted-rejected-value';
 await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,receipt)
  VALUES($1,$2,$3,'glm',$4,'superseded','Previous rejected plan','{}',$5)`,[prior,f.goal,f.entries[0].scope,'a'.repeat(64),
    JSON.stringify({code:'PLANNING_CONTRACT_INVALID',issues:[{path:['tasks',0,'title'],code:'too_big',maximum:180},
      {path:['blockers',0,'blockedByTaskIds',0],code:'invalid_format',expected:'uuid',message:secret}],secret})]);
 await createPlanningDemands(f.goal);
 const request=(await pool.query("SELECT id FROM planning_requests WHERE goal_id=$1 AND state='pending'",[f.goal])).rows[0];
 await assignPlanningRequest(request.id);await preparePending();const id=await assignPlanningRequest(request.id);
 const task=(await pool.query('SELECT description FROM tasks WHERE id=$1',[id])).rows[0];
 const snapshot=JSON.parse(task.description.slice(task.description.indexOf('Catálogo do escopo: ')+19));
 expect(snapshot.previousPlanFeedback).toMatchObject({requestId:prior,code:'PLANNING_CONTRACT_INVALID',
   issues:[{path:['tasks',0,'title'],maximum:180},{path:['blockers',0,'blockedByTaskIds',0],expected:'uuid'}]});
 expect(snapshot.contract.fields.title.maxLength).toBe(180);
 expect(task.description).not.toContain(secret);
 expect(snapshot.previousPlanFeedback.authority).toContain('catálogo atual');
});
it('requires evidence for every unfinished criterion in no_work and rejects cross-scope proposals',async()=>{
  const f=await fixture(1);await createPlanningDemands(f.goal);
  const req=(await pool.query('SELECT * FROM planning_requests')).rows[0];
  const plan={version:1,requestId:req.id,sourceRevision:req.source_revision,reason:'The current branch requires one serialized consumer',tasks:[],blockers:[]};
  expect(()=>validateScopedPlan(plan,req)).toThrow('PLANNING_NO_WORK_UNCOVERED');
  const artifact=req.snapshot.artifacts[0].id;
  expect(validateScopedPlan({...plan,blockers:[{taskId:f.entries[0].consumer,criterionId:'passed',
    reason:'Evidence records a missing producer result',nextAction:'Consume the accepted repair in the integrator',blockedByTaskIds:[],artifactIds:[artifact]}]},req).blockers).toHaveLength(1);
  expect(()=>validateScopedPlan({...plan,tasks:[{coverageKey:'cross-scope',title:'Cross scope repair',description:'This proposal should never escape the authorized scope',
    baseWorkspace:'test/other',deliverToTaskId:f.entries[0].consumer,sourceTaskId:f.entries[0].source,workKind:'repair',lane:'glm'}]},req)).toThrow('PLANNING_SCOPE_INVALID');
});
it('identifies the exact proposal using an unaccepted source instead of returning a generic scope failure',async()=>{
 const f=await fixture(1);await createPlanningDemands(f.goal);
 const request=(await pool.query('SELECT * FROM planning_requests')).rows[0];
 const proposal={coverageKey:'exact-source-validation',title:'Independent source check',description:'Inspect the exact accepted source in an isolated copy',
  baseWorkspace:f.entries[0].scope,deliverToTaskId:f.entries[0].consumer,sourceTaskId:f.entries[0].source,workKind:'diagnostic',lane:'glm'};
 const plan={version:1,requestId:request.id,sourceRevision:request.source_revision,reason:'Independent work needs a verified accepted baseline',tasks:[proposal],blockers:[]};
 expect(validateScopedPlan(plan,request).tasks).toHaveLength(1);
 const unaccepted=structuredClone(request);unaccepted.snapshot.tasks.find((t:any)=>t.id===f.entries[0].source).state='recovery_blocked';
 let rejected:any;try{validateScopedPlan(plan,unaccepted);}catch(e){rejected=e;}
 expect(scopedPlanningRejection(rejected)).toMatchObject({code:'PLANNING_SCOPE_INVALID',
  issues:[{path:['tasks',0,'sourceTaskId'],code:'source_not_accepted'}]});
 expect(request.snapshot.tasks.find((t:any)=>t.id===f.entries[0].source).state).toBe('accepted');
});
it('supersedes stale source requests before preparation can monopolize a scope',async()=>{
  const f=await fixture(1);await createPlanningDemands(f.goal);
  const first=(await pool.query('SELECT * FROM planning_requests')).rows[0];
  await pool.query("UPDATE artifacts SET digest=repeat('a',64) WHERE run_id=$1",[f.entries[0].run]);
  expect(await assignPlanningRequest(first.id)).toBeNull();
  expect((await pool.query('SELECT state,receipt FROM planning_requests WHERE id=$1',[first.id])).rows[0])
    .toMatchObject({state:'superseded',receipt:{code:'SOURCE_CHANGED_BEFORE_PREPARATION'}});
  expect((await pool.query('SELECT * FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect(await createPlanningDemands(f.goal)).toHaveLength(1);
  expect((await pool.query("SELECT count(*)::int n FROM planning_requests WHERE goal_id=$1 AND state='pending'",[f.goal])).rows[0].n).toBe(1);
});
it('reports the exact invalid plan field from preserved bytes without echoing values',async()=>{
  const f=await fixture(1);await createPlanningDemands(f.goal);
  const request=(await pool.query('SELECT * FROM planning_requests')).rows[0];
  const secret='sk-sensitive-rejected-value';
  const invalid={version:1,requestId:request.id,sourceRevision:request.source_revision,reason:'Keep all valid independent work unchanged',tasks:[],
    blockers:[{taskId:f.entries[0].consumer,criterionId:'passed',reason:'A missing producer still blocks this exact criterion',
      nextAction:'Recover the original authorized producer bytes',blockedByTaskIds:[],artifactIds:[secret]}]};
  let error:any;try{validateScopedPlan(invalid,request);}catch(e){error=e;}
  const feedback=scopedPlanningRejection(error);
  expect(feedback).toMatchObject({code:'PLANNING_CONTRACT_INVALID',issues:[{path:['blockers',0,'artifactIds',0],expected:'uuid'}]});
  expect(JSON.stringify(feedback)).not.toContain(secret);
  const limits=scopedPlanningRejection({issues:[{path:['tasks',0,'title'],code:'too_big',maximum:180,message:secret}]});
  expect(limits.issues).toEqual([{path:['tasks',0,'title'],code:'too_big',maximum:180}]);
  expect(JSON.stringify(limits)).not.toContain(secret);
  // Replay a real preserved artifact from a stopped run, as the controller does.
  await assignPlanningRequest(request.id);await preparePending();const taskId=await assignPlanningRequest(request.id);
  const run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[taskId]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,taskId,fence]);
  const artifact=await writeArtifact(run,fence,'scope-plan.json',JSON.stringify(invalid));
  await failRun(run,fence,'PLANNING_CONTRACT_INVALID','Plan contract rejected',false);
  request.artifact_id=artifact.id;request.error_code='PLANNING_CONTRACT_INVALID';
  expect(await refreshScopedPlanningFeedback(request)).toMatchObject({...feedback,artifactId:artifact.id});
  expect((await pool.query('SELECT receipt FROM planning_requests WHERE id=$1',[request.id])).rows[0].receipt).toMatchObject(feedback);
});
it('identifies a well-formed artifact ID that is absent from the frozen scope',async()=>{
  const f=await fixture(1);await createPlanningDemands(f.goal);
  const request=(await pool.query('SELECT * FROM planning_requests')).rows[0];
  const plan={version:1,requestId:request.id,sourceRevision:request.source_revision,reason:'Verify the exact immutable source references',tasks:[],
    blockers:[{taskId:f.entries[0].consumer,criterionId:'passed',reason:'The dependent acceptance is still not available',
      nextAction:'Acquire only the original authorized source bytes',blockedByTaskIds:[],artifactIds:[randomUUID()]}]};
  let error:any;try{validateScopedPlan(plan,request);}catch(e){error=e;}
  expect(scopedPlanningRejection(error)).toMatchObject({code:'PLANNING_BLOCKER_UNVERIFIED',
    issues:[{path:['blockers',0,'artifactIds',0],code:'artifact_not_in_snapshot'}]});
});
it.each([true,false])('recovers a stopped planner without a protocol suite and preserves rejection: valid=%s',async(valid)=>{
  const f=await fixture(1);await createPlanningDemands(f.goal);
  const request=(await pool.query('SELECT * FROM planning_requests')).rows[0];
  await assignPlanningRequest(request.id);await preparePending();const taskId=await assignPlanningRequest(request.id);
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[taskId])).rows[0];
  const run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[taskId]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,taskId,fence]);
  await failRun(run,fence,'RESULT_FORMAT','Native final response lost its envelope',false);
  await pool.query("UPDATE tasks SET state='recovering_artifacts' WHERE id=$1",[taskId]);
  const plan={version:1,requestId:request.id,sourceRevision:request.source_revision,reason:'The original source evidence proves this dependency',tasks:[],
    blockers:[{taskId:f.entries[0].consumer,criterionId:'passed',reason:'A current source documents the pending acceptance requirement',
      nextAction:'Compose the accepted repair before final independent validation',blockedByTaskIds:[],
      artifactIds:[valid?request.snapshot.artifacts[0].id:randomUUID()]}]};
  const candidate={candidateDigest:'a'.repeat(64),baseDigest:'b'.repeat(64)};
  const files=[['scope-plan.json',JSON.stringify(plan)],['candidate.json',JSON.stringify(candidate)],['changes.patch','']];
  const delivery={runId:run,workspace:task.execution.workspace,checkedAt:new Date().toISOString(),
    process:{unit:'agentos-run-'+run,stopped:true,mainPid:0,populated:false,activeState:'inactive'},candidate,
    summary:'Exact scope-plan bytes preserved',collectorVersion:'scoped-plan-files-v1',
    artifacts:files.map(([name,content])=>({name,mime:'application/json',digest:hash(content),base64:Buffer.from(content).toString('base64')}))};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,updated_at)
    SELECT gen_random_uuid(),$1,'old-unrecoverable-'||n,'Old noncollectable task','No named output contract',
    'qa.backend','recovery_blocked','work','[]','{"kind":"opencode"}','2020-01-01'::timestamptz FROM generate_series(1,31) n`,[f.goal]);
  const collect=vi.fn().mockResolvedValue(delivery);
  const [result]=await recoverMissingOpenCodeArtifacts(collect);
  expect(collect).toHaveBeenCalledTimes(1);expect(result).toBeTruthy();
  const collected=(await pool.query('SELECT * FROM runs WHERE id=$1',[result.runId])).rows[0];
  expect(collected.checkpoint).toMatchObject({sourceRunId:run,modelInvoked:false});
  expect((await pool.query('SELECT state,error_code FROM runs WHERE id=$1',[run])).rows[0]).toMatchObject({state:'failed',error_code:'RESULT_FORMAT'});
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[taskId])).rows[0].state).toBe(valid?'accepted':'failed');
  expect((await pool.query('SELECT * FROM artifacts WHERE run_id=$1 AND name=$2',[result.runId,'scope-plan.json'])).rowCount).toBe(1);
  expect((await pool.query("SELECT * FROM artifacts WHERE run_id=$1 AND name='runtime-plan-check.json'",[result.runId])).rowCount).toBe(valid?1:0);
  if(!valid){
    expect(collected.error_code).toBe('PLANNING_BLOCKER_UNVERIFIED');
    expect((await pool.query('SELECT state,receipt FROM planning_requests WHERE id=$1',[request.id])).rows[0])
      .toMatchObject({state:'replanning_required',receipt:{issues:[{path:['blockers',0,'artifactIds',0]}]}});
    const proof={runId:result.runId,workspace:task.execution.workspace,checkedAt:new Date().toISOString(),
      process:{unit:'agentos-run-'+run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};
    expect(await retryRejectedScopedPlan(request.id,proof)).toBe(true);
  }
});
it('queues preparations durably without touching a live consumer; applies only after confirmed stop',async()=>{
  const f=await fixture(1),entry=f.entries[0],run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[entry.consumer]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[run,entry.consumer,fence]);
  const proposal={reason:'Independent repair on immutable snapshot',tasks:[{coverageKey:'immutable-repair',title:'Stage one missing repair',
    description:'Reproduce the discriminating failure and deliver an independent verified repair',baseWorkspace:entry.scope,
    deliverToTaskId:entry.consumer,sourceTaskId:entry.source,workKind:'repair',delivery:'deferred'}]};
  expect(await applyBacklog(f.goal,entry.run,proposal,{durablePreparation:true})).toEqual([]);
  expect(accountBridge.request).not.toHaveBeenCalled();await preparePending();
  const [producer]=await applyBacklog(f.goal,entry.run,proposal,{durablePreparation:true});expect(producer).toBeTruthy();
  expect((await pool.query('SELECT state,execution FROM tasks WHERE id=$1',[entry.consumer])).rows[0]).toMatchObject({state:'running'});
  expect((await pool.query('SELECT * FROM dependencies WHERE task_id=$1',[entry.consumer])).rowCount).toBe(0);
  const handoff=(await pool.query('SELECT * FROM deferred_handoffs')).rows[0];
  expect(await applyDeferredHandoff(handoff.id,null,null)).toBe(false);
  await completeRun(run,fence,'Not accepted yet',{});await validateTask(entry.consumer);
  const pRun=randomUUID(),pFence=randomUUID();
  // Isolate handoff concurrency from repair semantic validation (covered by
  // existing backlog tests); this fixture has a real acceptance receipt.
  await pool.query("UPDATE tasks SET criteria='[]',state='running',attempt_count=1 WHERE id=$1",[producer]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[pRun,producer,pFence]);
  await completeRun(pRun,pFence,'Accepted handoff fixture',{});await validateTask(producer);
  const pTask=(await pool.query('SELECT execution FROM tasks WHERE id=$1',[producer])).rows[0];
  const proof=(runId:string,workspace:string)=>({runId,workspace,checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+runId,stopped:true,mainPid:0,populated:false,activeState:'inactive'}});
  await expect(applyDeferredHandoff(handoff.id,proof(run,'test/wrong'),proof(pRun,pTask.execution.workspace))).rejects.toThrow('STOP_UNCONFIRMED');
  expect(await applyDeferredHandoff(handoff.id,proof(run,'test/current-0'),proof(pRun,pTask.execution.workspace))).toBe(true);
  expect(await applyDeferredHandoff(handoff.id,null,null)).toBe(false);
  expect((await pool.query('SELECT * FROM dependencies WHERE task_id=$1 AND depends_on=$2',[entry.consumer,producer])).rowCount).toBe(1);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[entry.consumer])).rows[0].state).not.toBe('accepted');
});
it('retains the immediate consumer-live guard and requires an accepted source for deferred work',async()=>{
  const f=await fixture(1),e=f.entries[0];await pool.query("UPDATE tasks SET state='running' WHERE id=$1",[e.consumer]);
  const p={reason:'Preserve current consumer guard',tasks:[{coverageKey:'safe-guard',title:'Independent source check',description:'Run a discriminating regression for the missing requirement',deliverToTaskId:e.consumer,baseWorkspace:e.scope}]};
  expect(await applyBacklog(f.goal,e.run,p)).toEqual([]);
  expect((await pool.query('SELECT receipts FROM workforce_backlog_plans WHERE run_id=$1',[e.run])).rows[0].receipts[0].code).toBe('CONSUMER_LIVE');
  expect(await applyBacklog(f.goal,e.run,{...p,tasks:p.tasks.map(t=>({...t,delivery:'deferred'}))})).toEqual([]);
  expect((await pool.query('SELECT receipts FROM workforce_backlog_plans WHERE run_id=$1',[e.run])).rows[0].receipts[0].code).toBe('DEFERRED_SOURCE_REQUIRED');
});
it('binds snapshot identity to artifact hashes, source fence and source acceptance',async()=>{
  const f=await fixture(1),e=f.entries[0];const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[e.source])).rows[0];
  const first=await tx(db=>acceptedSnapshotProof(db,task));
  await pool.query("UPDATE artifacts SET digest=repeat('0',64) WHERE run_id=$1",[e.run]);
  const next=await tx(db=>acceptedSnapshotProof(db,task));expect(next.manifestDigest).not.toBe(first.manifestDigest);
  await pool.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1",[e.source]);
  await expect(tx(async db=>acceptedSnapshotProof(db,(await db.query('SELECT * FROM tasks WHERE id=$1',[e.source])).rows[0]))).rejects.toThrow('SOURCE_NOT_CURRENT_ACCEPTED');
});
it('dispatches the next ready native task after completion without a manager review',async()=>{
  const f=await fixture(1),entry=f.entries[0];
  await pool.query("UPDATE settings SET value='{"+ '\"maxRuns\":4,\"browserSlots\":2'+"}' WHERE id='capacity'");
  await pool.query("UPDATE settings SET value='{"+ '\"maxRuns\":4'+"}' WHERE id='remote-capacity'");
  await pool.query("UPDATE accounts SET max_concurrency=1 WHERE id='glm'");
  const p={reason:'Two distinct missing requirements with runnable reserve',tasks:[0,1].map(i=>({
    coverageKey:'refill-'+i,title:'Distinct negative case '+i,description:i===0
      ? 'Reproduce the missing-file branch against the immutable source and capture its actual validation error.'
      : 'Reproduce the truncated-response branch against the immutable source and capture the parser boundary.',
    baseWorkspace:entry.scope,deliverToTaskId:entry.consumer,sourceTaskId:entry.source,delivery:'deferred'}))};
  const ids=await applyBacklog(f.goal,entry.run,p);expect(ids).toHaveLength(2);
  await pool.query("UPDATE tasks SET criteria='[]' WHERE id=ANY($1::uuid[])",[ids]);
  const before={remote:config.remoteExecution,host:config.bridgeHost};config.remoteExecution=true;config.bridgeHost='test';
  try{
    const [first]=await schedule();expect(first).toBeTruthy();
    const r=(await pool.query('SELECT * FROM runs WHERE id=$1',[first])).rows[0];
    await pool.query("UPDATE runs SET state='running' WHERE id=$1",[first]);await pool.query("UPDATE tasks SET state='running' WHERE id=$1",[r.task_id]);
    await completeRun(first,r.fence,'Completed fixture',{});await validateTask(r.task_id);
    const [second]=await schedule();expect(second).toBeTruthy();expect(second).not.toBe(first);
    expect((await pool.query("SELECT count(*)::int n FROM management_reviews WHERE goal_id=$1",[f.goal])).rows[0].n).toBe(0);
  }finally{config.remoteExecution=before.remote;config.bridgeHost=before.host;}
});
it('limits workspace preparation to three owners and survives controller reconstruction',async()=>{
  const f=await fixture(1);for(let i=0;i<5;i++)await requestWorkspacePreparation(f.goal,'test/oprep-'+i,'test/source-0');
  const releases:Array<()=>void>=[];
  vi.mocked(accountBridge.request).mockImplementation(()=>new Promise(resolve=>{releases.push(()=>resolve({}));}));
  const a=new WorkspacePreparationWorker(),b=new WorkspacePreparationWorker();
  await Promise.all([a.tick(),b.tick()]);
  await vi.waitFor(()=>expect(releases).toHaveLength(3));
  await b.reconcileOwners();expect((await pool.query("SELECT count(*)::int n FROM workspace_preparations WHERE state='preparing'")).rows[0].n).toBe(3);
  releases.splice(0).forEach(r=>r());await Promise.all([a.close(),b.close()]);
  expect((await pool.query("SELECT count(*)::int n FROM workspace_preparations WHERE state='prepared'")).rows[0].n).toBe(3);
  const lost=randomUUID();await pool.query("UPDATE workspace_preparations SET state='preparing',owner=$1 WHERE state='pending'",[lost]);
  const recovered=new WorkspacePreparationWorker();await recovered.reconcileOwners();
  expect((await pool.query("SELECT count(*)::int n FROM workspace_preparations WHERE state='retry_scheduled'")).rows[0].n).toBe(2);await recovered.close();
});
it('refills preparation slots until the durable batch is copied without another controller tick',async()=>{
 const f=await fixture(1);for(let i=0;i<7;i++)await requestWorkspacePreparation(f.goal,'test/o-drain-'+i,'test/source-0');
 let active=0,peak=0,calls=0;
 vi.mocked(accountBridge.request).mockImplementation(async()=>{
  active++;calls++;peak=Math.max(peak,active);
  try{await new Promise(resolve=>setTimeout(resolve,20));return{};}finally{active--;}
 });
 const worker=new WorkspacePreparationWorker();
 try{
  await worker.tick();
  await vi.waitFor(async()=>expect((await pool.query("SELECT count(*)::int n FROM workspace_preparations WHERE goal_id=$1 AND state='prepared'",[f.goal])).rows[0].n).toBe(7),{timeout:5000});
  expect(calls).toBe(7);expect(peak).toBeLessThanOrEqual(3);
 }finally{await worker.close();}
});
it('versions auxiliary readiness from an accepted snapshot while preserving final QA dependencies',async()=>{
  const f=await fixture(1),entry=f.entries[0];const diagnostic=randomUUID(),qa=randomUUID();
  for(const [id,kind,stage] of [[diagnostic,'diagnostic','work'],[qa,null,'verify']]){
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
      VALUES($1,$2,$5,'Pending branch','Criterion-preserving readiness','qa.backend','blocked_dependencies',$3,'[]',$4)`,
      [id,f.goal,stage,JSON.stringify({kind:'opencode',workKind:kind,baseWorkspace:entry.scope,workspace:'test/child-'+String(id).slice(0,8)}),'child-'+id]);
    await pool.query('INSERT INTO dependencies VALUES($1,$2)',[id,entry.consumer]);
  }
  await createPlanningDemands(f.goal);const request=(await pool.query('SELECT * FROM planning_requests')).rows[0];
  const change={taskId:diagnostic,expectedAttempt:0,removeDependsOnId:entry.consumer,sourceTaskId:entry.source,
    reason:'The immutable source independently reproduces the observer failure before final integration',artifactIds:[request.snapshot.artifacts[0].id]};
  expect(await applyPlanningDependencyChange(request,change)).toMatchObject({preparing:true});await preparePending();
  expect(await applyPlanningDependencyChange(request,change)).toMatchObject({applied:true});
  expect(await applyPlanningDependencyChange(request,change)).toMatchObject({applied:true,existing:true});
  expect((await pool.query('SELECT depends_on FROM dependencies WHERE task_id=$1',[diagnostic])).rows).toEqual([{depends_on:entry.source}]);
  expect((await pool.query('SELECT depends_on FROM dependencies WHERE task_id=$1',[qa])).rows).toEqual([{depends_on:entry.consumer}]);
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[diagnostic])).rows[0];
  expect(task.criteria).toEqual([]);expect(task.state).toBe('ready');expect(task.execution.workspace).toContain('/o-rs-');
  await expect(applyPlanningDependencyChange(request,{...change,taskId:qa})).rejects.toThrow('AUXILIARY_SOURCE_CHANGED');
});
it('enforces five planning actors in PostgreSQL even for independent resumptions',async()=>{
  const f=await fixture(1),ids=[];
  for(let i=0;i<6;i++){
    const id=randomUUID();ids.push(id);
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,account_id,model,execution)
      VALUES($1,$2,$3,'Scope planner','Plan safely','research.requirements','ready','work','[]','glm','glm-5.3-flash',$4)`,
      [id,f.goal,'p-'+i,JSON.stringify({kind:'opencode',workspace:'test/oplanner-'+i,baseWorkspace:'test/scope-'+i,workKind:'planning'})]);
  }
  const results=await Promise.allSettled(ids.map(id=>pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id)
    VALUES($1,$2,1,$3,'opencode',1,'queued','glm')`,[randomUUID(),id,randomUUID()])));
  expect(results.filter(r=>r.status==='fulfilled')).toHaveLength(5);
  expect(String((results.find(r=>r.status==='rejected') as PromiseRejectedResult).reason)).toContain('SCOPED_PLANNER_CAPACITY');
});
async function planningCandidate(goal:string,key:string,workKind='planning'){
 const id=randomUUID();
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,account_id,model,execution)
  VALUES($1,$2,$3,'Independent useful work','Preserve the actual scope and acceptance','research.requirements','ready','[]','glm','glm-5.3-flash',$4)`,
  [id,goal,key,JSON.stringify({kind:'opencode',workforceLane:'glm',workKind,baseWorkspace:'test/'+key,workspace:'test/run-'+key})]);
 return id;
}
const admitPlannerCandidate=(id:string)=>pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id)
 VALUES($1,$2,1,$3,'opencode',1,'queued','glm') RETURNING id`,[randomUUID(),id,randomUUID()]);

it('reserves at most five independent scopes when controller assignments race',async()=>{
 const f=await fixture(6);await createPlanningDemands(f.goal);
 const requests=(await pool.query('SELECT id FROM planning_requests WHERE goal_id=$1 ORDER BY scope',[f.goal])).rows;
 await Promise.all(requests.map(request=>assignPlanningRequest(request.id)));await preparePending();
 const assigned=await Promise.all(requests.map(request=>assignPlanningRequest(request.id)));
 expect(assigned.filter(Boolean)).toHaveLength(5);
 expect(await readScopedPlannerCapacity(pool,f.goal)).toMatchObject({maxScopePlanners:5,globalMaxScopePlanners:5,
  occupiedScopePlanners:5,occupiedGlobalPlanners:5});
 expect((await pool.query("SELECT count(*)::int n FROM planning_requests WHERE goal_id=$1 AND state='pending'",[f.goal])).rows[0].n).toBe(1);
});

it.each([{plannerMaxActive:2,glmTarget:12},{plannerMaxActive:5,glmTarget:2}])('applies the goal policy and shared pool ceiling together: %o',async(policy)=>{
 const f=await fixture(1,policy),ids=[];
 for(let i=0;i<3;i++)ids.push(await planningCandidate(f.goal,'policy-scope-'+i));
 const results=await Promise.allSettled(ids.map(admitPlannerCandidate));
 expect(results.filter(result=>result.status==='fulfilled')).toHaveLength(2);
 expect(String((results.find(result=>result.status==='rejected') as PromiseRejectedResult).reason)).toContain('SCOPED_PLANNER_CAPACITY');
 expect(await readScopedPlannerCapacity(pool,f.goal)).toMatchObject({maxScopePlanners:2,occupiedScopePlanners:2});
});

it('shares the global planner ceiling across independent objectives',async()=>{
 const f=await fixture(1),second=randomUUID(),ids=[];
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Another objective','Independent authorization','test',$2)",[second,'goal-'+second]);
 await pool.query("INSERT INTO settings(id,value) SELECT $1,value FROM settings WHERE id=$2",['workforce:'+second,'workforce:'+f.goal]);
 for(let i=0;i<6;i++)ids.push(await planningCandidate(i%2?second:f.goal,'global-scope-'+i));
 const results=await Promise.allSettled(ids.map(admitPlannerCandidate));
 expect(results.filter(result=>result.status==='fulfilled')).toHaveLength(5);
 expect(String((results.find(result=>result.status==='rejected') as PromiseRejectedResult).reason)).toContain('SCOPED_PLANNER_CAPACITY');
 expect((await readScopedPlannerCapacity(pool,f.goal)).occupiedGlobalPlanners).toBe(5);
});

it('keeps planners and workers inside the same twelve GLM sessions under concurrent admission',async()=>{
 const f=await fixture(1);
 for(let i=0;i<11;i++)await admitPlannerCandidate(await planningCandidate(f.goal,'worker-'+i,'diagnostic'));
 const planners=await Promise.all([planningCandidate(f.goal,'last-slot-a'),planningCandidate(f.goal,'last-slot-b')]);
 const results=await Promise.allSettled(planners.map(admitPlannerCandidate));
 expect(results.filter(result=>result.status==='fulfilled')).toHaveLength(1);
 expect(String((results.find(result=>result.status==='rejected') as PromiseRejectedResult).reason)).toContain('WORKFORCE_GLM_CAPACITY');
 expect((await pool.query("SELECT count(*)::int n FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 AND r.state='queued'",[f.goal])).rows[0].n).toBe(12);
 expect((await pool.query('SELECT value FROM settings WHERE id=$1',['workforce:'+f.goal])).rows[0].value.glmRequestLimit).toBe(6);
});

it('retains an uncertain actor reservation when its request no longer advertises execution',async()=>{
 const f=await fixture(2,{plannerMaxActive:1});await createPlanningDemands(f.goal);
 const requests=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1 ORDER BY scope',[f.goal])).rows;
 await assignPlanningRequest(requests[0].id);await preparePending();
 const first=await assignPlanningRequest(requests[0].id);expect(first).toBeTruthy();
 const live=(await admitPlannerCandidate(first!)).rows[0];
 await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1",[live.id]);
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision' WHERE id=$1",[requests[0].id]);
 await pool.query("UPDATE tasks SET state='failed' WHERE id=$1",[first]);
 expect(await readScopedPlannerCapacity(pool,f.goal)).toMatchObject({occupiedScopePlanners:1});
 expect(await assignPlanningRequest(requests[1].id)).toBeNull();
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[live.id])).rows[0].state).toBe('reconciling');
});

it('counts only candidate reservations once while retaining live actors with closed task bookkeeping',async()=>{
 const f=await fixture(1),planner=await planningCandidate(f.goal,'capacity-current'),closed=await planningCandidate(f.goal,'capacity-closed');
 for(const [task,scope] of [[planner,'test/capacity-current'],[closed,'test/capacity-closed']])
  await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,task_id)
    VALUES($1,$2,$3,'glm','revision','assigned','test','{}',$4)`,[randomUUID(),f.goal,scope,task]);
 await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[closed]);
 const run=(await admitPlannerCandidate(planner)).rows[0];
 await admitPlannerCandidate(await planningCandidate(f.goal,'ordinary-worker','diagnostic'));
 expect(await readScopedPlannerCapacity(pool,f.goal)).toMatchObject({occupiedScopePlanners:1,occupiedGlobalPlanners:1});
 await pool.query("UPDATE tasks SET state='superseded' WHERE id=$1",[planner]);
 expect((await readScopedPlannerCapacity(pool,f.goal)).occupiedGlobalPlanners).toBe(1);
 await pool.query("UPDATE runs SET state='failed' WHERE id=$1",[run.id]);
 expect((await readScopedPlannerCapacity(pool,f.goal)).occupiedGlobalPlanners).toBe(0);
});

it('a lowered planner policy blocks new admission without stopping existing actors',async()=>{
 const f=await fixture(1),a=await planningCandidate(f.goal,'lower-limit-a'),b=await planningCandidate(f.goal,'lower-limit-b');
 const first=(await admitPlannerCandidate(a)).rows[0],second=(await admitPlannerCandidate(b)).rows[0];
 await pool.query("UPDATE settings SET value=value||'{\"plannerMaxActive\":1}'::jsonb WHERE id=$1",['workforce:'+f.goal]);
 await pool.query("UPDATE runs SET state='running' WHERE id=ANY($1::uuid[])",[[first.id,second.id]]);
 const next=await planningCandidate(f.goal,'lower-limit-next');
 await expect(admitPlannerCandidate(next)).rejects.toThrow('SCOPED_PLANNER_CAPACITY');
 expect((await pool.query("SELECT count(*)::int n FROM runs WHERE id=ANY($1::uuid[]) AND state='running'",[[first.id,second.id]])).rows[0].n).toBe(2);
});

it('preserves full accepted and pending task contracts in the mounted catalog',async()=>{
 const f=await fixture(1),entry=f.entries[0],longContract='Requirement prefix. '+ 'Exact requirement. '.repeat(800)+'Final mandatory condition.';
 await pool.query('UPDATE tasks SET description=$2 WHERE id=ANY($1::uuid[])',[[entry.source,entry.consumer],longContract]);
 const snapshot=await scopeSnapshot(pool,f.goal,entry.scope);
 expect(snapshot.tasks.find(task=>task.id===entry.consumer).intent).toBe(longContract);
 expect(snapshot.tasks.find(task=>task.id===entry.source).intent).toBe(longContract);
});

it('admits one planning actor per objective scope even when two stopped planners resume concurrently',async()=>{
 const f=await fixture(1),scope=f.entries[0].scope,ids=[];
 for(let i=0;i<2;i++){
  const id=randomUUID();ids.push(id);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution)
   VALUES($1,$2,$3,'Resumed scope planner','Preserve scope ownership','research.requirements','ready','[]',$4)`,
   [id,f.goal,'same-scope-'+i,JSON.stringify({kind:'opencode',workKind:'planning',baseWorkspace:scope,workspace:'test/oscope-'+i})]);
 }
 const results=await Promise.allSettled(ids.map(id=>pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
  VALUES($1,$2,1,$3,'opencode',1,'queued')`,[randomUUID(),id,randomUUID()])));
 expect(results.filter(result=>result.status==='fulfilled')).toHaveLength(1);
 expect(String((results.find(result=>result.status==='rejected') as PromiseRejectedResult).reason)).toContain('SCOPED_PLANNER_SCOPE_BUSY');
 expect((await pool.query("SELECT count(*)::int n FROM runs WHERE task_id=ANY($1::uuid[]) AND state='queued'",[ids])).rows[0].n).toBe(1);
});
it('replans rejected output with persisted feedback only after a fresh stop proof',async()=>{
  const f=await fixture(1);await createPlanningDemands(f.goal);
  const request=(await pool.query('SELECT * FROM planning_requests')).rows[0];
  await assignPlanningRequest(request.id);await preparePending();const id=await assignPlanningRequest(request.id);expect(id).toBeTruthy();
  const run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',criteria='[]',attempt_count=1 WHERE id=$1",[id]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,id,fence]);
  await completeRun(run,fence,'Preserved invalid proposal',{});await validateTask(id!);
  await pool.query("UPDATE planning_requests SET state='replanning_required',receipt=$2 WHERE id=$1",[request.id,JSON.stringify({code:'CONSUMER_REVISION_CHANGED'})]);
  await expect(retryRejectedScopedPlan(request.id,null)).rejects.toThrow('STOP_UNCONFIRMED');
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
  const proof={runId:run,workspace:task.execution.workspace,checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};
  expect(await retryRejectedScopedPlan(request.id,proof)).toBe(true);
  expect((await pool.query('SELECT state,execution FROM tasks WHERE id=$1',[id])).rows[0]).toMatchObject({state:'ready',execution:{revisionFeedback:{code:'CONSUMER_REVISION_CHANGED'}}});
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[run])).rows[0].state).toBe('submitted');
  expect(await retryRejectedScopedPlan(request.id,proof)).toBe(false);
});
it('injects an accepted staged repair and prioritizes its consumption without claiming integration',async()=>{
  const f=await fixture(1),e=f.entries[0];
  const [producer]=await applyBacklog(f.goal,e.run,{reason:'An independent missing repair is already available',tasks:[{
    coverageKey:'unconsumed-fix',title:'Compose an existing repair',description:'Deliver verified missing bytes for the consumer without repeating the diagnosis',
    baseWorkspace:e.scope,deliverToTaskId:e.consumer,sourceTaskId:e.source,workKind:'repair',delivery:'deferred'}]});
  const run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET criteria='[]',state='running',attempt_count=1 WHERE id=$1",[producer]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,producer,fence]);
  await writeArtifact(run,fence,'implementation.patch','--- a/module.js\n+++ b/module.js\n@@ -1 +1 @@\n-old\n+fixed\n');
  await writeArtifact(run,fence,'repair-report.json',JSON.stringify({fixVerified:true,sourceHashes:[{path:'module.js',before:'old-hash',after:'fixed-hash'}]}));
  await completeRun(run,fence,'Accepted repair fixture',{});await validateTask(producer);
  await pool.query("UPDATE deferred_handoffs SET state='staged' WHERE producer_task_id=$1",[producer]);
  const original=(await pool.query('SELECT * FROM tasks WHERE id=$1',[e.source])).rows[0];
  const consumer=(await pool.query('SELECT * FROM tasks WHERE id=$1',[e.consumer])).rows[0];
  const context=await supervisionInputs(pool,original,consumer);
  expect(context.provenance).toContainEqual(expect.objectContaining({taskId:producer,role:'accepted_repair',name:'implementation.patch'}));
  expect((await pool.query('SELECT * FROM dependencies WHERE task_id=$1',[e.consumer])).rowCount).toBe(0);
  await createPlanningDemands(f.goal);const request=(await pool.query('SELECT * FROM planning_requests')).rows[0];
  expect(request.snapshot.opportunities.acceptedSources[0].taskId).toBe(producer);
  expect(request.snapshot.deferredDeliveries[0].state).toBe('staged');
  expect(request.snapshot.repairConsumption[0].consumptionStatus).toBe('unverified');
});

it('assigns a large auxiliary catalog without losing mounted instructions or final criteria',async()=>{
  const f=await fixture(1),entry=f.entries[0];
  // Historical parked tasks keep the catalog large without claiming forty
  // runnable slots in a fixture intended to test context, not admission limits.
  for(let i=0;i<40;i++)await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
    VALUES($1,$2,$3,'Existing auxiliary work',$4,'qa.backend','recovery_blocked','work',$5,$6)`,[
      randomUUID(),f.goal,'large-'+i,'Exact original diagnostic instruction '.repeat(250),
      JSON.stringify([{id:'original',type:'artifact',name:'report.json',label:'Original acceptance'}]),
      JSON.stringify({kind:'opencode',workKind:'diagnostic',baseWorkspace:entry.scope,workspace:'test/large-'+i})]);
  await createPlanningDemands(f.goal);const request=(await pool.query('SELECT * FROM planning_requests')).rows[0];
  expect(JSON.stringify(request.snapshot).length).toBeGreaterThan(115000);
  await assignPlanningRequest(request.id);await preparePending();const id=await assignPlanningRequest(request.id);
  expect(id).toBeTruthy();
  const task=(await pool.query('SELECT description,execution FROM tasks WHERE id=$1',[id])).rows[0];
  expect(task.description.length).toBeLessThan(115000);expect(task.description).toContain(scopedPlannerContextVersion);
  const input=task.execution.inputs.find((item:any)=>item.name==='planning-catalog.json');
  const bytes=Buffer.from(input.base64,'base64');expect(hash(bytes)).toBe(input.digest);
  const {provenance,inputManifest,omitted,...mountedSnapshot}=JSON.parse(bytes.toString());
  expect(mountedSnapshot).toEqual(request.snapshot);
  expect(Array.isArray(omitted)).toBe(true);
  for(const mounted of inputManifest){
    const file=task.execution.inputs.find((item:any)=>item.id===mounted.id && item.name===mounted.name);
    expect(file).toBeDefined();
    expect({digest:hash(Buffer.from(file.base64,'base64')),size:Buffer.from(file.base64,'base64').length})
      .toEqual({digest:mounted.digest,size:mounted.size});
  }
  const prompt=JSON.parse(task.description.slice(task.description.indexOf('Catálogo do escopo: ')+19));
  expect(prompt.fullCatalog).toMatchObject({digest:input.digest,provenanceIncluded:true});
  expect(prompt.provenance).toMatchObject({representation:'queryable_directory',recordCount:provenance.length,
    $catalog:{jsonPath:['provenance'],valueSha256:hash(JSON.stringify(provenance))}});
  const reader=task.execution.inputs.find((item:any)=>item.name==='planning-query.mjs');
  expect(reader.id).not.toBe(input.id);expect(hash(Buffer.from(reader.base64,'base64'))).toBe(reader.digest);
  expect(prompt.fullCatalog).toMatchObject({readerPath:'/home/agent/inputs/'+reader.id+'/planning-query.mjs',readerDigest:reader.digest});
  expect(Buffer.from(reader.base64,'base64').toString()).toContain(input.digest);
  const preflight=task.execution.inputs.find((item:any)=>item.name==='planning-preflight.mjs');
  const schema=task.execution.inputs.find((item:any)=>item.name==='scope-plan.schema.json');
  for(const file of [preflight,schema])expect(hash(Buffer.from(file.base64,'base64'))).toBe(file.digest);
  expect(prompt.contract).toMatchObject({catalogDigest:input.digest,cliDigest:preflight.digest,schemaDigest:schema.digest,
    cliPath:'/home/agent/inputs/'+preflight.id+'/planning-preflight.mjs',
    schemaPath:'/home/agent/inputs/'+schema.id+'/scope-plan.schema.json'});
  expect(prompt.contract.commands.check).toEqual(['node',prompt.contract.cliPath,'--check','/workspace/evidence/scope-plan.json']);
  expect(Buffer.from(preflight.base64,'base64').toString()).toContain(input.digest);
  expect(task.description).not.toContain('artifactIds:[]');
  expect(task.description).toContain('execute contract.commands.check');
  expect(mountedSnapshot.tasks.find((t:any)=>t.id===entry.consumer).criteria)
    .toEqual([{id:'passed',label:'Actual acceptance',type:'json_field',name:'verification.json',field:'passed',equals:true}]);
});

it('mounts a catalog larger than 8 MiB as exact bound parts without losing the source contract',async()=>{
 const f=await fixture(1),entry=f.entries[0],intent='Original indivisible source contract '.repeat(250000);
 await pool.query('UPDATE tasks SET description=$2 WHERE id=$1',[entry.consumer,intent]);
 await createPlanningDemands(f.goal);
 const request=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1',[f.goal])).rows[0];
 expect(Buffer.byteLength(JSON.stringify(request.snapshot))).toBeGreaterThan(8*1024*1024);
 await assignPlanningRequest(request.id);await preparePending();const taskId=await assignPlanningRequest(request.id);
 const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[taskId])).rows[0];
 expect(task.description.length).toBeLessThan(115000);
 const inputs=task.execution.inputs,indexInput=inputs.find((i:any)=>i.id===request.id),index=JSON.parse(Buffer.from(indexInput.base64,'base64').toString());
 expect(index.format).toBe(plannerCatalogInputVersion);expect(index.parts.length).toBeGreaterThan(1);
 const bytes=Buffer.concat(index.parts.map((part:any)=>{
  const item=inputs.find((i:any)=>i.id===part.id),value=Buffer.from(item.base64,'base64');
  expect(item.name).toBe(part.name);expect(hash(value)).toBe(part.digest);expect(value.length).toBe(part.size);return value;
 }));
 expect(hash(bytes)).toBe(index.catalogDigest);expect(bytes.length).toBe(index.catalogBytes);
 const original=JSON.parse(bytes.toString());expect(original.tasks.find((t:any)=>t.id===entry.consumer).intent).toBe(intent);
 expect(inputs.every((i:any)=>Buffer.byteLength(i.base64,'base64')<=8*1024*1024)).toBe(true);
 expect(inputs.reduce((n:number,i:any)=>n+Buffer.byteLength(i.base64,'base64'),0)).toBeLessThanOrEqual(24*1024*1024);
 expect(task.description).toContain(index.catalogDigest);expect(task.description).toContain(indexInput.digest);
 const preflight=Buffer.from(inputs.find((i:any)=>i.name==='planning-preflight.mjs').base64,'base64').toString();
 const reader=Buffer.from(inputs.find((i:any)=>i.name==='planning-query.mjs').base64,'base64').toString();
 for(const source of [reader,preflight]){expect(source).toContain(index.catalogDigest);expect(source).toContain(indexInput.digest);}
 expect((await pool.query('SELECT criteria FROM tasks WHERE id=$1',[entry.consumer])).rows[0].criteria)
  .toEqual(original.tasks.find((t:any)=>t.id===entry.consumer).criteria);
});

it('recovers an unassigned legacy input-size failure once per transport while refusing assigned and already-current requests',async()=>{
 const f=await fixture(3);await createPlanningDemands(f.goal);
 const rows=(await pool.query('SELECT id FROM planning_requests WHERE goal_id=$1 ORDER BY scope',[f.goal])).rows;
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision',receipt=$2 WHERE goal_id=$1",[
  f.goal,JSON.stringify({code:'SUPERVISION_INPUTS_TOO_LARGE',contextProjectionVersion:scopedPlannerContextVersion})]);
 await pool.query('UPDATE planning_requests SET task_id=$2 WHERE id=$1',[rows[1].id,f.entries[1].consumer]);
 await pool.query('UPDATE planning_requests SET receipt=receipt||$2::jsonb WHERE id=$1',[rows[2].id,JSON.stringify({catalogInputVersion:plannerCatalogInputVersion})]);
 expect(await recoverScopedPlannerContexts()).toEqual([rows[0].id]);
 expect((await pool.query('SELECT state,receipt FROM planning_requests WHERE id=$1',[rows[0].id])).rows[0])
  .toMatchObject({state:'pending',receipt:{code:'SUPERVISION_INPUTS_TOO_LARGE',catalogInputVersion:plannerCatalogInputVersion}});
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision' WHERE id=$1",[rows[0].id]);
 expect(await recoverScopedPlannerContexts()).toEqual([]);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SCOPED_PLANNING_CONTEXT_RECOVERED' AND entity_id=$1",[f.goal])).rows[0].n).toBe(1);
 expect((await pool.query('SELECT state FROM planning_requests WHERE id=ANY($1::uuid[])',[[rows[1].id,rows[2].id]])).rows.every(r=>r.state==='awaiting_supervision')).toBe(true);
});

it('recovers only unassigned context failures once per projection and supersedes materially stale demands',async()=>{
  const f=await fixture(3);await createPlanningDemands(f.goal);
  const rows=(await pool.query('SELECT * FROM planning_requests ORDER BY scope')).rows;
  await pool.query("UPDATE planning_requests SET state='awaiting_supervision',receipt='{\"code\":\"SCOPED_CONTEXT_TOO_LARGE\",\"contextProjectionVersion\":\"SCOPED_PLANNER_V4\"}'");
  await pool.query("UPDATE artifacts SET digest=repeat('c',64) WHERE run_id=$1",[f.entries[1].run]);
  await pool.query("UPDATE planning_requests SET receipt='{\"code\":\"OTHER_PREPARATION_ERROR\"}' WHERE id=$1",[rows[2].id]);
  const before=(await pool.query('SELECT snapshot FROM planning_requests WHERE id=$1',[rows[0].id])).rows[0].snapshot;
  const strategy={key:'preserve-authorized-strategy',reviewRunId:randomUUID(),sourceRevision:rows[0].source_revision};
  const historicalSnapshot={...before,strategy,tasks:before.tasks.map((task:any)=>({...task,intent:'Previously truncated instructions'}))};
  await pool.query("UPDATE planning_requests SET snapshot=$2,receipt=receipt||'{\"priorDiagnostic\":\"retained\"}'::jsonb WHERE id=$1",[rows[0].id,JSON.stringify(historicalSnapshot)]);
  expect(await recoverScopedPlannerContexts()).toEqual([rows[0].id]);
  const recovered=(await pool.query('SELECT state,receipt,snapshot FROM planning_requests WHERE id=$1',[rows[0].id])).rows[0];
  expect(recovered).toMatchObject({state:'pending',snapshot:{...before,strategy,
    laneAvailability:{...before.laneAvailability,observedAt:expect.any(String)}},
    receipt:{contextProjectionVersion:scopedPlannerContextVersion,priorDiagnostic:'retained'}});
  expect(Date.parse(recovered.snapshot.laneAvailability.observedAt)).toBeGreaterThanOrEqual(Date.parse(before.laneAvailability.observedAt));
  expect(Date.parse(recovered.snapshot.laneAvailability.observedAt)).toBeLessThanOrEqual(Date.now());
  expect((await pool.query('SELECT state FROM planning_requests WHERE id=$1',[rows[1].id])).rows[0].state).toBe('superseded');
  expect((await pool.query('SELECT state FROM planning_requests WHERE id=$1',[rows[2].id])).rows[0].state).toBe('awaiting_supervision');
  await pool.query("UPDATE planning_requests SET state='awaiting_supervision' WHERE id=$1",[rows[0].id]);
  expect(await recoverScopedPlannerContexts()).toEqual([]);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SCOPED_PLANNING_CONTEXT_RECOVERED'")).rows[0].n).toBe(1);
});

it.each(['criterion','pause','request'].flatMap(change=>['SCOPED_CONTEXT_TOO_LARGE','SUPERVISION_INPUTS_TOO_LARGE'].map(code=>({change,code}))))('reads recovery catalogs without blocking admission and refuses a concurrent $change change for $code',async({change,code})=>{
 const f=await fixture(1);await createPlanningDemands(f.goal);
 const request=(await pool.query('SELECT id FROM planning_requests WHERE goal_id=$1',[f.goal])).rows[0];
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision',receipt=$2 WHERE id=$1",[request.id,JSON.stringify({code})]);
 let enter!:()=>void,release!:()=>void;
 const entered=new Promise<void>(resolve=>enter=resolve),held=new Promise<void>(resolve=>release=resolve);
 const recovering=recoverScopedPlannerContexts(async(db,...args)=>{
  expect((await db.query("SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND objid=4280001 AND granted")).rowCount).toBe(0);
  enter();await held;return scopeSnapshot(db,...args);
 });
 try{
  await Promise.race([entered,recovering.then(()=>{throw Error('RECOVERY_CATALOG_NOT_REACHED');})]);
  await tx(async db=>{
   await db.query("SET LOCAL lock_timeout='2s'");await lock(db);
   if(change==='criterion')await db.query("UPDATE tasks SET criteria=criteria||'{\"id\":\"new\",\"type\":\"artifact\",\"name\":\"extra.json\",\"label\":\"New obligation\"}'::jsonb WHERE id=$1",[f.entries[0].consumer]);
   if(change==='pause')await db.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
   if(change==='request')await db.query("UPDATE planning_requests SET receipt=receipt||'{\"supervision\":\"changed\"}'::jsonb WHERE id=$1",[request.id]);
  });
 }finally{release();}
 expect(await recovering).toEqual([]);
 expect((await pool.query('SELECT state FROM planning_requests WHERE id=$1',[request.id])).rows[0].state).toBe('awaiting_supervision');
 expect((await pool.query("SELECT 1 FROM events WHERE kind='SCOPED_PLANNING_CONTEXT_RECOVERED'")).rowCount).toBe(0);
});

async function stoppedPlanner(attempts=1){
  const f=await fixture(1);await createPlanningDemands(f.goal);
  const request=(await pool.query('SELECT * FROM planning_requests')).rows[0];
  await assignPlanningRequest(request.id);await preparePending();const id=await assignPlanningRequest(request.id);
  let run='';
  for(let i=1;i<=attempts;i++){
    run=randomUUID();const fence=randomUUID();
    await pool.query("UPDATE tasks SET state='running',criteria='[]',attempt_count=$2 WHERE id=$1",[id,i]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,$4,$3,'opencode',1,'running')",[run,id,fence,i]);
    await completeRun(run,fence,'Stopped rejected proposal fixture',{});
  }
  await pool.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1",[id]);
  await pool.query("UPDATE planning_requests SET state='replanning_required',receipt='{\"code\":\"PLANNING_SCOPE_INVALID\"}' WHERE id=$1",[request.id]);
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
  const proof={runId:run,workspace:task.execution.workspace,checkedAt:new Date().toISOString(),
    process:{unit:'agentos-run-'+run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};
  return{f,request,id,run,proof};
}

it.each(['newer','older'])('records a rejected historical plan without colliding with a %s scope reservation or touching actors',async age=>{
 const p=await stoppedPlanner(),other=randomUUID();
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision' WHERE id=$1",[p.request.id]);
 await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,created_at)
  VALUES($1,$2,$3,'glm',$4,'pending','A separately authorized strategy',$5,$6)`,
  [other,p.f.goal,p.request.scope,'d'.repeat(64),JSON.stringify(p.request.snapshot),new Date(Date.parse(p.request.created_at)+(age==='newer'?1000:-1000))]);
 const request={...p.request,task_id:p.id,run_id:p.run};
 const before=(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[p.id])).rows;
 const otherBefore=(await pool.query('SELECT * FROM planning_requests WHERE id=$1',[other])).rows[0];
 const receipt={code:'PLANNING_DEPENDENCY_CHANGE_INVALID',issues:[{path:['dependencyChanges',0]}]};
 const results=await Promise.all([rejectScopedPlanningResult(request,receipt),rejectScopedPlanningResult(request,receipt)]);
 expect(results.filter(Boolean)).toHaveLength(1);
 expect((await pool.query('SELECT state,receipt,snapshot FROM planning_requests WHERE id=$1',[p.request.id])).rows[0])
  .toMatchObject({state:age==='newer'?'superseded':'awaiting_supervision',snapshot:p.request.snapshot,
   receipt:{...receipt,evaluatedRunId:p.run,scopeReservation:{requestId:other,code:'SCOPED_DEMAND_RESERVED'}}});
 expect((await pool.query('SELECT * FROM planning_requests WHERE id=$1',[other])).rows[0]).toEqual(otherBefore);
 expect((await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[p.id])).rows).toEqual(before);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SCOPED_PLANNING_RESULT_REJECTED'")).rows[0].n).toBe(1);
});

it('routes a current rejected plan for bounded replanning when no other demand owns the scope',async()=>{
 const p=await stoppedPlanner();
 const request={...p.request,task_id:p.id,run_id:p.run};
 expect(await rejectScopedPlanningResult(request,{code:'PLANNING_BLOCKER_UNVERIFIED'})).toBe(true);
 expect((await pool.query('SELECT state,receipt FROM planning_requests WHERE id=$1',[p.request.id])).rows[0])
  .toMatchObject({state:'replanning_required',receipt:{code:'PLANNING_BLOCKER_UNVERIFIED',evaluatedRunId:p.run}});
 expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[p.id])).rows[0].n).toBe(1);
});

it.each(['paused','stale_run','live_run','foreign_task'])('does not change planning ownership for a %s observation',async reason=>{
 const p=await stoppedPlanner(),request={...p.request,task_id:p.id,run_id:p.run};
 if(reason==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[p.f.goal]);
 if(reason==='stale_run')request.run_id=randomUUID();
 if(reason==='live_run')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[p.run]);
 if(reason==='foreign_task')request.task_id=p.f.entries[0].source;
 const before=(await pool.query('SELECT * FROM planning_requests WHERE id=$1',[p.request.id])).rows[0];
 expect(await rejectScopedPlanningResult(request,{code:'PLANNING_BLOCKER_UNVERIFIED'})).toBe(false);
 expect((await pool.query('SELECT * FROM planning_requests WHERE id=$1',[p.request.id])).rows[0]).toEqual(before);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SCOPED_PLANNING_RESULT_REJECTED'")).rows[0].n).toBe(0);
});

it('observes a resumed historical planner without reclaiming another demand or altering its live process',async()=>{
 const p=await stoppedPlanner(),other=randomUUID();
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision' WHERE id=$1",[p.request.id]);
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[p.run]);
 await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot)
  VALUES($1,$2,$3,'glm',$4,'pending','Current strategy owns the reservation',$5)`,
  [other,p.f.goal,p.request.scope,'e'.repeat(64),JSON.stringify(p.request.snapshot)]);
 const original=(await pool.query('SELECT * FROM runs WHERE id=$1',[p.run])).rows[0];
 const request={...p.request,task_id:p.id,run_id:p.run};
 expect(await observeRunningScopedPlanner(request)).toBe(false);
 expect((await pool.query('SELECT state FROM planning_requests WHERE id=$1',[p.request.id])).rows[0].state).toBe('awaiting_supervision');
 expect((await pool.query('SELECT * FROM runs WHERE id=$1',[p.run])).rows[0]).toEqual(original);
 await pool.query("UPDATE planning_requests SET state='superseded' WHERE id=$1",[other]);
 expect(await observeRunningScopedPlanner(request)).toBe(true);
 expect((await pool.query('SELECT state FROM planning_requests WHERE id=$1',[p.request.id])).rows[0].state).toBe('running');
 expect(await observeRunningScopedPlanner(request)).toBe(false);
 expect((await pool.query('SELECT * FROM runs WHERE id=$1',[p.run])).rows[0]).toEqual(original);
});

it('a stopped or stale planning attempt cannot advertise itself as a running demand',async()=>{
 const p=await stoppedPlanner(),request={...p.request,task_id:p.id,run_id:p.run};
 const before=(await pool.query('SELECT * FROM planning_requests WHERE id=$1',[p.request.id])).rows[0];
 expect(await observeRunningScopedPlanner(request)).toBe(false);
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[p.run]);
 expect(await observeRunningScopedPlanner({...request,run_id:randomUUID()})).toBe(false);
 expect((await pool.query('SELECT * FROM planning_requests WHERE id=$1',[p.request.id])).rows[0]).toEqual(before);
});

async function replacedPlanner(){
 const p=await stoppedPlanner(),replacement=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),caseId=randomUUID();
 await pool.query("UPDATE planning_requests SET state='superseded' WHERE id=$1",[p.request.id]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count,execution)
   VALUES($1,$2,$3,'Replacement plan','Useful distinct work','research.requirements','running',$4,1,$5)`,[
    task,p.f.goal,'replacement-'+task,JSON.stringify([{id:'plan',label:'Plan',type:'artifact',name:'scope-plan.json'}]),
    JSON.stringify({kind:'opencode',workKind:'planning',baseWorkspace:p.request.scope,workspace:'test/replacement-'+task})]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,task,fence]);
 await writeArtifact(run,fence,'scope-plan.json',JSON.stringify({version:1,reason:'Applied replacement fixture'}));
 await completeRun(run,fence,'Accepted replacement fixture',{});await validateTask(task);
 await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,task_id)
   VALUES($1,$2,$3,'glm',$4,'applied','Replacement applied','{}',$5)`,[replacement,p.f.goal,p.request.scope,'b'.repeat(64),task]);
 await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence)
   VALUES($1,$2,$3,$4,'SCOPED_PLANNING_STAGNATION',$5)`,[caseId,p.f.goal,p.id,p.run,JSON.stringify([{kind:'retained-failure',artifact:'original'}])]);
 return{...p,replacement,replacementTask:task,replacementRun:run,caseId};
}

it('retires a superseded stopped planner once after a newer accepted plan is applied, preserving all historical evidence',async()=>{
 const p=await replacedPlanner();
 const history=(await pool.query('SELECT * FROM runs WHERE task_id=$1',[p.id])).rows;
 const inspect=vi.fn().mockResolvedValue(p.proof);
 const results=await Promise.all([retireSupersededPlanningTasks(inspect),retireSupersededPlanningTasks(inspect)]);
 expect(results.flat()).toEqual([p.id]);
 expect((await pool.query('SELECT state,evidence FROM supervision_cases WHERE id=$1',[p.caseId])).rows[0])
  .toEqual({state:'superseded',evidence:[{kind:'retained-failure',artifact:'original'}]});
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[p.id])).rows[0].state).toBe('superseded');
 expect((await pool.query('SELECT * FROM runs WHERE task_id=$1',[p.id])).rows).toEqual(history);
 expect((await pool.query("SELECT payload FROM events WHERE kind='SCOPED_PLANNING_RETIRED'")).rows)
  .toEqual([{payload:expect.objectContaining({taskId:p.id,caseIds:[p.caseId],replacementRequestId:p.replacement,
   replacementProof:expect.objectContaining({taskId:p.replacementTask,runId:p.replacementRun})})}]);
 expect(await retireSupersededPlanningTasks(inspect)).toEqual([]);
});

it('preserves the old case when stop is unconfirmed and publishes one diagnostic instead of retry noise',async()=>{
 const p=await replacedPlanner(),inspect=vi.fn().mockResolvedValue({...p.proof,process:{...p.proof.process,stopped:false}});
 expect(await retireSupersededPlanningTasks(inspect)).toEqual([]);expect(await retireSupersededPlanningTasks(inspect)).toEqual([]);
 expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[p.caseId])).rows[0].state).toBe('open');
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[p.id])).rows[0].state).toBe('recovery_blocked');
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SCOPED_PLANNING_RETIREMENT_BLOCKED'")).rows[0].n).toBe(1);
});

it('does not retire work still referenced by a consumer or with an older uncertain actor',async()=>{
 const p=await replacedPlanner(),inspect=vi.fn().mockResolvedValue(p.proof);
 await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[p.f.entries[0].consumer,p.id]);
 expect(await retireSupersededPlanningTasks(inspect)).toEqual([]);expect(inspect).not.toHaveBeenCalled();
 await pool.query('DELETE FROM dependencies WHERE depends_on=$1',[p.id]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'opencode',1,'reconciling')",[randomUUID(),p.id,randomUUID()]);
 expect(await retireSupersededPlanningTasks(inspect)).toEqual([]);expect(inspect).not.toHaveBeenCalled();
 expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[p.caseId])).rows[0].state).toBe('open');
});

it('requires an explicitly superseded request, active objective and current accepted replacement proof',async()=>{
 const p=await replacedPlanner(),inspect=vi.fn().mockResolvedValue(p.proof);
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision' WHERE id=$1",[p.request.id]);
 expect(await retireSupersededPlanningTasks(inspect)).toEqual([]);
 await pool.query("UPDATE planning_requests SET state='superseded' WHERE id=$1",[p.request.id]);
 await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[p.f.goal]);
 expect(await retireSupersededPlanningTasks(inspect)).toEqual([]);
 await pool.query("UPDATE goals SET state='active' WHERE id=$1",[p.f.goal]);
 await pool.query("UPDATE acceptances SET passed=false WHERE run_id=$1",[p.replacementRun]);
 expect(await retireSupersededPlanningTasks(inspect)).toEqual([]);
 expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[p.caseId])).rows[0].state).toBe('open');
});

it('routes a stopped planner with an active supervisor case without falsely claiming a retry or touching its attempt',async()=>{
  const p=await stoppedPlanner(),caseId=randomUUID();
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+p.f.goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence)
    VALUES($1,$2,$3,$4,'PLANNING_SCOPE_INVALID','[]')`,[caseId,p.f.goal,p.id,p.run]);
  const before=(await pool.query('SELECT * FROM runs WHERE id=$1',[p.run])).rows[0];
  expect(await retryRejectedScopedPlan(p.request.id,p.proof)).toBe(false);
  expect(await retryRejectedScopedPlan(p.request.id,p.proof)).toBe(false);
  expect((await pool.query('SELECT state,receipt FROM planning_requests WHERE id=$1',[p.request.id])).rows[0])
    .toMatchObject({state:'awaiting_supervision',receipt:{code:'PLANNING_SCOPE_INVALID',retryBlocked:{caseId,code:'SUPERVISION_PENDING'}}});
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[p.id])).rows[0].state).toBe('recovery_blocked');
  expect((await pool.query('SELECT * FROM runs WHERE id=$1',[p.run])).rows[0]).toEqual(before);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SCOPED_PLANNING_RETRY_REQUESTED'")).rows[0].n).toBe(0);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SCOPED_PLANNING_RETRY_BLOCKED'")).rows[0].n).toBe(1);
});

it('escalates three stopped model attempts with array evidence consumable by the supervisor',async()=>{
  const p=await stoppedPlanner(3);
  expect(await retryRejectedScopedPlan(p.request.id,p.proof)).toBe(false);
  expect(await retryRejectedScopedPlan(p.request.id,p.proof)).toBe(false);
  const cases=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[p.id])).rows;
  expect(cases).toHaveLength(1);
  expect(cases[0].evidence).toEqual([{kind:'SCOPED_PLANNING_STAGNATION',reason:{code:'PLANNING_SCOPE_INVALID'},
    scope:p.request.scope,requestId:p.request.id,sourceRevision:p.request.source_revision}]);
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[p.id])).rows[0].n).toBe(3);
});
