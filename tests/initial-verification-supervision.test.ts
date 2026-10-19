import {beforeAll,beforeEach,afterAll,afterEach,it,expect} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {pool,migrate,tx} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installSupervision,recordSupervisionDecisions,applySupervisionDecisions} from '../server/supervision.js';
import {requestDeliverySupervision} from '../server/delivery-supervision.js';
import {installNativeSupervision} from '../server/native-supervision.js';
import {acceptedSnapshotProof,stageDeferredHandoff,applyDeferredHandoff} from '../server/deferred-handoffs.js';
import {applyInitialVerificationDecision} from '../server/initial-verification-supervision.js';
import {completeRun,validateTask,writeArtifact,schedule,failRun} from '../server/service.js';
import {config} from '../server/config.js';

const initial={bridgeHost:config.bridgeHost,remoteExecution:config.remoteExecution};
const sha=(s:string)=>createHash('sha256').update(s).digest('hex');
const getTask=async(id:string)=>(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
const getCase=async(id:string)=>(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[id])).rows[0];
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await installBacklogPlanner();await installSupervision();await installNativeSupervision(pool);});
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();
 await pool.query("DELETE FROM settings WHERE id IN('maintenance','storage:agentos-workspaces','autonomy:initial-admission')");
 await pool.query('UPDATE settings SET value=$1 WHERE id=\'capacity\'',[JSON.stringify({maxRuns:8,browserSlots:2})]);
 Object.assign(config,{bridgeHost:'test-only',remoteExecution:true});
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
  VALUES('initial-account','Fixture','qwen','glm-5.3-flash','["glm-5.3-flash"]',true,16,true,'test-only')`);
});
afterEach(()=>Object.assign(config,initial));afterAll(()=>pool.end());
async function start(taskId:string){const t=await getTask(taskId),id=randomUUID(),fence=randomUUID();
 await pool.query("UPDATE tasks SET state='running',attempt_count=attempt_count+1 WHERE id=$1",[taskId]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,started_at)
  VALUES($1,$2,$3,$4,'harness',1,'running',now())`,[id,taskId,t.attempt_count+1,fence]);return{id,fence,taskId};}
async function publish(r:Awaited<ReturnType<typeof start>>,name:string,value:unknown){
 const artifact=await writeArtifact(r.id,r.fence,name,JSON.stringify(value));
 await completeRun(r.id,r.fence,'Fixture retained evidence',{});expect((await validateTask(r.taskId)).passed).toBe(true);
 await pool.query("UPDATE goals SET state='active' WHERE id=(SELECT goal_id FROM tasks WHERE id=$1)",[r.taskId]);return artifact;
}
async function fixture(){
 const goal=randomUUID(),source=randomUUID(),integration=randomUUID(),qa=randomUUID(),manager=randomUUID(),scope='initial-admission/game';
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Initial QA','Validate delivery','initial-admission',$2)",[goal,goal]);
 await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
 const text='An immutable QA fixture and existing expectation.';
 const input={id:randomUUID(),name:'fixture.txt',digest:sha(text),base64:Buffer.from(text).toString('base64')};
 for(const [id,stage,execution,artifact] of [
  [source,'work',{kind:'harness',workKind:'diagnostic',baseWorkspace:scope,workspace:scope+'/diagnostic'},'diagnostic.json'],
  [integration,'integrate',{kind:'harness',baseWorkspace:scope,workspace:scope+'/integration'},'candidate.json'],
  [qa,'verify',{kind:'harness',baseWorkspace:scope,workspace:scope+'/qa',sourceWorkspace:scope+'/integration',readOnlySource:true,inputs:[input]},'verification.json'],
  [manager,'coordinate',null,'coordination.json'],
 ] as const)await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,account_id,model,allowed_accounts)
  VALUES($1,$2,$3,'Fixture task','Preserve the original acceptance contract.','qa.backend','ready',$4,$5,$6,'initial-account','glm-5.3-flash','[]')`,
  [id,goal,id,stage,JSON.stringify([{id:'evidence',type:'artifact',label:'Original acceptance evidence',name:artifact}]),execution===null?null:JSON.stringify(execution)]);
 await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2),($1,$3)',[qa,source,integration]);
 const sourceRun=await start(source);await publish(sourceRun,'diagnostic.json',{case:'retained evidence'});
 const sourceArtifact=(await pool.query("SELECT id FROM artifacts WHERE run_id=$1 AND name='diagnostic.json'",[sourceRun.id])).rows[0];
 const integrationRun=await start(integration);
 const receipt=await tx(async db=>requestDeliverySupervision(db,{id:goal},await getTask(qa),null,{
  kind:'SCOPED_DELIVERY_ACTION',actionKey:'fixture-action',sourcePlanRunId:sourceRun.id,
  action:{kind:'reuse_existing',taskId:qa,sourceTaskId:source,expectedAttempt:0,expectedSourceRunId:sourceRun.id,artifactIds:[sourceArtifact.id]},
  sourceProof:await acceptedSnapshotProof(db,await getTask(source)),consumerAttempt:0,consumerRunId:null}));
 expect(receipt.applied).toBe(true);
 await pool.query("UPDATE tasks SET state='ready' WHERE id=$1",[qa]);
 expect((await getTask(qa)).state).toBe('recovery_blocked');
 return{goal,source,integration,qa,manager,sourceRun,integrationRun,caseId:receipt.caseId!,input};
}
async function review(f:Awaited<ReturnType<typeof fixture>>,binding?:any){
 const r=await start(f.manager);await publish(r,'coordination.json',{supervision:{actions:[{
  caseId:f.caseId,kind:'revise',diagnosis:'The initial verification is waiting for its existing integration dependency.',
  instruction:'Retain every original acceptance criterion and input. Perform the existing read-only verification once all dependencies are accepted.',
  ...(binding?{initialAdmission:binding}:{}),
 }]}});await recordSupervisionDecisions();return r;
}
async function phase(f:Awaited<ReturnType<typeof fixture>>){
 const prior=await review(f);expect((await getCase(f.caseId)).state).toBe('decided');
 await applySupervisionDecisions(async()=>{throw Error('No physical actor exists for initial QA');});
 const c=await getCase(f.caseId);expect(c.state).toBe('open');expect(c.decision_run_id).toBeNull();
 const event=(await pool.query("SELECT id::text,payload,created_at FROM events WHERE kind='FIRST_ADMISSION_CONTRACT_READY' AND entity_id=$1 ORDER BY id DESC LIMIT 1",[f.qa])).rows[0];
 expect(event.payload).toMatchObject({decisionRunId:prior.id,contract:{attempt:0,readOnlySource:true,handoffsConsumed:false,acceptance:false}});
 return{event,binding:{phaseEventId:event.id,contractDigest:event.payload.contractDigest},prior};
}
async function apply(f:Awaited<ReturnType<typeof fixture>>){await applySupervisionDecisions(async()=>{throw Error('Initial QA must not invoke physical stop inspection');});return getCase(f.caseId);}
async function stopped(r:Awaited<ReturnType<typeof start>>){return{runId:r.id,workspace:(await getTask(r.taskId)).execution.workspace,
 checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+r.id,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};}
async function integratorHandoffFixture(){
 const f=await fixture(),scope=(await getTask(f.qa)).execution.baseWorkspace;
 await pool.query('DELETE FROM dependencies WHERE task_id=$1 AND depends_on=$2',[f.qa,f.source]);
 await failRun(f.integrationRun.id,f.integrationRun.fence,'RESULT_FORMAT','Retain stopped integration fixture.');
 await tx(async db=>stageDeferredHandoff(db,f.goal,f.source,await getTask(f.source),await getTask(f.integration),{
  taskId:f.source,plannerRunId:f.sourceRun.id,coverageKey:'existing-integrator-diagnostic',lane:'glm',workKind:'diagnostic'}));
 const applied=(await pool.query('SELECT * FROM deferred_handoffs WHERE producer_task_id=$1',[f.source])).rows[0];
 expect(await applyDeferredHandoff(applied.id,await stopped(f.integrationRun),await stopped(f.sourceRun))).toBe(true);
 const second=randomUUID();
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,account_id,model)
  VALUES($1,$2,$3,'Second diagnostic','Retained separate evidence','qa.backend','ready','work',$4,$5,'initial-account','glm-5.3-flash')`,
  [second,f.goal,second,JSON.stringify([{id:'proof',type:'artifact',label:'Retained diagnostic',name:'diagnostic.json'}]),
   JSON.stringify({kind:'harness',workKind:'diagnostic',baseWorkspace:scope,workspace:scope+'/second-diagnostic'})]);
 const secondRun=await start(second);await publish(secondRun,'diagnostic.json',{diagnostic:'retained conflict'});
 await tx(async db=>stageDeferredHandoff(db,f.goal,second,await getTask(second),await getTask(f.integration),{
  taskId:second,plannerRunId:secondRun.id,coverageKey:'conflicted-integrator-diagnostic',lane:'glm',workKind:'diagnostic'}));
 const conflict=(await pool.query('SELECT * FROM deferred_handoffs WHERE producer_task_id=$1',[second])).rows[0];
 await pool.query("UPDATE tasks SET description=description||' Existing semantic revision.' WHERE id=$1",[f.integration]);
 const next=await start(f.integration);await failRun(next.id,next.fence,'RESULT_FORMAT','Retain revised integration fixture.');
 expect(await applyDeferredHandoff(conflict.id,await stopped(next),await stopped(secondRun))).toBe(false);
 expect((await pool.query('SELECT receipt FROM deferred_handoffs WHERE id=$1',[conflict.id])).rows[0].receipt.code).toBe('HANDOFF_CONTRACT_CHANGED');
 const firstEvidence=(await getCase(f.caseId)).evidence[0];
 const refs=(await pool.query("SELECT id FROM artifacts WHERE run_id=$1 AND name='diagnostic.json'",[secondRun.id])).rows;
 const evidence=[{...firstEvidence,conflict:'EXISTING_HANDOFF',handoffId:applied.id},{
  ...firstEvidence,actionKey:'second-existing-handoff',conflict:'EXISTING_HANDOFF',handoffId:conflict.id,
  action:{...firstEvidence.action,sourceTaskId:second,expectedSourceRunId:secondRun.id,artifactIds:refs.map(r=>r.id)},
  sourceProof:await acceptedSnapshotProof(pool,await getTask(second)),
 }];
 await pool.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1',[f.caseId,JSON.stringify(evidence)]);
 f.integrationRun=await start(f.integration);
 return{...f,second,secondRun,handoffIds:[applied.id,conflict.id]};
}

it('pins existing integrator handoffs without consuming, moving or resolving their pending conflict',async()=>{
 const f=await integratorHandoffFixture();
 const before=(await pool.query('SELECT * FROM deferred_handoffs WHERE id=ANY($1::uuid[]) ORDER BY id',[f.handoffIds])).rows;
 const deps=(await pool.query('SELECT task_id,depends_on FROM dependencies ORDER BY task_id,depends_on')).rows;
 const p=await phase(f);expect(p.event.payload.contract.handoffs).toHaveLength(2);
 expect(p.event.payload.contract.handoffs.map((h:any)=>h.disposition)).toEqual(['preserve_existing_consumer','preserve_existing_consumer']);
 expect(p.event.payload.contract.handoffs.some((h:any)=>h.state==='replanning_required'&&h.code==='HANDOFF_CONTRACT_CHANGED')).toBe(true);
 await review(f,p.binding);expect(await apply(f)).toMatchObject({state:'applied',applied_attempt:0});
 expect((await pool.query('SELECT * FROM deferred_handoffs WHERE id=ANY($1::uuid[]) ORDER BY id',[f.handoffIds])).rows).toEqual(before);
 expect((await pool.query('SELECT task_id,depends_on FROM dependencies ORDER BY task_id,depends_on')).rows).toEqual(deps);
 expect(await schedule()).toEqual([]);
 await publish(f.integrationRun,'candidate.json',{candidate:'accepted by original integration criteria'});
 const runs=await schedule();expect(runs).toHaveLength(1);
 expect((await pool.query('SELECT task_id,attempt FROM runs WHERE id=$1',[runs[0]])).rows[0]).toMatchObject({task_id:f.qa,attempt:1});
 expect((await pool.query('SELECT * FROM deferred_handoffs WHERE id=ANY($1::uuid[]) ORDER BY id',[f.handoffIds])).rows).toEqual(before);
 expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1',[f.qa])).rowCount).toBe(0);
});

it.each(['handoff-target','handoff-scope','cycle'])('does not create initial authority when the existing handoff has %s drift',async change=>{
 const f=await integratorHandoffFixture();
 if(change==='handoff-target')await pool.query('UPDATE deferred_handoffs SET consumer_task_id=$2 WHERE id=$1',[f.handoffIds[0],f.qa]);
 if(change==='handoff-scope')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}','\"another-scope\"') WHERE id=$1",[f.integration]);
 if(change==='cycle')await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[f.source,f.qa]);
 await review(f);expect(await applyInitialVerificationDecision(f.caseId)).toBe(false);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='FIRST_ADMISSION_CONTRACT_READY' AND entity_id=$1",[f.qa])).rowCount).toBe(0);
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.qa])).rowCount).toBe(0);
});

it('requires a fresh contract-bound review, preserves instruction and dependencies, then admits exactly the first QA run',async()=>{
 const f=await fixture(),original=await getTask(f.qa),p=await phase(f);
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.qa])).rowCount).toBe(0);
 await recordSupervisionDecisions();expect((await getCase(f.caseId)).state).toBe('open');
 const fresh=await review(f,p.binding);expect((await getCase(f.caseId)).decision_run_id).toBe(fresh.id);
 expect(await apply(f)).toMatchObject({state:'applied',source_run_id:null,applied_attempt:0});
 const {continuation,...originalExecution}=original.execution;
 expect(continuation).toBeDefined();
 const revised=await getTask(f.qa);expect(revised).toMatchObject({attempt_count:0,state:'blocked_dependencies',criteria:original.criteria});
 expect(revised.execution).toEqual(originalExecution);expect(revised.execution.continuation).toBeUndefined();expect(revised.description).toContain(original.description);
 expect(revised.description).toContain('Retain every original acceptance criterion and input.');
 expect((await pool.query('SELECT depends_on FROM dependencies WHERE task_id=$1 ORDER BY depends_on',[f.qa])).rows.map(r=>r.depends_on).sort()).toEqual([f.source,f.integration].sort());
 const revision=(await pool.query('SELECT * FROM task_instruction_revisions WHERE task_id=$1',[f.qa])).rows[0];
 expect(revision.provenance).toMatchObject({sourceRunId:null,sourceAttempt:0,firstAdmission:true,initialAdmission:p.binding,handoffsConsumed:false});
 expect(await schedule()).toEqual([]);
 await publish(f.integrationRun,'candidate.json',{candidate:'accepted by original integration criterion'});
 const [runId]=await schedule();expect(runId).toBeTruthy();expect(await schedule()).toEqual([]);
 const r=(await pool.query('SELECT * FROM runs WHERE id=$1',[runId])).rows[0];
 expect(r).toMatchObject({task_id:f.qa,attempt:1,state:'queued'});
 expect(r.instruction_snapshot).toMatchObject({revisionId:revision.id,task:{description:revised.description,execution:originalExecution,criteria:original.criteria}});
 expect(r.instruction_snapshot.executionInputs).toContainEqual({id:f.input.id,name:f.input.name,digest:f.input.digest,size:Buffer.from(f.input.base64,'base64').length,source:'execution_input'});
 expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1',[f.qa])).rowCount).toBe(0);
 expect((await pool.query('SELECT 1 FROM delivery_consumptions WHERE consumer_task_id=$1',[f.qa])).rowCount).toBe(0);
});

it('uses PostgreSQL microsecond ordering for a later review in the same millisecond',async()=>{
 const f=await fixture(),p=await phase(f),fresh=await review(f,p.binding);
 await pool.query("UPDATE events SET created_at='2026-10-11T00:00:00.123400Z' WHERE id=$1",[p.event.id]);
 await pool.query("UPDATE runs SET created_at='2026-10-11T00:00:00.123500Z' WHERE id=$1",[fresh.id]);
 expect(await applyInitialVerificationDecision(f.caseId)).toBe(true);
 expect((await getCase(f.caseId)).applied_attempt).toBe(0);
});

it('rejects missing, wrong and replayed bindings with deterministic case feedback',async()=>{
 const f=await fixture(),p=await phase(f);
 for(const binding of [undefined,{...p.binding,contractDigest:'0'.repeat(64)},{...p.binding,phaseEventId:String(BigInt(p.binding.phaseEventId)+1000n)}]){
  const r=await review(f,binding);expect((await getCase(f.caseId)).state).toBe('open');
  expect((await pool.query("SELECT payload FROM events WHERE kind='SUPERVISION_PLAN_REJECTED' AND payload->>'runId'=$1",[r.id])).rows[0].payload).toMatchObject({code:'FIRST_ADMISSION_BINDING_REQUIRED',caseId:f.caseId});
 }
 expect((await getCase(f.caseId)).evidence.some((e:any)=>e.kind==='FIRST_ADMISSION_DECISION_REJECTED'&&e.nextAction.includes('initialAdmission'))).toBe(true);
 expect((await pool.query('SELECT 1 FROM task_instruction_revisions WHERE task_id=$1',[f.qa])).rowCount).toBe(0);
});

it.each(['criteria','input','workspace','dependency','source-description'])('refreshes the phase when %s changed, without applying the stale review',async change=>{
 const f=await fixture(),p=await phase(f);
 if(change==='criteria')await pool.query("UPDATE tasks SET criteria=criteria||'[ {\"id\":\"second\",\"type\":\"artifact\",\"name\":\"extra.json\"} ]'::jsonb WHERE id=$1",[f.qa]);
 if(change==='input')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{inputs}', $2::jsonb) WHERE id=$1",[f.qa,JSON.stringify([{...f.input,digest:sha('changed'),base64:Buffer.from('changed').toString('base64')}])]);
 if(change==='workspace')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}','\"initial-admission/game/qa-revised\"') WHERE id=$1",[f.qa]);
 if(change==='dependency'){
  const dependency=randomUUID();await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria)
   VALUES($1,$2,$3,'Additional condition','Preserved new prerequisite','qa.backend','accepted','work','[]')`,[dependency,f.goal,dependency]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)',[f.qa,dependency]);
 }
 if(change==='source-description')await pool.query("UPDATE tasks SET description=description||' New semantic scope.' WHERE id=$1",[f.source]);
 await review(f,p.binding);const c=await getCase(f.caseId);
 expect(c.state).toBe('open');const latest=c.evidence.filter((e:any)=>e.kind==='FIRST_ADMISSION_CONTRACT_READY').at(-1);
 expect(latest.phaseEventId).not.toBe(p.binding.phaseEventId);expect(latest.contractDigest).not.toBe(p.binding.contractDigest);
 expect((await pool.query('SELECT 1 FROM task_instruction_revisions WHERE task_id=$1',[f.qa])).rowCount).toBe(0);
});

it.each(['cancel','paused-goal','disabled-policy','writable','other-case'])('does not authorize first admission when %s removes authority',async condition=>{
 const f=await fixture(),p=await phase(f);await review(f,p.binding);
 if(condition==='cancel')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.qa,JSON.stringify({taskId:f.qa})]);
 if(condition==='paused-goal')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 if(condition==='disabled-policy')await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1",['supervision:'+f.goal]);
 if(condition==='writable')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{readOnlySource}','false') WHERE id=$1",[f.qa]);
 if(condition==='other-case'){
  await pool.query("UPDATE supervision_cases SET state='forwarded' WHERE id=$1",[f.caseId]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence,request_key)
   VALUES($1,$2,$3,NULL,'New pending authority','[]','new-condition')`,[randomUUID(),f.goal,f.qa]);
 }
 expect(await applyInitialVerificationDecision(f.caseId)).toBe(false);
 expect((await pool.query('SELECT 1 FROM task_instruction_revisions WHERE task_id=$1',[f.qa])).rowCount).toBe(0);
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.qa])).rowCount).toBe(0);
});

it('applies once under concurrent consumers and rolls back every DB instruction effect on a failing write',async()=>{
 const f=await fixture(),p=await phase(f);await review(f,p.binding);
 await pool.query(`CREATE FUNCTION fixture_initial_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  IF NEW.task_id='${f.qa}'::uuid THEN RAISE EXCEPTION 'FIXTURE_FAIL_INITIAL_REVISION'; END IF; RETURN NEW; END $$;
  CREATE TRIGGER fixture_initial_fail BEFORE INSERT ON task_instruction_revisions FOR EACH ROW EXECUTE FUNCTION fixture_initial_fail();`);
 const before=await getTask(f.qa);
 try{await expect(applyInitialVerificationDecision(f.caseId)).rejects.toThrow('FIXTURE_FAIL_INITIAL_REVISION');}
 finally{await pool.query('DROP TRIGGER fixture_initial_fail ON task_instruction_revisions; DROP FUNCTION fixture_initial_fail()');}
 expect(await getTask(f.qa)).toEqual(before);expect((await getCase(f.caseId)).state).toBe('decided');
 expect((await pool.query('SELECT 1 FROM task_instruction_origins WHERE task_id=$1',[f.qa])).rowCount).toBe(0);
 const result=await Promise.all([applyInitialVerificationDecision(f.caseId),applyInitialVerificationDecision(f.caseId)]);
 expect(result.filter(Boolean)).toHaveLength(1);
 expect((await pool.query('SELECT 1 FROM task_instruction_revisions WHERE task_id=$1',[f.qa])).rowCount).toBe(1);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_INITIAL_ADMISSION_APPLIED' AND entity_id=$1",[f.qa])).rowCount).toBe(1);
});
