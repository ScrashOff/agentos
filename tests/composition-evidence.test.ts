import {afterAll,beforeAll,beforeEach,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,tx,event} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {readArtifact} from '../server/service.js';
import {executeDetachedHarness} from '../server/harness-supervision.js';
import {installNativeSupervision,reserveNativeSupervisor,superviseNativeRun,nativeSupervisorCgroup} from '../server/native-supervision.js';
import {readRuntimeComposition,readManagementCompositions} from '../server/composition-evidence.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';

beforeAll(async()=>{await migrate();await installNativeSupervision(pool);await installTaskExecutionMetadata();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
 await pool.query("DELETE FROM settings WHERE id='maintenance'");});
afterAll(()=>pool.end());
async function fixture(){
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),workspace='composition-test/'+run;
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Composition','Actual callback','composition-test',$2)",[goal,goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
  VALUES($1,$2,$3,'Integrator','Keep immutable evidence','engineering.integration','queued','integrate',
   '[{"id":"receipt","type":"artifact","name":"runtime-composition.json"}]',$4,1)`,
 [task,goal,'task-'+task,JSON.stringify({kind:'harness',workspace,sourceWorkspace:'composition-test/source',baseWorkspace:'composition-test/source'})]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'queued')",[run,task,fence]);
 await pool.query('INSERT INTO opencode_jobs(run_id) VALUES($1)',[run]);
 await pool.query('INSERT INTO leases VALUES($1,$2),($3,$2)',['remote-slot',run,'workspace:'+workspace]);
 await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+goal,'{"enabled":true,"detachedHarnessEnabled":true}']);
 const composition={version:1,workspace,dryRun:false,complete:true,beforeDigest:'a'.repeat(64),afterDigest:'b'.repeat(64),
  projectedDigest:'b'.repeat(64),acceptance:false,independentValidationRequired:true,
  receipts:[{producerTaskId:randomUUID(),status:'applied',inputs:{patch:randomUUID(),report:randomUUID()},patchDigest:'c'.repeat(64),reportDigest:'d'.repeat(64)}]};
 return{goal,task,run,fence,workspace,composition};
}

it.each([true,false])('reads the actual detached Harness event writer and artifact without turning complete=%s into acceptance',async complete=>{
 const f=await fixture();f.composition.complete=complete;
 if(!complete)f.composition.receipts[0].status='conflict';
 const reservation=await tx(db=>reserveNativeSupervisor(db,f.run,{
  candidate:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',digest:'a'.repeat(64)}));
 expect(reservation).not.toBeNull();if(!reservation)throw Error('RESERVATION_REQUIRED');
 await superviseNativeRun(f.run,reservation.launchToken,async()=>({unit:reservation.unit,pid:process.pid,
   cwd:reservation.candidate,cgroup:nativeSupervisorCgroup(f.run)}),
  (id,r)=>executeDetachedHarness(id,r,async input=>{
   // Use the real production callback: no fixture insertion of a run-entity event.
   await input.onEvent('PATCH_COMPOSITION_RECORDED',f.composition);
   return{summary:'Composition receipt only, no independent candidate QA.',
    artifacts:[{name:'runtime-composition.json',content:JSON.stringify(f.composition)}]};
  }));
 const recorded=(await pool.query("SELECT * FROM events WHERE kind='PATCH_COMPOSITION_RECORDED'")).rows;
 expect(recorded).toHaveLength(1);expect(recorded[0].entity_id).toBe(f.task);
 expect(recorded[0].payload).toEqual({runId:f.run,...f.composition});
 const id=(await pool.query("SELECT id FROM artifacts WHERE run_id=$1 AND name='runtime-composition.json'",[f.run])).rows[0].id;
 const bytes=await readArtifact(id),proof=await readRuntimeComposition(pool,f.task,f.run,f.workspace);
 expect(proof?.payload).toEqual(JSON.parse(bytes.bytes.toString()));
 expect(proof?.identity).toEqual({taskId:f.task,runId:f.run,fenceRecorded:false});
 expect(proof?.payload.complete).toBe(complete);expect(proof?.payload.acceptance).toBe(false);
 expect((await readManagementCompositions(pool,f.goal,'composition-test/source',[f.task]))[0])
  .toMatchObject({taskId:f.task,expectedAttempt:1,bindingStatus:'BOUND',payload:{complete}});
 expect((await pool.query("SELECT 1 FROM events WHERE kind='PATCH_COMPOSITION_RECORDED' AND entity_id=$1",[f.run])).rowCount).toBe(0);
});

it.each(['wrong-task','wrong-run','wrong-fence','null-fence','run-entity','workspace','duplicate','latest-invalid'] as const)
 ('rejects %s composition evidence without falling back to an unrelated or older event',async change=>{
  const f=await fixture(),payload={runId:f.run,...f.composition};
  if(change==='duplicate'||change==='latest-invalid')await event(pool,'PATCH_COMPOSITION_RECORDED',f.task,payload);
  await event(pool,'PATCH_COMPOSITION_RECORDED',change==='wrong-task'?randomUUID():change==='run-entity'?f.run:f.task,
   {...payload,...(change==='wrong-run'?{runId:randomUUID()}:{}),
    ...(change==='wrong-fence'||change==='latest-invalid'?{runFence:randomUUID()}:{}),
    ...(change==='null-fence'?{runFence:null}:{}),
    ...(change==='workspace'?{workspace:'composition-test/other'}:{})});
  expect(await readRuntimeComposition(pool,f.task,f.run,f.workspace)).toBeNull();
 });

it('retains unknown payload fields and exact receipts while stripping only validated run identity',async()=>{
 const f=await fixture(),extra={runId:f.run,runFence:f.fence,...f.composition,unrecognizedSemanticField:{checked:false}};
 await event(pool,'PATCH_COMPOSITION_RECORDED',f.task,extra);
 const proof=await readRuntimeComposition(pool,f.task,f.run,f.workspace);
 expect(proof?.payload).toEqual({...f.composition,unrecognizedSemanticField:{checked:false}});
 expect(proof?.payload).not.toEqual(f.composition);expect(proof?.identity.fenceRecorded).toBe(true);
 const other=await fixture();await event(pool,'PATCH_COMPOSITION_RECORDED',f.task,{runId:other.run,...other.composition});
 expect(await readRuntimeComposition(pool,f.task,f.run,f.workspace)).toEqual(proof);
 expect(await readRuntimeComposition(pool,f.task,other.run,f.workspace)).toBeNull();
});
