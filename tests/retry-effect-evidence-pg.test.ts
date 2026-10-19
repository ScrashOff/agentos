import {afterAll,beforeAll,beforeEach,it,expect} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {installDeferredHandoffs} from '../server/deferred-handoffs.js';
import {installSupervision} from '../server/supervision.js';
import {readManagerCases} from '../server/manager-material-consumption.js';

const sha=(value:string)=>createHash('sha256').update(value).digest('hex');
const h=(char:string)=>char.repeat(64);
const criteria=[{id:'menu_rules',type:'artifact',name:'verification.json',label:'Original rules content'},
  {id:'native_boot',type:'artifact',name:'verification.json',label:'Native boot'}];

beforeAll(async()=>{await migrate();await installTaskExecutionMetadata();await installDeferredHandoffs();await installSupervision();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>pool.end());

async function fixture(options:any={}) {
  const goalId=randomUUID(),taskId=randomUUID(),priorRunId=randomUUID(),currentRunId=randomUUID();
  const priorCaseId=randomUUID(),currentCaseId=randomUUID(),revisionId=randomUUID(),originId=randomUUID();
  const fence1=randomUUID(),fence2=randomUUID();
  const criterionSet=options.criteria??criteria;
  await pool.query(`INSERT INTO goals(id,title,description,project,input_hash,idempotency_key)
    VALUES($1,'Retry effect','Preserve the exact final criteria','retry-effect-test',$2,$3)`,[goalId,goalId,goalId]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1::uuid,$2::uuid,$1::text,'Implement core fix','Original task instructions','engineering.integration','running','integrate',$3,$4,1)`,
    [taskId,goalId,JSON.stringify(criterionSet),JSON.stringify({kind:'harness',workKind:options.workKind??'repair',
      workspace:'game/work',sourceWorkspace:'game/source',baseWorkspace:'game/source',readOnlySource:false,inputs:[]})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$3,1,$2,'harness',1,'submitted')`,[priorRunId,fence1,taskId]);
  await pool.query(`INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'prior immutable context')`,
    [fence1,priorRunId,JSON.stringify(manifest(options.priorInputs,[],options.priorControlMemories))]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[priorRunId,fence1]);
  const priorDiff=options.priorDiff??h('c');
  await pool.query(`INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'change.diff','text/plain',$3,5518270)`,
    [randomUUID(),priorRunId,priorDiff]);
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
    VALUES($1,$2,$3,false,$4,'fixture')`,[randomUUID(),taskId,priorRunId,JSON.stringify(checks(options.priorFailed))]);

  const decision={kind:'revise',actions:[{kind:'revise'}]};
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision,decision_run_id,applied_attempt)
    VALUES($1,$2,$3,$4,'applied','Prior supervised revise','[]',$5,$4,1)`,
    [priorCaseId,goalId,taskId,priorRunId,JSON.stringify(decision)]);
  const provenance={caseId:priorCaseId,kind:'revise',sourceRunId:priorRunId,sourceAttempt:1,
    decisionRunId:priorRunId,decisionSnapshot:decision};
  const rendered='Original task instructions\n\nSupervised revise: change the implementation.';
  await pool.query(`INSERT INTO task_instruction_origins(id,task_id,goal_id,baseline,baseline_hash,original,original_hash,proof)
    VALUES($1,$2,$3,'Original task instructions',$4,'Original task instructions',$4,'{}')`,
    [originId,taskId,goalId,sha('Original task instructions')]);
  await pool.query(`INSERT INTO task_instruction_revisions(id,task_id,goal_id,origin_id,number,operation_key,instruction,
    instruction_hash,provenance,rendered,rendered_hash) VALUES($1,$2,$3,$4,1,$5,$6,$7,$8,$9,$10)`,
    [revisionId,taskId,goalId,originId,`${priorCaseId}:revise`,'Change implementation only',sha('Change implementation only'),
      JSON.stringify(provenance),rendered,sha(rendered)]);
  await pool.query('UPDATE tasks SET description=$2,instruction_revision_id=$3,attempt_count=2 WHERE id=$1',
    [taskId,rendered,revisionId]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$3,2,$2,'harness',1,'submitted')`,[currentRunId,fence2,taskId]);
  const currentMemory=options.currentMemory;
  const currentContextId=randomUUID();
  await pool.query(`INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'current immutable context')`,
    [currentContextId,currentRunId,JSON.stringify(manifest(options.currentInputs,currentMemory,options.currentControlMemories))]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[currentRunId,currentContextId]);
  const currentDiff=options.currentDiff??priorDiff;
  await pool.query(`INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'change.diff','text/plain',$3,5518270)`,
    [randomUUID(),currentRunId,currentDiff]);
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
    VALUES($1,$2,$3,false,$4,'fixture')`,[randomUUID(),taskId,currentRunId,JSON.stringify(checks(options.currentFailed))]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
    VALUES($1,$2,$3,$4,'open','Current failed integration','[]')`,[currentCaseId,goalId,taskId,currentRunId]);
  await pool.query(`UPDATE tasks SET state='recovery_blocked' WHERE id=$1`,[taskId]);
  return {goalId,currentCaseId,taskId};
}
function checks(failed:string[]=['menu_rules']) {
  return criteria.map(c=>({id:c.id,passed:!failed.includes(c.id)}));
}
function manifest(inputs:string[]=[],selected:any[]=[],controlMemories:any[]=[]){
  return {dependencyArtifacts:inputs.map((digest,index)=>({id:`00000000-0000-4000-8000-${String(index+101).padStart(12,'0')}`,
    name:`accepted-${index}.patch`,digest,size:4,task_id:'00000000-0000-4000-8000-000000000060',
    run_id:'00000000-0000-4000-8000-000000000061'})),recoveryArtifacts:[],recoveryArtifactAliases:[],selected,
    supervisionMemory:{memories:controlMemories}};
}

async function read(options:any={}) {
  const fixtureValue=await fixture(options);
  const entry=(await readManagerCases(pool,fixtureValue.goalId,false)).find((row:any)=>row.id===fixtureValue.currentCaseId);
  return {fixture:fixtureValue,entry};
}

it('projects exact no-effect comparison only for an unchanged supervised integration failure',async()=>{
  const {entry}=await read();
  expect(entry.retryEffectEvidence).toMatchObject({status:'same_change_same_inputs_same_failed_criteria',
    sameChangeDiff:true,sameFrozenInputs:true,sameCriteria:true,sameFailedCriteria:true,
    priorFailedCriteria:['menu_rules'],currentFailedCriteria:['menu_rules'],acceptanceRemainsFalse:true});
});

it('preserves meaningful changed failures and changed implementation as separate outcomes',async()=>{
  const changedFailure=await read({currentFailed:['native_boot']});
  expect(changedFailure.entry.retryEffectEvidence.status).toBe('same_change_same_inputs_failure_set_changed');
  const changedDiff=await read({currentDiff:h('d')});
  expect(changedDiff.entry.retryEffectEvidence.status).toBe('change_diff_changed');
  const changedInput=await read({priorInputs:[h('a')],currentInputs:[h('b')]});
  expect(changedInput.entry.retryEffectEvidence.status).toBe('material_inputs_changed');
});

it('reports same patch separately from real control-memory context change',async()=>{
  const ref={id:'00000000-0000-4000-8000-000000000099',version:1,hash:h('f'),source:'operator:review-control'};
  const {fixture:created,entry}=await read({currentMemory:[ref],currentControlMemories:[ref]});
  expect(entry.retryEffectEvidence).toMatchObject({
    status:'same_change_same_material_inputs_memory_context_changed_same_failed_criteria',
    sameCorePatch:true,sameMaterialInputs:true,sameSelectedMemoryContext:false,
    sameControlMemoryContext:false,sameFrozenInputs:false,acceptanceRemainsFalse:true,decisionRequired:true,
  });
  expect((await pool.query('SELECT decision FROM supervision_cases WHERE id=$1',[created.currentCaseId])).rows[0].decision)
    .toBeNull();
});

it('omits the hint for diagnostic/test work and does not mutate case, acceptance or task',async()=>{
  const {fixture:created,entry}=await read({workKind:'diagnostic'});
  expect(entry.retryEffectEvidence).toBeUndefined();
  expect((await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1',[created.taskId])).rows[0])
    .toMatchObject({state:'recovery_blocked',attempt_count:2});
  expect((await pool.query('SELECT state,decision FROM supervision_cases WHERE id=$1',[created.currentCaseId])).rows[0])
    .toMatchObject({state:'open',decision:null});
  expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=(SELECT source_run_id FROM supervision_cases WHERE id=$1)',
    [created.currentCaseId])).rows[0].passed).toBe(false);
});
