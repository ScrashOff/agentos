import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installDeferredHandoffs} from '../server/deferred-handoffs.js';
import {installSupervision} from '../server/supervision.js';
import {installPlanningDemand} from '../server/planning-demand.js';
import {installDeliveryTransitions} from '../server/delivery-transitions.js';
import {readDeliveryConsumptionPage,readDeliveryConsumptionWindow,deliveryConsumptionRecord,deliveryGenerationContext,deliveryResolutionGenerationGuide} from '../server/delivery-consumption-catalog.js';
import {projectManagerReview,managerContextGuide} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {readManagementRetrieval,managementRetrievalCharacterLimit} from '../server/management-retrieval.js';
import {readManagementFollowThrough} from '../server/management-follow-through.js';
import {readManagementMateriality,canWaitForManagementFacts,hasActionableManagementConsumption} from '../server/management-materiality.js';
import {hash} from '../server/memory.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();
  await installDeferredHandoffs();await installSupervision();await installPlanningDemand();await installDeliveryTransitions();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});
async function fixture(count=1) {
  const goal=randomUUID(),consumer=randomUUID(),ids:string[]=[];
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Deliver','Preserve criteria','catalog-test',$2)",[goal,goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria)
    VALUES($1,$2,$3,'Consume','Actual instructions','engineering.integration','waiting_resource','integrate','[]')`,[consumer,goal,consumer]);
  for(let i=0;i<count;i++) {
    const id=randomUUID(),producer=randomUUID(),run=randomUUID(),handoff=randomUUID();ids.push(id);
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count)
      VALUES($1,$2,$3,'Repair','Original repair','engineering.integration','accepted','[]',1)`,[producer,goal,producer]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'submitted')",[run,producer,randomUUID()]);
    await pool.query(`INSERT INTO deferred_handoffs(id,goal_id,producer_task_id,consumer_task_id,source_task_id,source_proof,consumer_proof,handoff,state)
      VALUES($1,$2,$3,$4,$3,'{}','{}','{}','applied')`,[handoff,goal,producer,consumer]);
    await pool.query(`INSERT INTO delivery_consumptions(id,handoff_id,goal_id,producer_task_id,producer_run_id,consumer_task_id,
      scope,consumer_contract_digest,producer_manifest_digest,binding,state,receipt,created_at)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8,'{}','attached',$9,'2026-01-01'::timestamptz+$10*interval '1 second')`,
      [id,handoff,goal,producer,run,consumer,i<25?'scope/a':'scope/b','a'.repeat(64),JSON.stringify({code:'CAPACITY_WAIT',owner:'scheduler',nextAction:'Await existing capacity.'}),i]);
  }
  return {goal,consumer,ids};
}
async function request(goal:string,contextRequests:any[]) {
  await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    ['management-state:'+goal,JSON.stringify({contextRequests})]);
}
const noArtifact=async(_id:string):Promise<{bytes:Buffer}>=>{throw Error('UNEXPECTED_ARTIFACT_READ');};

it('keeps consumption and source-conflict generations distinct in the focal catalog, directory and recovered full record',async()=>{
 const f=await fixture(),consumption='2f83e491c65a6c29e8e33cc92f37b9367409bdc197c356379dfcb87cda2be553',
  conflict='65816cd6fb5e2d5626eaa0f793163eb1da7b41c89e1c35903e886ab9e285d9ee';
 const originalRun=(await pool.query('SELECT producer_run_id FROM delivery_consumptions WHERE id=$1',[f.ids[0]])).rows[0].producer_run_id;
 const receipt={code:'SUPERVISION_REQUESTED',generation:consumption,sourceConflict:{generation:conflict,
  original:{runId:originalRun},observed:{runId:randomUUID()}}};
 await pool.query('UPDATE delivery_consumptions SET receipt=$2 WHERE id=$1',[f.ids[0],JSON.stringify(receipt)]);
 const before=(await pool.query('SELECT * FROM delivery_consumptions WHERE id=$1',[f.ids[0]])).rows[0];
 const expected={consumptionGeneration:consumption,sourceConflictGeneration:conflict,
  deliveryResolutionBinding:{obligationId:f.ids[0],generation:conflict}};
 const window=await readDeliveryConsumptionWindow(pool,f.goal,'scope/a');expect(window.rows[0]).toMatchObject(expected);
 await request(f.goal,[{kind:'delivery_consumptions',offset:0},{kind:'delivery_consumption',obligationId:f.ids[0],offset:0}]);
 const recovered=await readManagementRetrieval(pool,f.goal,noArtifact),directory=recovered.results[0].rows[0],full=JSON.parse(recovered.results[1].text);
 expect(directory).toMatchObject(expected);expect(full).toMatchObject(expected);
 expect(full.receipt).toEqual(receipt);expect(full.deliveryResolutionBinding).not.toHaveProperty('expectedSourceRunId');
 expect(directory.deliveryResolutionBinding).not.toHaveProperty('expectedSourceRunId');
 expect((await pool.query('SELECT * FROM delivery_consumptions WHERE id=$1',[f.ids[0]])).rows[0]).toEqual(before);
 // A real budgeted manager projection must retain the unambiguous identities,
 // even when its generic history is large. No validator or current proof changes.
 const prefix=managerContextGuide+'\n'+deliveryResolutionGenerationGuide+'\n';
 const projected=projectManagerReview({cases:[{id:randomUUID(),evidence:[]}],operation:{focus:{taskId:f.consumer},
  recoveredContext:recovered,deliveryConsumptions:window.rows,
  history:Array.from({length:80},(_,i)=>({id:i,text:'Historical observation '.repeat(80)}))}},prefix,0,150000);
 expect(projected.withinBudget).toBe(true);expect(JSON.stringify(projected.description).length).toBeLessThanOrEqual(150000);
 const delivered=expandSharedManagerContext(JSON.parse(projected.description.slice(prefix.length)));
 expect(delivered.operation.deliveryConsumptions[0]).toMatchObject(expected);
 expect(delivered.operation.recoveredContext.results[0].rows[0]).toMatchObject(expected);
});

it.each([undefined,null,'invalid','a'.repeat(63)])('never invents a source binding from consumption generation when conflict generation is %s',generation=>{
 const row={id:randomUUID(),producer_run_id:randomUUID(),receipt:{generation:'b'.repeat(64),sourceConflict:{generation}}};
 expect(deliveryGenerationContext(row)).toEqual({consumptionGeneration:'b'.repeat(64),sourceConflictGeneration:null,deliveryResolutionBinding:null});
 expect(deliveryResolutionGenerationGuide).toContain('never consumptionGeneration');
});

it('recovers every obligation beyond the old 24-row window with goal-scoped digest-bound pages',async()=>{
  const f=await fixture(45),seen:string[]=[];let offset:number|null=0,digest:string|undefined;
  do {const page=await readDeliveryConsumptionPage(pool,f.goal,{offset,limit:10,expectedDigest:digest});
    expect(page.status).toBe('OK');digest??=page.snapshotDigest;expect(page.snapshotDigest).toBe(digest);
    seen.push(...page.rows.map(r=>r.id));offset=page.nextOffset;
  }while(offset!==null);
  expect(seen).toEqual(f.ids);expect(new Set(seen).size).toBe(45);
  expect((await readDeliveryConsumptionPage(pool,randomUUID(),{obligationId:f.ids[0]})).rows).toEqual([]);
});
it('prioritizes focal obligations after position25 and rotates both lanes without changing source rows',async()=>{
  const f=await fixture(45),before=(await pool.query('SELECT * FROM delivery_consumptions ORDER BY id')).rows;
  const first=await readDeliveryConsumptionWindow(pool,f.goal,'scope/b');
  expect(first.rows.slice(0,16).map(r=>r.id)).toEqual(f.ids.slice(25,41));
  expect(first.selection).toMatchObject({totalRows:45,includedRows:24,truncated:true,omittedRows:21});
  const second=await readDeliveryConsumptionWindow(pool,f.goal,'scope/b',first.selection.nextCursor);
  expect(second.rows.slice(0,4).map(r=>r.id)).toEqual(f.ids.slice(41));
  expect(second.rows.map(r=>r.id)).toContain(f.ids[24]);
  const changed=await readDeliveryConsumptionWindow(pool,f.goal,'scope/a',first.selection.nextCursor);
  expect(changed.selection.focal?.offset).toBe(0);
  expect(changed.rows.slice(0,16).map(r=>r.id)).toEqual(f.ids.slice(0,16));
  expect((await pool.query('SELECT * FROM delivery_consumptions ORDER BY id')).rows).toEqual(before);
});
it('rejects mixed-generation pages but ignores observation-only checked/updated timestamps',async()=>{
  const f=await fixture(12),first=await readDeliveryConsumptionPage(pool,f.goal);
  await pool.query('UPDATE delivery_consumptions SET checked_at=now(),updated_at=now() WHERE goal_id=$1',[f.goal]);
  expect((await readDeliveryConsumptionPage(pool,f.goal,{offset:10,expectedDigest:first.snapshotDigest})).status).toBe('OK');
  await pool.query("UPDATE delivery_consumptions SET receipt=receipt||'{\"code\":\"PRODUCER_CONTRACT_CHANGED\"}'::jsonb WHERE id=$1",[f.ids[11]]);
  expect(await readDeliveryConsumptionPage(pool,f.goal,{offset:10,expectedDigest:first.snapshotDigest})).toMatchObject({status:'STALE_CATALOG',rows:[],nextOffset:0});
  const exact=await readDeliveryConsumptionPage(pool,f.goal,{obligationId:f.ids[0]});
  await pool.query("UPDATE delivery_consumptions SET created_at=created_at+interval '1 microsecond' WHERE id=$1",[f.ids[0]]);
  expect((await readDeliveryConsumptionPage(pool,f.goal,{obligationId:f.ids[0],expectedDigest:exact.snapshotDigest})).status).toBe('STALE_CATALOG');
});
it('keeps huge receipts discoverable and reconstructs exact UTF8 JSON pages without timestamp drift or artifact reads',async()=>{
  const f=await fixture(),receipt={code:'SOURCE_CONFLICT',owner:'supervisor',nextAction:'Resolve exact source.',
    sourceConflict:{instruction:'linha "\\\n ü 😀'.repeat(1800),generation:'b'.repeat(64)}};
  await pool.query('UPDATE delivery_consumptions SET receipt=$2 WHERE id=$1',[f.ids[0],JSON.stringify(receipt)]);
  await request(f.goal,[{kind:'delivery_consumptions',offset:0}]);
  const directory=await readManagementRetrieval(pool,f.goal,noArtifact);
  expect(directory.results[0]).toMatchObject({status:'OK',rows:[{id:f.ids[0],receipt:{code:'SOURCE_CONFLICT',owner:'supervisor'},
    receiptProjection:{recovery:{kind:'delivery_consumption',obligationId:f.ids[0]}}}]});
  let offset:number|null=0,text='',snapshotDigest:string|undefined,digest:string|undefined;
  do {await request(f.goal,[{kind:'delivery_consumption',obligationId:f.ids[0],offset,snapshotDigest}]);
    const result=await readManagementRetrieval(pool,f.goal,noArtifact),page=result.results[0];
    expect(page.status).toBe('OK');expect(JSON.stringify(result).length).toBeLessThanOrEqual(managementRetrievalCharacterLimit);
    snapshotDigest??=page.snapshotDigest;digest??=page.digest;expect(page.digest).toBe(digest);
    text+=page.text;offset=page.nextOffset;
    await pool.query('UPDATE delivery_consumptions SET checked_at=now(),updated_at=now() WHERE id=$1',[f.ids[0]]);
  }while(offset!==null);
  expect(hash(text)).toBe(digest);expect(JSON.parse(text).receipt).toEqual(receipt);
  expect(JSON.parse(text)).toEqual(JSON.parse(JSON.stringify(deliveryConsumptionRecord((await readDeliveryConsumptionPage(pool,f.goal)).rows[0]))));
  await request(f.goal,[{kind:'delivery_consumption',obligationId:f.ids[0],offset:1}]);
  expect((await readManagementRetrieval(pool,f.goal,noArtifact)).results[0].status).toBe('CATALOG_DIGEST_REQUIRED');
});

it('keeps scope ownership after plan publication and requires action for the exact blocked consumer',async()=>{
  const f=await fixture(),planner=randomUUID(),run=randomUUID(),requestId=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria)
    VALUES($1,$2,$3,'Planner','Plan valid work','engineering.integration','accepted','coordinate','[]')`,[planner,f.goal,planner]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'submitted')",[run,planner,randomUUID()]);
  await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,receipt)
    VALUES($1,$2,'scope/a','glm','rev','applied','Scoped plan','{}','{}')`,[requestId,f.goal]);
  await pool.query(`INSERT INTO delivery_transitions(id,goal_id,review_run_id,scope,state,proposal,receipt,planning_request_id)
    VALUES($1,$2,$3,'scope/a','tracking','{}','{}',$4)`,[randomUUID(),f.goal,run,requestId]);
  expect(await readManagementFollowThrough(pool,f.goal,run)).toMatchObject({code:'MANAGEMENT_SCOPE_WAIT',satisfied:true,owner:'consumer',obligationIds:f.ids});
  await pool.query("UPDATE delivery_consumptions SET state='blocked',receipt='{\"code\":\"PRODUCER_CONTRACT_CHANGED\",\"owner\":\"supervisor\"}' WHERE id=$1",[f.ids[0]]);
  expect(await readManagementFollowThrough(pool,f.goal,run)).toMatchObject({code:'MANAGEMENT_SCOPE_CONSUMPTION_REQUIRED',satisfied:false,blockedTaskIds:[f.consumer]});
  await pool.query("UPDATE planning_requests SET state='replanning_required' WHERE id=$1",[requestId]);
  expect(await readManagementFollowThrough(pool,f.goal,run)).toMatchObject({code:'MANAGEMENT_PLAN_EFFECT_REQUIRED',satisfied:false});
  expect((await pool.query('SELECT 1 FROM acceptances')).rowCount).toBe(0);
});

it('coalesces only exact qualified waits; semantic evidence after the 24th row still changes materiality',async()=>{
  const f=await fixture(26),first=await readManagementMateriality(pool,f.goal,'capacity');
  await pool.query("UPDATE runs SET heartbeat_at=now(),summary='Observation text only' WHERE task_id IN(SELECT id FROM tasks WHERE goal_id=$1)",[f.goal]);
  await pool.query('UPDATE delivery_consumptions SET updated_at=now(),checked_at=now() WHERE goal_id=$1',[f.goal]);
  expect(await readManagementMateriality(pool,f.goal,'capacity')).toBe(first);
  const previous={runId:'review',reviewedAt:new Date().toISOString(),wakeMateriality:first,contextRequests:[]};
  const wait={code:'MANAGEMENT_WAIT_CAPACITY',satisfied:true},conditions={runId:'review',runState:'submitted',openCases:0,contextRepair:false,refill:false};
  expect(canWaitForManagementFacts(previous,first,wait,conditions)).toBe(true);
  for(const changed of [{runState:'failed'},{openCases:1},{contextRepair:true},{refill:true},{runId:'other'}])
    expect(canWaitForManagementFacts(previous,first,wait,{...conditions,...changed})).toBe(false);
  expect(canWaitForManagementFacts({...previous,contextRequests:[{kind:'task'}]},first,wait,conditions)).toBe(false);
  expect(canWaitForManagementFacts(previous,first,{code:'MANAGEMENT_PLAN_EFFECT_REQUIRED',satisfied:false},conditions)).toBe(false);
  await pool.query("UPDATE delivery_consumptions SET receipt=receipt||'{\"code\":\"NEW_EXACT_CONFLICT\"}'::jsonb WHERE id=$1",[f.ids[25]]);
  const changed=await readManagementMateriality(pool,f.goal,'capacity');expect(changed).not.toBe(first);
  expect(canWaitForManagementFacts(previous,changed,wait,conditions)).toBe(false);
  await pool.query('UPDATE tasks SET description=description||$2 WHERE id=$1',[f.consumer,' Semantic requirement.']);
  expect(await readManagementMateriality(pool,f.goal,'capacity')).not.toBe(changed);
  const beforeAcceptance=await readManagementMateriality(pool,f.goal,'capacity');
  const source=(await pool.query('SELECT producer_task_id,producer_run_id FROM delivery_consumptions WHERE id=$1',[f.ids[25]])).rows[0];
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,'[{\"code\":\"NEW_CRITERION_FAILURE\"}]','fixture')",
    [randomUUID(),source.producer_task_id,source.producer_run_id]);
  expect(await readManagementMateriality(pool,f.goal,'capacity')).not.toBe(beforeAcceptance);
});

it('refuses idle coalescing for an actionable obligation outside the focal window, except a physically owned live wait',async()=>{
  const f=await fixture(45),id=f.ids[44];
  await pool.query("UPDATE delivery_consumptions SET state='blocked',receipt='{\"code\":\"SOURCE_CONTRACT_CHANGED\"}' WHERE id=$1",[id]);
  const window=await readDeliveryConsumptionWindow(pool,f.goal,'scope/a');
  expect(window.rows.some(r=>r.id===id)).toBe(false);
  const baseline=await readManagementMateriality(pool,f.goal);
  const previous={runId:'review',reviewedAt:'2026-01-01',wakeMateriality:baseline,contextRequests:[]};
  const input={runId:'review',runState:'submitted',openCases:0,contextRepair:false,refill:false,
    actionableObligations:await hasActionableManagementConsumption(pool,f.goal)};
  expect(input.actionableObligations).toBe(true);
  expect(canWaitForManagementFacts(previous,baseline,{code:'MANAGEMENT_WAIT_CAPACITY',satisfied:true},input)).toBe(false);
  await pool.query("UPDATE delivery_consumptions SET receipt='{\"code\":\"SOURCE_REVIEW_WAIT_CONSUMER\"}' WHERE id=$1",[id]);
  expect(await hasActionableManagementConsumption(pool,f.goal)).toBe(true);
  const run=randomUUID();await pool.query("UPDATE tasks SET attempt_count=1,state='running' WHERE id=$1",[f.consumer]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[run,f.consumer,randomUUID()]);
  expect(await hasActionableManagementConsumption(pool,f.goal)).toBe(false);
  await pool.query("UPDATE delivery_consumptions SET receipt='{}' WHERE id=$1",[id]);
  expect(await hasActionableManagementConsumption(pool,f.goal)).toBe(true);
  await pool.query("UPDATE delivery_consumptions SET receipt='{\"code\":\"SOURCE_REVIEW_WAIT_CONSUMER\"}' WHERE id=$1",[id]);
  await pool.query("UPDATE runs SET state='failed' WHERE id=$1",[run]);
  expect(await hasActionableManagementConsumption(pool,f.goal)).toBe(true);
});
