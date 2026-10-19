import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision} from '../server/supervision.js';
import {installDeliveryTransitions,reconcileDeliveryTransitions} from '../server/delivery-transitions.js';
beforeAll(async()=>{await migrate();await installSupervision();await installDeliveryTransitions();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});
async function fixture(){
  const goal=randomUUID(),task=randomUUID(),review=randomUUID(),run=randomUUID(),transition=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Delivery comparison','Keep real state','test',$2)",[goal,goal]);
  for(const [id,stage] of [[task,'integrate'],[review,'coordinate']])await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria)
    VALUES($1,$2,$3,'Tracked delivery','Original contract','engineering.integration','recovery_blocked',$4,'[]')`,[id,goal,id,stage]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'submitted')",[run,review,randomUUID()]);
  await pool.query(`INSERT INTO delivery_transitions(id,goal_id,review_run_id,task_id,expected_attempt,state,proposal)
    VALUES($1,$2,$3,$4,0,'tracking','{"expectedArtifact":"verification.json"}')`,[transition,goal,run,task]);
  const ids=[randomUUID(),randomUUID()].sort().reverse();
  for(const [index,id] of ids.entries())await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,state,reason,evidence) VALUES($1,$2,$3,$4,'Inspect real failed delivery','[]')",[id,goal,task,index===0?'open':'waiting_evidence']);
  return {goal,task,transition,cases:ids};
}
it('does not write or emit another transition event for semantically identical JSONB receipts',async()=>{
  const f=await fixture();await reconcileDeliveryTransitions();
  const first=(await pool.query('SELECT receipt,updated_at,xmin::text version FROM delivery_transitions WHERE id=$1',[f.transition])).rows[0];
  expect(first.receipt.code).toBe('SUPERVISION_REQUIRED');
  expect(first.receipt.cases.map((c:any)=>c.id)).toEqual([...f.cases].sort());
  // Prove the live incident's precondition: identical semantic data is emitted
  // in a different object key order than PostgreSQL's JSONB representation.
  const rebuilt={code:first.receipt.code,taskId:first.receipt.taskId,attempt:first.receipt.attempt,cases:first.receipt.cases};
  expect(JSON.stringify(rebuilt)).not.toBe(JSON.stringify(first.receipt));
  await reconcileDeliveryTransitions();await reconcileDeliveryTransitions();
  const next=(await pool.query('SELECT receipt,updated_at,xmin::text version FROM delivery_transitions WHERE id=$1',[f.transition])).rows[0];
  expect(next).toEqual(first);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_TRANSITION_CHANGED' AND payload->>'transitionId'=$1",[f.transition])).rowCount).toBe(1);
});
it('records genuine case/state changes exactly once after a no-op observation',async()=>{
  const f=await fixture();await reconcileDeliveryTransitions();
  await pool.query("UPDATE supervision_cases SET state='waiting_evidence' WHERE id=$1",[f.cases[0]]);
  await reconcileDeliveryTransitions();await reconcileDeliveryTransitions();
  expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_TRANSITION_CHANGED'")).rowCount).toBe(2);
  await pool.query("UPDATE tasks SET state='blocked_dependencies' WHERE id=$1",[f.task]);
  await reconcileDeliveryTransitions();await reconcileDeliveryTransitions();
  expect((await pool.query('SELECT state,receipt FROM delivery_transitions WHERE id=$1',[f.transition])).rows[0]).toMatchObject({state:'tracking',receipt:{code:'TASK_PENDING'}});
  expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_TRANSITION_CHANGED'")).rowCount).toBe(3);
});
