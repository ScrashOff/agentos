import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,tx} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installDeferredHandoffs} from '../server/deferred-handoffs.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {selectDeliveryConsumptionBatch} from '../server/delivery-consumption-selection.js';

beforeAll(async()=>{await migrate();await installTaskExecutionMetadata();await installDeferredHandoffs();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});

// Queue-only PG fixtures: each obligation has its own real FK rows. These rows
// intentionally provide no accepted artifact proof; selection must not create
// acceptance, dispatch or a consumption receipt from the scalar hints.
async function queue(size:number,priority:number[]=[]){
 const goal=randomUUID(),project='selection-'+goal;
 const entries=Array.from({length:size},(_,index)=>({id:randomUUID(),handoff:randomUUID(),producer:randomUUID(),
  consumer:randomUUID(),producerRun:randomUUID(),consumerRun:randomUUID(),index,
  state:priority.includes(index)?'executing':'waiting_supervision',
  consumerState:priority.includes(index)?'accepted':'running',runState:priority.includes(index)?'submitted':'running'}));
 await tx(async db=>{
  await db.query(`INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Queue','Selection only',$2::text,$1::uuid::text)`,[goal,project]);
  const rows=JSON.stringify(entries);
  await db.query(`WITH x AS(SELECT * FROM jsonb_to_recordset($2::jsonb) AS e(producer uuid,consumer uuid,"consumerState" text))
   INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
   SELECT producer,$1::uuid,producer::text,'Producer','Queue fixture','engineering.integration','failed','work','[]'::jsonb,1 FROM x
   UNION ALL SELECT consumer,$1::uuid,consumer::text,'Consumer','Queue fixture','engineering.integration',"consumerState",'work','[]'::jsonb,1 FROM x`,[goal,rows]);
  await db.query(`WITH x AS(SELECT * FROM jsonb_to_recordset($1::jsonb)
    AS e(producer uuid,consumer uuid,"producerRun" uuid,"consumerRun" uuid,"runState" text))
   INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
   SELECT "producerRun",producer,1,"producerRun",'harness',1,'failed' FROM x
   UNION ALL SELECT "consumerRun",consumer,1,"consumerRun",'harness',1,"runState" FROM x`,[rows]);
  await db.query(`INSERT INTO deferred_handoffs(id,goal_id,producer_task_id,consumer_task_id,source_task_id,source_proof,consumer_proof,handoff,state)
   SELECT handoff,$1::uuid,producer,consumer,producer,'{}','{}','{}','applied'
   FROM jsonb_to_recordset($2::jsonb) AS e(handoff uuid,producer uuid,consumer uuid)`,[goal,rows]);
  await db.query(`INSERT INTO delivery_consumptions(id,handoff_id,goal_id,producer_task_id,producer_run_id,consumer_task_id,
    consumer_contract_digest,producer_manifest_digest,binding,state,created_at,checked_at)
   SELECT id,handoff,$1::uuid,producer,"producerRun",consumer,repeat('a',64),repeat('b',64),'{}',state,
     '2020-01-01'::timestamptz+index*interval '1 second','2020-01-01'::timestamptz+index*interval '1 second'
   FROM jsonb_to_recordset($2::jsonb) AS e(id uuid,handoff uuid,producer uuid,"producerRun" uuid,consumer uuid,state text,index int)`,[goal,rows]);
 });
 return {goal,project,entries};
}

it('selects an accepted consumer at position272 of319 promptly while retaining the oldest fair obligation',async()=>{
 const f=await queue(319,[270,271,318]);
 const old=(await pool.query(`SELECT id FROM delivery_consumptions ORDER BY checked_at NULLS FIRST,created_at,id`)).rows;
 expect(old[271].id).toBe(f.entries[271].id);
 const before=(await pool.query(`SELECT id,state,receipt,checked_at FROM delivery_consumptions ORDER BY id`)).rows;
 const selected=await selectDeliveryConsumptionBatch(pool);
 expect(selected.map(r=>r.id)).toEqual([f.entries[270].id,f.entries[271].id,f.entries[318].id,f.entries[0].id]);
 expect(new Set(selected.map(r=>r.id)).size).toBe(4);
 expect((await pool.query(`SELECT id,state,receipt,checked_at FROM delivery_consumptions ORDER BY id`)).rows).toEqual(before);
 expect((await pool.query('SELECT 1 FROM acceptances')).rowCount).toBe(0);
 expect((await pool.query('SELECT 1 FROM events')).rowCount).toBe(0);
});

it('preserves fair service when the same three terminal hints remain unchanged across passages',async()=>{
 const f=await queue(12,[9,10,11]),visited=new Set<string>();
 for(let passage=0;passage<9;passage++){
  const selected=await selectDeliveryConsumptionBatch(pool);
  expect(selected).toHaveLength(4);expect(new Set(selected.map(r=>r.id)).size).toBe(4);
  expect(selected.slice(0,3).map(r=>r.id).sort()).toEqual(f.entries.slice(9).map(r=>r.id).sort());
  visited.add(selected[3].id);
  // Same checked_at stamp the unchanged reconciler writes after taking its
  // per-obligation lock. No receipt/actor/acceptance is fabricated here.
  await pool.query('UPDATE delivery_consumptions SET checked_at=clock_timestamp() WHERE id=ANY($1::uuid[])',[selected.map(r=>r.id)]);
 }
 expect([...visited].sort()).toEqual(f.entries.slice(0,9).map(r=>r.id).sort());
});

it.each([0,1,2,3,4])('fills the unchanged four-slot batch with %i priority candidates without duplicates',async priorities=>{
 const f=await queue(8,Array.from({length:priorities},(_,i)=>i+4));
 const selected=await selectDeliveryConsumptionBatch(pool);
 expect(selected).toHaveLength(4);expect(new Set(selected.map(r=>r.id)).size).toBe(4);
 const urgent=f.entries.slice(4,4+Math.min(priorities,3));
 expect(selected.slice(0,urgent.length).map(r=>r.id)).toEqual(urgent.map(r=>r.id));
 expect(selected.slice(urgent.length).map(r=>r.id)).toEqual(f.entries.slice(0,4-urgent.length).map(r=>r.id));
});

it.each([0,1,2,3])('returns only existing obligations for a queue of%i',async size=>{
 const f=await queue(size,size?[size-1]:[]),selected=await selectDeliveryConsumptionBatch(pool);
 expect(selected).toHaveLength(size);
 expect(new Set(selected.map(r=>r.id))).toEqual(new Set(f.entries.map(r=>r.id)));
});

it('keeps multiple obligations for one consumer independent and never duplicates an obligation',async()=>{
 const f=await queue(7,[4,5,6]),shared=f.entries[4].consumer;
 await pool.query('UPDATE delivery_consumptions SET consumer_task_id=$2 WHERE id=ANY($1::uuid[])',[[f.entries[5].id,f.entries[6].id],shared]);
 const selected=await selectDeliveryConsumptionBatch(pool);
 expect(selected.slice(0,3).map(r=>r.id)).toEqual(f.entries.slice(4).map(r=>r.id));
 expect(selected.slice(0,3).map(r=>r.consumer_task_id)).toEqual([shared,shared,shared]);
 expect(selected[3].id).toBe(f.entries[0].id);expect(new Set(selected.map(r=>r.id)).size).toBe(4);
});

it.each(['no_run','attempt_mismatch','queued','running','cancelling','reconciling','other_live_run','lease','task_paused'] as const)
('does not grant terminal priority for %s; the ordinary fair path remains available',async mode=>{
 const f=await queue(8,[4,5,6,7]),target=f.entries[4];
 if(mode==='no_run')await pool.query('DELETE FROM runs WHERE id=$1',[target.consumerRun]);
 else if(mode==='attempt_mismatch')await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[target.consumer]);
 else if(mode==='task_paused')await pool.query("UPDATE tasks SET state='paused' WHERE id=$1",[target.consumer]);
 else if(mode==='lease')await pool.query("INSERT INTO leases(resource,run_id) VALUES('selection-test',$1)",[target.consumerRun]);
 else if(mode==='other_live_run'){
  await pool.query("UPDATE runs SET state='running' WHERE id=$1",[target.consumerRun]);
  await pool.query("UPDATE tasks SET attempt_count=2 WHERE id=$1",[target.consumer]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,2,$1,'harness',1,'submitted')`,[randomUUID(),target.consumer]);
 }else await pool.query('UPDATE runs SET state=$2 WHERE id=$1',[target.consumerRun,mode]);
 expect((await selectDeliveryConsumptionBatch(pool)).map(r=>r.id)).toEqual([f.entries[5].id,f.entries[6].id,f.entries[7].id,f.entries[0].id]);
 await pool.query("UPDATE delivery_consumptions SET state='completed' WHERE id=ANY($1::uuid[])",[f.entries.filter(e=>e.id!==target.id).map(e=>e.id)]);
 expect((await selectDeliveryConsumptionBatch(pool)).map(r=>r.id)).toEqual([target.id]);
});

it.each(['completed','superseded','goal_paused','autonomy_disabled'] as const)('retains existing selection exclusions for%s',async mode=>{
 const f=await queue(4,[0,1,2,3]);
 if(mode==='goal_paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 else if(mode==='autonomy_disabled')await pool.query('INSERT INTO settings VALUES($1,$2)',['autonomy:'+f.project,JSON.stringify({enabled:false})]);
 else await pool.query('UPDATE delivery_consumptions SET state=$2 WHERE goal_id=$1',[f.goal,mode]);
 expect(await selectDeliveryConsumptionBatch(pool)).toEqual([]);
});
