import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installTaskInstructions} from '../server/task-instructions.js';
import {assembleContext,saveMemory} from '../server/memory.js';
import {analyticalPrompt} from '../server/analytical-prompt.js';
import * as rank from '../server/context-rank.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await installTaskInstructions(pool);});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();config.recallUrl='';config.recallKey='';config.layaUrl='';});
afterAll(()=>pool.end());

async function fixture(stage='coordinate') {
 const goal=randomUUID(),task=randomUUID(),run=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Budget','Test','transport-test',$2)",[goal,goal]);
 const description='Original complete instruction. '+ 'd'.repeat(144000);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,stage,attempt_count)
  VALUES($1,$2,'manager','Budget manager',$3,'qa.backend','running','[]',$4,1)`,[task,goal,description,stage]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[run,task,randomUUID()]);
 return {task:(await pool.query('SELECT * FROM tasks WHERE id=$1',[task])).rows[0],run:(await pool.query('SELECT * FROM runs WHERE id=$1',[run])).rows[0]};
}

it('freezes a fitting context and matching selection without changing instructions, criteria or earlier evidence',async()=>{
 const f=await fixture();
 const decision=await saveMemory({project:'transport-test',title:'Mandatory decision',content:'r'.repeat(21347),kind:'decision',verification:'confirmed',source:'test:required'});
 const optional=await saveMemory({project:'transport-test',title:'Budget diagnosis',content:'"'.repeat(7900),kind:'observation',verification:'confirmed',source:'test:optional'});
 const ranking=vi.spyOn(rank,'rankContext').mockResolvedValue({[optional.id]:1});
 const previous=config.layaUrl;config.layaUrl='http://127.0.0.1:1';
 try {
  const before=await pool.query('SELECT description,criteria,required_memories FROM tasks WHERE id=$1',[f.task.id]);
  const context=await assembleContext(f.run,f.task,[]);
  const manifest=context.manifest as any;
  expect(manifest.selected.map((m:any)=>m.id)).toEqual([decision.id]);
  expect(context.content).toContain('r'.repeat(21347));
  expect(context.content).not.toContain(optional.id);
  expect(manifest.memorySelection.omitted).toContainEqual({id:optional.id,characters:7900,reason:'serialized_transport_budget'});
  expect(manifest.memorySelection.transportBudget.requiredFits).toBe(true);
  const payload=analyticalPrompt(f.run,f.task,context,[]);
  expect(payload.length).toBeLessThanOrEqual(180000);
  expect(manifest.memorySelection.transportBudget.payloadCharacters).toBe(payload.length);
  const stored=(await pool.query('SELECT content,manifest FROM contexts WHERE id=$1',[context.id])).rows[0];
  expect(stored.content).toBe(context.content);expect(stored.manifest).toEqual(manifest);
  expect((await pool.query('SELECT description,criteria,required_memories FROM tasks WHERE id=$1',[f.task.id])).rows).toEqual(before.rows);
  expect(JSON.parse(payload).description).toBe(f.task.description);
 }finally{ranking.mockRestore();config.layaUrl=previous;}
});

it('keeps worker memory and required overflow intact',async()=>{
 const f=await fixture('work');
 const decision=await saveMemory({project:'transport-test',title:'Required',content:'r'.repeat(50000),kind:'decision',verification:'confirmed',source:'test:required'});
 const context=await assembleContext(f.run,f.task,[]);
 expect(context.manifest.selected.map(m=>m.id)).toEqual([decision.id]);
 expect((context.manifest.memorySelection as any).transportBudget).toBeUndefined();
 expect(context.content).toContain('r'.repeat(50000));
});
