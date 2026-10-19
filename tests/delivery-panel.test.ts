import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision} from '../server/supervision.js';
import {deliveryPanel} from '../server/delivery-panel.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await installSupervision();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(()=>pool.end());

it('shows the technical case owner without treating an old passed attempt as current delivery',async()=>{
  const goal=randomUUID(),task=randomUUID(),oldRun=randomUUID(),run=randomUUID(),caseId=randomUUID();
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1::uuid,'Delivery panel','Delivery panel',$1::text,$1::text,'active')`,[goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1,$2,'integrate','Integrate','Integrate','engineering.backend.node','submitted','integrate','[]','{}',2)`,[task,goal]);
  for(const [id,attempt] of [[oldRun,1],[run,2]])await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,$3,$4,'harness',1,'submitted')`,[id,task,attempt,randomUUID()]);
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
    VALUES($1,$2,$3,true,'[]','test')`,[randomUUID(),task,oldRun]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
    VALUES($1,$2,$3,$4,'decided','Private long reason','[]')`,[caseId,goal,task,run]);
  const pending=await deliveryPanel(pool,goal);
  expect(pending.accepted).toEqual([]);
  expect(pending.cases).toEqual([expect.objectContaining({task_id:task,state:'decided',total:1,case_id:caseId})]);
  expect(JSON.stringify(pending)).not.toContain('Private long reason');
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
    VALUES($1,$2,$3,true,'[]','test')`,[randomUUID(),task,run]);
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[task]);
  expect((await deliveryPanel(pool,goal)).accepted).toEqual([expect.objectContaining({task_id:task})]);
  expect((await deliveryPanel(pool,randomUUID())).cases).toEqual([]);
});
