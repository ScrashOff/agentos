import {randomUUID} from 'node:crypto';
import {beforeAll,beforeEach,afterAll,expect,it} from 'vitest';
import {pool,migrate,type DB} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {readWorkforceHealth} from '../server/workforce-health.js';
beforeAll(async()=>{await migrate();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});
it('reads only current routing metadata beside large completed history while preserving an actual live actor',async()=>{
 const goal=randomUUID(),ready=randomUUID(),uncertain=randomUUID(),scalar=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Capacity','Capacity','test',$2)",[goal,'read-health-'+goal]);
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency)
   VALUES('glm-read-model','GLM','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,12)`);
 for(const state of ['accepted','cancelled','superseded'])
   await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution)
     SELECT gen_random_uuid(),$1,$2||'-'||n,'History','History','qa.backend',$2,'[]',
       jsonb_build_object('kind','opencode','unusedTranscript',repeat('immutable completed history ',10000))
     FROM generate_series(1,20) n`,[goal,state]);
 for(const [id,key,state,execution] of [
   [ready,'ready','ready',{kind:'opencode',workKind:'diagnostic',testFile:'data/check.mjs',protocolDelivery:'v1',
     backlogContext:{immutableSource:'fixture'},unusedTranscript:'irrelevant payload '.repeat(10000)}],
   [uncertain,'uncertain','accepted',{kind:'opencode',workKind:'diagnostic'}],
   [scalar,'scalar','recovery_blocked','legacy scalar'],
 ] as const)await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution)
   VALUES($1,$2,$3,'Current','Current','qa.backend',$4,'[]',$5)`,[id,goal,key,state,JSON.stringify(execution)]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,account_id)
   VALUES($1,$2,1,$3,1,'reconciling','opencode','glm-read-model')`,[randomUUID(),uncertain,randomUUID()]);
 let currentRows:any[]=[];
 const db={query:async(sql:string,args?:any[])=>{const result=await pool.query(sql,args);
   if(sql.includes('dependency_blocked'))currentRows=result.rows;return result;}} as DB;
 const health=await readWorkforceHealth(db,goal,{enabled:true,glmTarget:12,glmAccountId:'glm-read-model',
   workerAccounts:[],coordinatorAccountId:'glm-read-model',coordinatorModel:'gpt-6.1-sol'});
 expect(currentRows.map(row=>row.id).sort()).toEqual([ready,uncertain,scalar].sort());
 expect(currentRows.find(row=>row.id===ready).execution).toEqual({kind:'opencode',workforceLane:null,
   workKind:'diagnostic',testFile:'data/check.mjs',protocolDelivery:'v1',backlogContext:{}});
 expect(currentRows.find(row=>row.id===uncertain)).toMatchObject({has_live_run:true,live_state:'reconciling'});
 expect(Object.values(currentRows.find(row=>row.id===scalar).execution)).toEqual([null,null,null,null,null,null]);
 expect(health.glm.totalPending).toBe(1);
 expect((await pool.query("SELECT count(*)::int n FROM runs WHERE state='reconciling'")).rows[0].n).toBe(1);
});
