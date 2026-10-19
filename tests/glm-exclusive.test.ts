import { afterAll, afterEach, beforeAll, beforeEach, expect, it } from 'vitest';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { readModelPolicy, assertModelRoute } from '../bridge/model-policy.mjs';
import { chooseWorkforceRoute } from '../server/workforce-policy.js';
// @ts-ignore JavaScript request gate is exercised without external transport.
import { withAccountRequestGate } from '../bridge/gated-generation.mjs';

const policy={version:1,mode:'glm-only',accountId:'agentos-api-00000000-0000-4000-8000-000000000001',
  bridgeId:'agentos:api:00000000-0000-4000-8000-000000000001',model:'glm-5.3-flash',requestLimit:6,sessionLimit:12};
let dir:string, originalFile:string|undefined, originalFlag:string|undefined;
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{
  originalFile=process.env.AGENTOS_MODEL_POLICY_FILE;originalFlag=process.env.AGENTOS_GLM_ONLY;
  dir=await mkdtemp(tmpdir()+'/agentos-policy-');process.env.AGENTOS_MODEL_POLICY_FILE=dir+'/policy.json';
  process.env.AGENTOS_GLM_ONLY='1';await writeFile(dir+'/policy.json',JSON.stringify(policy));
});
afterEach(async()=>{process.env.AGENTOS_MODEL_POLICY_FILE=originalFile;process.env.AGENTOS_GLM_ONLY=originalFlag;
  await rm(dir,{recursive:true,force:true});await pool.query("DELETE FROM settings WHERE id='model-policy'");});
afterAll(async()=>{await pool.end();});

it('fails closed for missing or corrupt required policy and never accepts a different provider',async()=>{
  expect(()=>assertModelRoute(policy.bridgeId,policy.model)).not.toThrow();
  for(const id of ['harness:openai-codex','agentos:muse:1',policy.bridgeId])
    expect(()=>assertModelRoute(id,'gpt-6.1-sol')).toThrow('MODEL_POLICY_GLM_ONLY');
  await writeFile(dir+'/policy.json','{}');expect(()=>readModelPolicy()).toThrow('MODEL_POLICY_INVALID');
  await rm(dir+'/policy.json');expect(()=>readModelPolicy()).toThrow('MODEL_POLICY_UNAVAILABLE');
});
it('routes coordinator, planner, worker, integrator and QA to the exact GLM account preserving explicit restrictions',()=>{
  const accounts=[{id:policy.accountId,bridge_id:policy.bridgeId,model:policy.model,models:[policy.model],enabled:true,allow_unknown:true,provider:'openai'},
    {id:'codex',bridge_id:'harness:openai-codex',model:'gpt-6.1-sol',models:['gpt-6.1-sol'],enabled:true,allow_unknown:true,provider:'codex'}];
  const workforce:any={enabled:true,glmAccountId:policy.accountId,glmTarget:12,workerAccounts:[],coordinatorAccountId:'codex',coordinatorModel:'gpt-6.1-sol'};
  for(const stage of ['coordinate','plan','work','integrate','verify'])
    expect(chooseWorkforceRoute({stage},accounts,workforce)).toEqual({accountId:policy.accountId,model:policy.model});
  expect(chooseWorkforceRoute({stage:'coordinate',allowed_accounts:['codex']},accounts,workforce)).toBeNull();
  expect(chooseWorkforceRoute({},[{...accounts[0],quota:{ordinaryUsageAllowed:false}}],workforce)).toBeNull();
});
it('rejects a forbidden model before acquiring permits or invoking a provider',async()=>{
  let effects=0;const operation=()=>{effects++;};
  expect(()=>withAccountRequestGate({accountId:'harness:openai-codex',model:'gpt-6.1-sol'},new AbortController().signal,operation)).toThrow('MODEL_POLICY_GLM_ONLY');
  expect(effects).toBe(0);
});
it('SQL refuses non-GLM new runs and serializes two GLM coordinators by role',async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',['model-policy',JSON.stringify(policy)]);
  const goal=randomUUID(),agent=(await pool.query('SELECT id FROM agents LIMIT 1')).rows[0].id;
  await pool.query("INSERT INTO accounts(id,label,provider,model) VALUES($1,'GLM','openai',$2)",[policy.accountId,policy.model]);
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'test','test','test',$1::text)",[goal]);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['workforce:'+goal,JSON.stringify({enabled:true,glmTarget:12})]);
  const ids=[randomUUID(),randomUUID()];
  for(const id of ids) await pool.query("INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,stage,account_id,model) VALUES($1::uuid,$2::uuid,$1::text,'test','test',$3,'[]','coordinate',$4,$5)",[id,goal,agent,policy.accountId,policy.model]);
  const run=(task:string,model=policy.model)=>pool.query("INSERT INTO runs(id,task_id,attempt,fence,account_id,runtime,agent_version,model) VALUES($1,$2,1,$3,$4,'harness',1,$5)",[randomUUID(),task,randomUUID(),policy.accountId,model]);
  await expect(run(ids[0],'gpt-6.1-sol')).rejects.toThrow('MODEL_POLICY_GLM_ONLY');
  const results=await Promise.allSettled(ids.map(id=>run(id)));
  expect(results.filter(r=>r.status==='fulfilled')).toHaveLength(1);
  const live=(await pool.query('SELECT id FROM runs WHERE task_id=ANY($1::uuid[])',[ids])).rows[0].id;
  await expect(pool.query("UPDATE runs SET model='gpt-6.1-sol' WHERE id=$1",[live])).rejects.toThrow('MODEL_POLICY_GLM_ONLY');
  expect(String((results.find(r=>r.status==='rejected') as PromiseRejectedResult).reason)).toContain('WORKFORCE_COORDINATOR_BUSY');
});


it('confirms no-start when the controller rejects a route before creating any SSH actor',async()=>{
 const {AccountBridge}=await import('../server/bridge.js');const bridge=new AccountBridge();
 bridge.connect=()=>{throw Error('TRANSPORT_MUST_NOT_START');};const events:any[]=[];
 await expect(bridge.request('worker.execute',{accountId:'harness:openai-codex',model:'gpt-6.1-sol'},
  {onEvent:(kind,payload)=>events.push({kind,payload})})).rejects.toThrow('MODEL_POLICY_GLM_ONLY');
 expect(events).toEqual([{kind:'HARNESS_NOT_STARTED',payload:{confirmed:true,phase:'model_policy',code:'MODEL_POLICY_GLM_ONLY'}}]);
});
