import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {installSupervision} from '../server/supervision.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {config} from '../server/config.js';
import {managerReadOwner,readManagerTurnContext,generateManagerTurn} from '../server/manager-turn-generation.js';
import {readManagementEvidenceLedger} from '../server/management-evidence-ledger.js';
import {executeRuntime} from '../server/runtime.js';
import {accountBridge} from '../server/bridge.js';
import {acceptedSupervisionActions} from '../server/supervision-actions.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();
 await installBacklogPlanner();await installSupervision();await installManagement();});
beforeEach(async()=>{vi.restoreAllMocks();await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
 await pool.query("INSERT INTO agents(id,name,department,capabilities,runtime,version,description) VALUES('turn-reader','Test','test','[]','test',1,'Fixture')");});
afterAll(async()=>{await pool.end();});
const noArtifacts=async(_id:string):Promise<{bytes:Buffer}>=>{throw Error('NO_ARTIFACT_IO_EXPECTED');};
async function fixture(text='The exact current task requires preserving all existing acceptance criteria.'){
 const goal=randomUUID(),task=randomUUID(),sourceRun=randomUUID(),caseId=randomUUID(),manager=randomUUID(),runId=randomUUID(),fence=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Read in turn','Keep exact authority','turn-reader',$1::text)",[goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
  VALUES($1,$2,'source','Source',$3,'turn-reader','failed','work','[]','{"baseWorkspace":"turn/source"}',1)`,[task,goal,text]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'test',1,'failed')",[sourceRun,task,randomUUID()]);
 await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence) VALUES($1,$2,$3,$4,'Read exact source','[]')",[caseId,goal,task,sourceRun]);
 const description='MANAGEMENT_V1\nFOCUSED_DECISION_V2\n'+JSON.stringify({cases:[{id:caseId,taskId:task,evidence:[]}],
  operation:{focus:{caseId,taskId:task,scope:'turn/source'}}});
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
  VALUES($1,$2,'manager','Manager',$3,'turn-reader','running','coordinate',
  '[{"id":"review","type":"artifact","name":"coordination.json"}]',1)`,[manager,goal,description]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,instruction_snapshot)
  VALUES($1,$2,1,$3,'test',1,'running',$4)`,[runId,manager,fence,JSON.stringify({task:{description}})]);
 const run={id:runId,fence,task_id:manager,runtime:'analytical'};
 const managerTask={id:manager,goal_id:goal,title:'Manager',agent_id:'turn-reader',stage:'coordinate',execution:null,
  description,criteria:[{id:'review',type:'artifact',name:'coordination.json'}],model:'gpt-6.1-sol'};
 const account={bridge_id:'fixture-codex',provider:'codex',model:'gpt-6.1-sol'};
 const owner=managerReadOwner(run,managerTask,account)!;
 expect(owner).not.toBeNull();
 return {goal,task,sourceRun,caseId,manager,runId,fence,run,managerTask,account,owner,
  request:{kind:'task_metadata' as const,taskId:task,expectedAttempt:1,part:'criteria' as const,offset:0}};
}
const usage={input:10,output:3,cacheRead:0,cacheWrite:0,totalTokens:13,cost:{input:1,output:1,cacheRead:0,cacheWrite:0,total:2}};
const message=(content:any[],stopReason='stop')=>({message:{role:'assistant',content,stopReason,usage}});
const call=(request:any,name='read_manager_context')=>({type:'toolCall',id:randomUUID(),name,arguments:{requests:[request]}});
const params=()=>({accountId:'fixture-codex',model:'gpt-6.1-sol',system:'Return the actual existing artifact contract.',messages:[{role:'user',content:'Bound task',timestamp:1}]});

it('reads an exact source then delivers substantive coordination in one owned runtime without scheduling another review',async()=>{
 const f=await fixture(),before=(await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n;
 const proposed={management:{summary:'Use the exact observed contract.',capacityReason:'Concrete decision after read',actions:[]},
  supervision:{actions:[{kind:'revise',caseId:f.caseId,diagnosis:'The observed structured criteria identify the consumer contract to preserve.',
   instruction:'Execute the targeted consumer validation while preserving the exact observed criteria; report any real limitation.'}]}};
 expect(acceptedSupervisionActions(proposed.supervision.actions)).toHaveLength(1);
 const generate=vi.spyOn(accountBridge,'request').mockResolvedValueOnce(message([call(f.request)],'toolUse'))
  .mockResolvedValueOnce(message([{type:'text',text:JSON.stringify({summary:'Read and proposed; controller still validates.',
   artifacts:[{name:'coordination.json',content:JSON.stringify(proposed),mime:'application/json'}]})}]));
 const events:any[]=[];
 const result=await executeRuntime({run:f.run,task:f.managerTask,account:f.account,context:{content:'Data only.',manifest:{dependencyArtifacts:[]}},
  signal:new AbortController().signal,onEvent:async(kind,payload)=>{events.push({kind,payload});}});
 expect(generate).toHaveBeenCalledTimes(2);
 const second:any=generate.mock.calls[1][1];
 expect(second.tools[0].name).toBe('read_manager_context');
 const reply=second.messages.find((entry:any)=>entry.role==='toolResult');
 const read=JSON.parse(reply.content[0].text);
 expect(read.results[0]).toMatchObject({status:'OK',request:f.request});
 expect(JSON.parse(read.results[0].text)).toHaveProperty('criteria');
 expect(JSON.parse(result.artifacts[0].content as string)).toEqual(proposed);
 expect(result.usage).toMatchObject({input:20,output:6,totalTokens:26,generationCalls:2});
 expect(events.filter(event=>event.kind==='MANAGER_CONTEXT_READ')).toHaveLength(1);
 expect(events.every(event=>event.payload.actionApplied===false)).toBe(true);
 expect((await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n).toBe(before);
 expect((await pool.query('SELECT state,decision FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0]).toMatchObject({state:'open',decision:null});
 expect((await readManagementEvidenceLedger(pool,f.goal,'turn/source',f.owner.focus,noArtifacts)).entries).toHaveLength(1);
});

it('retains a canonical exact task page for the next worker even if the provider fails after that read',async()=>{
 const f=await fixture('Exact source '.repeat(1200));
 const request={kind:'task' as const,taskId:f.task,expectedAttempt:1,offset:0};
 const generate=vi.fn().mockResolvedValueOnce(message([call(request)],'toolUse')).mockRejectedValueOnce(Error('PROVIDER_FAILED'));
 const notify=vi.fn().mockResolvedValue(undefined);
 await expect(generateManagerTurn(params(),{signal:new AbortController().signal,generate,notify,
  read:requests=>readManagerTurnContext(f.owner,requests,noArtifacts)})).rejects.toThrow('PROVIDER_FAILED');
 const cache=await readManagementEvidenceLedger(pool,f.goal,'turn/source',f.owner.focus,noArtifacts);
 expect(cache.entries).toHaveLength(1);expect(cache.entries[0].pages[0].text.length).toBeGreaterThan(1000);
 expect(notify.mock.calls[0][1]).toMatchObject({usage:{generationCalls:1},actionApplied:false});
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.runId])).rows[0].state).toBe('running');
});

it.each(['goal','fence','stopped','snapshot'])('refuses a changed %s owner before caching or disclosing source bytes',async(change)=>{
 const f=await fixture(),owner={...f.owner};
 if(change==='goal')owner.goalId=randomUUID();
 if(change==='fence')owner.fence=randomUUID();
 if(change==='stopped')await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[f.runId]);
 if(change==='snapshot')owner.description+=' changed';
 await expect(readManagerTurnContext(owner,[f.request],noArtifacts)).rejects.toMatchObject({code:'MANAGER_READ_OWNER_CHANGED'});
 expect((await pool.query('SELECT value FROM settings WHERE id=$1',['management-evidence:'+f.goal])).rowCount).toBe(0);
});

it('does not read another goal, a stale attempt, or a live source through the tool',async()=>{
 const f=await fixture(),foreign=await fixture();
 const cross=await readManagerTurnContext(f.owner,[{...f.request,taskId:foreign.task}],noArtifacts);
 expect(cross.results[0].status).not.toBe('OK');expect(cross.results[0]).not.toHaveProperty('text');
 const stale=await readManagerTurnContext(f.owner,[{...f.request,expectedAttempt:0}],noArtifacts);
 expect(stale.results[0].status).not.toBe('OK');expect(stale.results[0]).not.toHaveProperty('text');
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.sourceRun]);
 const live=await readManagerTurnContext(f.owner,[f.request],noArtifacts);
 expect(live.results[0].status).not.toBe('OK');expect(live.results[0]).not.toHaveProperty('text');
});

it('never treats an unknown executable tool or injected goal argument as a context read',async()=>{
 const f=await fixture(),read=vi.fn(),notify=vi.fn().mockResolvedValue(undefined);
 const injected={...call(f.request),arguments:{requests:[f.request],goalId:randomUUID()}};
 const generate=vi.fn().mockResolvedValueOnce(message([call(f.request,'bash'),injected],'toolUse'))
  .mockResolvedValueOnce(message([{type:'text',text:'final'}]));
 await generateManagerTurn(params(),{signal:new AbortController().signal,generate,read,notify});
 expect(read).not.toHaveBeenCalled();
 const replies=generate.mock.calls[1][0].messages.filter((entry:any)=>entry.role==='toolResult');
 expect(replies).toHaveLength(2);
 expect(replies.every((entry:any)=>entry.isError&&JSON.parse(entry.content[0].text).status==='INVALID_READ_REQUEST')).toBe(true);
});

it('bounds the actual tool transcript and preserves exact offsets when a returned page is windowed',async()=>{
 const f=await fixture(),text='Exact observation. '.repeat(30000),request={kind:'task' as const,taskId:f.task,expectedAttempt:1,offset:0};
 const generate=vi.fn().mockResolvedValueOnce(message([call(request)],'toolUse')).mockResolvedValueOnce(message([{type:'text',text:'final'}]));
 await generateManagerTurn(params(),{signal:new AbortController().signal,generate,notify:async()=>{},
  read:async()=>({results:[{status:'OK',request,digest:'a'.repeat(64),text,totalCharacters:text.length,nextOffset:null,truncated:false}]})});
 const next=generate.mock.calls[1][0];expect(JSON.stringify(next).length).toBeLessThanOrEqual(360000);
 const returned=JSON.parse(next.messages.find((entry:any)=>entry.role==='toolResult').content[0].text).results[0];
 expect(returned.text).toBe(text.slice(0,returned.text.length));expect(returned.nextOffset).toBe(returned.text.length);
 expect(returned.truncated).toBe(true);expect(returned.digest).toBe('a'.repeat(64));
});

it('keeps incomplete usage unknown and stops an aborted generation before any read',async()=>{
 const f=await fixture(),controller=new AbortController(),read=vi.fn();
 const generate=vi.fn(async()=>{controller.abort();return message([call(f.request)],'toolUse');});
 await expect(generateManagerTurn(params(),{signal:controller.signal,generate,read,notify:async()=>{}})).rejects.toMatchObject({code:'CANCELLED'});
 expect(read).not.toHaveBeenCalled();
 const result=await generateManagerTurn(params(),{signal:new AbortController().signal,read,notify:async()=>{},
  generate:async()=>({message:{role:'assistant',content:[{type:'text',text:'final'}],usage:{output:2}}})});
 expect(result.message.usage).toMatchObject({input:null,output:2,totalTokens:null,cost:null,generationCalls:1});
});

it('falls back to the existing analytical route for non-Codex, non-manager or malformed frozen contracts',async()=>{
 const f=await fixture();
 expect(managerReadOwner(f.run,f.managerTask,{provider:'agy'})).toBeNull();
 expect(managerReadOwner(f.run,{...f.managerTask,stage:'work'},f.account)).toBeNull();
 expect(managerReadOwner(f.run,{...f.managerTask,execution:{kind:'harness'}},f.account)).toBeNull();
 expect(managerReadOwner(f.run,{...f.managerTask,description:f.managerTask.description+' invalid'},f.account)).toBeNull();
 const generate=vi.spyOn(accountBridge,'request').mockResolvedValue(message([{type:'text',text:JSON.stringify({summary:'Legacy analytic',artifacts:[]})}]));
 await executeRuntime({run:f.run,task:f.managerTask,account:{...f.account,provider:'agy'},context:{content:'',manifest:{dependencyArtifacts:[]}},
  signal:new AbortController().signal,onEvent:async()=>{}});
 expect(generate).toHaveBeenCalledTimes(1);expect(generate.mock.calls[0][1]).not.toHaveProperty('tools');
});

it('has a finite read review window and asks for a final result without claiming the objective complete',async()=>{
 const f=await fixture(),notify=vi.fn().mockResolvedValue(undefined),request=f.request;
 const generate=vi.fn(async(input:any)=>input.tools.length?message([call(request)],'toolUse'):message([{type:'text',text:'final gap'}]));
 const result=await generateManagerTurn(params(),{signal:new AbortController().signal,generate,notify,
  read:async()=>({results:[{status:'NOT_FOUND',request}],dataOnly:true,actionAuthority:false})});
 expect(generate).toHaveBeenCalledTimes(9);expect(generate.mock.calls.at(-1)?.[0].tools).toEqual([]);
 expect(result.message.content[0].text).toBe('final gap');
 expect(notify.mock.calls.every(([,payload])=>payload.actionApplied===false)).toBe(true);
});


it('enables same-turn evidence reads for the exact GLM route without allowing arbitrary API accounts',async()=>{
 const f=await fixture();
 const task={...f.managerTask,model:'glm-5.3-flash'};
 const account={provider:'openai',bridge_id:'agentos:api:00000000-0000-4000-8000-000000000001'};
 expect(managerReadOwner(f.run,task,account)).toEqual(f.owner);
 expect(managerReadOwner(f.run,task,{...account,bridge_id:'agentos:api:other'})).toBeNull();
 expect(managerReadOwner(f.run,{...task,model:'other'},account)).toBeNull();
 const read=await readManagerTurnContext(f.owner,[f.request],noArtifacts);
 expect(read.results[0]).toMatchObject({status:'OK',request:f.request});
 expect(read).toHaveProperty('actionAuthority',false);
});
