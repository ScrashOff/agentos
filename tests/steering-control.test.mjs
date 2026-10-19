import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {HarnessSdkJsonRpcServer} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-sdk-jsonrpc-server/lib/index.js';
import {createUserMessage} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-llm/lib/index.js';
import {SteeringControl} from '../bridge/sdk-control/steering-control.mjs';
function fixture({createGate}={}){
  const runId=randomUUID(),runFence=randomUUID(),registry=new Map(),events=[];
  const ctx={on(){return ()=>{};},get(){return {listProviders:()=>[{id:'test'}],resolveCallConfig:async()=>{}};},
    agents:{get:id=>registry.get(id),async create({sessionId}){
      if(createGate)await createGate;
      const agent={id:sessionId,session:{id:sessionId},status:'running',inbox:{nextStep:[],nextTurn:[]},
        followup(m){this.inbox.nextTurn.push(m);},
        inject(m){this.inbox.nextStep.push(m);events.push(['next-step',m.id]);},
        steer(){throw Error('unsafe waking API');}};
      registry.set(sessionId,agent);
      return {agent,dispose:async()=>{registry.delete(sessionId);}};
    }}};
  const server=new HarnessSdkJsonRpcServer(ctx,{notify(){}});
  const control=new SteeringControl({server,ctx,runId,runFence,createUserMessage});
  const params=()=>({sessionId:runId,runId,runFence,actionId:randomUUID(),contentBlocks:[{type:'text',text:'Fix the bounded existing diagnostic.'}]});
  const initialize=()=>control.request('initialize',{cwd:'/tmp',provider:'test',model:'test'});
  const prompt=()=>control.request('session/prompt',{sessionId:runId,contentBlocks:[{type:'text',text:'Work.'}]});
  return {runId,runFence,registry,events,control,params,initialize,prompt};
}
test('real upstream SDK prompt remains next-turn; live RPC injects next-step exactly once',async()=>{
  const f=fixture();await f.initialize();await f.prompt();const p=f.params();
  const [a,b]=await Promise.all([f.control.request('session/steer',p),f.control.request('session/steer',p)]);
  assert.deepEqual(a,b);assert.equal(a.status,'queued_next_step');
  const agent=f.registry.get(f.runId);
  assert.equal(agent.inbox.nextTurn.length,1);assert.equal(agent.inbox.nextStep.length,1);
  assert.equal(agent.inbox.nextStep[0].id,a.messageId);
  assert.throws(()=>f.control.steer({...p,contentBlocks:[{type:'text',text:'Different intent.'}]}),/STEERING_ACTION_CONFLICT/);
  await f.control.request('shutdown');assert.equal(f.registry.size,0);
});
test('steering cannot create sessions or cross run/fence',async()=>{
  const f=fixture();await f.initialize();
  assert.throws(()=>f.control.steer(f.params()),/SESSION_NOT_READY/);
  assert.equal(f.registry.size,0);await f.prompt();
  assert.throws(()=>f.control.steer({...f.params(),sessionId:randomUUID()}),/SESSION_MISMATCH/);
  assert.throws(()=>f.control.steer({...f.params(),runFence:randomUUID()}),/FENCE_MISMATCH/);
  assert.equal(f.events.length,0);
});
test('prompt creation racing shutdown cannot acquire steering ownership',async()=>{
  let release;const createGate=new Promise(r=>release=r);const f=fixture({createGate});
  await f.initialize();const prompt=f.prompt();const stopped=f.control.request('shutdown');release();
  await Promise.allSettled([prompt,stopped]);
  assert.equal(f.control.agent,null);assert.equal(f.registry.size,0);
  await assert.rejects(f.control.request('session/steer',f.params()),/RUNTIME_CLOSED/);
});
test('idle and disposed/replaced session fail without enqueue',async()=>{
  const f=fixture();await f.initialize();await f.prompt();const agent=f.registry.get(f.runId);
  agent.status='idle';assert.throws(()=>f.control.steer(f.params()),/TURN_CLOSED/);
  agent.status='running';f.registry.set(f.runId,{...agent});
  assert.throws(()=>f.control.steer(f.params()),/SESSION_CLOSED/);
  f.registry.delete(f.runId);assert.throws(()=>f.control.steer(f.params()),/SESSION_CLOSED/);
  assert.equal(f.events.length,0);
});
test('queued receipt is not consumption; closure rejects replay after shutdown',async()=>{
  const f=fixture();await f.initialize();await f.prompt();const p=f.params();
  const r=await f.control.request('session/steer',p);assert.equal(r.status,'queued_next_step');
  await f.control.request('shutdown');await assert.rejects(f.control.request('session/steer',p),/RUNTIME_CLOSED/);
});
