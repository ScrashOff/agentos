import test from 'node:test';
import assert from 'node:assert/strict';
import {PassThrough} from 'node:stream';
import {randomUUID} from 'node:crypto';
import {apply} from '../bridge/sdk-control/sdk-steering-plugin.mjs';
test('real SDK transport awaits loader; typed failures and shutdown preserve flush/disposal',async()=>{
  const input=new PassThrough(),output=new PassThrough();const runId=randomUUID(),runFence=randomUUID();
  let cleanup,loaderCalls=0,rootDisposed=0,exitCode;const agents=new Map();
  const services=new Map();
  const ctx={root:{fiber:{async dispose(){rootDisposed++;await cleanup();}}},on(){return ()=>{};},provide(key,value){services.set(key,value);},
    effect(fn){cleanup=fn();},get(key){if(services.has(key))return services.get(key);if(key==='loader')return {await:async()=>{loaderCalls++;}};
      return {listProviders:()=>[{id:'test'}],resolveCallConfig:async()=>{}};},
    agents:{get:id=>agents.get(id),async create({sessionId}){
      const agent={id:sessionId,session:{id:sessionId},status:'running',inbox:{nextStep:[]},
        followup(){},inject(m){this.inbox.nextStep.push(m);}};
      agents.set(sessionId,agent);return {agent,dispose:async()=>agents.delete(sessionId)};
    }}};
  const frames=[],waiting=[];let partial='';
  output.on('data',b=>{partial+=b;let i;while((i=partial.indexOf('\n'))>=0){
    const frame=JSON.parse(partial.slice(0,i));partial=partial.slice(i+1);frames.push(frame);
    waiting.splice(0).forEach(r=>r());}});
  apply(ctx,{runId,runFence,input,output,exit:c=>exitCode=c});
  let id=0;
  const rpc=async(method,params)=>{
    const requestId=++id;input.write(JSON.stringify({jsonrpc:'2.0',id:requestId,method,params})+'\n');
    while(!frames.some(f=>f.id===requestId))await new Promise(r=>waiting.push(r));
    return frames.find(f=>f.id===requestId);
  };
  const init=await rpc('initialize',{cwd:'/tmp',provider:'test',model:'test'});
  assert.equal(init.result.serverInfo.name,'deepseek-harness-sdk-runtime');assert.equal(loaderCalls,1);
  await rpc('session/prompt',{sessionId:runId,contentBlocks:[{type:'text',text:'Work.'}]});
  const bad=await rpc('session/steer',{runId,runFence:randomUUID(),sessionId:runId,
    actionId:randomUUID(),contentBlocks:[{type:'text',text:'Bounded correction.'}]});
  assert.equal(bad.error.message,'STEERING_FENCE_MISMATCH');
  const receipt=await rpc('session/steer',{runId,runFence,sessionId:runId,
    actionId:randomUUID(),contentBlocks:[{type:'text',text:'Bounded correction.'}]});
  assert.equal(receipt.result.status,'queued_next_step');
  assert.equal(agents.get(runId).inbox.nextStep[0].id,receipt.result.messageId);
  const shutdown=await rpc('shutdown');assert.deepEqual(shutdown.result,{});
  await new Promise(r=>setImmediate(r));await new Promise(r=>setImmediate(r));
  assert.equal(rootDisposed,1);assert.equal(exitCode,0);assert.equal(agents.size,0);
});
