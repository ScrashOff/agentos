import test from 'node:test';import assert from 'node:assert/strict';import {randomUUID} from 'node:crypto';import {ownedGuidanceClaim} from '../bridge/sdk-control/guidance-claim.mjs';
test('only current registry-owned running agent with admitted message produces claim provenance',()=>{
 const runId=randomUUID(),runFence=randomUUID(),messageId=randomUUID(),actionId=randomUUID(),agent={session:{id:runId},status:'running'};
 const control={runId,runFence,agent,closed:false,initialized:true,actions:new Map([[actionId,{receipt:{messageId,actionId}}]])},registry={get:()=>agent},payload={agent,message:{id:messageId},turn:1};
 assert.deepEqual(ownedGuidanceClaim(control,registry,payload),{version:1,source:'sdk-agent-inbox-claimed',sessionId:runId,runId,runFence,actionId,messageId,turn:1});
 assert.equal(ownedGuidanceClaim(control,registry,{...payload,message:{id:randomUUID()}}),null);
 assert.equal(ownedGuidanceClaim(control,registry,{...payload,agent:{...agent}}),null);
 assert.equal(ownedGuidanceClaim(control,{get:()=>({...agent})},payload),null);
 assert.equal(ownedGuidanceClaim({...control,closed:true},registry,payload),null);
 assert.equal(ownedGuidanceClaim({...control,initialized:false},registry,payload),null);
 agent.status='idle';assert.equal(ownedGuidanceClaim(control,registry,payload),null);
});
