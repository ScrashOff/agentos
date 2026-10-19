import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveGenerationAccount } from '../bridge/generation-account.mjs';
const local={id:'agentos:test:glm',family:'qwen',model:'glm-test',credential:{type:'api_key',key:'test-only-private-key'}};
const catalog=(family,route,account)=>account?.model?[account.model]:[family+'-test'];
const familyOf=route=>route==='agy-2'?'agy':route==='qwen-cloud'?'qwen':'codex';

test('twelve own-account generations do not fetch the unrelated Harness catalog and never expose credentials',async()=>{
 let inheritedCalls=0;
 const dependencies={own:()=>[local],catalog,familyOf,inheritedState:async()=>{inheritedCalls++;throw Error('UNRELATED_PROVIDER_TIMEOUT');}};
 const accounts=await Promise.all(Array.from({length:12},()=>resolveGenerationAccount(local.id,dependencies)));
 assert.equal(inheritedCalls,0);
 for(const account of accounts){assert.equal(account.authStatus,'connected');assert.deepEqual(account.models,['glm-test']);
  assert.equal(JSON.stringify(account).includes(local.credential.key),false);assert.equal(account.source,'agentos');}
});
test('an unknown own account fails before consulting or falling back to another provider',async()=>{
 await assert.rejects(resolveGenerationAccount('agentos:missing',{own:()=>[],catalog,familyOf,
  inheritedState:async()=>{throw Error('MUST_NOT_REFRESH');}}),/ACCOUNT_MISSING/);
});
test('a disconnected own account remains disconnected',async()=>{
 const account=await resolveGenerationAccount(local.id,{own:()=>[{...local,credential:null}],catalog,familyOf,
  inheritedState:async()=>{throw Error('MUST_NOT_REFRESH');}});
 assert.equal(account.authStatus,'disconnected');
});
test('inherited generation retains the requested account model, authentication and exhausted quota',async()=>{
 const quota={ordinaryUsageAllowed:false,groups:[{label:'codex',windows:[{remainingPercent:0}]}]};
 const data={accounts:[{provider:'openai-codex-2',connected:true}],quotas:{accounts:{'openai-codex-2':quota}}};
 const account=await resolveGenerationAccount('harness:openai-codex-2',{own:()=>[],catalog,familyOf,inheritedState:async()=>data});
 assert.equal(account.provider,'openai-codex-2');assert.equal(account.authStatus,'connected');
 assert.equal(account.quota,quota);assert.deepEqual(account.models,['codex-test']);assert.equal(account.source,'harness');
 data.quotas.accounts['openai-codex-2'].reconnectRequired=true;
 assert.equal((await resolveGenerationAccount(account.id,{own:()=>[],catalog,familyOf,inheritedState:async()=>data})).authStatus,'reconnect_required');
});
test('inherited AGY and configured Qwen resolve exactly, without accepting a nonexistent route',async()=>{
 const dependencies={own:()=>[],catalog,familyOf,inheritedState:async()=>({accounts:[],agyAccounts:[{provider:'agy-2',connected:true}],qwen:{configured:true}})};
 assert.equal((await resolveGenerationAccount('harness:agy-2',dependencies)).family,'agy');
 assert.equal((await resolveGenerationAccount('harness:qwen-cloud',dependencies)).family,'qwen');
 await assert.rejects(resolveGenerationAccount('harness:openai-codex-9',dependencies),/ACCOUNT_MISSING/);
});
