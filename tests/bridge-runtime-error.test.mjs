import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import {harnessFailureCode} from '../bridge/harness-termination.mjs';

// Evaluate the exact production sanitizer without importing the bridge's
// credential adapters or starting its stdin RPC loop. The extraction is bounded
// by the next named function and fails if the production boundary changes.
const source=await readFile(new URL('../bridge/server.mjs',import.meta.url),'utf8');
const parts=source.match(/function safeError\(error\) \{[\s\S]*?\n\}\r?\nasync function getCredential/gu);
assert.equal(parts?.length,1,'one production sanitizer boundary');
const declaration=parts[0].replace(/\r?\nasync function getCredential$/u,'');
const safeError=new vm.Script('('+declaration+')').runInNewContext(Object.create(null),{timeout:1000});
const wire=error=>JSON.parse(JSON.stringify({id:'opaque-request-id',error:safeError(error)})).error;
const typed=(code,message='private diagnostic sentinel')=>Object.assign(new Error(message),{code});
const unit='agentos-run-66a98663-615e-4964-8d40-f462e5f7b755';

test('trusted Harness OOM survives the actual RPC sanitizer',()=>{
 const code=harnessFailureCode('RUNTIME_EXITED',{source:'systemd-journal',result:'oom-kill',unit:unit+'.service'},unit);
 const result=wire(typed(code,'private diagnostic sentinel: account header'));
 assert.equal(result.code,'HARNESS_SANDBOX_OOM');
 assert.match(result.message,/HARNESS_SANDBOX_OOM/);
 assert.equal(JSON.stringify(result).includes('private diagnostic sentinel'),false);
});

test('owner-confirmed progress failure is not converted to provider failure',()=>{
 const result=wire(typed('RUNTIME_PROGRESS_STALLED'));
 assert.equal(result.code,'RUNTIME_PROGRESS_STALLED');
 assert.equal(JSON.stringify(result).includes('private diagnostic sentinel'),false);
});

test('stop uncertainty and ordinary runtime exit retain their distinct codes',()=>{
 for(const code of ['STOP_UNCONFIRMED','RUNTIME_EXITED'])assert.equal(wire(typed(code)).code,code);
});

test('OOM text alone cannot create a typed runtime classification',()=>{
 assert.equal(wire(new Error('HARNESS_SANDBOX_OOM')).code,'PROVIDER_ERROR');
 assert.equal(wire(new Error('RUNTIME_PROGRESS_STALLED')).code,'PROVIDER_ERROR');
});

test('unknown error codes and private messages are not reflected',()=>{
 const result=wire(typed('UNREVIEWED_RUNTIME_CODE','private diagnostic sentinel'));
 assert.equal(result.code,'PROVIDER_ERROR');
 assert.equal(JSON.stringify(result).includes('UNREVIEWED_RUNTIME_CODE'),false);
 assert.equal(JSON.stringify(result).includes('private diagnostic sentinel'),false);
});

test('untrusted or wrong-unit termination does not become Harness OOM',()=>{
 for(const termination of [{source:'model',result:'oom-kill',unit:unit+'.service'},
  {source:'systemd-journal',result:'oom-kill',unit:'agentos-run-00000000-0000-0000-0000-000000000000.service'}]){
  const code=harnessFailureCode('RUNTIME_EXITED',termination,unit);
  assert.equal(wire(typed(code)).code,'RUNTIME_EXITED');
 }
});

test('existing quota, authentication, cancellation and timeout sanitization is unchanged',()=>{
 assert.equal(wire(new Error('429 quota private diagnostic sentinel')).code,'QUOTA_EXCEEDED');
 assert.equal(wire(new Error('401 token private diagnostic sentinel')).code,'AUTH_REQUIRED');
 assert.equal(wire(new Error('cancelled private diagnostic sentinel')).code,'CANCELLED');
 assert.equal(wire(Object.assign(new Error('private diagnostic sentinel'),{name:'TimeoutError'})).code,'PROVIDER_TIMEOUT');
});

test('typed truncated output survives RPC without reflecting model or private diagnostic text',()=>{
 assert.equal(wire(typed('MODEL_OUTPUT_LIMIT')).code,'MODEL_OUTPUT_LIMIT');
 assert.equal(wire(new Error('MODEL_OUTPUT_LIMIT')).code,'PROVIDER_ERROR');
 assert.equal(JSON.stringify(wire(typed('MODEL_OUTPUT_LIMIT'))).includes('private diagnostic sentinel'),false);
});
