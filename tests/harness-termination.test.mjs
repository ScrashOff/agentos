import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash,randomUUID} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';
import {inspectHarnessTermination} from '../bridge/inspect-harness-termination.mjs';
import {harnessFailureCode} from '../bridge/harness-termination.mjs';
import {unitExitFromJournal} from '../bridge/unit-exit.mjs';
const sha=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
function fixture() {
  const runId=randomUUID(),runFence=randomUUID(),workspace='oom-tests/aztec',unit='agentos-run-'+runId;
  const state={runId,runFence,workspace,runtime:'harness',state:'failed',stopped:true,code:'RUNTIME_EXITED'},calls=[];
  const typed=JSON.stringify({_PID:'1',_COMM:'systemd',UNIT:unit+'.service',UNIT_RESULT:'oom-kill',MESSAGE:'private message never returned'});
  const io={readState:async()=>{calls.push('state');return{state,digest:sha(state)};},
    inspectUnit:async input=>{calls.push('process');assert.equal(input,unit);return{unit,stopped:true,mainPid:0,populated:false,activeState:'inactive',checkedAt:new Date().toISOString()};},
    inspectUnitExit:async input=>{calls.push('journal');assert.equal(input,unit);return unitExitFromJournal(input,typed);}};
  return {runId,runFence,workspace,unit,state,calls,io};
}
test('classifies only the exact unit typed OOM; keeps cancellation, provider and integrity causes',()=>{
  const f=fixture(),termination={source:'systemd-journal',unit:f.unit+'.service',result:'oom-kill'};
  assert.equal(harnessFailureCode('RUNTIME_EXITED',termination,f.unit),'HARNESS_SANDBOX_OOM');
  for(const code of ['CANCELLED','MODEL_TIMEOUT','QUOTA_EXCEEDED','INPUT_DIGEST','RESULT_FORMAT'])
    assert.equal(harnessFailureCode(code,termination,f.unit),code);
  for(const invalid of [null,{...termination,unit:'agentos-run-'+randomUUID()+'.service'},
    {...termination,source:'model'}, {...termination,result:'exit-code'}])
    assert.equal(harnessFailureCode('RUNTIME_EXITED',invalid,f.unit),'RUNTIME_EXITED');
});
test('historical inspection proves stop twice and preserves state while returning only typed journal fields',async()=>{
  const f=fixture(),before=JSON.stringify(f.state),proof=await inspectHarnessTermination(f.runId,f.workspace,f.runFence,f.io);
  assert.deepEqual(f.calls,['state','process','journal','state','process']);
  assert.equal(JSON.stringify(f.state),before);assert.equal(proof.stateDigest,sha(f.state));
  assert.deepEqual(proof.termination,{source:'systemd-journal',unit:f.unit+'.service',result:'oom-kill'});
  assert.equal(proof.runFence,f.runFence);assert.equal(proof.process.stopped,true);
  assert.equal(JSON.stringify(proof).includes('private message'),false);
  assert.equal('candidate' in proof,false);
});
test('historical inspection rejects wrong identity and cannot infer OOM from state or untrusted journal text',async()=>{
  for(const field of ['runId','runFence','workspace','runtime','state','stopped','code']) {
    const f=fixture();f.state[field]=field==='stopped'?false:'wrong';
    await assert.rejects(inspectHarnessTermination(f.runId,f.workspace,f.runFence,f.io),/RUN_IDENTITY_MISMATCH/);
    assert.deepEqual(f.calls,['state']);
  }
  const f=fixture();f.state.code='HARNESS_SANDBOX_OOM';
  f.io.inspectUnitExit=async()=>null;
  assert.equal((await inspectHarnessTermination(f.runId,f.workspace,f.runFence,f.io)).termination,null);
});
test('historical inspection rejects populated/PID-active/stale observations and changing state',async()=>{
  for(const fields of [{stopped:false},{mainPid:4},{populated:true},{activeState:'active'},
    {checkedAt:'2000-01-01T00:00:00Z'},{unit:'agentos-run-'+randomUUID()}]) {
    const f=fixture(),original=f.io.inspectUnit;
    f.io.inspectUnit=async unit=>({...await original(unit),...fields});
    await assert.rejects(inspectHarnessTermination(f.runId,f.workspace,f.runFence,f.io),/STOP_UNCONFIRMED/);
    assert.equal(f.calls.includes('journal'),false);
  }
  const f=fixture(),original=f.io.readState;let n=0;
  f.io.readState=async()=>{const value=await original();return ++n===2?{...value,digest:'f'.repeat(64)}:value;};
  await assert.rejects(inspectHarnessTermination(f.runId,f.workspace,f.runFence,f.io),/RUN_STATE_CHANGED/);
});
test('historical inspection refuses a newly active process after reading the journal',async()=>{
  const f=fixture(),original=f.io.inspectUnit;let n=0;
  f.io.inspectUnit=async unit=>({...await original(unit),...(++n===2?{stopped:false,mainPid:7,populated:true}: {})});
  await assert.rejects(inspectHarnessTermination(f.runId,f.workspace,f.runFence,f.io),/STOP_UNCONFIRMED/);
});
test('the real read-only CLI rejects malformed identity without exposing diagnostics or starting a runtime',async()=>{
  const cli=fileURLToPath(new URL('../bridge/inspect-harness-termination.mjs',import.meta.url));
  for(const args of [[],['invalid','oom-tests/aztec',randomUUID()],[randomUUID(),'../wrong',randomUUID()]]) {
    await assert.rejects(promisify(execFile)(process.execPath,[cli,...args],{timeout:3000}),error=>{
      assert.equal(error.code,1);assert.equal(error.stderr,'');
      const result=JSON.parse(error.stdout);
      assert.ok(['INVALID_INSPECTION','RUN_IDENTITY_INVALID'].includes(result.error));
      assert.deepEqual(Object.keys(result),['error']);return true;
    });
  }
});

test('typed turn limit remains distinct from missing completion, successful delivery and OOM',async()=>{
 const {harnessTurnFailureCode}=await import('../bridge/harness-termination.mjs');
 assert.equal(harnessTurnFailureCode({kind:'completed'}),null);
 assert.equal(harnessTurnFailureCode({kind:'max-tokens'}),'MODEL_OUTPUT_LIMIT');
 for(const reason of [undefined,null,{},'max-tokens'])assert.equal(harnessTurnFailureCode(reason),'MISSING_TURN_END');
 for(const kind of ['cancelled','error','MODEL_OUTPUT_LIMIT','unknown'])assert.equal(harnessTurnFailureCode({kind}),'INCOMPLETE_TURN');
});
