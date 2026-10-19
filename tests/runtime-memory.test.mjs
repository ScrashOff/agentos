import {test} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {openCodeMemory,harnessMemory} from '../bridge/runtime-memory.mjs';
import {inspectRuntimeMemory} from '../bridge/inspect-runtime-memory.mjs';
function params(memoryMiB) {
  const runId=randomUUID(),workspace='memory-tests/example';
  return {runId,workspace,memoryReservation:{version:1,id:randomUUID(),runId,workspace,
    runFence:randomUUID(),launchToken:randomUUID(),state:'reserved',reservedAt:new Date().toISOString(),memoryMiB,
    ...(memoryMiB===3072?{oomSource:{runId:randomUUID(),terminationEventId:'1',stopEventId:'2'}}:{})}};
}
test('normal and elevated reservations select the same memory across cgroup, slot runner and preflight',()=>{
  for(const amount of [1800,3072]) {
    const input=params(amount),value=openCodeMemory(input);
    assert.equal(value.memoryMiB,amount);assert.equal(value.memoryMax,`MemoryMax=${amount}M`);
    assert.equal(value.slotMemory,String(amount));assert.equal(value.minAvailableKiB,(amount+512)*1024);
    assert.equal(value.reservationId,input.memoryReservation.id);
  }
});
test('missing, changed, released and unsupported reservations cannot launch',()=>{
  const base=params(3072);
  for(const memoryReservation of [undefined,{}, {...base.memoryReservation,state:'released'},
    {...base.memoryReservation,workspace:'memory-tests/other'},
    {...base.memoryReservation,runId:randomUUID()}, {...base.memoryReservation,memoryMiB:4096},
    {...base.memoryReservation,oomSource:undefined}, {...base.memoryReservation,runFence:'invalid'}])
    assert.throws(()=>openCodeMemory({...base,memoryReservation}),{code:'RUNTIME_MEMORY_RESERVATION_INVALID'});
});
test('Harness legacy and ordinary reservations retain existing code/default limits',()=>{
  for(const [profile,amount] of [['code',1400],['general',1800],[undefined,1800]]) {
    const legacy=harnessMemory({profile});
    assert.equal(legacy.memoryMiB,amount);assert.equal(legacy.memoryMax,`MemoryMax=${amount}M`);
    assert.equal(legacy.slotMemory,String(amount));assert.equal(legacy.minAvailableKiB,1800000);
    const p=params(1800);p.memoryReservation.runtime='harness';
    const current=harnessMemory({...p,profile,runFence:p.memoryReservation.runFence,launchToken:p.memoryReservation.launchToken});
    assert.equal(current.memoryMiB,amount);assert.equal(current.reservationId,p.memoryReservation.id);
  }
});
test('Harness elevated cgroup, slot and preflight share the exact authenticated reservation',()=>{
  const p=params(3072);p.memoryReservation.runtime='harness';
  const value=harnessMemory({...p,profile:'code',runFence:p.memoryReservation.runFence,launchToken:p.memoryReservation.launchToken});
  assert.equal(value.memoryMiB,3072);assert.equal(value.memoryMax,'MemoryMax=3072M');
  assert.equal(value.slotMemory,'3072');assert.equal(value.minAvailableKiB,3584*1024);
});
test('Harness rejects missing or mismatched launch authority and self-referential OOM lineage',()=>{
  const p=params(3072);p.memoryReservation.runtime='harness';
  const base={...p,runFence:p.memoryReservation.runFence,launchToken:p.memoryReservation.launchToken};
  for(const bad of [{...base,runFence:undefined},{...base,runFence:randomUUID()},
    {...base,launchToken:undefined},{...base,launchToken:randomUUID()},
    {...base,memoryReservation:null},{...base,memoryReservation:{...p.memoryReservation,runtime:'opencode'}},
    {...base,memoryReservation:{...p.memoryReservation,oomSource:{...p.memoryReservation.oomSource,runId:p.runId}}},
    {...base,memoryReservation:{...p.memoryReservation,state:'released'}},
    {...base,memoryReservation:{...p.memoryReservation,memoryMiB:4096}},
    {...base,memoryReservation:{...p.memoryReservation,oomSource:null}}])
    assert.throws(()=>harnessMemory(bad),{code:'RUNTIME_MEMORY_RESERVATION_INVALID'});
});
test('memory release inspection reads only bounded state and the exact process unit, never workspace contents',async()=>{
  const p=params(1800),calls=[];
  const io={readState:async id=>{calls.push('state:'+id);return{runId:id,runtime:'opencode',workspace:p.workspace};},
    inspectUnit:async unit=>{calls.push('unit:'+unit);return{unit,stopped:true,mainPid:0,populated:false,activeState:'inactive',checkedAt:new Date().toISOString()};},
    inspectWorkspace:()=>{throw Error('Workspace scan must never run');}};
  const result=await inspectRuntimeMemory(p.runId,p.workspace,false,io);
  assert.equal(result.workspace,p.workspace);assert.equal(result.runId,p.runId);
  assert.deepEqual(calls,['state:'+p.runId,'unit:agentos-run-'+p.runId]);
  assert.equal('candidate' in result,false);assert.equal('state' in result,false);
});
test('Harness memory inspection requires the explicit runtime and cannot read an OpenCode state as Harness',async()=>{
  const p=params(1800),io={readState:async()=>({runId:p.runId,runtime:'harness',workspace:p.workspace}),
    inspectUnit:async unit=>({unit,stopped:true,mainPid:0,populated:false,checkedAt:new Date().toISOString()})};
  assert.equal((await inspectRuntimeMemory(p.runId,p.workspace,false,io,'harness')).runtime,'harness');
  await assert.rejects(inspectRuntimeMemory(p.runId,p.workspace,false,io),/RUN_IDENTITY_MISMATCH/);
  await assert.rejects(inspectRuntimeMemory(p.runId,p.workspace,false,{...io,
    readState:async()=>({runId:p.runId,runtime:'opencode',workspace:p.workspace})},'harness'),/RUN_IDENTITY_MISMATCH/);
  await assert.rejects(inspectRuntimeMemory(p.runId,p.workspace,false,io,'muse'),/RUN_IDENTITY_INVALID/);
});
test('absent state requires explicit never-started authority and cannot mask a mismatched or live run',async()=>{
  const p=params(1800),io={readState:async()=>{throw Object.assign(Error('missing'),{code:'ENOENT'});},
    inspectUnit:async unit=>({unit,stopped:true,checkedAt:new Date().toISOString()})};
  await assert.rejects(inspectRuntimeMemory(p.runId,p.workspace,false,io),{code:'ENOENT'});
  assert.equal((await inspectRuntimeMemory(p.runId,p.workspace,true,io)).runId,p.runId);
  await assert.rejects(inspectRuntimeMemory(p.runId,p.workspace,true,{...io,readState:async()=>({runId:randomUUID(),runtime:'opencode',workspace:p.workspace})}),/RUN_IDENTITY_MISMATCH/);
  await assert.rejects(inspectRuntimeMemory(p.runId,p.workspace,true,{...io,inspectUnit:async()=>({stopped:false})}),/STOP_UNCONFIRMED/);
});
