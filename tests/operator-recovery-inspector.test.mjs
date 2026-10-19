import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {inspectOperatorRecovery} from '../bridge/inspect-operator-recovery.mjs';

const run='11111111-1111-4111-8111-111111111111',fence='22222222-2222-4222-8222-222222222222';
const workspace='tests/operator-recovery',candidate='/opt/agentos/releases/managed-33333333-3333-4333-8333-333333333333';
function fixture(mutate=()=>{}){
 let reads=0;const calls=[];
 const io={
  async read(path,root,maxBytes){
   calls.push({path,root,maxBytes});const round=Math.floor(reads++/7);
   if(path.endsWith('/state.json')){
    const state={runId:run,runFence:fence,workspace,runtime:'harness',state:'running'};mutate('state',state,round);
    const text=JSON.stringify(state);return{text,digest:createHash('sha256').update(text).digest('hex')};
   }
   const file={digest:'a'.repeat(64)};mutate('source',file,round);return file;
  },
  async inspect(unit,controlGroup){
   const value={unit,controlGroup,mainPid:unit.startsWith('agentos-native')?101:102,
    startTicks:'1234',activeState:'active',subState:'running'};
   mutate('process',value,Math.floor((reads-1)/7));return value;
  },
  async realpath(path){const value={path:path.startsWith('/proc/')?candidate:path};mutate('path',value,Math.floor(reads/7));return value.path;},
 };
 return{io,calls};
}
test('two exact identity samples include only bounded state and six source digests',async()=>{
 const f=fixture(),result=await inspectOperatorRecovery(run,workspace,fence,candidate,f.io);
 assert.equal(result.runId,run);assert.equal(result.runFence,fence);assert.equal(result.workspace,workspace);
 assert.equal(result.process.mainPid,102);assert.equal(result.owner.mainPid,101);
 assert.equal(Object.keys(result.owner.sourceHashes).length,6);assert.equal(f.calls.length,14);
 assert.equal(f.calls.filter(c=>c.path.endsWith('/state.json')).every(c=>c.maxBytes===262144),true);
 assert.equal(f.calls.filter(c=>!c.path.endsWith('/state.json')).every(c=>c.maxBytes===1048576),true);
 assert.equal(JSON.stringify(result).includes('prompt'),false);
});
for(const change of ['fence','workspace','state','runtime','owner-cwd','source-change','pid-change','state-change']){
 test('refuses '+change+' without signaling any process',async()=>{
  const f=fixture((kind,value,round)=>{
   if(kind==='state'&&change==='fence')value.runFence=run;
   if(kind==='state'&&change==='workspace')value.workspace='tests/other';
   if(kind==='state'&&change==='state')value.state='completed';
   if(kind==='state'&&change==='runtime')value.runtime='opencode';
   if(kind==='path'&&change==='owner-cwd')value.path='/opt/other';
   if(kind==='source'&&change==='source-change'&&round===1)value.digest='b'.repeat(64);
   if(kind==='process'&&change==='pid-change'&&round===1)value.startTicks='9999';
   if(kind==='state'&&change==='state-change'&&round===1)value.observation='changed';
  });
  await assert.rejects(()=>inspectOperatorRecovery(run,workspace,fence,candidate,f.io));
 });
}
for(const args of [[run,'../../etc',fence,candidate],[run,workspace,'bad',candidate],[run,workspace,fence,candidate+'/../other']]){
 test('rejects malformed routing before file or unit inspection: '+JSON.stringify(args),async()=>{
  const f=fixture();await assert.rejects(()=>inspectOperatorRecovery(...args,f.io));assert.equal(f.calls.length,0);
 });
}
test('unreadable, symlink-rejected, or oversized state remains a closed error',async()=>{
 const f=fixture();f.io.read=async()=>{throw Error('UNSAFE_FILE');};
 await assert.rejects(()=>inspectOperatorRecovery(run,workspace,fence,candidate,f.io),/UNSAFE_FILE/);
});
