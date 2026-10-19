import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {inspectOperatorOpenCodeRecovery,validateOperatorOpenCodeIdentity,operatorOpenCodeSourceFiles}
 from '../bridge/inspect-operator-opencode-recovery.mjs';
import {stopOperatorOpenCodeRecovery} from '../bridge/stop-operator-opencode-recovery.mjs';

const run='11111111-1111-4111-8111-111111111111',fence='22222222-2222-4222-8222-222222222222';
const context='33333333-3333-4333-8333-333333333333',requestId='44444444-4444-4444-8444-444444444444';
const workspace='tests/opencode-recovery',candidate='/opt/agentos/releases/managed-55555555-5555-4555-8555-555555555555';
const sha=value=>createHash('sha256').update(value).digest('hex');
function inspectorFixture(mutate=()=>{}){
 let reads=0;const calls=[];
 const io={
  async read(path,root,maxBytes){
   const round=Math.floor(reads++/8);calls.push({path,root,maxBytes});
   if(path.endsWith('/state.json')){
    const state={runId:run,workspace,runtime:'opencode',state:'running'};mutate('state',state,round);
    const text=JSON.stringify(state);return {text,digest:sha(text)};
   }
   const value={digest:'a'.repeat(64)};mutate('source',value,round);return value;
  },
  async inspect(unit,controlGroup){const value={unit,controlGroup,mainPid:unit.startsWith('agentos-native')?101:102,
   startTicks:'1234',activeState:'active',subState:'running'};mutate('process',value,Math.floor((reads-1)/8));return value;},
  async realpath(path){const value={path:path.startsWith('/proc/')?candidate:path};mutate('path',value,Math.floor(reads/8));return value.path;},
 };
 return {io,calls};
}
function identity(){
 const process={unit:`agentos-run-${run}.service`,mainPid:102,startTicks:'1234',activeState:'active',subState:'running',
  controlGroup:`/system.slice/agentos-run-${run}.service`};
 const owner={unit:`agentos-native-supervisor-${run}.service`,mainPid:101,startTicks:'2345',activeState:'active',subState:'running',
  controlGroup:`/agentos_native_supervisors.slice/agentos-native-supervisor-${run}.service`,candidate,
  sourceHashes:Object.fromEntries(operatorOpenCodeSourceFiles.map(name=>[name,'a'.repeat(64)]))};
 return {version:1,runId:run,runFence:fence,contextVersion:context,workspace,state:'running',runtime:'opencode',
  stateDigest:'b'.repeat(64),checkedAt:new Date().toISOString(),stateIdentityFields:{runFence:false,contextVersion:false},process,owner};
}
function stopperFixture(options={}){
 const request={requestId,identity:identity()},intents=new Map(),events=[];
 const state={stopped:false,ownerStopped:false,receipt:false};let inspections=0;
 const io={
  attempts:2,observationAttempts:2,wait:async()=>{},
  async readIntent(path){events.push('read_intent');return intents.get(path)??null;},
  async createIntent(path,value){events.push('claim');if(intents.has(path))return false;intents.set(path,structuredClone(value));return true;},
  async inspectIdentity(){events.push('inspect_live');inspections++;const value=structuredClone(request.identity);
   if(options.changedAfterClaim&&inspections===2)value.process.startTicks='9999';return value;},
  async stopSandbox(unit){events.push('stop:'+unit);assert.equal(intents.size,1);
   state.stopped=!options.stayAlive;state.ownerStopped=!options.ownerAlive;state.receipt=!options.noReceipt;
   if(options.stopError)throw Error('transport_failed');},
  async inspectSandbox(unit){assert.equal(unit,`agentos-run-${run}`);return {unit,checkedAt:new Date().toISOString(),
   loadState:'loaded',activeState:state.stopped?'inactive':'active',mainPid:state.stopped?0:102,
   populated:!state.stopped,stopped:state.stopped};},
  async inspectOwner(){return {unit:request.identity.owner.unit,checkedAt:new Date().toISOString(),loadState:'loaded',
   activeState:state.ownerStopped?'inactive':'active',mainPid:state.ownerStopped?0:101,pendingJob:false,cgroupEmpty:state.ownerStopped};},
  async readActorReceipt(){return state.receipt?{state:'failed',code:'RUNTIME_EXITED',stateDigest:'c'.repeat(64),stopReceiptDigest:'d'.repeat(64)}:null;},
 };
 return {request,io,intents,events,state};
}
test('legacy OpenCode inspection declares absent fence/context fields and binds only canonical arguments',async()=>{
 const f=inspectorFixture(),proof=await inspectOperatorOpenCodeRecovery(run,workspace,fence,context,candidate,f.io);
 assert.equal(proof.runFence,fence);assert.equal(proof.contextVersion,context);
 assert.deepEqual(proof.stateIdentityFields,{runFence:false,contextVersion:false});
 assert.equal(Object.keys(proof.owner.sourceHashes).length,7);assert.equal(f.calls.length,16);
 assert.equal(f.calls.filter(c=>c.path.endsWith('/state.json')).every(c=>c.maxBytes===262144),true);
 assert.equal(JSON.stringify(proof).includes('prompt'),false);
});
test('matching fields are reported as present and are never overwritten by canonical arguments',async()=>{
 const f=inspectorFixture((kind,value)=>{if(kind==='state'){value.runFence=fence;value.contextVersion=context;}});
 const proof=await inspectOperatorOpenCodeRecovery(run,workspace,fence,context,candidate,f.io);
 assert.deepEqual(proof.stateIdentityFields,{runFence:true,contextVersion:true});
});
for(const change of ['fence','context','workspace','runtime','state','source-change','pid-change','owner-cwd','state-change']){
 test('identity inspection refuses '+change+' without mutation',async()=>{
  const f=inspectorFixture((kind,value,round)=>{
   if(kind==='state'&&change==='fence')value.runFence=context;
   if(kind==='state'&&change==='context')value.contextVersion=fence;
   if(kind==='state'&&change==='workspace')value.workspace='tests/other';
   if(kind==='state'&&change==='runtime')value.runtime='harness';
   if(kind==='state'&&change==='state')value.state='failed';
   if(kind==='source'&&change==='source-change'&&round===1)value.digest='b'.repeat(64);
   if(kind==='process'&&change==='pid-change'&&round===1)value.startTicks='7777';
   if(kind==='path'&&change==='owner-cwd'&&value.path===candidate)value.path='/opt/other';
   if(kind==='state'&&change==='state-change'&&round===1)value.at='changed';
  });
  await assert.rejects(()=>inspectOperatorOpenCodeRecovery(run,workspace,fence,context,candidate,f.io));
 });
}
test('malformed routing is refused before any state read',async()=>{
 const f=inspectorFixture();await assert.rejects(()=>inspectOperatorOpenCodeRecovery(run,'../../etc',fence,context,candidate,f.io));
 assert.equal(f.calls.length,0);
});
test('closed identity shape rejects caller commands, source omission and arbitrary units',()=>{
 for(const mutate of [p=>p.command='arbitrary',p=>delete p.owner.sourceHashes['server/service.ts'],
  p=>p.process.unit='other.service',p=>p.owner.controlGroup='/system.slice/other.service',p=>p.process.mainPid=0]){
  const value=identity();mutate(value);assert.throws(()=>validateOperatorOpenCodeIdentity(value));
 }
});
test('one owned request signals only its sandbox after durable ownership and confirms independent termination',async()=>{
 const f=stopperFixture(),out=await stopOperatorOpenCodeRecovery(f.request,f.io);
 assert.equal(out.state,'stopped');assert.equal(out.stopConfirmed,true);assert.equal(out.newAttemptQueued,false);
 assert.equal(out.signalAttempted,true);assert.equal(out.duplicate,false);
 assert.deepEqual(f.events.filter(s=>s.startsWith('stop:')),[`stop:agentos-run-${run}`]);
 assert.ok(f.events.indexOf('claim')<f.events.indexOf(`stop:agentos-run-${run}`));
 assert.equal(out.actorReceipt.code,'RUNTIME_EXITED');assert.equal(out.owner.mainPid,0);
});
test('duplicate request only observes, including after prior uncertain termination becomes confirmed',async()=>{
 const f=stopperFixture({ownerAlive:true}),first=await stopOperatorOpenCodeRecovery(f.request,f.io);
 assert.equal(first.stopConfirmed,false);assert.equal(first.state,'uncertain');
 f.state.ownerStopped=true;f.io.inspectIdentity=async()=>{throw Error('must_not_require_running_again');};
 const again=await stopOperatorOpenCodeRecovery(f.request,f.io);
 assert.equal(again.stopConfirmed,true);assert.equal(again.duplicate,true);assert.equal(again.signalAttempted,false);
 assert.equal(f.events.filter(s=>s.startsWith('stop:')).length,1);
});
test('concurrent same request claims one durable stop and never duplicates the signal',async()=>{
 const f=stopperFixture(),results=await Promise.all([stopOperatorOpenCodeRecovery(f.request,f.io),stopOperatorOpenCodeRecovery(f.request,f.io)]);
 assert.equal(f.intents.size,1);assert.equal(f.events.filter(s=>s.startsWith('stop:')).length,1);
 assert.equal(results.filter(r=>!r.duplicate).length,1);
});
test('different request cannot reuse an already claimed run',async()=>{
 const f=stopperFixture();await stopOperatorOpenCodeRecovery(f.request,f.io);
 await assert.rejects(()=>stopOperatorOpenCodeRecovery({...f.request,requestId:context},f.io),/REQUEST_CONFLICT/);
 assert.equal(f.events.filter(s=>s.startsWith('stop:')).length,1);
});
test('changed physical identity after claim leaves uncertainty and never re-signals on replay',async()=>{
 const f=stopperFixture({changedAfterClaim:true});const out=await stopOperatorOpenCodeRecovery(f.request,f.io);
 assert.equal(out.stopConfirmed,false);assert.equal(out.signalAttempted,false);assert.equal(f.intents.size,1);
 const again=await stopOperatorOpenCodeRecovery(f.request,f.io);
 assert.equal(again.duplicate,true);assert.equal(again.signalAttempted,false);
 assert.equal(f.events.filter(s=>s.startsWith('stop:')).length,0);
});
for(const options of [{stayAlive:true},{ownerAlive:true},{noReceipt:true},{stayAlive:true,stopError:true}]){
 test('incomplete evidence remains uncertain: '+JSON.stringify(options),async()=>{
  const f=stopperFixture(options),out=await stopOperatorOpenCodeRecovery(f.request,f.io);
  assert.equal(out.state,'uncertain');assert.equal(out.stopConfirmed,false);assert.equal(out.newAttemptQueued,false);
  const again=await stopOperatorOpenCodeRecovery(f.request,f.io);
  assert.equal(again.stopConfirmed,false);assert.equal(f.events.filter(s=>s.startsWith('stop:')).length,1);
 });
}
test('transport failure can only be superseded by actual fresh sandbox, owner and actor receipts',async()=>{
 const f=stopperFixture({stopError:true}),out=await stopOperatorOpenCodeRecovery(f.request,f.io);
 assert.equal(out.stopConfirmed,true);assert.equal(out.state,'stopped');
});
test('malformed or foreign durable intent cannot grant a stop',async()=>{
 const f=stopperFixture();f.io.readIntent=async()=>({version:1,requestId,authority:'administrative_request'});
 await assert.rejects(()=>stopOperatorOpenCodeRecovery(f.request,f.io),/REQUEST_CONFLICT/);
 assert.equal(f.events.filter(s=>s.startsWith('stop:')).length,0);
});
test('additional request fields are rejected before inspection or signal',async()=>{
 const f=stopperFixture();await assert.rejects(()=>stopOperatorOpenCodeRecovery({...f.request,command:'stop all'},f.io),/REQUEST_INVALID/);
 assert.equal(f.events.length,0);
});
