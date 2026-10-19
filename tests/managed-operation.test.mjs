import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,readFile,writeFile,rm,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {randomUUID,createHash} from 'node:crypto';
import {executeManagedOperation,operationIdentity,validateOperationPolicy,validateOperationSpecification} from '../bridge/managed-operation.mjs';

const linux={skip:process.platform!=='linux'};
async function fixture(){
  const base=await mkdtemp(path.join(tmpdir(),'agentos-managed-operation-'));
  const workspace=path.join(base,'workspace');await mkdir(workspace);
  const snapshot=JSON.stringify({version:1,candidate:'test-'+randomUUID()});await writeFile(path.join(workspace,'.agentos-snapshot.json'),snapshot);
  const policy={version:1,goalId:randomUUID(),taskId:randomUUID(),runId:randomUUID(),runFence:randomUUID(),contextVersion:randomUUID(),
    workspace:'/workspace',sourceSnapshotDigest:createHash('sha256').update(snapshot).digest('hex'),maxDeadlineMs:10000};
  const options={policy,root:path.join(base,'operations'),workspaceRoot:workspace,graceMs:100};
  return {base,workspace,policy,options,cleanup:()=>rm(base,{recursive:true,force:true})};
}
const specification=(stageKey,code,deadlineMs=5000)=>({stageKey,argv:[process.execPath,'-e',code],deadlineMs});

test('fixed host policy rejects missing bindings and unbounded deadlines',()=>{
  assert.throws(()=>validateOperationPolicy({version:1}),/OPERATION_POLICY_INVALID/);
  const policy={version:1,goalId:randomUUID(),taskId:randomUUID(),runId:randomUUID(),runFence:randomUUID(),contextVersion:randomUUID(),workspace:'/workspace',sourceSnapshotDigest:'a'.repeat(64),maxDeadlineMs:3600000};
  assert.deepEqual(validateOperationPolicy(policy),policy);
  assert.throws(()=>validateOperationPolicy({...policy,maxDeadlineMs:3600001}),/OPERATION_POLICY_INVALID/);
  assert.throws(()=>validateOperationSpecification({stageKey:'t',argv:['node'],deadlineMs:1000},policy),/OPERATION_SPEC_INVALID/);
  assert.throws(()=>validateOperationSpecification({stageKey:'t',argv:['/bin/true'],cwd:'/workspace/../tmp',deadlineMs:1000},policy),/OPERATION_CWD_OUTSIDE_WORKSPACE/);
});

test('one awaited operation persists incremental output and negative exit without claiming acceptance',linux,async()=>{
  const f=await fixture();try{
    const spec=specification('negative','process.stdout.write("early\\n");setTimeout(()=>{console.error("known-negative");process.exit(3)},300)');
    const operation=executeManagedOperation(spec,f.options),id=operationIdentity(f.policy,spec);
    let early=false;
    for(let n=0;n<1000;n++){try{if((await readFile(path.join(f.options.root,id,'stdout.log'),'utf8')).includes('early')){early=true;break;}}catch{}
      await new Promise(r=>setTimeout(r,5));}
    const result=await operation;assert.equal(early,true);assert.equal(result.outcome,'exited');assert.equal(result.exitCode,3);
    assert.equal(result.physicalExitConfirmed,true);assert.equal(result.acceptance,false);assert.equal(result.verification,'unverified_observation');
    assert.equal(result.passed,false);assert.equal(result.logs.length,2);
    assert.equal(JSON.stringify(result).includes('known-negative'),false);
  }finally{await f.cleanup();}
});

test('concurrent callers never duplicate command, and a completed receipt is reused',linux,async()=>{
  const f=await fixture();try{
    const spec=specification('once','require("fs").appendFileSync("counter","one\\n");setTimeout(()=>{},250)');
    const a=executeManagedOperation(spec,f.options),b=executeManagedOperation(spec,f.options);
    const results=await Promise.all([a,b]);
    assert.equal(results.filter(v=>v.outcome==='exited').length,1);
    assert.equal(results.filter(v=>v.outcome==='uncertain'&&v.code==='STOP_UNCONFIRMED').length,1);
    assert.equal(await readFile(path.join(f.workspace,'counter'),'utf8'),'one\n');
    const reused=await executeManagedOperation(spec,f.options);assert.equal(reused.reused,true);assert.equal(reused.outcome,'exited');
    assert.equal(await readFile(path.join(f.workspace,'counter'),'utf8'),'one\n');
    await assert.rejects(executeManagedOperation({...spec,deadlineMs:4000},f.options),/OPERATION_INPUT_CHANGED/);
  }finally{await f.cleanup();}
});

test('preexisting incomplete claim remains uncertain without spawning',linux,async()=>{
  const f=await fixture();try{
    const spec=specification('lost','require("fs").writeFileSync("must-not-exist","x")'),id=operationIdentity(f.policy,spec);
    await mkdir(path.join(f.options.root,id),{recursive:true});
    const result=await executeManagedOperation(spec,f.options);assert.equal(result.outcome,'uncertain');assert.equal(result.physicalExitConfirmed,false);
    await assert.rejects(readFile(path.join(f.workspace,'must-not-exist')),{code:'ENOENT'});
  }finally{await f.cleanup();}
});

test('deadline stops only its owned process group and records confirmed negative outcome',linux,async()=>{
  const f=await fixture();try{
    const spec=specification('deadline','process.on("SIGTERM",()=>{});setInterval(()=>{},100)',300);
    const result=await executeManagedOperation(spec,f.options);assert.equal(result.outcome,'deadline_exceeded');
    assert.equal(result.physicalExitConfirmed,true);assert.equal(result.deadlineReached,true);assert.ok(['SIGKILL','SIGTERM'].includes(result.exitSignal));
    assert.equal(result.acceptance,false);
  }finally{await f.cleanup();}
});

test('changed immutable source snapshot rejects admission before command',linux,async()=>{
  const f=await fixture();try{
    await writeFile(path.join(f.workspace,'.agentos-snapshot.json'),'changed');
    await assert.rejects(executeManagedOperation(specification('source','console.log(1)'),f.options),/OPERATION_SOURCE_SNAPSHOT_CHANGED/);
    await assert.rejects(readFile(f.options.root),{code:'ENOENT'});
  }finally{await f.cleanup();}
});

test('changed evidence cannot reuse a previous receipt',linux,async()=>{
  const f=await fixture();try{
    const spec=specification('log','console.log("original")'),result=await executeManagedOperation(spec,f.options);
    await writeFile(path.join(f.options.root,result.operationId,'stdout.log'),'altered');
    await assert.rejects(executeManagedOperation(spec,f.options),/OPERATION_LOG_CHANGED/);
  }finally{await f.cleanup();}
});

test('output directory or cwd symlink cannot escape the admitted sandbox paths',linux,async()=>{
  const f=await fixture();try{
    const outside=path.join(f.base,'elsewhere');await mkdir(outside);await symlink(outside,f.options.root);
    await assert.rejects(executeManagedOperation(specification('dir','console.log(1)'),f.options),/OPERATION_UNSAFE_DIRECTORY/);
    await rm(f.options.root);await symlink(outside,path.join(f.workspace,'linked'));
    await assert.rejects(executeManagedOperation({...specification('cwd','console.log(1)'),cwd:'/workspace/linked'},f.options),/OPERATION_UNSAFE_CWD/);
  }finally{await f.cleanup();}
});

test('failed executable launch produces a negative observation with no invented success',linux,async()=>{
  const f=await fixture();try{
    const result=await executeManagedOperation({stageKey:'missing',argv:['/agentos-test-executable-does-not-exist'],deadlineMs:1000},f.options);
    assert.equal(result.outcome,'exited');assert.notEqual(result.exitCode,0);assert.equal(result.passed,false);assert.equal(result.physicalExitConfirmed,true);
    assert.equal(result.acceptance,false);
  }finally{await f.cleanup();}
});

test('caller cancellation stops only this operation and is a negative terminal receipt',linux,async()=>{
  const f=await fixture(),controller=new AbortController();try{
    const timer=setTimeout(()=>controller.abort(),200);
    const result=await executeManagedOperation(specification('cancel','setInterval(()=>{},100)'),{...f.options,signal:controller.signal});
    clearTimeout(timer);assert.equal(result.outcome,'cancelled');assert.equal(result.passed,false);assert.equal(result.physicalExitConfirmed,true);
  }finally{await f.cleanup();}
});

test('setsid descendant cannot survive the owned operation PID namespace',linux,async()=>{
  const f=await fixture();try{
    const escaped=path.join(f.workspace,'escaped-heartbeat');
    const descendant='import time\nwhile True:\n with open('+JSON.stringify(escaped)+',"a") as f: f.write("x")\n time.sleep(.01)';
    const command='require("child_process").spawn("/usr/bin/python3",["-c",'+JSON.stringify(descendant)+'],{detached:true,stdio:"ignore"}).unref();setTimeout(()=>process.exit(0),150)';
    const result=await executeManagedOperation(specification('descendant',command),f.options);
    assert.equal(result.physicalExitConfirmed,true);assert.equal(result.exitCode,0,await readFile(path.join(f.options.root,result.operationId,'stderr.log'),'utf8'));assert.equal(result.passed,undefined);
    const before=await readFile(escaped,'utf8');await new Promise(r=>setTimeout(r,150));
    assert.equal(await readFile(escaped,'utf8'),before);
  }finally{await f.cleanup();}
});

test('large output is drained with bounded disk capture and explicit truncation',linux,async()=>{
  const f=await fixture();try{
    const result=await executeManagedOperation(specification('large','process.stdout.write("x".repeat(10000))'),{...f.options,maxLogBytes:4096});
    assert.equal(result.exitCode,0);assert.equal(result.outputTruncated,true);assert.equal(result.logs[0].bytes,4096);
    assert.equal(result.logs[0].observedBytes,10000);assert.equal(result.passed,undefined);assert.equal(result.acceptance,false);
  }finally{await f.cleanup();}
});

test('same stage cannot reuse an old result after its literal script input changed',linux,async()=>{
  const f=await fixture();try{
    const file=path.join(f.workspace,'test.mjs');await writeFile(file,'console.log("v1")');
    const spec={stageKey:'script',argv:[process.execPath,'test.mjs'],deadlineMs:3000};
    const first=await executeManagedOperation(spec,f.options);assert.equal(first.exitCode,0);
    await writeFile(file,'console.log("v2")');
    await assert.rejects(executeManagedOperation(spec,f.options),/OPERATION_INPUT_CHANGED/);
  }finally{await f.cleanup();}
});

test('explicit indirect input is bound even when command reads it dynamically',linux,async()=>{
  const f=await fixture();try{
    const file=path.join(f.workspace,'indirect.json');await writeFile(file,'{"v":1}');
    const spec={...specification('indirect','console.log(require("fs").readFileSync("indirect.json","utf8"))'),inputFiles:['indirect.json']};
    const first=await executeManagedOperation(spec,f.options);assert.equal(first.exitCode,0);
    await writeFile(file,'{"v":2}');
    await assert.rejects(executeManagedOperation(spec,f.options),/OPERATION_INPUT_CHANGED/);
    await assert.rejects(executeManagedOperation({...spec,stageKey:'escape',inputFiles:['../outside']},f.options),/OPERATION_INPUT_OUTSIDE_WORKSPACE/);
  }finally{await f.cleanup();}
});
