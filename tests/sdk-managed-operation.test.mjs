import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,cp,appendFile,writeFile,readFile,readdir,rm} from 'node:fs/promises';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {spawn} from 'node:child_process';
import {createInterface} from 'node:readline';
import {randomUUID,createHash} from 'node:crypto';

for(const mode of ['success','negative'])test('actual SDK managed operation registers, executes and reuses receipt: '+mode,{timeout:45000},async t=>{
 const root=await mkdtemp(path.join(tmpdir(),'agentos-sdk-operation-'));
 const home=root+'/home',work=root+'/workspace',bridge=root+'/bridge';
 await mkdir(home);await mkdir(work);await mkdir(work+'/evidence');await mkdir(work+'/data');
 await cp(fileURLToPath(new URL('../bridge',import.meta.url)),bridge,{recursive:true});
 await cp(fileURLToPath(new URL('./fixtures/dsh-managed-operation-driver.mjs',import.meta.url)),bridge+'/operation-driver.mjs');
 await appendFile(bridge+'/sdk-control/worker-steering.patch.yml',
  '\n- insert:\n    - id: managed-operation-driver-fixture\n      name: /opt/agentos-bridge/operation-driver.mjs\n      inject: [llm, tools, managedOperationToolReady, agentosReadonlyFsReady]\n');
 const identity=Object.fromEntries(['goalId','taskId','runId','runFence','contextVersion'].map(key=>[key,randomUUID()]));
 const snapshot=JSON.stringify({version:1,runId:identity.runId,candidate:'actual-sdk-fixture'});
 await writeFile(work+'/.agentos-snapshot.json',snapshot);
 const policy={version:1,...identity,workspace:'/workspace',
  sourceSnapshotDigest:createHash('sha256').update(snapshot).digest('hex'),maxDeadlineMs:10000};
 await writeFile(root+'/operation-policy.json',JSON.stringify(policy));
 await writeFile(work+'/operation-input.txt','immutable fixture input');
 await writeFile(work+'/operation-fixture.mjs',
  "import{appendFile,readFile}from'node:fs/promises';if(await readFile('/workspace/operation-input.txt','utf8')!=='immutable fixture input')throw Error('FIXTURE_INPUT_CHANGED');await appendFile('/workspace/evidence/operation-effect.txt','one execution\\n');console.log('managed fixture output');await new Promise(resolve=>setTimeout(resolve,100));if(process.argv[2]==='negative'){console.error('negative fixture output');process.exitCode=3;}\n");
 const nodeRoot=path.dirname(path.dirname(process.execPath));
 const args=['--unshare-all','--die-with-parent','--new-session','--clearenv',
  '--ro-bind','/usr','/usr','--symlink','usr/bin','/bin','--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64',
  '--proc','/proc','--dev','/dev','--tmpfs','/tmp','--dir','/etc','--dir','/run',
  '--ro-bind','/etc/ld.so.cache','/etc/ld.so.cache','--ro-bind',nodeRoot,nodeRoot,
  '--ro-bind','/opt/agentos-runtime','/opt/agentos-runtime',
  '--ro-bind','/opt/agentos-runtime/harness-node_modules','/opt/deepseek-harness/app/node_modules',
  '--ro-bind',bridge,'/opt/agentos-bridge','--bind',home,'/home/agent',
  '--ro-bind',work,'/workspace','--bind',work+'/evidence','/workspace/evidence','--bind',work+'/data','/workspace/data',
  '--ro-bind',root+'/operation-policy.json','/run/operation-policy.json',
  '--chdir','/workspace','--setenv','HOME','/home/agent','--setenv','PATH',nodeRoot+'/bin:/usr/bin:/bin',
  '--setenv','LANG','C.UTF-8','--setenv','AGENTOS_RUN_ID',identity.runId,'--setenv','AGENTOS_RUN_FENCE',identity.runFence,
  '--setenv','AGENTOS_MANAGED_OPERATIONS','1','--setenv','AGENTOS_OPERATION_FIXTURE_MODE',mode,
  '--',process.execPath,'/opt/agentos-bridge/sandbox-entry.mjs'];
 const child=spawn('/usr/bin/bwrap',args,{stdio:['pipe','pipe','pipe']});
 let stderr='',seq=0;
 const frames=[],pending=new Map();
 child.stderr.on('data',bytes=>{stderr=(stderr+bytes).slice(-32000);});
 createInterface({input:child.stdout}).on('line',line=>{
  let frame;try{frame=JSON.parse(line);}catch{return;}
  frames.push(frame);const done=pending.get(frame.id);
  if(done){pending.delete(frame.id);done(frame);}
 });
 const closed=new Promise(resolve=>child.once('close',code=>{
  for(const done of pending.values())done({error:{message:'FIXTURE_EXITED',code}});
  pending.clear();resolve(code);
 }));
 t.after(async()=>{
  child.stdin.end();if(child.exitCode===null)child.kill('SIGTERM');await closed;
  await rm(root,{recursive:true,force:true});
 });
 const rpc=(method,params)=>new Promise(resolve=>{
  const id=++seq;pending.set(id,resolve);child.stdin.write(JSON.stringify({jsonrpc:'2.0',id,method,params})+'\n');
 });
 const until=async(predicate,from)=>{
  for(let i=0;i<2000;i++){
   const frame=frames.slice(from).find(predicate);if(frame)return frame;
   if(child.exitCode!==null)throw Error('FIXTURE_EXITED '+stderr);
   await new Promise(resolve=>setTimeout(resolve,5));
  }
  throw Error('FIXTURE_BOUNDARY_TIMEOUT '+stderr);
 };
 const initialized=await rpc('initialize',{cwd:'/workspace',provider:'managed-operation-scripted-fixture',model:'scripted-no-provider',maxTokens:1024});
 assert.equal(initialized.result?.serverInfo?.name,'deepseek-harness-sdk-runtime',JSON.stringify({initialized,stderr}));
 for(let invocation=1;invocation<=2;invocation++){
  const from=frames.length;
  await rpc('session/prompt',{sessionId:identity.runId,contentBlocks:[{type:'text',text:'Execute the same bounded managed fixture stage; reuse its receipt if it already completed.'}]});
  await until(frame=>frame.method==='session.event'&&frame.params.event.type==='turn/end',from);
  const proof=JSON.parse(await readFile(work+'/evidence/sdk-operation-proof.json','utf8'));
  assert.equal(proof.invocations,invocation);assert.equal(proof.modelSteps,invocation*2);
  assert.equal(proof.exitCode,mode==='negative'?3:0);assert.equal(proof.acceptance,false);
  assert.equal(proof.singleEffect,true);assert.equal(proof.fixedPolicyReadOnly,true);assert.equal(proof.snapshotReadOnly,true);
  const events=frames.slice(from).filter(frame=>frame.method==='session.event').map(frame=>frame.params.event);
  assert.deepEqual(events.filter(event=>event.type==='tool/call').map(event=>event.data.name),['managed_operation']);
  const result=events.find(event=>event.type==='tool/result');
  assert.ok(result,'actual SDK tool result is required');
  const block=result.data.message.content.find(value=>value.type==='tool-result');
  assert.equal(block?.isError??false,false,JSON.stringify(block));
  const receipt=JSON.parse(block.content.filter(value=>value.type==='text').map(value=>value.text).join('\n'));
  assert.equal(receipt.operationId,proof.operationId);assert.equal(receipt.acceptance,false);
  assert.equal(receipt.physicalExitConfirmed,true);assert.equal(receipt.exitCode,mode==='negative'?3:0);
  assert.equal(receipt.passed,mode==='negative'?false:undefined);
  if(invocation===2)assert.equal(receipt.reused,true);
 }
 assert.equal((await readdir(work+'/evidence/managed-operations')).length,1);
 assert.equal(await readFile(work+'/evidence/operation-effect.txt','utf8'),'one execution\n');
 assert.deepEqual(JSON.parse(await readFile(root+'/operation-policy.json','utf8')),policy);
 assert.equal(await readFile(work+'/.agentos-snapshot.json','utf8'),snapshot);
 assert.deepEqual((await rpc('shutdown')).result,{});assert.equal(await closed,0,stderr);
});
