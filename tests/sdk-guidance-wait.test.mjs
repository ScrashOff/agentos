import test from 'node:test';import assert from 'node:assert/strict';
import {mkdtemp,mkdir,cp,appendFile,writeFile,rm} from 'node:fs/promises';import path from 'node:path';import {tmpdir} from 'node:os';import {fileURLToPath} from 'node:url';import {spawn} from 'node:child_process';import {createInterface} from 'node:readline';import {randomUUID} from 'node:crypto';
import {createRuntimeGuidanceServer} from '../bridge/runtime-guidance.mjs';
for(const mode of ['claim','close_before_claim','deadline'])test('actual complete SDK profile guidance wait: '+mode,{timeout:30000},async t=>{
 const root=await mkdtemp(path.join(tmpdir(),'guidance-wait-profile-')),home=root+'/home',work=root+'/workspace',bridge=root+'/bridge';await mkdir(home);await mkdir(work);
 await cp(fileURLToPath(new URL('../bridge',import.meta.url)),bridge,{recursive:true});await cp(fileURLToPath(new URL('./fixtures/dsh-guidance-wait-driver.mjs',import.meta.url)),bridge+'/wait-driver.mjs');
 await appendFile(bridge+'/sdk-control/worker-steering.patch.yml','\n- insert:\n    - id: wait-driver-fixture\n      name: /opt/agentos-bridge/wait-driver.mjs\n      inject: [llm, tools, guidanceWaitToolReady, agentosReadonlyFsReady]\n');
 const identity=Object.fromEntries(['goalId','taskId','runId','runFence','contextVersion'].map(k=>[k,randomUUID()]));const nodeRoot=path.dirname(path.dirname(process.execPath));
 const args=['--unshare-all','--die-with-parent','--new-session','--clearenv','--ro-bind','/usr','/usr','--symlink','usr/bin','/bin','--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64','--proc','/proc','--dev','/dev','--tmpfs','/tmp','--dir','/etc','--ro-bind','/etc/ld.so.cache','/etc/ld.so.cache','--ro-bind',nodeRoot,nodeRoot,'--ro-bind','/opt/agentos-runtime','/opt/agentos-runtime','--ro-bind','/opt/agentos-runtime/harness-node_modules','/opt/deepseek-harness/app/node_modules','--ro-bind',bridge,'/opt/agentos-bridge','--bind',home,'/home/agent','--bind',work,'/workspace','--chdir','/workspace','--setenv','HOME','/home/agent','--setenv','PATH',nodeRoot+'/bin:/usr/bin:/bin','--setenv','AGENTOS_RUN_ID',identity.runId,'--setenv','AGENTOS_RUN_FENCE',identity.runFence,'--setenv','AGENTOS_GUIDANCE_DEADLINE_NS',(process.hrtime.bigint()+(mode==='deadline'?1_500_000_000n:20_000_000_000n)).toString(),'--setenv','AGENTOS_WAIT_FIXTURE_MODE',mode,'--',process.execPath,'/opt/agentos-bridge/sandbox-entry.mjs'];
 const child=spawn('/usr/bin/bwrap',args,{stdio:['pipe','pipe','pipe']});let stderr='',seq=0,owner;const frames=[],pending=new Map(),observations=[];child.stderr.on('data',b=>stderr+=b);
 createInterface({input:child.stdout}).on('line',line=>{let frame;try{frame=JSON.parse(line);}catch{return;}frames.push(frame);if(frame.method==='session.guidanceClaimed'&&owner)observations.push(owner.observe([frame.params.messageId],frame.params));const done=pending.get(frame.id);if(done){pending.delete(frame.id);done(frame);}});
 const closed=new Promise(resolve=>child.on('close',code=>{for(const done of pending.values())done({error:{message:'FIXTURE_EXITED',code}});pending.clear();resolve(code);}));
 t.after(async()=>{if(owner)await owner.close();child.stdin.end();if(child.exitCode===null)child.kill('SIGTERM');await closed;await rm(root,{recursive:true,force:true});});
 const rpc=(method,params)=>new Promise(resolve=>{const id=++seq;pending.set(id,resolve);child.stdin.write(JSON.stringify({jsonrpc:'2.0',id,method,params})+'\n');});
 const until=async predicate=>{for(let i=0;i<1000;i++){const value=frames.find(predicate);if(value)return value;if(child.exitCode!==null)throw Error('FIXTURE_EXITED '+stderr);await new Promise(r=>setTimeout(r,5));}throw Error('FIXTURE_BOUNDARY_TIMEOUT '+stderr);};
 assert.ok((await rpc('initialize',{cwd:'/workspace',provider:'guidance-wait-scripted-fixture',model:'scripted-no-provider',maxTokens:1024})).result,'initialize failed '+stderr);
 await rpc('session/prompt',{sessionId:identity.runId,contentBlocks:[{type:'text',text:'Run the controlled waiting fixture.'}]});
 await until(f=>f.method==='session.event'&&f.params.event.type==='tool/call'&&f.params.event.data.name==='wait_for_guidance');
 if(mode==='deadline'){
  await until(f=>f.method==='session.event'&&f.params.event.type==='turn/end');
  assert.equal(frames.filter(f=>f.method==='session.guidanceClaimed').length,0);
  const result=frames.find(f=>f.method==='session.event'&&f.params.event.type==='tool/result');
  assert.ok(JSON.stringify(result.params.event.data.message).includes('deadline'));
  assert.deepEqual((await rpc('shutdown')).result,{});assert.equal(await closed,0,stderr);return;
 }
 assert.equal((await rpc('session/steer',{sessionId:identity.runId,runId:identity.runId,runFence:randomUUID(),actionId:randomUUID(),contentBlocks:[{type:'text',text:'FOREIGN_GUIDANCE_REJECTED'}]})).error?.message,'STEERING_FENCE_MISMATCH');
 const directory=root+'/journal';await mkdir(directory);
 owner=await createRuntimeGuidanceServer({directory,identity,dispatch:async req=>{const response=await rpc('session/steer',{sessionId:identity.runId,runId:identity.runId,runFence:identity.runFence,actionId:req.actionId,contentBlocks:[{type:'text',text:req.instruction}]});if(response.error)throw Error(response.error.message);return response.result;}});
 const req={...identity,actionId:randomUUID(),instruction:'WAIT_FIXTURE_VALIDATED_GUIDANCE',facts:[]};const queued=await owner.handle(req);assert.equal(queued.state,'queued_next_step');
 await until(f=>f.method==='session.event'&&f.params.event.type==='tool/call'&&f.params.event.data.name==='fixture_boundary_gate');
 assert.equal((await owner.readReceipt(req.actionId)).state,'queued_next_step');assert.equal(frames.filter(f=>f.method==='session.guidanceClaimed').length,0);
 assert.equal((await owner.handle(req)).messageId,queued.messageId);
 const duplicate=await rpc('session/steer',{sessionId:identity.runId,runId:identity.runId,runFence:identity.runFence,actionId:req.actionId,contentBlocks:[{type:'text',text:req.instruction}]});
 assert.equal(duplicate.result.messageId,queued.messageId);
 const insertions=frames.filter(f=>f.method==='session.event'&&f.params.event.type==='agent/inbox/spliced'&&f.params.event.data.inserted?.some(m=>m.id===queued.messageId));assert.equal(insertions.length,1);
 if(mode==='close_before_claim'){
  assert.deepEqual((await rpc('shutdown')).result,{});assert.equal(await closed,0,stderr);await Promise.all(observations);
  assert.equal(frames.filter(f=>f.method==='session.guidanceClaimed').length,0);
  const saved=await owner.readReceipt(req.actionId);assert.equal(saved.state,'queued_next_step');assert.equal(saved.consumptionProof,undefined);return;
 }
 await writeFile(work+'/release-boundary','release actual tool batch');
 await until(f=>f.method==='session.guidanceClaimed');await until(f=>f.method==='session.event'&&f.params.event.type==='turn/end');await Promise.all(observations);
 const receipt=await owner.readReceipt(req.actionId);assert.equal(receipt.state,'consumed');assert.equal(receipt.consumptionProof.source,'sdk-agent-inbox-claimed');assert.equal(receipt.consumptionProof.messageId,queued.messageId);assert.equal(receipt.consumptionProof.runFence,identity.runFence);
 assert.equal(frames.filter(f=>f.method==='session.guidanceClaimed'&&f.params.messageId===queued.messageId).length,1);assert.equal(receipt.effectVerified,undefined);
 assert.deepEqual((await rpc('shutdown')).result,{});assert.equal(await closed,0,stderr);
});
