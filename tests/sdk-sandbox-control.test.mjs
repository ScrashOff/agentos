import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,rm,cp,writeFile,readFile,appendFile,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {createInterface} from 'node:readline';
import {randomUUID} from 'node:crypto';
import {fileURLToPath} from 'node:url';

test('actual isolated DSH profile loads the versioned steering plugin and shuts down without a provider request', {timeout:45000}, async t=>{
 const root=await mkdtemp(path.join(tmpdir(),'agentos-sdk-control-'));
 const home=path.join(root,'home'),work=path.join(root,'workspace');
 await mkdir(home);await mkdir(work);
 await writeFile(path.join(work,'fixture.txt'),'safe fixture\n');
 await writeFile(path.join(home,'outside.txt'),'private fixture\n');
 await symlink('/home/agent/outside.txt',path.join(work,'escape.txt'));
 const bridge=path.join(root,'bridge');
 await cp(fileURLToPath(new URL('../bridge',import.meta.url)),bridge,{recursive:true});
 await cp(fileURLToPath(new URL('./fixtures/dsh-readonly-profile-plugin.mjs',import.meta.url)),path.join(bridge,'profile-proof-plugin.mjs'));
 // Test-only addon is appended solely to the disposable bridge copy.
 await appendFile(path.join(bridge,'sdk-control/worker-steering.patch.yml'),'\n- insert:\n    - id: readonly-profile-proof\n      name: /opt/agentos-bridge/profile-proof-plugin.mjs\n      inject: [sdkAppStartup, tools, fs, systemPrompt, agentosReadonlyFsReady]\n');
 const nodeRoot=path.dirname(path.dirname(process.execPath));
 const runId=randomUUID(),runFence=randomUUID();
 const args=['--unshare-all','--die-with-parent','--clearenv','--new-session','--ro-bind','/usr','/usr',
 '--symlink','usr/bin','/bin','--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64','--proc','/proc','--dev','/dev','--tmpfs','/tmp',
 '--dir','/etc','--ro-bind','/etc/ld.so.cache','/etc/ld.so.cache','--ro-bind',nodeRoot,nodeRoot,
 '--ro-bind','/opt/agentos-runtime','/opt/agentos-runtime',
 '--ro-bind','/opt/agentos-runtime/harness-node_modules','/opt/deepseek-harness/app/node_modules',
 '--ro-bind',bridge,'/opt/agentos-bridge','--bind',home,'/home/agent','--bind',work,'/workspace','--chdir','/workspace',
 '--setenv','HOME','/home/agent','--setenv','PATH',nodeRoot+'/bin:/usr/bin:/bin','--setenv','LANG','C.UTF-8',
 '--setenv','AGENTOS_RUN_ID',runId,'--setenv','AGENTOS_RUN_FENCE',runFence,
 '--',process.execPath,'/opt/agentos-bridge/sandbox-entry.mjs'];
 const child=spawn('/usr/bin/bwrap',args,{stdio:['pipe','pipe','pipe']});
 let stderr='',frames=[];child.stderr.on('data',b=>stderr+=b);
 const pending=new Map();
 const lines=createInterface({input:child.stdout});lines.on('line',line=>{
  let frame;try{frame=JSON.parse(line);}catch{return;}
  frames.push(frame);const fn=pending.get(frame.id);if(fn){pending.delete(frame.id);fn(frame);}
 });
 const closed=new Promise(resolve=>child.on('close',code=>{for(const fn of pending.values())fn({error:{message:'EXITED '+code+' '+stderr}});pending.clear();resolve(code);}));
 t.after(async()=>{child.stdin.end();if(child.exitCode===null)child.kill('SIGTERM');await closed;await rm(root,{recursive:true,force:true});});
 let nextId=0;
 const rpc=(method,params)=>new Promise(resolve=>{const id=++nextId;pending.set(id,resolve);child.stdin.write(JSON.stringify({jsonrpc:'2.0',id,method,params})+'\n');});
 const init=await rpc('initialize',{cwd:'/workspace',provider:'deepseek-official',model:'runtime-no-provider-test',maxTokens:1024});
 assert.equal(init.result?.serverInfo?.name,'deepseek-harness-sdk-runtime',JSON.stringify({init,stderr}));
 const steer=await rpc('session/steer',{sessionId:runId,runId,runFence,actionId:randomUUID(),contentBlocks:[{type:'text',text:'Bounded guidance before any prompt.'}]});
 assert.equal(steer.error?.message,'STEERING_SESSION_NOT_READY',JSON.stringify({steer,stderr}));
 const proof=JSON.parse(await readFile(path.join(work,'profile-proof.json'),'utf8'));
 assert.equal(proof.fixtureRead,true);assert.equal(proof.lexicalAndRealpathEscapesRejected,true);
 assert.equal(proof.modelInvoked,false);assert.equal(proof.rawTelemetryPersisted,false);
 const shut=await rpc('shutdown');assert.deepEqual(shut.result,{});
 assert.equal(await closed,0,stderr);
 assert.ok(!frames.some(f=>f.method==='session.event'));
});
