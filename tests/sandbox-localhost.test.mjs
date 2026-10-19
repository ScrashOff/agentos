import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readFile,stat,symlink,mkdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {prepareSandboxLocalhost} from '../bridge/sandbox-localhost.mjs';
const fresh=async()=>path.join(await mkdtemp(path.join(tmpdir(),'agentos-loopback-')),'home');
test('creates only static loopback hosts in a fresh private home and returns a readonly mount',async()=>{
 const home=await fresh(),result=await prepareSandboxLocalhost(home);
 assert.equal(await readFile(result.hostsPath,'utf8'),'127.0.0.1 localhost\n::1 localhost\n');
 assert.equal((await stat(home)).mode&0o777,0o700);assert.equal((await stat(result.hostsPath)).mode&0o777,0o444);
 assert.deepEqual(result.mount,['--ro-bind',result.hostsPath,'/etc/hosts']);
});
test('cannot reuse an existing home or overwrite an existing hosts file',async()=>{
 const home=await fresh(),result=await prepareSandboxLocalhost(home),before=await readFile(result.hostsPath);
 await assert.rejects(prepareSandboxLocalhost(home),{code:'EEXIST'});assert.deepEqual(await readFile(result.hostsPath),before);
});
test('rejects a preexisting symlink without writing through it',async()=>{
 const home=await fresh(),target=await fresh();await mkdir(target);await symlink(target,home);
 await assert.rejects(prepareSandboxLocalhost(home),{code:'EEXIST'});
 await assert.rejects(stat(path.join(target,'hosts')),{code:'ENOENT'});
});
test('rejects relative and missing home identities',async()=>{
 for(const home of ['relative',null,undefined])await assert.rejects(prepareSandboxLocalhost(home),TypeError);
});
test('real isolated namespace resolves localhost and hosts remains read-only without external IPv4 routes',async()=>{
 const result=await prepareSandboxLocalhost(await fresh());
 const nodeRoot=path.dirname(path.dirname(process.execPath));
 const args=['--unshare-all','--die-with-parent','--clearenv','--ro-bind','/usr','/usr','--symlink','usr/bin','/bin',
  '--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64','--proc','/proc','--dev','/dev','--tmpfs','/tmp',
  '--dir','/etc','--ro-bind','/etc/ld.so.cache','/etc/ld.so.cache','--ro-bind',nodeRoot,nodeRoot,...result.mount,
  process.execPath,'--input-type=module','-e',`import dns from 'node:dns/promises';import fs from 'node:fs';
const rows=await dns.lookup('localhost',{all:true});
if(!rows.length||rows.some(r=>!['127.0.0.1','::1'].includes(r.address)))process.exit(2);
let refused=false;try{fs.writeFileSync('/etc/hosts','changed');}catch(e){refused=e.code==='EROFS';}
if(!refused)process.exit(3);
const routes=fs.readFileSync('/proc/net/route','utf8').trim().split('\\n').slice(1);if(routes.length)process.exit(4);
console.log(JSON.stringify({rows,readOnly:true,externalRoutes:0}));`];
 const p=spawnSync('/usr/bin/bwrap',args,{encoding:'utf8',timeout:15000,env:{PATH:'/usr/bin:/bin',LANG:'C.UTF-8'}});
 assert.equal(p.status,0,JSON.stringify({status:p.status,error:p.error?.code,stderr:p.stderr}));
 const proof=JSON.parse(p.stdout);assert.equal(proof.readOnly,true);assert.equal(proof.externalRoutes,0);assert.ok(proof.rows.length);
});
