import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,cp,rm} from 'node:fs/promises';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

test('SDK control concurrency and closure tests run against the installed SDK in the worker dependency namespace',async()=>{
 const root=await mkdtemp(path.join(tmpdir(),'agentos-sdk-boundary-'));
 try{
  const bridge=path.join(root,'bridge'),tests=path.join(root,'tests'),home=path.join(root,'home');
  await cp(fileURLToPath(new URL('../bridge',import.meta.url)),bridge,{recursive:true});
  await mkdir(tests);await mkdir(home);
  for(const name of ['steering-control.test.mjs','sdk-steering-plugin.test.mjs'])
   await cp(fileURLToPath(new URL(name,import.meta.url)),path.join(tests,name));
  const nodeRoot=path.dirname(path.dirname(process.execPath));
  const args=['--unshare-all','--die-with-parent','--clearenv','--ro-bind','/usr','/usr','--symlink','usr/bin','/bin',
   '--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64','--proc','/proc','--dev','/dev','--tmpfs','/tmp',
   '--dir','/etc','--ro-bind','/etc/ld.so.cache','/etc/ld.so.cache','--ro-bind',nodeRoot,nodeRoot,
   '--ro-bind','/opt/agentos-runtime/harness-node_modules','/opt/deepseek-harness/app/node_modules',
   '--ro-bind',bridge,'/bridge','--ro-bind',tests,'/tests','--bind',home,'/home/agent',
   '--setenv','HOME','/home/agent','--setenv','PATH',nodeRoot+'/bin:/usr/bin:/bin',
   '--',process.execPath,'--test','/tests/steering-control.test.mjs','/tests/sdk-steering-plugin.test.mjs'];
  const result=spawnSync('/usr/bin/bwrap',args,{encoding:'utf8',timeout:30000});
  assert.equal(result.status,0,JSON.stringify({error:result.error?.code,stderr:result.stderr,stdout:result.stdout}));
  assert.match(result.stdout,/tests 6/);assert.match(result.stdout,/fail 0/);
 }finally{await rm(root,{recursive:true,force:true});}
});
