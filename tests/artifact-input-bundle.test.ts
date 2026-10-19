import {beforeAll,beforeEach,afterAll,afterEach,it,expect,vi} from 'vitest';
import {mkdir,writeFile,readFile,chmod,symlink,mkdtemp} from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import {randomUUID,createHash} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {config} from '../server/config.js';
import {runtimeInputs} from '../server/execution-inputs.js';
import * as bundles from '../server/artifact-input-bundle.js';
import {hydrateArtifactInputBundle,inputBundleDigest,inputBundleValueDigest} from '../bridge/artifact-input-bundle.mjs';
import {inputFailureMetadata} from '../server/input-failure.js';
import {executeRuntime} from '../server/runtime.js';
import {accountBridge} from '../server/bridge.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{vi.restoreAllMocks();await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
 await pool.query("INSERT INTO agents(id,name,department,capabilities,runtime,version,description) VALUES('bundle-fixture','Fixture','test','[]','test',1,'Fixture')");});
afterEach(()=>{vi.restoreAllMocks();config.remoteExecution=false;});
afterAll(()=>pool.end());

async function fixture(large=false){
 const root=await mkdtemp(path.join(os.tmpdir(),'agentos-bundle-')),bundleRoot=root+'/bundles',artifactRoot=root+'/artifacts';
 await mkdir(bundleRoot,{mode:0o700});await mkdir(artifactRoot,{mode:0o700});
 const goal=randomUUID(),taskId=randomUUID(),runId=randomUUID(),fence=randomUUID(),contextId=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Bundle','Exact original contract','bundle-test',$1::text)",[goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count,execution)
  VALUES($1,$2,'worker','Bundle worker','Preserve criteria','bundle-fixture','running','work',
  '[{"id":"report","type":"artifact","name":"report.json"}]',1,'{"kind":"harness","workspace":"test/isolated"}')`,[taskId,goal]);
 const run=(await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running') RETURNING *",[runId,taskId,fence])).rows[0];
 const refs:any[]=[],inputs:any[]=[],read=vi.fn();
 for(let n=0;n<(large?4:1);n++){
  const id=randomUUID(),bytes=Buffer.alloc(large?7*1024*1024:128,n+1),digest=createHash('sha256').update(bytes).digest('hex'),name='source-'+n+'.bin';
  await writeFile(artifactRoot+'/'+id,bytes,{flag:'wx',mode:0o600});
  refs.push({id,name,digest,task_id:taskId});inputs.push({id,name,digest,base64:bytes.toString('base64')});
 }
 read.mockImplementation(async(id:string)=>{const i=inputs.find(i=>i.id===id);if(!i)throw Error('UNKNOWN_ARTIFACT');return {...i,bytes:Buffer.from(i.base64,'base64')};});
 const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[taskId])).rows[0];
 const context={id:contextId,manifest:{executionInputs:[],dependencyArtifacts:refs},content:'Only exact current evidence.'};
 await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)',[contextId,runId,JSON.stringify(context.manifest),context.content]);
 await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[runId,contextId]);
 return {goal,task,run,context,refs,inputs,read,roots:{bundleRoot,artifactRoot},
  params:{runId,runFence:fence,inputs:[],context}};
}
const signal=()=>new AbortController().signal;

it('preserves all bytes above the old24MiB transport and hydrates only the fenced private reference',async()=>{
 const f=await fixture(true);
 await expect(runtimeInputs(f.task,f.context.manifest,f.read)).rejects.toMatchObject({code:'SUPERVISION_INPUTS_TOO_LARGE'});
 const inputs=await runtimeInputs(f.task,f.context.manifest,f.read,{transport:'referenced-v1'});
 const ref=await bundles.stageArtifactInputBundle(f.run,f.task,f.context,inputs,f.roots);
 const request={...f.params,inputBundle:ref};expect(JSON.stringify(request).length).toBeLessThan(5000);
 const result=await hydrateArtifactInputBundle(request,signal(),f.roots);
 expect(result.inputs).toEqual(inputs);expect(result.inputs).toHaveLength(4);
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run.id])).rows[0].state).toBe('running');
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task.id])).rows[0].state).toBe('running');
});

it('sends a compact real runtime request, preserving original result/stop and acceptance authority',async()=>{
 const f=await fixture(true),stage=bundles.stageArtifactInputBundle;
 // Isolated filesystem root only; real SQL/fence/manifest checks still execute.
 vi.spyOn(bundles,'stageArtifactInputBundle').mockImplementation((r,t,c,i)=>stage(r,t,c,i,f.roots));
 const readArtifact=await import('../server/service.js');vi.spyOn(readArtifact,'readArtifact').mockImplementation(f.read);
 config.remoteExecution=true;
 const events:any[]=[],request=vi.spyOn(accountBridge,'request').mockImplementation(async(_method,params:any)=>{
  expect(params.inputs).toEqual([]);expect(JSON.stringify(params).length).toBeLessThan(10000);
  const loaded=await hydrateArtifactInputBundle(params,signal(),f.roots);expect(loaded.inputs).toHaveLength(4);
  return {summary:'Transport verified; no game accepted.',artifacts:[],usage:{},evidence:{stopped:true}};
 });
 const result=await executeRuntime({run:f.run,task:f.task,account:{bridge_id:'fixture'},context:f.context,
  signal:signal(),onEvent:async(kind,payload)=>{events.push({kind,payload});}});
 expect(result.summary).toContain('no game accepted');expect(request).toHaveBeenCalledTimes(1);
 expect(events.find(e=>e.kind==='INPUT_BUNDLE_PREPARED').payload).toMatchObject({inputCount:4,totalBytes:28*1024*1024});
 expect((await pool.query('SELECT count(*)::int n FROM acceptances')).rows[0].n).toBe(0);
});

it.each(['fence','run','context','snapshot','stopped'])('refuses changed %s ownership before publishing a materialization manifest',async(change)=>{
 const f=await fixture(),run={...f.run},context:any={...f.context,manifest:{...f.context.manifest}};
 if(change==='fence')run.fence=randomUUID();
 if(change==='run')run.id=randomUUID();
 if(change==='context')context.manifest.extra='Changed';
 if(change==='snapshot')run.instruction_snapshot={...run.instruction_snapshot,unexpected:true};
 if(change==='stopped')await pool.query("UPDATE runs SET state='failed' WHERE id=$1",[run.id]);
 await expect(bundles.stageArtifactInputBundle(run,f.task,context,f.inputs,f.roots)).rejects.toMatchObject({code:'INPUT_BUNDLE_OWNER_CHANGED'});
});

it.each(['request-fence','request-run','context','corrupt','symlink','public-manifest'])('refuses %s at the bridge before returning any usable bundle',async(change)=>{
 const f=await fixture(),ref=await bundles.stageArtifactInputBundle(f.run,f.task,f.context,f.inputs,f.roots);
 const params:any={...f.params,inputBundle:{...ref}},file=path.join(f.roots.bundleRoot,ref.runId+'-'+ref.fence,'manifest.json');
 if(change==='request-fence')params.runFence=randomUUID();
 if(change==='request-run')params.runId=randomUUID();
 if(change==='context')params.context={...f.context,manifest:{...f.context.manifest,unexpected:true}};
 if(change==='corrupt')await writeFile(f.roots.artifactRoot+'/'+f.inputs[0].id,'changed');
 if(change==='symlink'){
  const {unlink}=await import('node:fs/promises');await unlink(f.roots.artifactRoot+'/'+f.inputs[0].id);
  await symlink(file,f.roots.artifactRoot+'/'+f.inputs[0].id);
 }
 if(change==='public-manifest')await chmod(file,0o644);
 await expect(hydrateArtifactInputBundle(params,signal(),f.roots)).rejects.toMatchObject({code:expect.stringMatching(/^INPUT_BUNDLE_/)});
});

it('rejects an injected descriptor path even if its manifest bytes are hashed',async()=>{
 const f=await fixture(),ref=await bundles.stageArtifactInputBundle(f.run,f.task,f.context,f.inputs,f.roots);
 const file=path.join(f.roots.bundleRoot,ref.runId+'-'+ref.fence,'manifest.json'),m=JSON.parse(await readFile(file,'utf8'));
 m.inputs[0].path='/etc/passwd';const bytes=Buffer.from(JSON.stringify(m));await writeFile(file,bytes);
 await expect(hydrateArtifactInputBundle({...f.params,inputBundle:{...ref,digest:inputBundleDigest(bytes)}},signal(),f.roots)).rejects.toMatchObject({code:'INPUT_BUNDLE_INVALID'});
});

it('keeps operator inline caps and corrupted artifact checks on the referenced route',async()=>{
 const f=await fixture(),bad={...f.task,execution:{...f.task.execution,inputs:[{...f.inputs[0],base64:Buffer.alloc(8*1024*1024+1).toString('base64')} ]}};
 await expect(runtimeInputs(bad,f.context.manifest,f.read,{transport:'referenced-v1'})).rejects.toMatchObject({code:'SUPERVISION_INPUT_DIGEST'});
 await expect(runtimeInputs(f.task,{...f.context.manifest,dependencyArtifacts:[{...f.refs[0],digest:'f'.repeat(64)}]},f.read,{transport:'referenced-v1'})).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED',inputFailure:{subcode:'ARTIFACT_METADATA'}});
});

it('honors cancellation and produces no fake tools/usage/acceptance',async()=>{
 const f=await fixture(),ref=await bundles.stageArtifactInputBundle(f.run,f.task,f.context,f.inputs,f.roots),controller=new AbortController();controller.abort();
 await expect(hydrateArtifactInputBundle({...f.params,inputBundle:ref},controller.signal,f.roots)).rejects.toMatchObject({name:'AbortError'});
 expect(await hydrateArtifactInputBundle({inputs:f.inputs},signal(),f.roots)).toEqual({inputs:f.inputs});
 expect(inputBundleValueDigest({a:1,b:2})).toBe(inputBundleValueDigest({b:2,a:1}));
 expect(inputFailureMetadata({code:'INPUT_MANIFEST_CHANGED',message:'private',inputFailure:{subcode:'DECISION_UNPROVEN',phase:'effective_source_manifest',token:'secret'}}))
  .toEqual({code:'INPUT_MANIFEST_CHANGED',subcode:'DECISION_UNPROVEN',phase:'effective_source_manifest'});
});
