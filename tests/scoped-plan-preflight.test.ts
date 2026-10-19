import {afterEach,expect,it} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,mkdir,readFile,rm,symlink,writeFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {createScopedPlannerPreflightInputs} from '../server/scoped-plan-preflight.js';
import {scopedPlanContractGuide,scopedPlanJsonSchema,scopedPlanPreflight,
  scopedPlanningRejection,validateScopedPlan,verifyScopedPlanningDelivery} from '../server/scoped-plan-contract.js';

const execute=promisify(execFile),directories:string[]=[];
const digest=(bytes:Buffer|string)=>createHash('sha256').update(bytes).digest('hex');
afterEach(async()=>{await Promise.all(directories.splice(0).map(dir=>rm(dir,{recursive:true,force:true})));});

function fixture(){
  const sourceId=randomUUID(),consumerId=randomUUID(),qaId=randomUUID(),artifactId=randomUUID(),sourceRunId=randomUUID();
  const request={id:randomUUID(),scope:'project/component',source_revision:'a'.repeat(64),snapshot:{
    tasks:[{id:sourceId,state:'accepted',stage:'work',attempt_count:2,acceptance_passed:true,
      has_live_run:false,last_run_id:sourceRunId,last_run_attempt:2,last_run_state:'submitted',dependencies:[],criteria:[],
      execution:{baseWorkspace:'project/component',workKind:'repair'}},
    {id:consumerId,state:'blocked',stage:'integrate',attempt_count:3,dependencies:[],criteria:[{id:'integration'}],
      execution:{baseWorkspace:'project/component',workKind:'implementation',readOnlySource:false}},
    {id:qaId,state:'blocked',stage:'verify',attempt_count:1,dependencies:[sourceId],criteria:[{id:'final-qa'}],
      execution:{baseWorkspace:'project/component',workKind:'verification',readOnlySource:true}}],
    artifacts:[{id:artifactId,task_id:sourceId,name:'implementation.patch',digest:'b'.repeat(64)}],deferredDeliveries:[] as any[]}};
  const plan={version:1,requestId:request.id,sourceRevision:request.source_revision,
    reason:'The accepted repair must reach its existing writable integrator.',tasks:[],blockers:[],dependencyChanges:[],
    actions:[{kind:'prepare_integration',taskId:consumerId,expectedAttempt:3,sourceTaskId:sourceId,
      expectedSourceRunId:sourceRunId,artifactIds:[artifactId],
      reason:'Compose the accepted repair into the existing isolated integration candidate.'}]};
  return {request,plan,sourceId,consumerId,qaId,artifactId,sourceRunId};
}

async function mounted(f=fixture()){
  // Deliberately outside the checkout: the worker does not have this app's
  // node_modules, config, database URL, tsx loader or esbuild at execution time.
  const root=await mkdtemp(path.join(os.tmpdir(),'agentos-planning-preflight-'));directories.push(root);
  const inputRoot=path.join(root,'inputs'),catalogPath=path.join(inputRoot,f.request.id,'planning-catalog.json');
  const catalog=Buffer.from(JSON.stringify(f.request.snapshot));
  const prepared=await createScopedPlannerPreflightInputs(f.request,digest(catalog),inputRoot);
  await mkdir(path.dirname(catalogPath),{recursive:true});await writeFile(catalogPath,catalog);
  for(const input of prepared.inputs){
    const file=path.join(inputRoot,input.id,input.name);
    await mkdir(path.dirname(file),{recursive:true});await writeFile(file,Buffer.from(input.base64,'base64'));
  }
  const draft=path.join(root,'scope-plan.json');await writeFile(draft,JSON.stringify(f.plan));
  const command=async(args:string[])=>{
    try{
      const {stdout,stderr}=await execute(process.execPath,[prepared.guide.cliPath,...args],{
        cwd:root,env:{PATH:process.env.PATH??''},timeout:10000,maxBuffer:4*1024*1024});
      return {exitCode:0,stdout,stderr,result:JSON.parse(stdout)};
    }catch(error:any){
      if(typeof error.code!=='number'||!error.stdout)throw error;
      return {exitCode:error.code,stdout:error.stdout,stderr:error.stderr,result:JSON.parse(error.stdout)};
    }
  };
  return {...f,...prepared,root,draft,catalogPath,catalog,command};
}

it('exports the same complete schema in the real standalone Node CLI, including distinct nested limits',async()=>{
  const f=await mounted(),response=await f.command(['--schema']);
  expect(response.exitCode).toBe(0);expect(response.stderr).toBe('');
  expect(response.result.schema).toEqual(scopedPlanJsonSchema());
  expect(response.result.liveAuthorization).toBe(false);
  expect(scopedPlanContractGuide().fields).toMatchObject({
    reason:{minLength:15,maxLength:3000},actionReason:{minLength:40,maxLength:2000},
    dependencyChangeReason:{minLength:40,maxLength:2000},actionArtifactIds:{minItems:1,maxItems:20},
    dependencyChangeArtifactIds:{minItems:1,maxItems:20},coverageKey:{pattern:'^[a-z0-9_-]{3,80}$'},
  });
  expect(response.result.schema.properties.actions.items.properties.kind.enum)
    .toEqual(['reuse_existing','prepare_integration','request_verification']);
  expect(response.result.schema.properties.sourceRevision.type).toBe('string');
  expect(JSON.parse(Buffer.from(f.inputs.find(i=>i.name==='scope-plan.schema.json')!.base64,'base64').toString()))
    .toEqual(response.result.schema);
});

it('runs read-only in the session, binds catalog/schema/request and never creates an acceptance artifact',async()=>{
  const f=await mounted(),before=JSON.stringify({request:f.request,plan:f.plan});
  const draftBefore=await readFile(f.draft),response=await f.command(['--check',f.draft]);
  expect(response.exitCode).toBe(0);expect(response.stderr).toBe('');
  expect(response.result).toMatchObject({validAgainstFrozenCatalog:true,liveAuthorization:false,effects:'none',
    code:'PLANNING_PREFLIGHT_VALID',requestId:f.request.id,scope:f.request.scope,sourceRevision:f.request.source_revision,
    catalogDigest:digest(f.catalog),draftDigest:digest(draftBefore),counts:{tasks:0,actions:1,dependencyChanges:0,blockers:0}});
  expect(JSON.stringify({request:f.request,plan:f.plan})).toBe(before);
  expect(await readFile(f.draft)).toEqual(draftBefore);expect(await readFile(f.catalogPath)).toEqual(f.catalog);
  for(const input of f.inputs)expect(digest(await readFile(path.join(f.root,'inputs',input.id,input.name)))).toBe(input.digest);
  const receipt=Buffer.from(JSON.stringify(response.result));
  expect(()=>verifyScopedPlanningDelivery({artifacts:[{name:'runtime-plan-check.json',base64:receipt.toString('base64'),digest:digest(receipt)}]}))
    .toThrow('PLANNING_DELIVERY_INVALID');
});

it('returns the authoritative safe errors and permits correcting the same draft before the session ends',async()=>{
  const f=await mounted(),bad=structuredClone(f.plan);
  bad.actions[0].reason='short';bad.actions[0].artifactIds=[];
  await writeFile(f.draft,JSON.stringify(bad));
  let authoritative:any;try{validateScopedPlan(bad,f.request);}catch(error){authoritative=scopedPlanningRejection(error);}
  const rejected=await f.command(['--check',f.draft]);
  expect(rejected.exitCode).toBe(2);expect(rejected.result).toMatchObject(authoritative);
  expect(rejected.result.issues).toEqual(expect.arrayContaining([
    {path:['actions',0,'reason'],code:'too_small',minimum:40},
    {path:['actions',0,'artifactIds'],code:'too_small',minimum:1}]));
  await writeFile(f.draft,JSON.stringify(f.plan));
  const corrected=await f.command(['--check',f.draft]);
  expect(corrected.exitCode).toBe(0);expect(corrected.result.validAgainstFrozenCatalog).toBe(true);
  expect(corrected.result.draftDigest).not.toBe(rejected.result.draftDigest);
  expect(corrected.result.catalogDigest).toBe(rejected.result.catalogDigest);
});

it('rejects a frozen applied reservation in the real CLI and the worker validator before final acceptance proof',async()=>{
  const base=fixture(),reserved=structuredClone(base.request.snapshot.tasks[1]);reserved.id=randomUUID();
  base.request.snapshot.tasks.push(reserved);
  base.request.snapshot.deferredDeliveries=[{id:randomUUID(),producer_task_id:base.sourceId,
    consumer_task_id:reserved.id,source_task_id:randomUUID(),state:'applied'}];
  const f=await mounted(base),draftBefore=await readFile(f.draft);
  const delivered=verifyScopedPlanningDelivery({artifacts:[{name:'scope-plan.json',base64:draftBefore.toString('base64'),digest:digest(draftBefore)}]});
  // These are the exact decoded bytes and validator used by opencode-batch
  // before it is allowed to write runtime-plan-check.json with verified:true.
  let authoritative:any;try{validateScopedPlan(JSON.parse(delivered[0].content.toString()),f.request);}
  catch(error){authoritative=scopedPlanningRejection(error);}
  expect(authoritative).toMatchObject({code:'PLANNING_HANDOFF_CONSUMER_RESERVED',
    issues:[{path:['actions',0,'taskId'],code:'handoff_consumer_reserved'}]});
  const rejected=await f.command(['--check',f.draft]);
  expect(rejected.exitCode).toBe(2);expect(rejected.result).toMatchObject(authoritative);
  expect(rejected.result).toMatchObject({validAgainstFrozenCatalog:false,effects:'none',liveAuthorization:false});
  expect(rejected.stdout+rejected.stderr).not.toContain(reserved.id);
  expect(await readFile(f.draft)).toEqual(draftBefore);expect(await readFile(f.catalogPath)).toEqual(f.catalog);
  const corrected=structuredClone(f.plan);corrected.actions[0].taskId=reserved.id;
  await writeFile(f.draft,JSON.stringify(corrected));
  const checked=await f.command(['--check',f.draft]);
  expect(checked.exitCode).toBe(0);expect(checked.result.catalogDigest).toBe(rejected.result.catalogDigest);
  expect(validateScopedPlan(corrected,f.request).actions[0].taskId).toBe(reserved.id);
  expect(f.guide.handoffReservations.instruction).toContain('One producer reserves one consumer');
});

it('catches conflicting new reservations before either action can reach the live applier',async()=>{
  const base=fixture(),other=structuredClone(base.request.snapshot.tasks[1]);other.id=randomUUID();
  base.request.snapshot.tasks.push(other);base.plan.actions.push({...base.plan.actions[0],kind:'reuse_existing',taskId:other.id});
  const f=await mounted(base),response=await f.command(['--check',f.draft]);
  expect(response.exitCode).toBe(2);expect(response.result).toMatchObject({code:'PLANNING_HANDOFF_CONSUMER_RESERVED',
    issues:[{path:['actions',1,'taskId'],code:'handoff_consumer_conflict_in_plan'}]});
  expect(()=>validateScopedPlan(f.plan,f.request)).toThrow('PLANNING_HANDOFF_CONSUMER_RESERVED');
  const corrected={...f.plan,actions:[f.plan.actions[0],{...f.plan.actions[1],kind:'request_verification',taskId:f.qaId,expectedAttempt:1}]};
  await writeFile(f.draft,JSON.stringify(corrected));expect((await f.command(['--check',f.draft])).exitCode).toBe(0);
});

it.each(['unknown-source','closed-consumer','source-run','consumer-attempt','cross-scope','foreign-evidence','duplicate-action','revision'])
  ('keeps semantic preflight rejection equivalent to final validation for %s',async(kind)=>{
    const base=fixture();
    if(kind==='unknown-source')base.plan.actions[0].sourceTaskId=randomUUID();
    if(kind==='closed-consumer')base.request.snapshot.tasks[1].state='accepted';
    if(kind==='source-run')base.plan.actions[0].expectedSourceRunId=randomUUID();
    if(kind==='consumer-attempt')base.plan.actions[0].expectedAttempt++;
    if(kind==='cross-scope')base.request.snapshot.tasks[1].execution.baseWorkspace='project/elsewhere';
    if(kind==='foreign-evidence')base.request.snapshot.artifacts[0].task_id=base.qaId;
    if(kind==='duplicate-action')base.plan.actions.push({...base.plan.actions[0]});
    if(kind==='revision')base.plan.sourceRevision='c'.repeat(64);
    const f=await mounted(base);
    let authoritative:any;try{validateScopedPlan(f.plan,f.request);}catch(error){authoritative=scopedPlanningRejection(error);}
    expect(authoritative).toBeTruthy();
    const response=await f.command(['--check',f.draft]);
    expect(response.exitCode).toBe(2);expect(response.result).toMatchObject(authoritative);
  });

it('checks new work, verification and dependency changes through the same semantic validator',async()=>{
  const f=await mounted(),plans:any[]=[];
  plans.push({...f.plan,actions:[{...f.plan.actions[0],kind:'request_verification',taskId:f.qaId,expectedAttempt:1}]});
  plans.push({...f.plan,actions:[],tasks:[{coverageKey:'repair-rebase-v2',title:'Rebase the accepted patch',
    description:'Rebase only the verified conflicting preimage and record exact source hashes.',
    baseWorkspace:f.request.scope,deliverToTaskId:f.consumerId,sourceTaskId:f.sourceId,workKind:'repair',lane:'glm'}]});
  // This invalid dependency proposal is a structural success but must still
  // fail semantics; the CLI is not merely a JSON-schema linter.
  plans.push({...f.plan,actions:[],dependencyChanges:[{taskId:f.consumerId,expectedAttempt:3,
    removeDependsOnId:f.sourceId,sourceTaskId:f.sourceId,reason:'This integrator is not an auxiliary eligible for dependency replacement.',artifactIds:[f.artifactId]}]});
  for(const plan of plans){
    const expected=scopedPlanPreflight(plan,f.request);await writeFile(f.draft,JSON.stringify(plan));
    const response=await f.command(['--check',f.draft]);expect(response.result).toMatchObject(expected);
  }
});

it('does not authorize a current source changed after a successful frozen preflight',async()=>{
  const f=await mounted(),checked=await f.command(['--check',f.draft]);expect(checked.exitCode).toBe(0);
  const current=structuredClone(f.request);current.snapshot.tasks[0].has_live_run=true;
  expect(()=>validateScopedPlan(f.plan,current)).toThrow('PLANNING_ACTION_INVALID');
  current.source_revision='d'.repeat(64);
  expect(()=>validateScopedPlan(f.plan,current)).toThrow('PLANNING_SOURCE_CHANGED');
  expect(checked.result.liveAuthorization).toBe(false);expect(f.request.snapshot.tasks[0].has_live_run).toBe(false);
});

it('rejects changed frozen catalog bytes even when the draft would validate against them',async()=>{
  const f=await mounted();await writeFile(f.catalogPath,JSON.stringify({...f.request.snapshot,unexpected:'changed'}));
  const response=await f.command(['--check',f.draft]);
  expect(response.exitCode).toBe(2);expect(response.result.code).toBe('PLANNING_PREFLIGHT_CATALOG_CHANGED');
});

it('binds the helper to its contract and does not accept an alternate catalog argument',async()=>{
  const f=await mounted();
  expect((await f.command(['--check',f.draft,'--catalog',f.catalogPath])).result.code).toBe('PLANNING_PREFLIGHT_ARGUMENTS');
  const cli=await readFile(f.guide.cliPath,'utf8');
  await writeFile(f.guide.cliPath,cli.replace(f.guide.schemaDigest,'e'.repeat(64)));
  expect((await f.command(['--schema'])).result.code).toBe('PLANNING_PREFLIGHT_SCHEMA_CHANGED');
});

it('refuses symlinked drafts and emits only sanitized field metadata for malformed values or JSON',async()=>{
  const f=await mounted(),link=path.join(f.root,'linked-plan.json');await symlink(f.draft,link);
  expect((await f.command(['--check',link])).result.code).toBe('PLANNING_PREFLIGHT_PATH');
  const secret='sk-private-value-never-returned';
  await writeFile(f.draft,JSON.stringify({...f.plan,requestId:secret,[secret]:'untrusted'}));
  const response=await f.command(['--check',f.draft]);
  expect(response.exitCode).toBe(2);expect(response.stdout+response.stderr).not.toContain(secret);
  expect(response.result.issues).toContainEqual({path:['requestId'],code:'invalid_format',expected:'uuid'});
  await writeFile(f.draft,'{"'+secret+'":');
  const malformed=await f.command(['--check',f.draft]);
  expect(malformed.result.code).toBe('PLANNING_JSON_INVALID');expect(malformed.stdout+malformed.stderr).not.toContain(secret);
});
