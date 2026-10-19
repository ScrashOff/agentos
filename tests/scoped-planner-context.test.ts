import { it, expect } from 'vitest';
import { createHash, randomUUID } from 'node:crypto';
import { createScopedPlannerCatalogReaderInput, projectScopedPlannerContext, scopedPlannerContextVersion } from '../server/scoped-planner-context.js';

const digest = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const unpack = (table: any) => table.rows.map((row: any[]) =>
  Object.fromEntries(table.schemas[row[0]].map((key: string, index: number) => [key, row[index + 1]])));

it.each(['inline','on_demand'])('exposes the exact producer reservation through a bounded catalog query in %s context',async mode=>{
  const producer=randomUUID(),consumer=randomUUID(),baseline=randomUUID();
  const deliveries=Array.from({length:603},()=>({id:randomUUID(),producer_task_id:randomUUID(),consumer_task_id:randomUUID(),
    source_task_id:randomUUID(),state:'applied',consumer_proof:{detail:'Preserved historical proof '.repeat(90)}}));
  deliveries[417]={...deliveries[417],producer_task_id:producer,consumer_task_id:consumer,source_task_id:baseline};
  const snapshot={tasks:[],artifacts:[],opportunities:{consumers:[]},coverage:[],repairConsumption:[],deferredDeliveries:deliveries};
  const bytes=Buffer.from(JSON.stringify(snapshot)),catalog={path:'/inputs/planning-catalog.json',digest:digest(snapshot),
    ...(mode==='on_demand'?{readerPath:'/inputs/planning-query.mjs',readerDigest:'b'.repeat(64)}:{})};
  const projected=projectScopedPlannerContext(snapshot,catalog,[]),guide=projected.handoffReservations;
  expect(guide).toMatchObject({kinds:['reuse_existing','prepare_integration'],authority:'frozen_catalog_only',
    query:{operation:'find',jsonPath:['deferredDeliveries'],where:{producer_task_id:'<sourceTaskId>'}}});
  expect(guide.instruction).toContain('including an applied handoff');
  expect(guide.instruction).toContain('request_verification');
  if(mode==='on_demand')expect(projected.catalogQueries.operations.deferredDeliveries).toEqual(guide.query);
  const {queryCatalog}=await import(new URL('../scripts/planner-catalog-query.mjs',import.meta.url).href);
  const page=queryCatalog(bytes,catalog.digest,{...guide.query,where:{producer_task_id:producer}});
  expect(page.selection).toMatchObject({total:1,returned:1,complete:true,nextOffset:null});
  expect(page.entries[0].identity).toMatchObject({producer_task_id:producer,consumer_task_id:consumer,source_task_id:baseline,state:'applied'});
  expect(page.entries[0].reference).toMatchObject({jsonPath:['deferredDeliveries',417],valueSha256:digest(deliveries[417])});
  expect(Buffer.from(JSON.stringify(snapshot))).toEqual(bytes);
});

it.each(['inline', 'on_demand'])('preserves current no-work reconciliation as an exact observation-only catalog reference in %s contexts', mode => {
  const previousNoWork = { requestId: randomUUID(), reconciliation: { authority: 'observation_only',
    consumerReadyClaimed: false, criterionAccepted: false,
    entries: Array.from({length: 300}, () => ({ taskId: randomUUID(), criterionId: 'final',
      acceptedTaskIds: [randomUUID()], pendingTaskIds: [randomUUID()],
      code: 'BLOCKER_PREREQUISITES_PENDING', evidenceContentRead: false })) } };
  const snapshot = { tasks: [], artifacts: [], opportunities: {consumers: []}, coverage: [],
    repairConsumption: [], deferredDeliveries: [], previousNoWork };
  const original = structuredClone(snapshot), catalog = { path: '/inputs/planning-catalog.json', digest: digest(snapshot),
    ...(mode === 'on_demand' ? { readerPath: '/inputs/planning-query.mjs', readerDigest: 'b'.repeat(64) } : {}) };
  const projected = projectScopedPlannerContext(snapshot, catalog, []);
  expect(projected.previousNoWork).toMatchObject({ requestId: previousNoWork.requestId, authority: 'observation_only',
    record: {$catalog: {jsonPath: ['previousNoWork'], valueSha256: digest(previousNoWork),
      bytes: Buffer.byteLength(JSON.stringify(previousNoWork))}} });
  expect(projected.previousNoWork).not.toHaveProperty('reconciliation');
  expect(projected.previousNoWork.instruction).toContain('does not prove consumption, criterion acceptance or QA readiness');
  expect(JSON.stringify(projected)).not.toContain(previousNoWork.reconciliation.entries[0].taskId);
  expect(JSON.stringify(projected).length).toBeLessThan(12000);
  expect(projected.fullCatalog).toEqual(catalog);
  expect(snapshot).toEqual(original);
});

it('keeps exact criteria and every live actor while recovering large historical prose and handoff proofs by hash', () => {
  const canonical = { id: randomUUID(), state: 'recovery_blocked', stage: 'integrate',
    intent: 'Exact objective instructions', criteria: [{ id: 'final', required: true, label: 'Final delivery only' }],
    dependencies: [randomUUID()], execution: { workspace: 'test/final' } };
  const tasks: any[] = [canonical, ...Array.from({ length: 40 }, (_, index) => ({ id: randomUUID(),
    state: index % 2 ? 'running' : 'reconciling', has_live_run: true, last_run_id: randomUUID(),
    intent: 'Original auxiliary instructions '.repeat(350), criteria: [{ id: 'source', exact: true }],
    dependencies: [canonical.id], execution: { workKind: 'diagnostic', workspace: 'test/actor-' + index } }))];
  const deliveries = tasks.slice(1).map(task => ({ id: randomUUID(), state: 'staged', producer_task_id: task.id,
    consumer_task_id: canonical.id, source_task_id: randomUUID(), receipt: { code: 'REAL_RECEIPT', detail: 'Proof '.repeat(300) },
    consumer_proof: { digest: 'a'.repeat(64), stopped: true, detail: 'Exact stop evidence '.repeat(250) } }));
  const coverage = tasks.map(task => ({ coverage_key: 'unique-' + task.id, id: task.id, title: 'Existing useful work', state: task.state }));
  const provenance = Array.from({ length: 180 }, () => ({ artifactId: randomUUID(), taskId: randomUUID(),
    runId: randomUUID(), name: 'report.json', role: 'accepted_repair', digest: 'b'.repeat(64) }));
  const snapshot = { tasks, artifacts: [], coverage, opportunities: { consumers: [{ taskId: canonical.id }],
    acceptedSources: [], liveActors: tasks.slice(1).map(task => ({ taskId: task.id, runId: task.last_run_id })),
    selection: { truncated: false } }, repairConsumption: [], deferredDeliveries: deliveries };
  const before = JSON.stringify(snapshot), fullCatalog = { path: '/inputs/planning-catalog.json', digest: digest(snapshot) };
  const projected = projectScopedPlannerContext(snapshot, fullCatalog, provenance);
  expect(projected.contextProjectionVersion).toBe(scopedPlannerContextVersion);
  expect(JSON.stringify(projected).length).toBeLessThan(115000);
  expect(JSON.stringify(snapshot)).toBe(before);
  const restoredTasks = unpack(projected.tasks);
  expect(restoredTasks[0]).toEqual(canonical);
  restoredTasks.slice(1).forEach((task: any, index: number) => {
    const original = tasks[index + 1];
    expect(task.criteria).toEqual(original.criteria); expect(task.dependencies).toEqual(original.dependencies);
    expect(task.last_run_id).toBe(original.last_run_id);
    expect(task.intent.$catalog).toMatchObject({ jsonPath: ['tasks', index + 1, 'intent'],
      valueSha256: digest(original.intent) });
  });
  expect(unpack(projected.provenance)).toEqual(provenance);
  expect(projected.fullCatalog).toEqual(fullCatalog);
  expect(unpack(projected.coverage)).toEqual(coverage);
  expect(unpack(projected.opportunities.liveActors)).toEqual(snapshot.opportunities.liveActors);
  unpack(projected.deferredDeliveries).forEach((delivery: any, index: number) => {
    expect(delivery.consumer_task_id).toBe(canonical.id);
    expect(delivery.receipt.detail.$catalog.valueSha256).toBe(digest(deliveries[index].receipt));
    expect(delivery.consumer_proof.$catalog.jsonPath).toEqual(['deferredDeliveries', index, 'consumer_proof']);
  });
});

it('marks a long canonical instruction as a preview with its exact catalog location without shortening criteria', () => {
  const canonical={id:randomUUID(),state:'recovery_blocked',intent:'Exact long objective instructions '.repeat(350),
    criteria:[{id:'final',label:'Every original final criterion'}],dependencies:[randomUUID()],execution:{workspace:'test/final'}};
  const snapshot={tasks:[canonical],artifacts:[],opportunities:{consumers:[]},coverage:[],repairConsumption:[],deferredDeliveries:[]};
  const fullCatalog={path:'/inputs/full.json',digest:digest(snapshot)};
  const projected=projectScopedPlannerContext(snapshot,fullCatalog,[]),[task]=unpack(projected.tasks);
  expect(task.intent).toMatchObject({previewOnly:true,preview:canonical.intent.slice(0,800),
    $catalog:{jsonPath:['tasks',0,'intent'],valueSha256:digest(canonical.intent)}});
  expect(task.criteria).toEqual(canonical.criteria);expect(task.dependencies).toEqual(canonical.dependencies);
  expect(projected.fullCatalog).toEqual(fullCatalog);
  expect(JSON.stringify(projected)).toContain('A preview is not the full instruction');
});

it('fits a growing auxiliary roster while preserving delivery dependency contracts and recoverable live identities',()=>{
 const ids=Array.from({length:143},()=>randomUUID()),criteria=Array.from({length:18},(_,index)=>({id:'criterion-'+index,label:'Required supporting evidence '.repeat(8)}));
 const canonical={id:ids[0],state:'recovery_blocked',intent:'Exact delivery authority',criteria:[{id:'final',label:'Independent final acceptance'}],dependencies:[ids[1]],execution:{workspace:'test/final'}};
 const support=(index:number)=>({id:ids[index],state:'running',has_live_run:true,last_run_id:randomUUID(),attempt_count:7,
  intent:'Original supporting instruction '.repeat(40),criteria,dependencies:index===1?[ids[2]]:Array.from({length:20},()=>randomUUID()),execution:{workKind:'repair',workspace:'test/support-'+index}});
 const tasks=[canonical,...Array.from({length:142},(_,i)=>support(i+1))],snapshot={tasks,artifacts:[],coverage:[],repairConsumption:[],deferredDeliveries:[],
  opportunities:{consumers:[{taskId:canonical.id}],acceptedSources:[],liveActors:tasks.slice(1).map(t=>({taskId:t.id,runId:(t as any).last_run_id}))}};
 const original=structuredClone(snapshot),catalog={path:'/home/agent/inputs/full.json',digest:digest(snapshot)};
 const projected=projectScopedPlannerContext(snapshot,catalog,[]),restored=unpack(projected.tasks);
 expect(JSON.stringify(projected).length).toBeLessThan(110000);
 expect(projected.contextSelection).toMatchObject({budgetAdjusted:true,fullCatalogPreserved:true});
 expect(restored.slice(0,3).map((task:any)=>task.criteria)).toEqual(tasks.slice(0,3).map(t=>t.criteria));
 expect(restored.slice(0,3).map((task:any)=>task.dependencies)).toEqual(tasks.slice(0,3).map(t=>t.dependencies));
 restored.slice(3).forEach((task:any,index:number)=>{
  const source=tasks[index+3] as any;
  expect(task).toMatchObject({id:source.id,state:source.state,has_live_run:true,last_run_id:source.last_run_id,attempt_count:7});
  for(const field of ['criteria','dependencies'])expect(task[field].$catalog).toMatchObject({jsonPath:['tasks',index+3,field],valueSha256:digest(source[field])});
 });
 expect(projected.fullCatalog).toEqual(catalog);expect(snapshot).toEqual(original);
});

it('preserves an oversized mandatory delivery contract rather than falsely claiming a fitting plan',()=>{
 const snapshot={tasks:[{id:randomUUID(),state:'blocked_dependencies',intent:'Original authority',criteria:[{id:'required',label:'Exact required evidence '.repeat(7000)}],dependencies:[],execution:{workspace:'test/final'}}],
  artifacts:[],coverage:[],repairConsumption:[],deferredDeliveries:[],opportunities:{consumers:[]}};
 const projected=projectScopedPlannerContext(snapshot,{path:'/inputs/full.json',digest:digest(snapshot)},[]);
 expect(JSON.stringify(projected).length).toBeGreaterThan(115000);
 expect(unpack(projected.tasks)[0].criteria).toEqual(snapshot.tasks[0].criteria);
});

it('recovers large mounted provenance proofs and consumption links without omitting source identity or hashes',()=>{
 const snapshot={tasks:[{id:randomUUID(),state:'recovery_blocked',intent:'Exact delivery',criteria:[{id:'final'}],dependencies:[],execution:{workspace:'test/final'}}],
  artifacts:[],coverage:[],deferredDeliveries:[],opportunities:{consumers:[]},repairConsumption:Array.from({length:40},()=>({artifactId:randomUUID(),digest:'a'.repeat(64),sourceTaskId:randomUUID(),accepted:true,
    consumers:Array.from({length:30},()=>({taskId:randomUUID(),state:'recovery_blocked',attempt:20}))}))};
 const provenance=Array.from({length:100},()=>({artifactId:randomUUID(),runId:randomUUID(),taskId:randomUUID(),digest:'b'.repeat(64),name:'repair-report.json',role:'accepted_repair',
  acceptedSnapshot:{criteria:'Exact proof '.repeat(800),stopped:true}}));
 const full={...snapshot,provenance},catalog={path:'/home/agent/inputs/full.json',digest:digest(full),provenanceIncluded:true};
 const projected=projectScopedPlannerContext(snapshot,catalog,provenance);
 expect(JSON.stringify(projected).length).toBeLessThan(100000);
 const records=projected.provenance.rows?unpack(projected.provenance):full.provenance;
 if(!projected.provenance.rows)expect(projected.provenance).toMatchObject({recordCount:provenance.length,
  representation:'mounted_provenance_directory',$catalog:{jsonPath:['provenance'],valueSha256:digest(provenance)}});
 records.forEach((record:any,index:number)=>{
  expect(record).toMatchObject({artifactId:provenance[index].artifactId,runId:provenance[index].runId,digest:provenance[index].digest});
  if(projected.provenance.rows)expect(record.acceptedSnapshot.$catalog).toMatchObject({jsonPath:['provenance',index,'acceptedSnapshot'],valueSha256:digest(provenance[index].acceptedSnapshot)});
  else expect(record).toEqual(provenance[index]);
 });
 unpack(projected.repairConsumption).forEach((record:any,index:number)=>expect(record.consumers.$catalog)
  .toMatchObject({jsonPath:['repairConsumption',index,'consumers'],valueSha256:digest(snapshot.repairConsumption[index].consumers)}));
 expect(projected.fullCatalog).toEqual(catalog);expect(unpack(projected.tasks)[0].criteria).toEqual(snapshot.tasks[0].criteria);
 const unmounted=projectScopedPlannerContext(snapshot,{...catalog,provenanceIncluded:false},provenance);
 expect(unpack(unmounted.provenance)[0].acceptedSnapshot).toEqual(provenance[0].acceptedSnapshot);
});

it('starts a 147-task scope with 95 mandatory dependency contracts as a small queryable directory', async () => {
  const ids = Array.from({ length: 147 }, () => randomUUID());
  const tasks = ids.map((id, index) => ({ id, title: 'Exact task ' + index,
    state: index < 5 ? 'running' : index < 95 ? 'blocked_dependencies' : 'accepted',
    stage: index === 0 ? 'verify' : 'work',
    intent: 'Original complete instruction '.repeat(500),
    criteria: Array.from({ length: 4 }, (_, n) => ({ id: 'criterion-' + n, label: 'Required evidence '.repeat(60), required: true })),
    dependencies: index < 94 ? [ids[index + 1]] : [],
    attempt_count: 17, last_run_id: randomUUID(), last_run_state: index < 5 ? 'running' : 'submitted',
    last_run_attempt: 17, has_live_run: index < 5, acceptance_passed: index >= 95,
    execution: { workKind: index < 95 ? 'integration' : 'repair', workspace: 'test/exact-' + index } }));
  const snapshot = { tasks, artifacts: [], opportunities: { consumers: [{ taskId: ids[0] }] },
    coverage: [], repairConsumption: [], deferredDeliveries: [] };
  const reader = await createScopedPlannerCatalogReaderInput(randomUUID(), digest(snapshot));
  const original = JSON.stringify(snapshot), mounted = { ...snapshot, provenance: [] };
  const fullCatalog = { path: '/inputs/planning-catalog.json', digest: digest(mounted), provenanceIncluded: true,
    readerPath: '/inputs/planning-query.mjs', readerDigest: reader.digest };
  const projected = projectScopedPlannerContext(snapshot, fullCatalog, []), directory = unpack(projected.tasks);
  expect(JSON.stringify(projected).length).toBeLessThan(70000);
  expect(projected.contextSelection).toMatchObject({ mode: 'on_demand', withinBudget: true,
    fullCatalogPreserved: true, taskCount: 147, liveActorCount: 5 });
  expect(directory).toHaveLength(tasks.length);
  for (const [index, row] of directory.entries()) {
    const source = tasks[index];
    expect(row).toMatchObject({ id: source.id, state: source.state, last_run_id: source.last_run_id,
      last_run_attempt: 17, attempt_count: 17, has_live_run: source.has_live_run,
      criteriaCount: 4, dependencyCount: source.dependencies.length,
      contract: { $catalog: { jsonPath: ['tasks', index], valueSha256: digest(source) } } });
  }
  expect(projected.catalogQueries.argv).toEqual(['node', fullCatalog.readerPath, fullCatalog.path, fullCatalog.digest, '<query JSON>']);
  expect(projected.artifacts.$catalog.valueSha256).toBe(digest(snapshot.artifacts));
  expect(JSON.stringify(snapshot)).toBe(original);
});

it('keeps an oversized mandatory criterion fully recoverable within the same execution', async () => {
  const task = { id: randomUUID(), state: 'blocked_dependencies', intent: 'Exact authority',
    criteria: [{ id: 'final', label: 'Every original required evidence '.repeat(10000) }], dependencies: [],
    execution: { workspace: 'test/final' } };
  const snapshot = { tasks: [task], artifacts: [], opportunities: {}, coverage: [], repairConsumption: [], deferredDeliveries: [] };
  const reader = await createScopedPlannerCatalogReaderInput(randomUUID(), digest(snapshot));
  const projected = projectScopedPlannerContext(snapshot, { path: '/inputs/planning-catalog.json',
    digest: digest(snapshot), readerPath: '/inputs/planning-query.mjs', readerDigest: reader.digest }, []);
  expect(JSON.stringify(projected).length).toBeLessThan(8000);
  expect(unpack(projected.tasks)[0].contract.$catalog).toMatchObject({ jsonPath: ['tasks', 0], valueSha256: digest(task) });
  expect(projected.contextSelection.withinBudget).toBe(true);
  expect(task.criteria[0].label).toHaveLength('Every original required evidence '.length * 10000);
  const bytes = Buffer.from(reader.base64, 'base64');
  expect(createHash('sha256').update(bytes).digest('hex')).toBe(reader.digest);
  expect(reader.name).toBe('planning-query.mjs');
});

it('never advertises an unmounted provenance directory or silently drops actors when the identity budget is too small', async () => {
  const snapshot = { tasks: [{ id: randomUUID(), title: 'Long title '.repeat(200), state: 'running',
    has_live_run: true, last_run_id: randomUUID(), criteria: [], dependencies: [] }], artifacts: [], opportunities: {},
    coverage: [], repairConsumption: [], deferredDeliveries: [] };
  const reader = await createScopedPlannerCatalogReaderInput(randomUUID(), digest(snapshot));
  const provenance = [{ artifactId: randomUUID(), digest: 'f'.repeat(64), proof: 'Exact unmounted proof '.repeat(1000) }];
  const projected = projectScopedPlannerContext(snapshot, { path: '/inputs/planning-catalog.json', digest: digest(snapshot),
    readerPath: '/inputs/planning-query.mjs', readerDigest: reader.digest, provenanceIncluded: false }, provenance, 1000);
  expect(unpack(projected.provenance)).toEqual(provenance);
  expect(unpack(projected.tasks)[0]).toMatchObject({ id: snapshot.tasks[0].id,
    last_run_id: snapshot.tasks[0].last_run_id, has_live_run: true });
  expect(projected.contextSelection.withinBudget).toBe(false);
});

it('pages a mature 265-task scope instead of repeating 257 closed identities in the prompt', async () => {
  const tasks = Array.from({length:265},(_,i)=>({id:randomUUID(),title:'Historical scoped evidence '.repeat(8)+i,
    state:i<257?'accepted':'recovery_blocked',stage:i<257?'work':'integrate',attempt_count:3,
    last_run_id:randomUUID(),last_run_attempt:3,last_run_state:'submitted',has_live_run:false,acceptance_passed:i<257,
    intent:'Exact original instruction '.repeat(200),criteria:[{id:'required',label:'Never weaken this contract'}],
    dependencies:i>0?['dep-'+i]:[],execution:{workKind:i<257?'diagnostic':'integration',workspace:'test/task-'+i}}));
  const snapshot={tasks,artifacts:[],coverage:[],repairConsumption:[],deferredDeliveries:[],
    opportunities:{consumers:tasks.slice(257).map(t=>({taskId:t.id})),liveActors:[]}};
  const original=JSON.stringify(snapshot),catalog={path:'/inputs/planning-catalog.json',digest:digest(snapshot),
    readerPath:'/inputs/planning-query.mjs',readerDigest:'a'.repeat(64),provenanceIncluded:true};
  const projected=projectScopedPlannerContext(snapshot,catalog,[]),inline=unpack(projected.tasks);
  expect(JSON.stringify(projected).length).toBeLessThan(40000);
  expect(projected.contextSelection).toMatchObject({withinBudget:true,taskCount:265,
    liveActorCount:0,identityDirectoryComplete:false,mandatoryInlineCount:8,inlineTaskCount:32,omittedInlineTaskCount:233});
  expect(projected.taskDirectory).toMatchObject({recordCount:265,inlineCount:32,omittedInlineCount:233,
    inlineComplete:false,$catalog:{jsonPath:['tasks'],valueSha256:digest(tasks)},
    query:{operation:'directory',jsonPath:['tasks'],offset:0,limit:12}});
  expect(inline.map((t:any)=>t.id)).toEqual(expect.arrayContaining(tasks.slice(257).map(t=>t.id)));
  expect(projected.encoding).toContain('NOT the complete task directory');
  const {queryCatalog}=await import(new URL('../scripts/planner-catalog-query.mjs',import.meta.url).href);
  const bytes=Buffer.from(original),recovered:any[]=[];let offset:number|null=0;
  while(offset!==null){const page:any=queryCatalog(bytes,catalog.digest,{...projected.taskDirectory.query,offset,maxCharacters:3000});
    expect(JSON.stringify(page).length).toBeLessThanOrEqual(3000);
    recovered.push(...page.entries);offset=page.selection.nextOffset;}
  const recoveredIds=recovered.map(entry=>{
    expect(entry.reference.valueSha256).toBe(digest(tasks[entry.key]));
    if(entry.identity?.id)return entry.identity.id;
    expect(entry.representation).toBe('query_child');
    const child=queryCatalog(bytes,catalog.digest,{operation:'read',
      jsonPath:[...entry.reference.jsonPath,'id'],maxCharacters:3000});
    expect(child.selection.complete).toBe(true);return child.text;
  });
  expect(recoveredIds).toEqual(tasks.map(t=>t.id));
  const omittedIndex=100,found=queryCatalog(bytes,catalog.digest,{operation:'find',jsonPath:['tasks'],where:{id:tasks[omittedIndex].id}});
  expect(found.entries[0].reference).toMatchObject({jsonPath:['tasks',omittedIndex],valueSha256:digest(tasks[omittedIndex])});
  const criteria=queryCatalog(bytes,catalog.digest,{operation:'read',jsonPath:['tasks',omittedIndex,'criteria'],valueSha256:digest(tasks[omittedIndex].criteria)});
  expect(criteria.selection.complete).toBe(true);
  for(const row of inline){const i=tasks.findIndex(t=>t.id===row.id);expect(row.contract.$catalog)
    .toMatchObject({jsonPath:['tasks',i],valueSha256:digest(tasks[i])});}
  expect(JSON.stringify(snapshot)).toBe(original);expect(projected.fullCatalog).toEqual(catalog);
});

it('retains every live or uncertain actor and consumer even outside the optional first page',()=>{
 const tasks:any[]=Array.from({length:500},(_,i)=>({id:randomUUID(),title:'Old scoped task '+i,state:'accepted',stage:'work',
  attempt_count:2,last_run_id:randomUUID(),last_run_attempt:2,last_run_state:'submitted',has_live_run:false,
  criteria:[{id:'exact',label:'Original criterion'}],dependencies:[],execution:{workKind:'diagnostic'}}));
 tasks[420].has_live_run=true;tasks[421].last_run_state='reconciling';tasks[422].state='queued';
 tasks[424].state='blocked_dependencies';tasks[424].stage='verify';
 const snapshot={tasks,artifacts:[],coverage:[],repairConsumption:[],deferredDeliveries:[],
  opportunities:{consumers:[{taskId:tasks[425].id}],liveActors:[{taskId:tasks[423].id,runId:tasks[423].last_run_id}]}};
 const catalog={path:'/inputs/catalog.json',digest:digest(snapshot),readerPath:'/inputs/query.mjs',readerDigest:'b'.repeat(64),provenanceIncluded:true};
 const projected=projectScopedPlannerContext(snapshot,catalog,[]),inline=unpack(projected.tasks);
 expect(projected.contextSelection).toMatchObject({withinBudget:true,mandatoryInlineCount:6,identityDirectoryComplete:false});
 for(let i=420;i<=425;i++)expect(inline.find((t:any)=>t.id===tasks[i].id)).toMatchObject({id:tasks[i].id,
  last_run_id:tasks[i].last_run_id,attempt_count:2,contract:{$catalog:{jsonPath:['tasks',i],valueSha256:digest(tasks[i])}}});
 const bounded=projectScopedPlannerContext(snapshot,catalog,[],1000),required=unpack(bounded.tasks);
 expect(bounded.contextSelection.withinBudget).toBe(false);expect(required).toHaveLength(6);
 expect(required.map((t:any)=>t.id)).toEqual(tasks.slice(420,426).map(t=>t.id));
 expect(bounded.taskDirectory.$catalog.valueSha256).toBe(digest(tasks));
});
