import { expect, it } from 'vitest';
import {
  compactManagementSources, projectManagerContext, managerTextHash,
  managerInlineMemoryHashes, projectPersistedManagerDescription,
  projectManagerReview,
} from '../server/manager-context.js';
import {managementContextRequests} from '../server/management-retrieval.js';
import {focusManagerTasks} from '../server/manager-focus.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';

// The projector may use an exact shared representation before omitting optional
// context. Compare delivered semantics through the production decoder.
const deliveredReview=(description:string,prefix='')=>expandSharedManagerContext(JSON.parse(description.slice(prefix.length)));

it('budgets nonfocal auxiliary summaries before losing actionable cases, preserving instructions, criteria, dependencies and live observations',()=>{
 const cases=Array.from({length:6},(_,i)=>({id:'case-'+i,taskId:'task-'+i,operationalRecovery:i>0,
   evidence:[{code:'MODEL_TIMEOUT',summary:'Complete current case evidence.'}]}));
 const tasks=Array.from({length:40},(_,i)=>({id:'task-'+i,state:'blocked_dependencies',workKind:'diagnostic',
   expectedAttempt:i+1,summary:'Historical dependency observation '.repeat(100),live:i===39,
   criteria:[{id:'criterion-'+i,label:'Original condition '+i}],intent:{text:'Exact original instruction '+i},
   dependencies:['required-'+i],routeOptions:[{accountId:'authorized',model:'configured'}]}));
 const snapshot={cases,operation:{tasks,focus:{taskId:'task-0'}}},before=structuredClone(snapshot);
 const review=projectManagerReview(snapshot,'',0,45000),delivered=deliveredReview(review.description);
 expect(review.withinBudget).toBe(true);expect(delivered.cases).toEqual(cases);
 expect(delivered.operation.tasks[0]).toEqual(tasks[0]);expect(delivered.operation.tasks[39]).toEqual(tasks[39]);
 for(let i=1;i<39;i++){
  const row=delivered.operation.tasks[i];
  expect(row).toMatchObject({criteria:tasks[i].criteria,intent:tasks[i].intent,dependencies:tasks[i].dependencies,
   routeOptions:tasks[i].routeOptions,expectedAttempt:tasks[i].expectedAttempt});
  if(row.summarySelection===undefined)expect(row).toEqual(tasks[i]);
  else {
   expect(row.summarySelection).toMatchObject({truncated:true,fullSourcePreserved:true,
    projectedSha256:managerTextHash(tasks[i].summary),recovery:{request:{kind:'task_metadata',taskId:tasks[i].id,
     expectedAttempt:tasks[i].expectedAttempt,offset:0}}});
   expect(tasks[i].summary.startsWith(row.summary)).toBe(true);
   expect(row.summary.length).toBeLessThan(tasks[i].summary.length);
  }
 }
 expect(snapshot).toEqual(before);
 expect(JSON.stringify(review.description).length).toBeLessThan(JSON.stringify(snapshot).length/2);
});

it('keeps a batch of current operational recoveries before unrelated historical decisions',()=>{
 const cases=Array.from({length:6},(_,i)=>({id:'case-'+i,taskId:'task-'+i,operationalRecovery:i>0,
   evidence:[{code:'MODEL_TIMEOUT',instruction:'Preserve the original criteria and inspect the stopped attempt.'}]}));
 const plans=Array.from({length:30},(_,i)=>({task_id:'historical-'+i,state:'waiting_evidence',
   actions:[{diagnosis:'Historical unrelated diagnosis. '.repeat(100)}]}));
 const snapshot={cases,alreadyPlanned:plans,operation:{tasks:[],focus:{taskId:'task-0'},workOpportunities:{existingCoverage:[]}}};
 const original=structuredClone(snapshot),review=projectManagerReview(snapshot,'',0,8000);
 const delivered=deliveredReview(review.description);
 expect(review.withinBudget).toBe(true);expect(delivered.cases).toEqual(cases);
 expect(review.caseWindow.selectedCaseIds).toEqual(cases.map(entry=>entry.id));
 if(delivered.plannedWindow===undefined)expect(delivered.alreadyPlanned).toEqual(plans);
 else {
  expect(delivered.plannedWindow).toMatchObject({fullSourcesPreserved:true,total:30,
   included:delivered.alreadyPlanned.length,omitted:30-delivered.alreadyPlanned.length,
   sourceSha256:managerTextHash(JSON.stringify(plans))});
  expect(delivered.alreadyPlanned.length).toBeLessThan(plans.length);
  for(const plan of delivered.alreadyPlanned)expect(plans).toContainEqual(plan);
 }
 expect(snapshot).toEqual(original);
});

it('bounds accumulated prior decisions while preserving the focal decision, authority and exact current coverage',()=>{
 const plans=Array.from({length:90},(_,i)=>({task_id:'task-'+i,state:'waiting_evidence',support_task_ids:['support-'+i],
   actions:[{kind:'wait_for_evidence',waitingFor:'Specific verified input '+i,diagnosis:'Past diagnosis '.repeat(100)}]}));
 const snapshot={cases:[{id:'current-case',task_id:'task-89',evidence:[{instruction:'Exact current authority and criteria.'}]}],
   alreadyPlanned:plans,operation:{focus:{taskId:'task-89'},tasks:[],workOpportunities:{existingCoverage:[{taskId:'covered',digest:'actual-digest'}]}}};
 const original=structuredClone(snapshot),prefix='Preserve authorization.\n';
 const result=projectManagerReview(snapshot,prefix,0,14000),delivered=deliveredReview(result.description,prefix);
 expect(result.withinBudget).toBe(true);expect(delivered.cases).toEqual(snapshot.cases);
 expect(delivered.alreadyPlanned).toContainEqual(plans[89]);
 expect(delivered.operation.workOpportunities.existingCoverage).toEqual(snapshot.operation.workOpportunities.existingCoverage);
 if(delivered.plannedWindow===undefined)expect(delivered.alreadyPlanned).toEqual(plans);
 else {
  expect(delivered.plannedWindow).toMatchObject({total:90,fullSourcesPreserved:true,
   sourceSha256:managerTextHash(JSON.stringify(plans)),included:delivered.alreadyPlanned.length,
   omitted:90-delivered.alreadyPlanned.length});
  expect(delivered.alreadyPlanned.length).toBeLessThan(plans.length);
  for(const plan of delivered.alreadyPlanned)expect(plans).toContainEqual(plan);
 }
 expect(snapshot).toEqual(original);
});

it('keeps fresh actionable evidence ahead of old decisions and receipts, retaining the latest and active focal transition',()=>{
 const transitions=Array.from({length:8},(_,i)=>({id:'transition-'+i,scope:i===7?'focus':'other',
   state:i===7?'tracking':'rejected',proposal:{cause:'Past receipt '.repeat(100)},receipt:{code:'PAST-'+i}}));
 const snapshot={cases:[{id:'case',evidence:[{instruction:'Exact current permission.'}]}],alreadyPlanned:[],
   operation:{focus:{scope:'focus'},tasks:[],deliveryTransitions:transitions,
     evidence:[{taskId:'actual-source',runId:'actual-run',digest:'verified-digest',text:'New discriminating failure '.repeat(60)}]}};
 const original=structuredClone(snapshot),prefix='Current authority.\n';
 const review=projectManagerReview(snapshot,prefix,0,6000),result=deliveredReview(review.description,prefix);
 expect(review.withinBudget).toBe(true);expect(result.operation.evidence).toEqual(snapshot.operation.evidence);
 expect(result.operation.deliveryTransitions).toContainEqual(transitions[0]);
 expect(result.operation.deliveryTransitions).toContainEqual(transitions[7]);
 expect(result.operation.deliveryTransitionWindow).toMatchObject({total:8,fullSourcesPreserved:true,
   sourceSha256:managerTextHash(JSON.stringify(transitions))});
 expect(snapshot).toEqual(original);
});

it('preserves exact focal records, every live actor and rotating recoverable global identities without repeating schemas',()=>{
 const tasks=Array.from({length:40},(_,i)=>({id:'task-'+i,title:'Specific task '+i,state:'blocked_dependencies',stage:'work',
  baseWorkspace:i<12?'project/focus':'project/other',expectedAttempt:i+1,live:i===39,workKind:'repair',
  dependencies:Array.from({length:20},(_,j)=>'source-'+j),criteria:[{id:'mandatory',label:'Keep actual acceptance'}],
  intent:{text:'Exact task instruction '+i},reason:null,routeOptions:[{accountId:'account',model:'model'}]}));
 const sources=Array.from({length:20},(_,i)=>({taskId:'source-'+i,expectedAttempt:i+1,runId:'run-'+i,
   acceptancePassed:true,baseWorkspace:'project/focus',workspace:'isolated/source-'+i,workKind:'diagnostic',
   covers:['criterion-'+i],inputs:[{artifactId:'artifact-'+i,name:'report.json',digest:'exact-'+i,size:100}],omittedInputs:0}));
 const opportunities={acceptedSources:sources,existingCoverage:sources.map(source=>({taskId:source.taskId,
   title:'Specific coverage '+source.taskId,state:'accepted',expectedAttempt:source.expectedAttempt,
   coverageKey:'unique-'+source.taskId,baseWorkspace:'project/focus'})),liveActors:[],consumers:[]};
 const snapshot={cases:[],operation:{focus:{scope:'project/focus',taskId:'task-0'},
  tasks:focusManagerTasks(tasks,{scope:'project/focus',taskId:'task-0'}),workOpportunities:opportunities,
  deferredWorkOpportunities:{...opportunities,acceptedSources:[sources[4],sources[0]]}}};
 const original=structuredClone(snapshot),projected=projectManagerContext(snapshot),operation=projected.operation;
 for(const reference of operation.tasks){
  const row=operation.focusedTaskDirectory.rows[reference.index],schema=operation.focusedTaskDirectory.schemas[row[0]];
  const record=Object.fromEntries(schema.map((key:string,index:number)=>[key,row[index+1]]));
  expect(record).toEqual(tasks.find(task=>task.id===reference.id));
 }
 expect(operation.tasks).toHaveLength(12);
 expect(operation.otherTaskDirectory.rows).toHaveLength(13);
 expect(operation.otherTaskDirectory.selection).toMatchObject({totalRows:28,includedRows:13,omittedRows:15,allLiveActorsIncluded:true});
 expect(operation.otherTaskDirectory.rows.some((row:any[])=>row[0]==='task-39')).toBe(true);
 for(const row of operation.otherTaskDirectory.rows){
  const record=Object.fromEntries(operation.otherTaskDirectory.columns.map((key:string,index:number)=>[key,row[index]]));
  const source=tasks.find(task=>task.id===record.id)!;
  expect(record).toMatchObject({id:source.id,title:source.title,state:source.state,stage:source.stage,
    baseWorkspace:source.baseWorkspace,expectedAttempt:source.expectedAttempt,live:source.live,workKind:source.workKind});
  expect(record.dependencyCount).toBe(20);
  expect(record.dependencySha256).toBe(managerTextHash(JSON.stringify(tasks[0].dependencies)));
 }
 expect(operation.otherTaskDirectory.recovery.graphApi).toBe('GET /api/goals/:goalId');
 const next=projectManagerContext({...snapshot,operation:{...snapshot.operation,
   workOpportunities:{...opportunities,selection:{cursor:12}}}});
 expect(next.operation.otherTaskDirectory.rows).not.toEqual(operation.otherTaskDirectory.rows);
 expect(next.operation.otherTaskDirectory.rows.some((row:any[])=>row[0]==='task-39')).toBe(true);
 const unpack=(directory:any)=>Array.isArray(directory)?directory:directory.rows.map((row:any[])=>
   Object.fromEntries(directory.schemas[row[0]].map((key:string,index:number)=>[key,row[index+1]])));
 for(const key of ['acceptedSources','existingCoverage','liveActors','consumers'])
   expect(unpack(operation.workOpportunities[key])).toEqual(opportunities[key as keyof typeof opportunities]);
 const reference=operation.deferredWorkOpportunities.acceptedSources;
 expect(reference.sha256).toBe(managerTextHash(JSON.stringify(sources)));
 expect(reference.rows.map((row:any)=>typeof row==='number'?unpack(operation.workOpportunities.acceptedSources)[row]:row))
  .toEqual(snapshot.operation.deferredWorkOpportunities.acceptedSources);
 expect(JSON.stringify(projected).length).toBeLessThan(JSON.stringify(snapshot).length*0.72);
 expect(projectManagerContext(projected)).toEqual(projected);
 expect(snapshot).toEqual(original);
});

it('pages growing coverage without excluding the focal case or exact explicitly requested evidence',()=>{
 const rows=Array.from({length:100},(_,i)=>({id:'coverage-'+i,title:'Real independent obligation '+i+' '+ 'Metadata '.repeat(20),
   state:'accepted',expected_attempt:2,covers:['criterion-'+i],coverage_key:'unique-'+i}));
 const snapshot={cases:[{id:'current',taskId:'consumer',evidence:[{instruction:'Mandatory operator instruction '.repeat(30),checks:[{passed:false}]}]}],
  operation:{focus:{taskId:'consumer',scope:'project/base'},tasks:[{id:'consumer',criteria:[{id:'original'}]}],
   coverageIndex:{scope:'project/base',rows,complete:true,nextOffset:null},
   recoveredContext:{results:[{request:{kind:'artifact',taskId:'f9442da7-fd40-400a-a0cb-c026cdd6a4a9',expectedAttempt:2,name:'report.json'},
     status:'OK',digest:'exact-digest',text:'Requested exact current evidence '.repeat(80)}]},
   workOpportunities:{selection:{cursor:0}}}};
 const original=structuredClone(snapshot),prefix='Preserve all permissions and acceptance requirements.\n';
 const first=projectManagerReview(snapshot,prefix,0,8500),delivered=deliveredReview(first.description,prefix);
 expect(first.withinBudget).toBe(true);
 expect(first.caseWindow.selectedCaseIds).toEqual(['current']);
 expect(delivered.operation.recoveredContext).toEqual(snapshot.operation.recoveredContext);
 expect(delivered.operation.tasks).toEqual(snapshot.operation.tasks);
 expect(delivered.operation.coverageIndex.complete).toBe(false);
 expect(delivered.operation.coverageIndex.selection).toMatchObject({totalKnownRows:100,sourceComplete:true,fullSourcesPreserved:true});
 expect(delivered.operation.coverageIndex.selection.omittedRows).toBeGreaterThan(0);
 expect(managementContextRequests.safeParse([delivered.operation.coverageIndex.recovery.request]).success).toBe(true);
 for(const row of delivered.operation.coverageIndex.rows)expect(rows).toContainEqual(row);
 expect(delivered.evidenceTexts[0].text).toBe(snapshot.cases[0].evidence[0].instruction);
 const next=projectManagerReview({...snapshot,operation:{...snapshot.operation,workOpportunities:{selection:{cursor:12}}}},prefix,0,8500);
 expect(deliveredReview(next.description,prefix).operation.coverageIndex.rows).not.toEqual(delivered.operation.coverageIndex.rows);
 expect(projectManagerReview(delivered,prefix,0,8500).description).toBe(first.description);
 expect(snapshot).toEqual(original);
});

it('keeps every closed source selectable with its exact base, attempt and workspace', () => {
  const completed = Array.from({ length: 200 }, (_, i) => ({
    id: 'source-' + i, title: 'Accepted source ' + i, state: 'accepted', stage: 'work',
    workKind: 'repair', expectedAttempt: i + 1, baseWorkspace: 'games/game-' + (i % 5),
    workspace: 'isolated/source-' + i, live: false, summary: 'Historical result '.repeat(200),
  }));
  const live = { id: 'consumer', state: 'running', dependencies: completed.map(t => t.id),
    routeOptions: [], live: true, errorCode: null };
  const original = structuredClone([...completed, live]);
  const projected = compactManagementSources([...completed, live]);
  const recovered: Array<Record<string, any>> = projected.sourceCatalog.rows.map(row => {
    const source = Object.fromEntries(projected.sourceCatalog.columns.map((key, i) => [key, row[i]]));
    return { ...source, baseWorkspace: projected.sourceCatalog.bases[source.baseIndex as number] };
  });
  for (const source of completed)
    expect(recovered.find(row => row.id === source.id)).toMatchObject({
      id: source.id, title: source.title, state: source.state, stage: source.stage,
      expectedAttempt: source.expectedAttempt, workspace: source.workspace,
      baseWorkspace: source.baseWorkspace, workKind: source.workKind, live: false,
    });
  expect(projected.tasks).toEqual([live]);
  expect([...completed, live]).toEqual(original);
  expect(JSON.stringify(projected).length).toBeLessThan(45000);
});

it('delivers mandatory evidence and patches once, with exact recovery from each original location', () => {
  const instruction = 'Never alter RTP90 or the original rules; reproduce MW2 independently.\n' + 'Observation '.repeat(600);
  const patch = '--- a/source.js\n+++ b/source.js\n' + '+corrected();\n'.repeat(120);
  const evidence = { instruction, patch, gameAcceptance: false, existingRunsUntouched: true };
  const snapshot = { cases: [{ id: 'case-a', evidence: [evidence] }, { id: 'case-b', evidence: [evidence] }] };
  const original = structuredClone(snapshot);
  const projected = projectManagerContext(snapshot);
  const serialized = JSON.stringify(projected);
  expect(projected.evidenceTexts).toHaveLength(2);
  for (const text of [instruction, patch]) {
    const entry = projected.evidenceTexts.find((entry: { hash: string; text: string; originals: any[] }) => entry.hash === managerTextHash(text))!;
    expect(entry.text).toBe(text);
    expect(entry.originals).toHaveLength(2);
    for (const ref of entry.originals) {
      const source = original.cases.find(c => c.id === ref.id)!;
      const recovered = ref.path.reduce((value: any, part: string | number) => value[part], source);
      expect(recovered).toBe(entry.text);
      expect(managerTextHash(recovered)).toBe(entry.hash);
    }
    expect(serialized.split(JSON.stringify(text)).length - 1).toBe(1);
  }
  expect(projected.cases.map((c: any) => c.evidence[0].gameAcceptance)).toEqual([false, false]);
  expect(snapshot).toEqual(original);
  expect(projectManagerContext(projected)).toEqual(projected);
});

it('preserves contracts and rejects a claimed inline hash unless its full text is actually delivered', () => {
  const instruction = 'Authorized independent reproduction. '.repeat(80);
  const snapshot = { cases: [{ id: 'case', evidence: [{ instruction }] }], operation: {
    tasks: [{ id: 'source', title: 'Source', state: 'accepted', expectedAttempt: 1, live: false }],
  } };
  const original = 'MANAGEMENT_V1\nMANAGER_CONTEXT_V3\nOld source catalog explanation.\nNever deploy. Keep mandatory criteria.\n' + JSON.stringify(snapshot);
  const projected = projectPersistedManagerDescription(original);
  expect(projected).toContain('Never deploy. Keep mandatory criteria.');
  expect(managerInlineMemoryHashes(projected)).toEqual([managerTextHash(instruction)]);
  expect(projectPersistedManagerDescription(projected)).toBe(projected);
  expect(managerInlineMemoryHashes('MANAGER_CONTEXT_V4\n' + JSON.stringify({ cases: [],
    inlineMemoryHashes: [managerTextHash(instruction)], evidenceTexts: [] }))).toEqual([]);
});

it('bounds the serialized historical directory while retaining current sources, rotating the selection and preserving the original',()=>{
 const completed=Array.from({length:1000},(_,i)=>({id:'source-'+i,title:'Accepted source '+i,state:'accepted',stage:'work',
  expectedAttempt:i+1,workspace:'isolated/source-'+i,baseWorkspace:'project/base',live:false}));
 const operation={...compactManagementSources(completed),
  tasks:[{id:'consumer',description:'Required substantive instruction '.repeat(2000),criteria:[{id:'original',label:'Original criterion'}]}],
  workOpportunities:{acceptedSources:[{taskId:'source-999'}],selection:{cursor:0}}};
 const original=structuredClone(operation);
 const before=JSON.stringify({description:JSON.stringify({cases:[],operation}),context:'Relevant context '.repeat(1100)});
 expect(before.length).toBeGreaterThan(180000);
 const first=projectManagerContext({cases:[],alreadyPlanned:[{actions:[{sourceTaskId:'source-998'}]}],operation});
 expect(first.operation.sourceCatalog.rows).toHaveLength(24);
 expect(first.operation.sourceCatalog.rows.some((row:any[])=>row[0]==='source-999')).toBe(true);
 expect(first.operation.sourceCatalog.rows.some((row:any[])=>row[0]==='source-998')).toBe(true);
 expect(first.operation.sourceCatalog.selection).toMatchObject({totalRows:1000,includedRows:24,omittedRows:976,truncated:true});
 expect(first.operation.sourceCatalog.recovery.completeDirectoryPreserved).toBe(true);
 expect(first.operation.tasks).toEqual(operation.tasks);
 expect(JSON.stringify({description:JSON.stringify(first),context:'Relevant context '.repeat(1100)}).length).toBeLessThan(180000);
 const second=projectManagerContext({cases:[],operation:{...operation,workOpportunities:{...operation.workOpportunities,selection:{cursor:24}}}});
 expect(second.operation.sourceCatalog.rows.map((row:any[])=>row[0])).not.toEqual(first.operation.sourceCatalog.rows.map((row:any[])=>row[0]));
 expect(projectManagerContext(first)).toEqual(first);
 expect(operation).toEqual(original);
});

it('reserves space for exact recovered evidence while keeping instructions and identities untouched', () => {
  const sources = Array.from({ length: 1000 }, (_, i) => ({ id: 'source-' + i, title: 'Historical source ' + i,
    state: 'accepted', stage: 'work', expectedAttempt: 1, workspace: 'research/source-' + i, live: false }));
  const recoveredContext = { results: [{ request: { kind: 'artifact', taskId: 'source-999', expectedAttempt: 1 },
    status: 'OK', digest: 'original-digest', text: '\\"Verifiable evidence\n'.repeat(150), nextOffset: 1500 }] };
  const snapshot = { cases: [], operation: { ...compactManagementSources(sources), recoveredContext,
    tasks: [{ id: 'consumer', description: 'Required instruction '.repeat(2000), criteria: [{ id: 'original' }] }],
    workOpportunities: { acceptedSources: [{ taskId: 'source-999' }], selection: { cursor: 0 } } } };
  const original = structuredClone(snapshot), projected = projectManagerContext(snapshot);
  expect(projected.operation.sourceCatalog.rows).toHaveLength(12);
  expect(projected.operation.sourceCatalog.rows.some((row: any[]) => row[0] === 'source-999')).toBe(true);
  expect(projected.operation.recoveredContext).toEqual(recoveredContext);
  expect(projected.operation.tasks).toEqual(snapshot.operation.tasks);
  expect(JSON.stringify({ description: JSON.stringify(projected), context: 'Relevant context '.repeat(1100) }).length).toBeLessThan(180000);
  expect(snapshot).toEqual(original); expect(projectManagerContext(projected)).toEqual(projected);
});

it('projects repeated admission history without losing the latest freshness gate or substantive evidence', () => {
  const instruction = 'Preserve every original acceptance criterion and patch. '.repeat(30);
  const observations = Array.from({ length: 1000 }, (_, i) => ({ kind: 'RUN_ADMISSION_BLOCKED',
    admissionEventId: String(i), at: new Date(Date.UTC(2026, 9, 7, 0, 0, i)).toISOString(),
    runId: 'stopped-run', attempt: 5, instructionRevisionId: 'original-revision', code: 'INSTRUCTION_CASE_CHANGED',
    message: 'Repeated operational observation '.repeat(10) }));
  const different = { ...observations[0], admissionEventId: 'other', code: 'TASK_INSTRUCTIONS_CHANGED' };
  const evidence = [{ instruction, patch: 'Exact patch bytes\n' }, ...observations, different, { checks: [{ passed: false }] }];
  const snapshot = { cases: [{ id: 'case', evidence }] }, original = structuredClone(snapshot);
  const projected = projectManagerContext(snapshot), c = projected.cases[0];
  expect(c.evidenceSelection).toMatchObject({ total: 1003, included: 4, omittedRepeatedAdmissions: 999 });
  expect(c.evidenceSelection.selectedIndices).toEqual([0, 1000, 1001, 1002]);
  expect(c.evidence[1]).toEqual(observations[999]); expect(c.evidence[2]).toEqual(different);
  expect(c.evidence[3]).toEqual({ checks: [{ passed: false }] });
  expect(projected.evidenceTexts.find((e: any) => e.hash === managerTextHash(instruction))?.text).toBe(instruction);
  expect(JSON.stringify(projected).length).toBeLessThan(8000);
  expect(snapshot).toEqual(original); expect(projectManagerContext(projected)).toEqual(projected);
});

it('fits complete supervision cases to the serialized envelope and rotates omitted cases across reviews', () => {
  const cases = Array.from({ length: 3 }, (_, i) => ({ id: 'case-' + i, taskId: 'task-' + i,
    evidence: [{ instruction: 'Original substantive instruction ' + i + ': ' + ('Evidence ' + i + ' ').repeat(600) }] }));
  const snapshot = { cases, operation: { tasks: [{ id: 'consumer', criteria: [{ id: 'unchanged', label: 'Final requirement' }] }] } };
  const original = structuredClone(snapshot), prefix = 'Never change permissions or criteria.\n';
  const first = projectManagerReview(snapshot, prefix, 0, 11000);
  expect(first.withinBudget).toBe(true); expect(first.caseWindow.included).toBe(1);
  expect(JSON.stringify(first.description).length).toBeLessThanOrEqual(11000);
  const delivered = deliveredReview(first.description,prefix);
  expect(delivered.cases[0].id).toBe(cases[0].id);
  expect(delivered.evidenceTexts[0].text).toBe(cases[0].evidence[0].instruction);
  expect(delivered.operation.tasks[0].criteria).toEqual(snapshot.operation.tasks[0].criteria);
  const second = projectManagerReview(snapshot, prefix, first.caseWindow.nextCursor, 11000);
  expect(second.caseWindow.selectedCaseIds).toEqual(['case-1']);
  expect(second.caseWindow.remaining).toBe(2);
  expect(snapshot).toEqual(original);
});

it('keeps an actionable complete case ahead of auxiliary reports and rotates recoverable report pages', () => {
  const cases = Array.from({ length: 3 }, (_, i) => ({ id: 'case-' + i, taskId: 'task-' + i,
    evidence: [{ instruction: 'Mandatory instruction ' + i + ': ' + ('Evidence ' + i + ' ').repeat(600),
      checks: [{ passed: false }] }] }));
  const evidence = Array.from({ length: 4 }, (_, i) => ({ taskId: 'source-' + i, runId: 'run-' + i,
    expectedAttempt: 3, artifactId: 'artifact-' + i, name: 'report.json', digest: 'digest-' + i,
    text: 'Complete report ' + i + ': ' + ('Report ' + i + ' ').repeat(260) }));
  const snapshot = { cases, operation: { tasks: [{ id: 'consumer', criteria: [{ id: 'original' }] }],
    evidence, recoveredContext: { results: [{ status: 'OK', text: 'Exact requested source' }] },
    workOpportunities: { selection: { cursor: 0 } } } };
  const original = structuredClone(snapshot), prefix = 'Preserve criteria and permissions.\n';
  const first = projectManagerReview(snapshot, prefix, 0, 13000);
  expect(first.withinBudget).toBe(true); expect(first.caseWindow.selectedCaseIds).toEqual(['case-0']);
  const delivered = deliveredReview(first.description,prefix);
  expect(delivered.evidenceTexts[0].text).toBe(cases[0].evidence[0].instruction);
  expect(delivered.cases[0].evidence[0].checks).toEqual([{ passed: false }]);
  expect(delivered.operation.tasks).toEqual(snapshot.operation.tasks);
  expect(delivered.operation.recoveredContext).toEqual(snapshot.operation.recoveredContext);
  expect(delivered.operation.evidenceWindow.remaining).toBeGreaterThan(0);
  expect(delivered.operation.evidenceWindow.recovery.fullArtifactsPreserved).toBe(true);
  for (const report of delivered.operation.evidence) expect(evidence).toContainEqual(report);
  for (const ref of delivered.operation.evidenceWindow.omitted)
    expect(evidence.find(e => e.artifactId === ref.artifactId)).toMatchObject(ref);
  const next = projectManagerReview({ ...snapshot, operation: { ...snapshot.operation,
    workOpportunities: { selection: { cursor: 1 } } } }, prefix, first.caseWindow.nextCursor, 13000);
  expect(next.caseWindow.selectedCaseIds).toEqual(['case-1']);
  const second = deliveredReview(next.description,prefix);
  expect(second.operation.evidence.map((e: any) => e.artifactId))
    .not.toEqual(delivered.operation.evidence.map((e: any) => e.artifactId));
  expect(snapshot).toEqual(original);
});
it('shares identical catalogs and preserves every retained page when exact sharing fits alongside the focal case',()=>{
 const coverage=Array.from({length:35},(_,i)=>({taskId:'source-'+i,title:'Distinct verified finding '+i,
   description:'Verified metadata '.repeat(10)}));
 const opportunities={acceptedSources:[{taskId:'source-0',workspace:'test/accepted'}],existingCoverage:coverage,liveActors:[],selection:{cursor:0}};
 const history=Array.from({length:6},(_,i)=>({request:{kind:'artifact',taskId:'source-'+i,expectedAttempt:2,name:'report.json'},
   runId:'run-'+i,artifactId:'artifact-'+i,digest:'digest-'+i,totalCharacters:5000,pages:[{offset:0,text:'Retained data '.repeat(400)}]}));
 const snapshot={cases:[{id:'focal',evidence:[{instruction:'Original permission and acceptance requirements remain unchanged.'}]}],
   operation:{tasks:[{id:'consumer',criteria:[{id:'original',label:'Final requirement'}]}],
     recoveredContext:{results:[{status:'OK',text:'Exact requested current evidence',digest:'current-hash'}]},
     retainedEvidence:{entries:history,fullSourcesPreserved:true},workOpportunities:opportunities,
     deferredWorkOpportunities:{...opportunities,consumers:[{taskId:'consumer',delivery:'deferred'}]}}};
 const original=structuredClone(snapshot),prefix='Preserve all criteria and permissions.\n';
 const review=projectManagerReview(snapshot,prefix,0,16000),delivered=deliveredReview(review.description,prefix);
 expect(review.withinBudget).toBe(true);expect(review.caseWindow.selectedCaseIds).toEqual(['focal']);
 expect(delivered.operation.tasks).toEqual(snapshot.operation.tasks);
 expect(delivered.operation.recoveredContext).toEqual(snapshot.operation.recoveredContext);
 expect(delivered.operation.deferredWorkOpportunities.existingCoverage).toMatchObject({$ref:'operation.workOpportunities.existingCoverage',items:35,
  sha256:managerTextHash(JSON.stringify(coverage))});
 expect(delivered.operation.workOpportunities.existingCoverage).toEqual(coverage);
 expect(delivered.operation.retainedEvidence).toEqual(snapshot.operation.retainedEvidence);
 expect(delivered.operation.retainedEvidence.entries).toHaveLength(6);
 expect(delivered.operation.retainedEvidence.selection).toBeUndefined();
 expect(delivered.operation.retainedEvidence.omittedEntries).toBeUndefined();
 expect(snapshot).toEqual(original);expect(JSON.stringify(review.description).length).toBeLessThanOrEqual(16000);
});

it('windows distinct retained pages with exact recovery metadata when lossless sharing alone cannot fit',()=>{
 const history=Array.from({length:6},(_,i)=>{
  const text=Array.from({length:80},(_,part)=>managerTextHash('unique-retained-page-'+i+'-'+part)).join('');
  return {request:{kind:'artifact',taskId:'source-'+i,expectedAttempt:2,name:'report.json'},
   runId:'run-'+i,artifactId:'artifact-'+i,digest:managerTextHash(text),totalCharacters:text.length,
   pages:[{offset:0,text}],complete:true};
 });
 const snapshot={cases:[{id:'focal',evidence:[{instruction:'Keep the exact current authority and acceptance criteria.'}]}],
  operation:{focus:{taskId:'consumer'},tasks:[{id:'consumer',criteria:[{id:'original',label:'Final requirement'}]}],
   recoveredContext:{results:[{status:'OK',text:'Exact requested current evidence',digest:'current-hash'}]},
   retainedEvidence:{entries:history,fullSourcesPreserved:true}}};
 const before=structuredClone(snapshot),prefix='Preserve all criteria and permissions.\n',maximum=16000;
 const review=projectManagerReview(snapshot,prefix,0,maximum),delivered=deliveredReview(review.description,prefix);
 expect(review.withinBudget).toBe(true);expect(JSON.stringify(review.description).length).toBeLessThanOrEqual(maximum);
 expect(review.caseWindow.selectedCaseIds).toEqual(['focal']);
 expect(delivered.cases).toEqual(snapshot.cases);expect(delivered.operation.focus).toEqual(snapshot.operation.focus);
 expect(delivered.operation.tasks).toEqual(snapshot.operation.tasks);
 expect(delivered.operation.recoveredContext).toEqual(snapshot.operation.recoveredContext);
 const retained=delivered.operation.retainedEvidence;
 expect(retained.selection).toMatchObject({windowed:true,total:6,included:retained.entries.length,
  omitted:6-retained.entries.length});
 expect(retained.selection.omitted).toBeGreaterThan(0);
 expect(retained.recovery).toMatchObject({tool:'management.contextRequests',fullSourcesPreserved:true});
 for(const entry of retained.entries)expect(history).toContainEqual(entry);
 expect(retained.omittedEntries).toHaveLength(retained.selection.omitted);
 for(const ref of retained.omittedEntries){
  const original=history.find(entry=>entry.artifactId===ref.artifactId)!;
  expect(ref).toEqual({request:original.request,runId:original.runId,artifactId:original.artifactId,digest:original.digest,
   totalCharacters:original.totalCharacters,pages:original.pages.map(page=>({offset:page.offset,characters:page.text.length}))});
 }
 expect([...retained.entries,...retained.omittedEntries].map(entry=>entry.artifactId).sort())
  .toEqual(history.map(entry=>entry.artifactId).sort());
 expect(snapshot).toEqual(before);
});

it('shares exact common rows of different catalogs without merging their distinct metadata or order',()=>{
 const rows=Array.from({length:8},(_,i)=>({taskId:'source-'+i,inputs:[{name:'report.json',provenance:'Exact source metadata '.repeat(30)}]}));
 const additional={taskId:'additional',inputs:[{name:'unique.json',digest:'unique'}]};
 const current={acceptedSources:rows,existingCoverage:[],liveActors:[]};
 const deferred={...current,acceptedSources:[rows[5],additional,rows[0],rows[7]]};
 const snapshot={cases:[],operation:{tasks:[],workOpportunities:current,deferredWorkOpportunities:deferred}};
 const original=structuredClone(snapshot),delivered=projectManagerContext(snapshot);
 const refs=delivered.operation.deferredWorkOpportunities.acceptedSources;
 expect(refs.$rowsOf).toBe('operation.workOpportunities.acceptedSources');
 expect(refs.sha256).toBe(managerTextHash(JSON.stringify(rows)));
 expect(refs.rows).toEqual([5,additional,0,7]);
 expect(refs.rows.map((row:any)=>typeof row==='number'?delivered.operation.workOpportunities.acceptedSources[row]:row)).toEqual(deferred.acceptedSources);
 expect(snapshot).toEqual(original);
 expect(projectManagerContext(delivered)).toEqual(delivered);
});

it('reports an oversized mandatory focal case instead of dropping it and sending an empty review',()=>{
 const snapshot={cases:[{id:'mandatory-case',evidence:[{instruction:'Required authority and task evidence '.repeat(1000)}]}],operation:{tasks:[]}};
 const prefix='Keep the original authority.\n',original=structuredClone(snapshot);
 const review=projectManagerReview(snapshot,prefix,0,5000);
 expect(review.withinBudget).toBe(false);
 expect(review.caseWindow.selectedCaseIds).toEqual(['mandatory-case']);
 expect(review.description).toContain('Required authority and task evidence');
 expect(snapshot).toEqual(original);
});

it('fits requested escaped pages without dropping their requests, hashes, original offsets or mandatory case evidence',()=>{
 const text='\\'.repeat(11800),request={kind:'artifact',taskId:'source',expectedAttempt:4,name:'report.json',offset:700};
 const snapshot={cases:[{id:'focal',evidence:[{instruction:'Preserve the exact acceptance requirement.'}]}],
   operation:{tasks:[{id:'consumer',criteria:[{id:'mandatory'}]}],recoveredContext:{characterLimit:24000,
     results:[{request,status:'OK',digest:'original-hash',runId:'run-4',artifactId:'artifact-4',totalCharacters:20000,text,truncated:true,nextOffset:12500}]}}};
 const original=structuredClone(snapshot),prefix='Original permission contract.\n';
 const projected=projectManagerReview(snapshot,prefix,0,16000),delivered=deliveredReview(projected.description,prefix);
 expect(projected.withinBudget).toBe(true);expect(projected.caseWindow.selectedCaseIds).toEqual(['focal']);
 expect(delivered.cases).toEqual(snapshot.cases);expect(delivered.operation.tasks).toEqual(snapshot.operation.tasks);
 const page=delivered.operation.recoveredContext.results[0];
 expect(page).toMatchObject({request,digest:'original-hash',runId:'run-4',artifactId:'artifact-4',totalCharacters:20000,status:'OK',truncated:true});
 expect(text.startsWith(page.text)).toBe(true);expect(page.text.length).toBeLessThan(text.length);
 expect(page.nextOffset).toBe(request.offset+page.text.length);
 expect(delivered.operation.recoveredContext.selection).toMatchObject({pageBudgetAdjusted:true,fullSourcesPreserved:true});
 expect(snapshot).toEqual(original);
});
