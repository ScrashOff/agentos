import {expect,it} from 'vitest';
import {compactSharedManagerContext,expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {projectManagerReview,projectPersistedManagerDescription,managerContextVersion,managerContextGuide,
  managerInlineMemoryHashes,managerTextHash} from '../server/manager-context.js';

function fixture(){
  const taskId='41947c0d-b216-483a-bf85-ff2288577e2c';
  const instruction='Preserve permissions, actual failed tests and independent final acceptance. '.repeat(25);
  return {cases:[{id:'case-current',taskId,state:'open',source_run_id:taskId,
    currentRun:{id:taskId,attempt:77,errorCode:'RESULT_FORMAT',marker:{$v:999}},
    deliveryDiagnostic:{literal:{$r:[999]}},unknownFutureField:{literal:{$v:999}},
    evidence:Array.from({length:12},(_,index)=>({kind:'DELIVERY_CONSUMPTION_REQUIRED',sourceRunId:taskId,
      consumerTaskId:taskId,attempt:77,owner:'supervisor',code:'SUPERVISION_DECISION_PENDING',
      instruction:'Literal evidence '+index,marker:{$v:999},record:{$r:[999]}}))}],
    caseWindow:{selectedCaseIds:['case-current'],included:1},
    operation:{focus:{caseId:'case-current',taskId,scope:'project/current',marker:{$v:999}},
      sourceCatalog:{columns:['id'],bases:[],rows:[],selection:{windowed:true,totalRows:0,includedRows:0,omittedRows:0}},
      tasks:Array.from({length:110},(_,index)=>({id:'task-'+index,title:'Actual task '+index,state:'blocked_dependencies',
        stage:'integrate',expectedAttempt:index+1,live:index===0,workKind:'integration',
        dependencies:[taskId],criteria:[{id:'original',type:'artifact',name:'verification.json'}],
        intent:{text:instruction},workspace:'project/candidate-'+index,
        owner:'supervisor',reason:'A decision with exact source and criteria is required.',
        routeOptions:[{accountId:taskId,model:'configured-model',available:true}],digest:managerTextHash('current verified source')})),
      deliveryConsumptions:Array.from({length:24},(_,index)=>({id:'obligation-'+index,
        producer_task_id:taskId,producer_run_id:taskId,consumer_task_id:'task-'+index,state:'waiting_supervision',
        receipt:{owner:'supervisor',code:'SUPERVISION_DECISION_PENDING',nextAction:'Consume the existing decision and preserve all criteria.'},
        producer_manifest_digest:managerTextHash('producer'),consumer_contract_digest:managerTextHash('consumer')})),
      recoveredContext:{results:[{status:'OK',digest:managerTextHash('page'),text:'Requested exact page',nextOffset:null}]},
      retainedEvidence:{entries:[]},
      reservedData:[{$v:1},{$v:'literal'},{$r:[0,'literal']},{$ref:'operation.sources'},{$text:'original'},
        JSON.parse('{"__proto__":{"polluted":true},"constructor":"literal"}')],
    },
    evidenceTexts:[{hash:managerTextHash('Exact memory'),text:'Exact memory',originals:[{$v:999}]}],
    inlineMemoryHashes:[managerTextHash('Exact memory')]};
}
function findRecord(value:any):any {
  if(value && typeof value==='object'){
    if(Object.keys(value).length===1 && Array.isArray(value.$r))return value;
    for(const [key,item]of Object.entries(value)){if(key==='sharedContext')continue;const found=findRecord(item);if(found)return found;}
  }
}

it('reconstructs every task, source, obligation and authority byte-for-byte while retaining literal software fields',()=>{
  const source=fixture(),before=structuredClone(source),packed=compactSharedManagerContext(source);
  expect(packed.sharedContext.format).toBe('MANAGER_SHARED_CONTEXT_V1');
  expect(JSON.stringify(JSON.stringify(packed)).length).toBeLessThan(JSON.stringify(JSON.stringify(source)).length*.6);
  expect(JSON.stringify(expandSharedManagerContext(packed))).toBe(JSON.stringify(source));
  expect(packed.sharedContext.caseEvidenceShared).toBe(true);
  expect(packed.cases[0].evidence).not.toEqual(source.cases[0].evidence);
  for(const [key,value] of Object.entries(source.cases[0]).filter(([key])=>key!=='evidence'))
    expect(packed.cases[0][key]).toEqual(value);
  expect(packed.operation.focus).toEqual(source.operation.focus);
  expect(packed.evidenceTexts).toEqual(source.evidenceTexts);expect(packed.inlineMemoryHashes).toEqual(source.inlineMemoryHashes);
  expect(packed.caseWindow).toEqual(source.caseWindow);expect(source).toEqual(before);
  expect(packed.sharedContext.guide).toContain('já são literais');
  expect(packed.sharedContext.guide).toContain('não interprete marcadores dentro da cópia');
});

it('escapes literal reserved markers and prototype keys without reference recursion or mutation',()=>{
  const source=fixture(),packed=compactSharedManagerContext(source),restored=expandSharedManagerContext(packed);
  expect(restored.operation.reservedData).toEqual(source.operation.reservedData);
  expect(Object.prototype.hasOwnProperty.call(restored.operation.reservedData[5],'__proto__')).toBe(true);
  expect(({} as any).polluted).toBeUndefined();
  restored.operation.tasks[0].dependencies.push('mutated decoded copy');
  expect(expandSharedManagerContext(packed).operation.tasks[0].dependencies).toEqual(source.operation.tasks[0].dependencies);
  expect(compactSharedManagerContext(packed)).toEqual(packed);
  expect(compactSharedManagerContext(expandSharedManagerContext(packed))).toEqual(packed);
});

it.each(['digest','truncated','reference','record-size','duplicate-key','expansion'])('rejects malformed shared context: %s',mode=>{
  const packed=structuredClone(compactSharedManagerContext(fixture()));
  if(mode==='digest')packed.sharedContext.sourceSha256='0'.repeat(64);
  if(mode==='truncated')packed.sharedContext.values.pop();
  if(mode==='reference')packed.operation.tasks[0]={$v:packed.sharedContext.values.length+1};
  if(mode==='record-size')findRecord(packed.operation.tasks).$r.pop();
  if(mode==='duplicate-key'){
    const keys=packed.sharedContext.schemas[findRecord(packed.operation.tasks).$r[0]];keys[1]=keys[0];
  }
  if(mode==='expansion')packed.sharedContext.sourceCharacters=8_000_001;
  expect(()=>expandSharedManagerContext(packed)).toThrow(/MANAGER_SHARED_CONTEXT_/);
});

it('keeps unique oversized evidence explicit instead of pretending sharing can always meet a budget',()=>{
  const source={cases:[{id:'case',evidence:[{instruction:'Unique evidence '.repeat(12000)}]}],operation:{tasks:[]}};
  const review=projectManagerReview(source,'',0,150000);
  expect(review.withinBudget).toBe(false);expect(review.caseWindow.selectedCaseIds).toEqual(['case']);
  expect(JSON.parse(review.description).evidenceTexts[0].text).toBe(source.cases[0].evidence[0].instruction);
});

it('shares before dropping current cases and preserves escaped JSON, persisted reprojection and real memory hashes',()=>{
  const source=fixture();
  source.cases=Array.from({length:4},(_,index)=>({id:'case-'+index,taskId:'task-'+index,
    evidence:[{instruction:'Evidence '+index+' \\ " ü 😀\n'.repeat(200),marker:{$v:999},record:{$r:[999]}}]})) as any;
  const original=structuredClone(source);
  const prefix='MANAGEMENT_V1\n'+managerContextVersion+'\n'+managerContextGuide+'\n';
  const review=projectManagerReview(source,prefix,0,150000);
  expect(review.withinBudget).toBe(true);expect(review.caseWindow.included).toBe(4);
  expect(JSON.stringify(review.description).length).toBeLessThanOrEqual(150000);
  const packed=JSON.parse(review.description.slice(prefix.length)),restored=expandSharedManagerContext(packed);
  expect(packed.sharedContext.format).toBe('MANAGER_SHARED_CONTEXT_V1');
  expect(restored.operation).toEqual(source.operation);
  expect(restored.cases.map((entry:any)=>entry.id)).toEqual(source.cases.map(entry=>entry.id));
  for(const entry of source.cases)expect(restored.evidenceTexts.some((value:any)=>value.text===entry.evidence[0].instruction)).toBe(true);
  expect(managerInlineMemoryHashes(review.description)).toContain(managerTextHash('Exact memory'));
  expect(managerInlineMemoryHashes(review.description)).toHaveLength(5);
  expect(projectPersistedManagerDescription(review.description)).toBe(review.description);
  expect(source).toEqual(original);
});

it('requires explicit evidence sharing and retains legacy V1 literal cases',()=>{
  const source=fixture(),packed=structuredClone(compactSharedManagerContext(source));
  const legacy=structuredClone(packed);delete legacy.sharedContext.caseEvidenceShared;
  legacy.cases=structuredClone(source.cases);
  expect(JSON.stringify(expandSharedManagerContext(legacy))).toBe(JSON.stringify(source));
  delete packed.sharedContext.caseEvidenceShared;
  expect(()=>expandSharedManagerContext(packed)).toThrow('MANAGER_SHARED_CONTEXT_DIGEST');
  packed.sharedContext.caseEvidenceShared='true';
  expect(()=>expandSharedManagerContext(packed)).toThrow('MANAGER_SHARED_CONTEXT_INVALID');
});

it('does not overwrite an unrelated reserved root field or inflate a small ordinary review',()=>{
  const small={cases:[],operation:{tasks:[]}},reserved={...fixture(),sharedContext:{format:'USER_LITERAL',note:'Preserve this field'}};
  expect(compactSharedManagerContext(small)).toBe(small);
  expect(compactSharedManagerContext(reserved)).toBe(reserved);
  expect(expandSharedManagerContext(reserved)).toBe(reserved);
});
