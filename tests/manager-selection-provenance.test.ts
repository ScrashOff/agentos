import {expect,it} from 'vitest';
import {projectManagerReview} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';

const request={kind:'artifact',taskId:'source',expectedAttempt:4,name:'report.json',offset:700};
const selection={status:'FOCUS_RESUMED',reviewRunId:'accepted-review-2234',
  binding:{caseId:'case-lucky',scope:'project/lucky',taskId:'consumer'}};
const fixture=(entry:object,provenance:object|null=selection)=>({
  cases:[{id:'case-lucky',taskId:'consumer',evidence:[{instruction:'Preserve the exact acceptance requirement.'}]}],
  operation:{focus:{caseId:'case-lucky',taskId:'consumer',scope:'project/lucky'},
    tasks:[{id:'consumer',criteria:[{id:'mandatory'}]}],
    recoveredContext:{characterLimit:24000,...(provenance?{selection:provenance}:{}),results:[entry]}}
});
const project=(source:unknown,maximum=16000)=>{
  const review=projectManagerReview(source,'',0,maximum);
  return {review,result:expandSharedManagerContext(JSON.parse(review.description))};
};

it('retains resumed review provenance and page identity when reducing escaped text',()=>{
  const text='\\'.repeat(11800),source=fixture({request,status:'OK',digest:'original-hash',runId:'source-run-4',
    artifactId:'source-artifact-4',totalCharacters:20000,text,truncated:true,nextOffset:12500});
  const before=structuredClone(source),{review,result}=project(source);
  expect(review.withinBudget).toBe(true);
  expect(result.operation.recoveredContext.selection).toMatchObject({...selection,pageBudgetAdjusted:true,fullSourcesPreserved:true});
  const page=result.operation.recoveredContext.results[0];
  expect(page).toMatchObject({request,status:'OK',digest:'original-hash',runId:'source-run-4',artifactId:'source-artifact-4',totalCharacters:20000});
  expect(page.text.length).toBeLessThan(text.length);
  expect(text.startsWith(page.text)).toBe(true);
  expect(page.nextOffset).toBe(request.offset+page.text.length);
  expect(result.cases).toEqual(source.cases);
  expect(result.operation.focus).toEqual(source.operation.focus);
  expect(result.operation.tasks).toEqual(source.operation.tasks);
  expect(source).toEqual(before);
});

it('retains provenance and catalog digest when reducing rows, without changing source offsets',()=>{
  const rowRequest={kind:'delivery_consumptions',offset:16,snapshotDigest:'frozen-catalog-digest'},
    rows=Array.from({length:16},(_,i)=>({id:'obligation-'+i,detail:String(i)+':'+('\\'+i).repeat(1200)}));
  const source=fixture({request:rowRequest,status:'OK',snapshotDigest:'frozen-catalog-digest',rows,totalRows:70,nextOffset:32});
  const before=structuredClone(source),{review,result}=project(source);
  expect(review.withinBudget).toBe(true);
  const recovered=result.operation.recoveredContext,page=recovered.results[0];
  expect(recovered.selection).toMatchObject({...selection,pageBudgetAdjusted:true,fullSourcesPreserved:true});
  expect(page).toMatchObject({request:rowRequest,status:'OK',snapshotDigest:'frozen-catalog-digest',totalRows:70,truncated:true});
  expect(page.rows.length).toBeLessThan(rows.length);
  expect(page.rows).toEqual(rows.slice(0,page.rows.length));
  expect(page.nextOffset).toBe(rowRequest.offset+page.rows.length);
  expect(source).toEqual(before);
});

it('leaves a fitting provenance envelope intact and does not invent resumed status for legacy pages',()=>{
  const source=fixture({request,status:'OK',digest:'original-hash',totalCharacters:100,text:'complete',nextOffset:null});
  const {result}=project(source);
  expect(result.operation.recoveredContext).toEqual(source.operation.recoveredContext);
  const legacy=fixture({request,status:'OK',digest:'original-hash',totalCharacters:20000,text:'\\'.repeat(11800)},null);
  const shrunk=project(legacy);
  expect(shrunk.review.withinBudget).toBe(true);
  expect(shrunk.result.operation.recoveredContext.selection).toMatchObject({pageBudgetAdjusted:true,fullSourcesPreserved:true});
  expect(shrunk.result.operation.recoveredContext.selection).not.toHaveProperty('status');
  expect(shrunk.result.operation.recoveredContext.selection).not.toHaveProperty('reviewRunId');
});
