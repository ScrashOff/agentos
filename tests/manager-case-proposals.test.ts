import {createHash,randomUUID} from 'node:crypto';
import {afterAll,expect,it} from 'vitest';
import {projectManagerCaseProposals} from '../server/manager-case-proposals.js';
import {managerCaseEvidenceSource,readManagerCaseEvidence} from '../server/manager-case-evidence.js';
import {projectManagerCases} from '../server/manager-material-consumption.js';
import {projectManagerReview,projectPersistedManagerDescription,managerContextVersion,managerContextGuide} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {pool,migrate} from '../server/db.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {installSupervision} from '../server/supervision.js';
import {seedRegistry} from '../server/registry.js';
import {readWorkOpportunities} from '../server/work-opportunities.js';
afterAll(()=>pool.end());
const id=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const hash=(v:string)=>createHash('sha256').update(v).digest('hex');
function fixture(attempt=4){
 const proposals=Array.from({length:18},(_,i)=>({kind:'SCOPED_DELIVERY_ACTION',
  action:{kind:'prepare_integration',taskId:id(2),expectedAttempt:attempt,sourceTaskId:id(100+i),expectedSourceRunId:id(200+i),
   artifactIds:[id(300+i)],reason:'Preserve the current source and exact acceptance contract. '+String(i).repeat(800)},
  sourceProof:{fence:id(400+i),runId:id(200+i),taskId:id(100+i),attempt:1,workspace:'source/'+i,manifestDigest:hash('manifest'+i)},
  consumerRunId:id(3),consumerAttempt:attempt,sourcePlanRunId:id(500+i),requestId:id(600+i),
  actionKey:'scoped-action:'+hash('action'+i),requestKey:'delivery:'+hash('request'+i)}));
 const mandatory={kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationId:id(8),generation:hash('generation'),
  expectedSourceRunId:id(9),observed:{manifestDigest:hash('current')},reason:'The current source is mandatory.'};
 const row={id:id(1),goal_id:id(10),task_id:id(2),source_run_id:id(3),current_run_id:id(3),current_attempt:attempt,
  current_run_state:'failed',base_workspace:'project/scope',work_kind:'integrate',reason:'Source repair review',
  evidence:[mandatory,...proposals,{kind:'UNKNOWN_NEW_CAUSAL',nested:{source:id(19),contract:'Keep completely'}}]};
 const snapshot={cases:projectManagerCases([row]),operation:{goalId:id(10),focus:{caseId:id(1),taskId:id(2),scope:'project/scope'},
  tasks:[{id:id(2),criteria:[{id:'end_to_end',label:'Independent final acceptance'}],dependencies:[id(11)],intent:{description:'Original task'}}],
  recoveredContext:{requests:[] as any[],results:[] as any[]}}};
 return {row,snapshot,mandatory,proposals};
}
it('projects only known current planner proposals, retaining causal evidence and all source bodies in the original',()=>{
 const f=fixture(),before=structuredClone(f.snapshot),r=projectManagerCaseProposals(f.snapshot,0);
 expect(r.discoveryDirectoryValidated).toBe(true);expect(f.snapshot).toEqual(before);
 expect(r.snapshot.cases[0].evidence).toEqual([f.mandatory,...f.snapshot.cases[0].evidence.slice(1,4),f.snapshot.cases[0].evidence.at(-1)]);
 const d=r.snapshot.operation.caseEvidenceDirectory.cases[0];
 expect(d.rows).toHaveLength(18);expect(d.selectedIndices).toEqual([1,2,3]);
 expect(d.source).not.toHaveProperty('entryDigests');
 expect(d.rows[0]).toEqual({index:1,kind:'prepare_integration',sourceTaskId:id(100),requestId:id(600),artifactCount:1});
 expect(d.recovery).toEqual({kind:'case_evidence',caseId:id(1),digest:f.snapshot.cases[0].evidenceSource!.digest,offset:0});
 expect(r.snapshot.cases[0].currentRun).toEqual(before.cases[0].currentRun);
 expect(r.snapshot.operation.tasks).toEqual(before.operation.tasks);
});
it('rotates complete units and never treats current proposals as historical',()=>{
 const f=fixture(),seen=new Set<number>();
 for(let cursor=0;cursor<18;cursor+=3){
  const r=projectManagerCaseProposals(f.snapshot,cursor),d=r.snapshot.operation.caseEvidenceDirectory.cases[0];
  d.selectedIndices.forEach((i:number)=>seen.add(i));
  expect(d.consumerAttempt).toBe(4);expect(d.source.sourceRunId).toBe(id(3));
 }
 expect([...seen].sort((a,b)=>a-b)).toEqual(Array.from({length:18},(_,i)=>i+1));
});
it('always includes requested proposal bodies in addition to the rotation',()=>{
 const f=fixture();f.snapshot.operation.recoveredContext={requests:[{kind:'case_evidence',caseId:id(1),evidenceIndex:18,
  digest:f.snapshot.cases[0].evidenceSource!.digest,offset:0}],results:[{status:'OK',caseId:id(1),evidenceIndex:17,
  evidenceDigest:f.snapshot.cases[0].evidenceSource!.digest,text:'Exact requested original page'}]};
 const r=projectManagerCaseProposals(f.snapshot,0);
 expect(r.snapshot.operation.caseEvidenceDirectory.cases[0].selectedIndices).toEqual([1,2,3,17,18]);
 expect(r.snapshot.operation.recoveredContext).toEqual(f.snapshot.operation.recoveredContext);
});
it('keeps initial verification attempt zero complete',()=>{
 const f=fixture(0),r=projectManagerCaseProposals(f.snapshot);
 expect(r.discoveryDirectoryValidated).toBe(false);expect(r.snapshot.cases[0].evidence).toEqual(f.snapshot.cases[0].evidence);
 expect(r.snapshot.cases[0].currentRun).toEqual(f.snapshot.cases[0].currentRun);
 expect(r.snapshot.cases[0].evidenceSource).not.toHaveProperty('entryDigests');
});
it.each(['unknown','actionUnknown','proofUnknown','wrongAttempt','wrongRun','wrongSource','changedBody'] as const)(
 'retains an unproven or unknown proposal completely: %s',(mode)=>{
 const f=fixture(),e=f.snapshot.cases[0].evidence[18] as any;
 if(mode==='unknown')e.futureField={mustRemain:true};
 if(mode==='actionUnknown')e.action.extra={currentContract:true};
 if(mode==='proofUnknown')e.sourceProof.extra={currentContract:true};
 if(mode==='wrongAttempt')e.consumerAttempt=3;
 if(mode==='wrongRun')e.consumerRunId=id(91);
 if(mode==='wrongSource')e.sourceProof.runId=id(92);
 if(mode==='changedBody')e.action.reason+=' changed without raw binding';
 const r=projectManagerCaseProposals(f.snapshot,0);
 expect(r.snapshot.cases[0].evidence).toContainEqual(e);
 expect(r.snapshot.operation.caseEvidenceDirectory.cases[0].rows.some((v:any)=>v.index===18)).toBe(false);
});
it.each(['sourceHash','sourceRun','entryCount','noSource'] as const)('keeps the case complete when origin is unavailable: %s',mode=>{
 const f=fixture(),c=f.snapshot.cases[0] as any;
 if(mode==='sourceHash')c.evidenceSource.arrayDigest='invalid';
 if(mode==='sourceRun')c.evidenceSource.sourceRunId=id(90);
 if(mode==='entryCount')c.evidenceSource.entryDigests.pop();
 if(mode==='noSource')delete c.evidenceSource;
 const actual=projectManagerCaseProposals(f.snapshot);
 expect(actual.discoveryDirectoryValidated).toBe(false);expect(actual.snapshot.cases[0].evidence).toEqual(f.snapshot.cases[0].evidence);
 expect(actual.snapshot.cases[0].currentRun).toEqual(f.snapshot.cases[0].currentRun);
});
it('advances the same focused case through the actual persisted management-review cursor',async()=>{
 await migrate();await installBacklogPlanner();await installManagement();await seedRegistry();
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),f=fixture();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Cursor fixture','Preserved objective','proposal-window',$1::text)",[goal]);
 try{
  const firstCatalog=await readWorkOpportunities(pool,goal,[]);
  const first=projectManagerReview({...f.snapshot,operation:{...f.snapshot.operation,workOpportunities:firstCatalog}},'',0);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,stage,state,criteria,covers,execution)
   VALUES($1,$2,'review','review','Preserved review','qa.backend','coordinate','accepted','[]','[]','{}')`,[task,goal]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'submitted','harness')",[run,task,randomUUID()]);
  await pool.query("INSERT INTO management_reviews(run_id,goal_id,summary,capacity_reason,receipts) VALUES($1,$2,'Reviewed','Preserved','[]')",[run,goal]);
  const nextCatalog=await readWorkOpportunities(pool,goal,[]);
  const next=projectManagerReview({...f.snapshot,operation:{...f.snapshot.operation,workOpportunities:nextCatalog}},'',0);
  expect(firstCatalog.selection.cursor).toBe(0);expect(nextCatalog.selection.cursor).toBe(1);
  const a=expandSharedManagerContext(JSON.parse(first.description)),b=expandSharedManagerContext(JSON.parse(next.description));
  expect(a.operation.focus).toEqual(b.operation.focus);
  expect(a.cases[0].proposalWindow.selectedProposalIndices).toEqual([1]);
  expect(b.cases[0].proposalWindow.selectedProposalIndices).toEqual([2]);
  const frozen=projectManagerCaseProposals(a,8);expect(frozen.snapshot.cases[0].proposalWindow).toEqual(a.cases[0].proposalWindow);
 }finally{
  await pool.query('DELETE FROM management_reviews WHERE goal_id=$1',[goal]);
  await pool.query('DELETE FROM runs WHERE task_id=$1',[task]);
  await pool.query('DELETE FROM tasks WHERE id=$1',[task]);
  await pool.query('DELETE FROM goals WHERE id=$1',[goal]);
 }
});
it('does not reinterpret an unknown directory or erase references inside it',()=>{
 const f=fixture();(f.snapshot.operation as any).caseEvidenceDirectory={version:2,reference:id(8)};
 expect(projectManagerCaseProposals(f.snapshot)).toEqual({snapshot:f.snapshot,discoveryDirectoryValidated:false});
});
it('keeps exact raw-entry provenance even when another causal summary was projected',()=>{
 const f=fixture();(f.row.evidence[0] as any).summary='raw '.repeat(600);
 const source=managerCaseEvidenceSource(f.row);f.snapshot.cases=projectManagerCases([f.row]);
 expect((f.snapshot.cases[0].evidence[0] as any).summary).toHaveLength(1800);
 const r=projectManagerCaseProposals(f.snapshot);
 expect(r.snapshot.operation.caseEvidenceDirectory.cases[0].source.digest).toBe(source!.digest);
 expect(r.snapshot.cases[0].evidence[0]).toEqual(f.snapshot.cases[0].evidence[0]);
});
it('reprojects a frozen window with exact selected evidence through the real text-sharing boundary',()=>{
 const f=fixture(),review=projectManagerReview(f.snapshot,'MANAGEMENT_V1\n',0,150000);
 const decoded=expandSharedManagerContext(JSON.parse(review.description.slice('MANAGEMENT_V1\n'.length)));
 expect(decoded.evidenceTexts.length).toBeGreaterThan(0);
 expect(projectManagerCaseProposals(decoded).discoveryDirectoryValidated).toBe(true);
 expect(()=>projectManagerReview(decoded,'MANAGEMENT_V1\n',0,150000)).not.toThrow();
 const tampered=structuredClone(decoded);tampered.cases[0].evidence[0].reason='changed';
 expect(()=>projectManagerCaseProposals(tampered)).toThrow('MANAGER_CASE_PROPOSAL_WINDOW_CHANGED');
 const changedDirectory=structuredClone(decoded);changedDirectory.operation.caseEvidenceDirectory.cases[0].rows[0].sourceTaskId=id(99);
 expect(()=>projectManagerCaseProposals(changedDirectory)).toThrow('MANAGER_CASE_PROPOSAL_DIRECTORY_CHANGED');
 const changedIndex=structuredClone(decoded);changedIndex.operation.caseEvidenceDirectory.cases[0].rows[0].index=19;
 expect(()=>projectManagerCaseProposals(changedIndex)).toThrow('MANAGER_CASE_PROPOSAL_DIRECTORY_CHANGED');
});
it('keeps a distinct original index for visually equal discovery rows and binds hidden source details',async()=>{
 await migrate();await installBacklogPlanner();await installManagement();await installSupervision();await seedRegistry();
 const f=fixture(),goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID();
 const evidence=structuredClone(f.row.evidence) as any[];
 for(const e of evidence.filter(v=>v.kind==='SCOPED_DELIVERY_ACTION')){
  e.action.taskId=task;e.consumerRunId=run;
 }
 evidence[2]={...structuredClone(evidence[1]),actionKey:'scoped-action:'+hash('different-action')};
 const row={...f.row,id:caseId,goal_id:goal,task_id:task,source_run_id:run,current_run_id:run,evidence};
 const snapshot={...f.snapshot,cases:projectManagerCases([row])};
 const projected=projectManagerCaseProposals(snapshot),directory=projected.snapshot.operation.caseEvidenceDirectory.cases[0];
 const {index:one,...a}=directory.rows[0],{index:two,...b}=directory.rows[1];
 expect(a).toEqual(b);expect([one,two]).toEqual([1,2]);expect(directory.rows).toHaveLength(18);
 const db=await pool.connect();
 try{
  await db.query('BEGIN');
  await db.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Evidence binding','Exact source','proposal-window',$1::text)",[goal]);
  await db.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,stage,state,criteria,covers,execution,attempt_count)
   VALUES($1,$2,'consumer','Consumer','Exact contract','qa.backend','work','failed','[]','[]','{}',4)`,[task,goal]);
  await db.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,4,$3,1,'failed','harness')",[run,task,randomUUID()]);
  await db.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence) VALUES($1,$2,$3,$4,'Review',$5)",[caseId,goal,task,run,JSON.stringify(evidence)]);
  const request={kind:'case_evidence' as const,caseId,evidenceIndex:1,digest:directory.source.digest,offset:0};
  expect((await readManagerCaseEvidence(db,goal,request)).status).toBe('OK');
  for(const field of ['manifestDigest','sourceRunId','actionKey']){
   const changed=structuredClone(evidence);
   if(field==='manifestDigest')changed[1].sourceProof.manifestDigest=hash('changed manifest');
   if(field==='sourceRunId'){changed[1].sourceProof.runId=id(999);changed[1].action.expectedSourceRunId=id(999);}
   if(field==='actionKey')changed[1].actionKey='scoped-action:'+hash('changed action');
   const current=projectManagerCaseProposals({...snapshot,cases:projectManagerCases([{...row,evidence:changed}])});
   expect(current.snapshot.operation.caseEvidenceDirectory.cases[0].rows).toEqual(directory.rows);
   await db.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1',[caseId,JSON.stringify(changed)]);
   expect((await readManagerCaseEvidence(db,goal,request)).status).toBe('STALE_CASE_EVIDENCE');
  }
 }finally{await db.query('ROLLBACK');db.release();}
});
it('does not collapse repeated admission evidence in a proposal window or alter its original indices',()=>{
 const f=fixture(),admission={kind:'RUN_ADMISSION_BLOCKED',admissionEventId:'1',code:'CAPACITY',runId:id(3),attempt:4,at:'2026-10-12T00:00:00Z'};
 f.row.evidence.push(admission as any,{...admission,admissionEventId:'2',at:'2026-10-12T01:00:00Z'} as any);
 f.snapshot.cases=projectManagerCases([f.row]);
 const review=projectManagerReview(f.snapshot,'',0,150000),decoded=expandSharedManagerContext(JSON.parse(review.description));
 expect(decoded.cases[0].evidence.filter((e:any)=>e.kind==='RUN_ADMISSION_BLOCKED')).toHaveLength(2);
 expect(projectManagerCaseProposals(decoded).discoveryDirectoryValidated).toBe(true);
 const prefix='MANAGEMENT_V1\n'+managerContextVersion+'\n'+managerContextGuide+'\n';
 const frozen=projectManagerReview(f.snapshot,prefix,0,150000).description;
 expect(()=>projectPersistedManagerDescription(frozen)).not.toThrow();
 const resumed=projectPersistedManagerDescription(frozen),start=resumed.lastIndexOf('\n{"cases":');
 const resumedSnapshot=expandSharedManagerContext(JSON.parse(resumed.slice(start+1)));
 expect(resumedSnapshot.cases[0].proposalWindow).toEqual(decoded.cases[0].proposalWindow);
 expect(resumedSnapshot.cases[0].evidence.filter((e:any)=>e.kind==='RUN_ADMISSION_BLOCKED')).toHaveLength(2);
});
