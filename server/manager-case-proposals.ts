import {createHash} from 'node:crypto';
import {scopedPlanActionSchema} from './scoped-plan-contract.js';

const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
const hex=/^[a-f0-9]{64}$/;
const consumptionCodes=['CONSUMER_INPUTS_NOT_BOUND','ACCEPTED_REPAIR_PENDING',
 'COMPOSITION_PROOF_REQUIRED','CONSUMER_CONTRACT_CHANGED'];
const object=(v:any)=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const keys=(v:any,allowed:string[])=>object(v)&&Object.keys(v).filter(k=>v[k]!==undefined).every(k=>allowed.includes(k));
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):object(v)?
 Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const digest=(v:any)=>createHash('sha256').update(JSON.stringify(canonical(v))).digest('hex');
const same=(a:any,b:any)=>a===undefined||b===undefined?a===b:digest(a)===digest(b);
const sourceKeys=['version','caseId','taskId','sourceRunId','evidenceCount','arrayDigest','digest','entryDigests'];
const publicSource=(source:any)=>Object.fromEntries(Object.entries(source).filter(([k])=>k!=='entryDigests'));
const validSource=(source:any,c:any,entries:boolean,current=true)=>keys(source,sourceKeys)&&source.version===1&&
 source.caseId===c.id&&source.taskId===c.taskId&&(!current||source.sourceRunId===(c.currentRun?.id??null))&&
 uuid.test(source.caseId)&&uuid.test(source.taskId)&&(source.sourceRunId===null||uuid.test(source.sourceRunId))&&
 Number.isSafeInteger(source.evidenceCount)&&source.evidenceCount>=0&&source.evidenceCount<=10000&&
 hex.test(source.arrayDigest)&&hex.test(source.digest)&&(!entries||Array.isArray(source.entryDigests)&&
 source.entryDigests.length===source.evidenceCount&&source.entryDigests.every((v:any)=>typeof v==='string'&&hex.test(v)));

/** Only planner proposals with the exact known shape may form a discovery
 * window. Current causal evidence, unknown fields and initial verification
 * remain complete. This does not establish source validity or action consent. */
function knownCurrentProposal(e:any,c:any,index:number){
 const parsed=scopedPlanActionSchema.safeParse(e?.action);
 if(!keys(e,['kind','action','actionKey','requestId','requestKey','sourceProof','consumerRunId','consumerAttempt','sourcePlanRunId','handoffId','conflict'])||
  e.kind!=='SCOPED_DELIVERY_ACTION'||!parsed.success||!same(parsed.data,e.action)||
  e.consumerRunId!==c.currentRun.id||e.consumerAttempt!==c.currentRun.attempt||
  e.action.taskId!==c.taskId||e.action.expectedAttempt!==c.currentRun.attempt||
  !/^scoped-action:[a-f0-9]{64}$/.test(e.actionKey??'')||!uuid.test(e.requestId??'')||
  !uuid.test(e.sourcePlanRunId??'')||!/^delivery:[a-f0-9]{64}$/.test(e.requestKey??'')||
  (e.handoffId!==undefined&&!uuid.test(e.handoffId))||(e.conflict!==undefined&&e.conflict!=='EXISTING_HANDOFF'))return false;
 const p=e.sourceProof;
 return keys(p,['fence','runId','taskId','attempt','workspace','manifestDigest'])&&
  uuid.test(p.fence??'')&&p.runId===e.action.expectedSourceRunId&&p.taskId===e.action.sourceTaskId&&
  Number.isSafeInteger(p.attempt)&&p.attempt>=1&&typeof p.workspace==='string'&&p.workspace.length>0&&
  p.workspace.length<=4096&&hex.test(p.manifestDigest??'')&&digest(e)===c.evidenceSource.entryDigests[index];
}

/** Exact recorded consumption requests for this consumer run may be read as
 * complete units. This is a view of still-pending obligations, not a statement
 * that a repair is currently applicable or that any obligation is satisfied. */
function knownConsumption(e:any,c:any,index:number){
 if(!keys(e,['kind','obligationKey','generation','code','handoffId','producerTaskId','producerRunId',
  'producerManifestDigest','repairArtifacts','consumerTaskId','consumerRunId','blockedTaskIds',
  'criteriaUnchanged','instruction','requestKey'])||e.kind!=='DELIVERY_CONSUMPTION_REQUIRED'||
  e.consumerTaskId!==c.taskId||e.consumerRunId!==c.currentRun.id||e.criteriaUnchanged!==true||
  typeof e.obligationKey!=='string'||!/^delivery-consumption:[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(e.obligationKey)||
  !hex.test(e.generation??'')||!hex.test(e.producerManifestDigest??'')||
  !uuid.test(e.handoffId??'')||!uuid.test(e.producerTaskId??'')||!uuid.test(e.producerRunId??'')||
  !consumptionCodes.includes(e.code)||typeof e.instruction!=='string'||!e.instruction||
  !/^delivery:[a-f0-9]{64}$/.test(e.requestKey??'')||
  !Array.isArray(e.blockedTaskIds)||!e.blockedTaskIds.every((id:any)=>typeof id==='string'&&uuid.test(id))||
  !Array.isArray(e.repairArtifacts)||e.repairArtifacts.length!==2)return false;
 const refs=e.repairArtifacts;
 if(!['implementation.patch','repair-report.json'].every(name=>refs.filter((ref:any)=>ref?.name===name).length===1)||
  !refs.every((ref:any)=>keys(ref,['id','run_id','name','digest','size'])&&uuid.test(ref.id??'')&&
   ref.run_id===e.producerRunId&&hex.test(ref.digest??'')&&Number.isSafeInteger(ref.size)&&ref.size>=0))return false;
 return digest(e)===c.evidenceSource.entryDigests[index];
}
const materialGuide='CURRENT_CASE_EVIDENCE_V2 presents complete selected units from current-run planner proposals and recorded consumption requests. Every consumption obligation remains pending until its existing controller verifies its own inputs, composition and independent QA; selection never resolves other obligations or chooses a repair. materialConsumptionReview and requiredDecision retain their entire bindings. The complete directory preserves original indices and the case source digest. Read any other entry with management.contextRequests kind:case_evidence, caseId, source digest, evidenceIndex and offset:0 before deciding about it. Each group rotates independently from the displayed cursor; explicitly requested entries are also complete. SOURCE_REVIEW, recertification, unknown evidence and attempt zero are never windowed. STALE_CASE_EVIDENCE requires a fresh origin/digest and offset:0; TOO_LARGE is an explicit evidence gap. All action, stop, source and acceptance guards remain authoritative.';

const guide='CURRENT_SCOPED_PROPOSALS_V1 é uma janela de propostas do planner para a tentativa atual, não histórico encerrado nem autoridade. As outras evidências causais permanecem integrais. operation.caseEvidenceDirectory lista cada proposta e sua origem. Para avaliar ou usar uma proposta não incluída, peça contextRequests case_evidence com caseId, digest da fonte, evidenceIndex e offset:0; leia a entrada inteira antes de decidir. O diretório não comprova validade atual da fonte, aplicação, aceite nem ausência de alternativas. STALE_CASE_EVIDENCE exige origem/digest frescos e offset:0; TOO_LARGE mantém lacuna explícita e não autoriza decidir sem a evidência. Os appliers mantêm todas as verificações.';

/** A previously persisted window must retain its exact selected bodies and
 * directory binding. Unknown directory shapes are never excluded as discovery. */
function existingDirectory(snapshot:any):boolean {
 const d=snapshot.operation?.caseEvidenceDirectory;
 if(!d)return false;
 const material=d.version===2&&d.kind==='CURRENT_CASE_EVIDENCE_V2';
 if(!material&&(d.version!==1||d.kind!=='CURRENT_SCOPED_PROPOSALS_V1'))return false;
 if(!keys(d,['version','kind','guide','cases','digest'])||d.guide!==(material?materialGuide:guide)||!Array.isArray(d.cases)||d.cases.length>10000||
  !hex.test(d.digest??''))throw Error('MANAGER_CASE_PROPOSAL_DIRECTORY_INVALID');
 const {digest:expected,...body}=d;if(digest(body)!==expected)throw Error('MANAGER_CASE_PROPOSAL_DIRECTORY_CHANGED');
 const texts=new Map((snapshot.evidenceTexts??[]).filter((r:any)=>typeof r.text==='string'&&
  createHash('sha256').update(r.text).digest('hex')===r.hash).map((r:any)=>[r.hash,r.text]));
 const restore=(v:any):any=>keys(v,['$text'])&&Object.keys(v).length===1&&texts.has(v.$text)?texts.get(v.$text):
  Array.isArray(v)?v.map(restore):object(v)?Object.fromEntries(Object.entries(v).map(([k,value])=>[k,restore(value)])):v;
 for(const row of d.cases){
  const c=snapshot.cases?.find((v:any)=>v.id===row.caseId);
  const integers=(a:any)=>Array.isArray(a)&&a.every((n:any)=>Number.isSafeInteger(n)&&n>=0&&n<10000);
  const knownRows=Array.isArray(row.rows)&&row.rows.length<=10000&&row.rows.every((r:any)=>
   keys(r,['index','kind','sourceTaskId','artifactCount','requestId'])&&
   Number.isSafeInteger(r.index)&&r.index>=0&&r.index<10000&&
   ['reuse_existing','prepare_integration','request_verification'].includes(r.kind)&&uuid.test(r.sourceTaskId??'')&&
   Number.isSafeInteger(r.artifactCount)&&r.artifactCount>=1&&r.artifactCount<=20&&
   uuid.test(r.requestId??'') || material&&keys(r,['index','kind','obligationId','generation','sourceTaskId','artifactCount','code'])&&
   Number.isSafeInteger(r.index)&&r.index>=0&&r.index<10000&&r.kind==='DELIVERY_CONSUMPTION_REQUIRED'&&
   (r.code===undefined||consumptionCodes.includes(r.code))&&
   uuid.test(r.obligationId??'')&&hex.test(r.generation??'')&&uuid.test(r.sourceTaskId??'')&&r.artifactCount===2);
  if(!keys(row,['caseId','source','consumerAttempt','total','selectedIndices','cursor','nextCursor','rows','recovery'])||!c||
   !validSource(row.source,c,false)||!same(row.source,c.evidenceSource)||!knownRows||row.total!==row.rows.length||
   row.consumerAttempt!==c.currentRun?.attempt||!integers(row.selectedIndices)||
   !Number.isSafeInteger(row.cursor)||row.cursor<0||row.cursor>=row.total||!Number.isSafeInteger(row.nextCursor)||row.nextCursor<0||row.nextCursor>=row.total||
   !same(row.recovery,{kind:'case_evidence',caseId:c.id,digest:row.source.digest,offset:0})||
   !keys(c.proposalWindow,material?['version','sourceEvidenceCount','selectedEvidenceIndices','selectedWindowIndices','omittedWindowEntries','projectedEvidenceDigest','directoryDigest']:
    ['version','sourceEvidenceCount','selectedEvidenceIndices','selectedProposalIndices','omittedProposals','projectedEvidenceDigest','directoryDigest'])||
   c.proposalWindow.sourceEvidenceCount!==row.source.evidenceCount||!integers(c.proposalWindow.selectedEvidenceIndices)||
   c.proposalWindow.selectedEvidenceIndices.length!==c.evidence?.length||
   (material?c.proposalWindow.omittedWindowEntries:c.proposalWindow.omittedProposals)!==row.total-row.selectedIndices.length||
   c.proposalWindow?.version!==(material?2:1)||c.proposalWindow.directoryDigest!==expected||
   c.proposalWindow.projectedEvidenceDigest!==digest(restore(c.evidence))||
   !same(material?c.proposalWindow.selectedWindowIndices:c.proposalWindow.selectedProposalIndices,row.selectedIndices))throw Error('MANAGER_CASE_PROPOSAL_WINDOW_CHANGED');
  if(material){
   const indices=row.rows.map((r:any)=>r.index),shown=c.proposalWindow.selectedEvidenceIndices;
   if(new Set(indices).size!==indices.length||new Set(row.selectedIndices).size!==row.selectedIndices.length||
    new Set(shown).size!==shown.length||indices.some((n:number)=>n>=row.source.evidenceCount)||
    shown.some((n:number,i:number)=>n>=row.source.evidenceCount||(i>0&&n<=shown[i-1]))||
    row.selectedIndices.some((n:number)=>!indices.includes(n)||!shown.includes(n))||
    indices.some((n:number)=>shown.includes(n)!==row.selectedIndices.includes(n)))throw Error('MANAGER_CASE_PROPOSAL_WINDOW_CHANGED');
  }
 }
 return true;
}

export function projectManagerCaseProposals(snapshot:any,cursor=0,limit=3,selectedCaseId?:string):{snapshot:any;discoveryDirectoryValidated:boolean}{
 if(existingDirectory(snapshot))return {snapshot,discoveryDirectoryValidated:true};
 // Unknown caller fields remain part of the mandatory envelope. Never replace
 // them with a directory that merely looks authoritative.
 if(snapshot.operation?.caseEvidenceDirectory!==undefined||!Array.isArray(snapshot.cases))return {snapshot,discoveryDirectoryValidated:false};
 const requested=new Map<string,Set<number>>();
 const visit=(v:any)=>{
  if(!v||typeof v!=='object')return;
  if(uuid.test(v.caseId??'')&&Number.isSafeInteger(v.evidenceIndex)&&v.evidenceIndex>=0&&
   (v.kind==='case_evidence'||typeof v.evidenceDigest==='string')){
   const set=requested.get(v.caseId)??new Set<number>();set.add(v.evidenceIndex);requested.set(v.caseId,set);
  }
  for(const child of Object.values(v))visit(child);
 };
 visit(snapshot.operation?.recoveredContext);
 const directoryCases:any[]=[],caseRows:any[]=[];
 const freshCases=new Set<any>();
 let materialWindow=false;
 const count=Number.isSafeInteger(limit)&&limit>=1&&limit<=3?limit:3;
 for(const c of snapshot.cases){
  const clean=validSource(c.evidenceSource,c,true,false)?{...c,evidenceSource:publicSource(c.evidenceSource)}:c;
  if((selectedCaseId!==undefined&&c.id!==selectedCaseId)||!validSource(c.evidenceSource,c,true)||!Array.isArray(c.evidence)||c.evidence.length!==c.evidenceSource.evidenceCount||
   !Number.isSafeInteger(c.currentRun?.attempt)||c.currentRun.attempt<=0||c.proposalWindow!==undefined){caseRows.push(clean);continue;}
  const entries=c.evidence.map((e:any,index:number)=>({e,index})).filter(({e,index}:any)=>
   knownCurrentProposal(e,c,index)||knownConsumption(e,c,index));
  const groups=['SCOPED_DELIVERY_ACTION','DELIVERY_CONSUMPTION_REQUIRED'].map(kind=>entries.filter(({e}:any)=>e.kind===kind));
  if(groups.every(group=>group.length<=count)){caseRows.push(clean);continue;}
  const material=groups[1].length>0;materialWindow ||= material;
  const offset=Number.isSafeInteger(cursor)?((cursor%entries.length)+entries.length)%entries.length:0;
  const selected=new Set<number>(entries.filter(({index}:any)=>requested.get(c.id)?.has(index)).map(({index}:any)=>index));
  for(const group of groups)for(let n=0;n<Math.min(count,group.length);n++)selected.add(group[(offset+n)%group.length].index);
  const indices=new Set(entries.map(({index}:any)=>index));
  const evidenceIndices=c.evidence.map((_:any,i:number)=>i).filter((i:number)=>!indices.has(i)||selected.has(i));
  const selectedIndices=[...selected].sort((a,b)=>a-b);
  const row={caseId:c.id,source:clean.evidenceSource,consumerAttempt:c.currentRun.attempt,
   total:entries.length,selectedIndices,cursor:offset,nextCursor:(offset+count)%entries.length,
   rows:entries.map(({e,index}:any)=>e.kind==='DELIVERY_CONSUMPTION_REQUIRED'?
    {index,kind:e.kind,code:e.code,obligationId:e.obligationKey.slice('delivery-consumption:'.length),generation:e.generation,
     sourceTaskId:e.producerTaskId,artifactCount:e.repairArtifacts.length}:
    {index,kind:e.action.kind,sourceTaskId:e.action.sourceTaskId,artifactCount:e.action.artifactIds.length,requestId:e.requestId}),
   recovery:{kind:'case_evidence',caseId:c.id,digest:c.evidenceSource.digest,offset:0}};
  directoryCases.push(row);
  const evidence=evidenceIndices.map((i:number)=>c.evidence[i]);
  const projectedCase={...clean,evidence,proposalWindow:{version:material?2:1,sourceEvidenceCount:c.evidence.length,
   selectedEvidenceIndices:evidenceIndices,...(material?{selectedWindowIndices:selectedIndices,omittedWindowEntries:entries.length-selected.size}:
    {selectedProposalIndices:selectedIndices,omittedProposals:entries.length-selected.size}),
   projectedEvidenceDigest:digest(evidence),directoryDigest:''}};
  caseRows.push(projectedCase);freshCases.add(projectedCase);
 }
 if(!directoryCases.length)return {snapshot:{...snapshot,cases:caseRows},discoveryDirectoryValidated:false};
 // One production case is windowed. For callers selecting several cases, the
 // directory has one explicit version and every case window follows it.
 if(materialWindow)for(const c of caseRows)if(freshCases.has(c)&&c.proposalWindow?.version===1){
  const {selectedProposalIndices,omittedProposals,...window}=c.proposalWindow;
  c.proposalWindow={...window,version:2,selectedWindowIndices:selectedProposalIndices,omittedWindowEntries:omittedProposals};
 }
 const body={version:materialWindow?2:1,kind:materialWindow?'CURRENT_CASE_EVIDENCE_V2':'CURRENT_SCOPED_PROPOSALS_V1',
  guide:materialWindow?materialGuide:guide,cases:directoryCases};
 const directory={...body,digest:digest(body)};
 const cases=caseRows.map(c=>freshCases.has(c)?{...c,proposalWindow:{...c.proposalWindow,directoryDigest:directory.digest}}:c);
 return {snapshot:{...snapshot,cases,operation:{...snapshot.operation,caseEvidenceDirectory:directory}},discoveryDirectoryValidated:true};
}
