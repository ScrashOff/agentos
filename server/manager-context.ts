import { createHash } from 'node:crypto';
import {compactSharedManagerContext,expandSharedManagerContext} from './manager-context-sharing.js';
import {projectManagerCaseProposals} from './manager-case-proposals.js';
import {projectManagerValidationHistory} from './manager-validation-history.js';
import {prepareManagerContextSections,managerContextSectionGoal,type ManagerContextSectionBlob} from './manager-context-sections.js';

export const managerContextVersion = 'MANAGER_CONTEXT_V21';
export const managerContextGuide = 'operation.tasks contém tarefas não encerradas, com dependências, rotas, erro e resumo. operation.sourceCatalog entrega uma JANELA de fontes encerradas em rows, conforme columns; baseIndex aponta para bases. selection informa total, cursor e omissões: fonte omitida não significa inexistente. Fontes da revisão atual têm preferência; as demais rotacionam. O catálogo completo e seus detalhes permanecem nas tasks/runs originais e em GET /api/goals/:goalId. Identidade, título, estado, etapa, tentativa e workspace dos itens selecionados são preservados. evidenceTexts compartilha cada texto longo exato uma vez; {$text:HASH} aponta para seu hash e originals indica caso/caminho recuperável. Leia esses textos antes de decidir; a referência não omite suas instruções. caseWindow informa quantos casos abertos entraram nesta revisão; os demais permanecem persistidos para a próxima revisão. Não trate uma entrega diagnóstica aceita como aceite do jogo.';
export const managerTextHash = (text: string) =>
  createHash('sha256').update(text).digest('hex');
const closed = new Set(['accepted', 'cancelled', 'superseded']);

/** Lossless JSON records: share only field names, never task values or evidence. */
export function packManagerRecords(records:any[]){
  const schemas:string[][]=[],rows:any[][]=[];
  for(const record of records){
    const fields=Object.keys(record).filter(key=>record[key]!==undefined);
    let schema=schemas.findIndex(keys=>JSON.stringify(keys)===JSON.stringify(fields));
    if(schema<0)schema=schemas.push(fields)-1;
    rows.push([schema,...fields.map(key=>record[key])]);
  }
  return {schemas,rows,lossless:true};
}

/** Repeated admission observations carry the same failed contract. Keep the
 * newest of each identity; original events and substantive evidence stay intact. */
function admissionEvidenceWindow(entry: any) {
  if (entry.evidenceSelection?.projected || [1,2].includes(entry.proposalWindow?.version)) return entry;
  const key = (e: any) => e?.kind === 'RUN_ADMISSION_BLOCKED' && typeof e.admissionEventId === 'string' &&
    typeof e.code === 'string' && typeof e.runId === 'string' && typeof e.at === 'string' &&
    Number.isFinite(Date.parse(e.at))
    ? JSON.stringify([e.instructionRevisionId ?? null, e.runId, e.attempt, e.code]) : null;
  const newest = new Map<string, number>();
  entry.evidence.forEach((e: any, index: number) => {
    const identity = key(e); if (!identity) return;
    const previous = newest.get(identity);
    if (previous === undefined || Date.parse(e.at) >= Date.parse(entry.evidence[previous].at)) newest.set(identity, index);
  });
  const indices = entry.evidence.map((e: any, index: number) => ({ identity: key(e), index }))
    .filter(({ identity, index }: any) => !identity || newest.get(identity) === index).map(({ index }: any) => index);
  if (indices.length === entry.evidence.length) return entry;
  return { ...entry, evidence: indices.map((index: number) => entry.evidence[index]), evidenceSelection: {
    projected: true, total: entry.evidence.length, included: indices.length,
    omittedRepeatedAdmissions: entry.evidence.length - indices.length, selectedIndices: indices,
    recovery: { entity: 'supervision_cases', id: entry.id, field: 'evidence', fullHistoryPreserved: true },
  } };
}

/** Keep every source identity, without repeating historical summaries or object keys. */
export function compactManagementSources(tasks: any[]) {
  const bases: string[] = [];
  const baseIndex = (base: string | undefined) => {
    if (base === undefined) return null;
    let index = bases.indexOf(base);
    if (index < 0) index = bases.push(base) - 1;
    return index;
  };
  return {
    tasks: tasks.filter((task) => !closed.has(task.state)),
    sourceCatalog: {
      columns: ['id', 'title', 'state', 'stage', 'workKind', 'expectedAttempt',
        'baseIndex', 'workspace', 'readOnlySource', 'live'],
      bases,
      rows: tasks.filter((task) => closed.has(task.state)).map((task) => [
        task.id, task.title, task.state, task.stage, task.workKind ?? null,
        task.expectedAttempt, baseIndex(task.baseWorkspace), task.workspace ?? null,
        task.readOnlySource ?? null, task.live,
      ]),
      recovery: { entity: 'tasks', idColumn: 'id', fields: ['description', 'execution', 'criteria'],
        runLookup: { entity: 'runs', taskColumn: 'id', attemptColumn: 'expectedAttempt' } },
    },
  };
}

/** Long operator evidence stays exact, shared once, with a verifiable original location. */
export function projectManagerContext(snapshot: any, windows:{sourceCatalogLimit?:number;idleTaskLimit?:number}={}) {
  snapshot=expandSharedManagerContext(snapshot);
  const texts = new Map<string, { hash: string; characters: number; text: string; originals: any[] }>();
  for (const entry of snapshot.evidenceTexts ?? [])
    if (typeof entry.text === 'string' && entry.hash === managerTextHash(entry.text))
      texts.set(entry.hash, { ...entry, originals: [...(entry.originals ?? [])] });
  const project = (value: any, original: any): any => {
    if (typeof value === 'string' && value.length > 600) {
      const digest = managerTextHash(value);
      let entry = texts.get(digest);
      if (!entry) {
        entry = { hash: digest, characters: value.length, text: value, originals: [] };
        texts.set(digest, entry);
      }
      entry.originals.push(original.path.at(-1) === 'summary'
        ? { ...original, representation: 'prefix', characters: value.length }
        : original);
      return { $text: digest };
    }
    if (Array.isArray(value))
      return value.map((item, index) => project(item, { ...original, path: [...original.path, index] }));
    if (value && typeof value === 'object')
      return Object.fromEntries(Object.entries(value).map(([key, item]) => [
        key, project(item, { ...original, path: [...original.path, key] }),
      ]));
    return value;
  };
  const catalog=snapshot.operation?.sourceCatalog;
  let operation=snapshot.operation;
  const otherTasks=operation?.tasks?.filter((task:any)=>task.dependencySummary&&task.selection?.detailOmitted);
  if(otherTasks?.length){
    const columns=['id','title','state','stage','baseWorkspace','expectedAttempt','live','workKind','dependencyCount','dependencySha256'];
    const focused=operation.tasks.filter((task:any)=>!otherTasks.includes(task));
    const active=(task:any)=>task.live===true||['queued','running','cancelling','reconciling'].includes(task.state);
    const actors=otherTasks.filter(active),idle=otherTasks.filter((task:any)=>!active(task));
    const cursor=Math.max(0,Math.trunc(Number(operation.workOpportunities?.selection?.cursor)||0));
    const offset=idle.length?cursor%idle.length:0;
    const idleLimit=windows.idleTaskLimit??12;
    const selected=[...actors,...idle.slice(offset),...idle.slice(0,offset)].slice(0,actors.length+idleLimit);
    operation={...operation,
      tasks:focused.map((task:any,index:number)=>({id:task.id,$rowOf:'operation.focusedTaskDirectory.rows',index})),
      focusedTaskDirectory:{...packManagerRecords(focused),
        guide:'Cada row começa com o índice de schemas; os demais valores seguem os campos desse schema. Reconstrua o objeto completo do foco. Instruções, critérios, rotas e dependências estão integrais.'},
      otherTaskDirectory:{columns,rows:selected.map((task:any)=>[
        task.id,task.title,task.state,task.stage,task.baseWorkspace,task.expectedAttempt,
        task.live,task.workKind,task.dependencySummary.count,task.dependencySummary.sha256]),
        selection:{detailsOmitted:true,omittedFields:['dependencies'],fullSourcesPreserved:true,
          totalRows:otherTasks.length,includedRows:selected.length,omittedRows:otherTasks.length-selected.length,
          cursor:offset,nextCursor:idle.length?(offset+idleLimit)%idle.length:0,allLiveActorsIncluded:true,
          sourceSha256:managerTextHash(JSON.stringify(otherTasks))},
        recovery:{tool:'management.contextRequests',kind:'task_metadata',taskIdColumn:'id',expectedAttemptColumn:'expectedAttempt',
          graphApi:'GET /api/goals/:goalId',graphTable:'dependencies',taskColumn:'task_id'}}};
  }
  if(Array.isArray(catalog?.rows) && !catalog.selection?.windowed){
    const rows=catalog.rows as any[][],originalLimit=snapshot.operation.recoveredContext?.results?.length ? 12 : 24;
    const cursor=Math.max(0,Number(snapshot.operation.workOpportunities?.selection?.cursor)||0);
    const preferred=new Set([
      ...(snapshot.operation.workOpportunities?.acceptedSources??[]).map((source:any)=>source.taskId),
      ...(snapshot.alreadyPlanned??[]).flatMap((plan:any)=>(plan.actions??[]).map((action:any)=>action.sourceTaskId)),
    ].filter(Boolean));
    const selected=rows.filter(row=>preferred.has(row[0])).slice(0,originalLimit);
    const limit=Math.max(selected.length,Math.min(originalLimit,windows.sourceCatalogLimit??originalLimit));
    const included=new Set(selected.map(row=>row[0]));
    for(let i=0;i<rows.length && selected.length<limit;i++){
      const row=rows[(cursor+i)%rows.length];
      if(!included.has(row[0])){selected.push(row);included.add(row[0])}
    }
    operation={...operation,sourceCatalog:{...catalog,rows:selected,
      selection:{windowed:true,totalRows:rows.length,includedRows:selected.length,
        omittedRows:rows.length-selected.length,cursor,nextCursor:rows.length?(cursor+limit)%rows.length:0,
        truncated:selected.length<rows.length},
      recovery:{...catalog.recovery,api:'GET /api/goals/:goalId',completeDirectoryPreserved:true}}};
  }
  const deferred=operation?.deferredWorkOpportunities,current=operation?.workOpportunities;
  if(deferred && current){
    const shared={...deferred};
    for(const key of ['acceptedSources','existingCoverage','liveActors']){
      if(!Array.isArray(deferred[key]) || !Array.isArray(current[key]))continue;
      const source=current[key],sourceJson=JSON.stringify(source);
      if(JSON.stringify(deferred[key])===sourceJson){
        shared[key]={$ref:'operation.workOpportunities.'+key,sha256:managerTextHash(sourceJson),items:source.length};
        continue;
      }
      const indices=new Map(source.map((row:any,index:number)=>[JSON.stringify(row),index]));
      const rows=deferred[key].map((row:any)=>indices.get(JSON.stringify(row))??row);
      const reference={$rowsOf:'operation.workOpportunities.'+key,sha256:managerTextHash(sourceJson),rows};
      if(JSON.stringify(reference).length<JSON.stringify(deferred[key]).length)shared[key]=reference;
    }
    operation={...operation,deferredWorkOpportunities:shared,
      sharedCatalogGuide:'$ref aponta para o array exato já entregue nesta revisão. $rowsOf usa rows na ordem original: inteiro significa índice no array referenciado; objeto é a linha exata adicional. sha256 confere o array fonte em JSON. Nada é omitido e as permissões permanecem iguais.'};
  }
  if(operation?.otherTaskDirectory)operation={...operation,
    contextWindowGuide:'operation.tasks aponta para focusedTaskDirectory: reconstrução exata do grafo decisório, por schemas e rows. otherTaskDirectory inclui TODOS os outros atores vivos e uma janela rotativa dos parados, inclusive auxiliares do mesmo escopo, com identidades, estados e tentativas; selection declara total/cursor/omissões. recovery usa management.contextRequests kind:task_metadata para critérios, dependências e identidade atuais; a instrução completa tem referência própria recuperável por kind:task. Consulte a parte necessária à decisão, sem tratar metadados ou excertos como substitutos de requisitos não estruturados. coverageIndex.selection declara sua janela; recovery.request busca qualquer página exata. Omissão não significa ausência. Critérios permanecem integrais. Evidências selecionadas permanecem integrais; demais entradas de diretórios recuperáveis não estão resolvidas e devem ser lidas antes de decidir sobre elas.'};
  if(operation?.otherTaskDirectory){
    for(const catalogKey of ['workOpportunities','deferredWorkOpportunities']){
      const source=operation[catalogKey];if(!source)continue;
      const packed={...source};
      for(const key of ['consumers','acceptedSources','existingCoverage','liveActors']){
        const values=source[key];
        if(!Array.isArray(values)||!values.length||!values.every(value=>value&&typeof value==='object'&&!Array.isArray(value)))continue;
        const directory=packManagerRecords(values);
        if(JSON.stringify(directory).length<JSON.stringify(values).length)packed[key]=directory;
      }
      operation={...operation,[catalogKey]:packed};
    }
    operation={...operation,opportunityDirectoryGuide:'Arrays de registros podem usar {schemas,rows,lossless:true}: cada row começa com o índice do schema; os valores seguintes reconstroem todos os campos do registro na ordem original. Reconstrua esses arrays antes de resolver $ref e $rowsOf; sha256 confere o array original reconstruído. Nenhuma fonte, hash, tentativa, cobertura ou ordem é omitida por essa representação.'};
  }
  return {
    ...snapshot,
    ...(operation?{operation}:{}),
    cases: snapshot.cases.map((raw: any) => {
      const entry = admissionEvidenceWindow(raw);
      return { ...entry, evidence: entry.evidence.map((item: any, index: number) => project(item,
        { entity: 'supervision_cases', id: entry.id, path: ['evidence', entry.proposalWindow?.selectedEvidenceIndices[index] ?? entry.evidenceSelection?.selectedIndices[index] ?? index] })) };
    }),
    evidenceTexts: [...texts.values()],
    inlineMemoryHashes: [...texts.keys()],
  };
}

/** Select complete cases before freezing a review, with room for its memory and
 * instruction manifest. Selected evidence units stay complete; other known units remain digest-bound and recoverable. */
export function projectManagerReview(snapshot: any, prefix: string, cursor = 0, maximum = 150000,
  options?:{goalId:string}):{description:string;caseWindow:any;withinBudget:boolean;sectionBlobs?:ManagerContextSectionBlob[]} {
  snapshot=expandSharedManagerContext(snapshot);
  const existingSectionGoal=managerContextSectionGoal(snapshot.operation);
  const sectionGoal=options?.goalId??existingSectionGoal;
  const sections=sectionGoal?prepareManagerContextSections(snapshot.operation,sectionGoal,snapshot.operation?.focus):undefined;
  let sectionPagingApplied=false;
  let usedSectionNames:string[]=[];
  if(existingSectionGoal){
    // A frozen projection already contains the selected case/page windows.
    // Preserve them and the immutable addresses; do not invent storage blobs.
    const plain=prefix+JSON.stringify(snapshot),shared=prefix+JSON.stringify(compactSharedManagerContext(snapshot));
    const description=JSON.stringify(shared).length<JSON.stringify(plain).length?shared:plain;
    return {description,caseWindow:snapshot.caseWindow,withinBudget:JSON.stringify(description).length<=maximum,sectionBlobs:[]};
  }
  if(JSON.stringify(prefix+JSON.stringify(snapshot)).length>maximum)
    snapshot=projectManagerValidationHistory(snapshot);
  const evidenceCursor = Math.max(0, Math.trunc(Number(snapshot.operation?.workOpportunities?.selection?.cursor) || cursor));
  const firstCaseOffset=snapshot.cases?.length?Math.max(0,Math.trunc(cursor))%snapshot.cases.length:0;
  // Only the mandatory first case receives a proposal directory. Other cases
  // may leave caseWindow under pressure, so their proposal origins must not
  // become orphaned directories in a persisted description.
  const proposals=projectManagerCaseProposals(snapshot,evidenceCursor,1,snapshot.cases?.[firstCaseOffset]?.id);
  snapshot=proposals.snapshot;
  const cases = snapshot.cases as any[];
  const offset = cases.length ? Math.max(0, Math.trunc(cursor)) % cases.length : 0;
  const ordered = [...cases.slice(offset), ...cases.slice(0, offset)];
  const evidence = snapshot.operation?.evidence as any[] | undefined;
  const evidenceOffset = evidence?.length ? evidenceCursor % evidence.length : 0;
  const evidenceOrder = evidence ? [...evidence.slice(evidenceOffset), ...evidence.slice(0, evidenceOffset)] : [];
  // Try each current read in full first. Rotate across all returned text pages,
  // including partial sources. Under pressure the selected page keeps a useful
  // prefix and an exact continuation, never a claim that its source was read.
  const currentReads=Array.isArray(snapshot.operation?.recoveredContext?.results)?snapshot.operation.recoveredContext.results:[];
  const textReads=currentReads.filter((entry:any)=>entry.status==='OK' && typeof entry.text==='string' && entry.text.length>0 &&
    typeof entry.digest==='string' && /^[a-f0-9]{64}$/.test(entry.digest) && Number.isSafeInteger(entry.totalCharacters) &&
    Number.isSafeInteger(entry.request?.offset??0) && (entry.request?.offset??0)>=0 &&
    (entry.request?.offset??0)+entry.text.length<=entry.totalCharacters);
  // Request order is stable when an already projected page is reprojected;
  // sorting by completeness would move a shortened page to another cursor.
  const readUnits=textReads;
  const protectedRead=readUnits.length?readUnits[evidenceCursor%readUnits.length]:undefined;
  const protectedReadIndex=protectedRead?currentReads.indexOf(protectedRead):-1;
  const minimumReadCharacters=protectedRead?Math.min(2048,protectedRead.text.length):0;
  const history=snapshot.operation?.retainedEvidence?.entries as any[]|undefined;
  const historyOffset=history?.length?evidenceCursor%history.length:0;
  const historyOrder=history?[...history.slice(historyOffset),...history.slice(0,historyOffset)]:[];
  let included = Math.min(6, cases.length), evidenceIncluded = evidenceOrder.length;
  let historyIncluded=historyOrder.length;
  const coverage=snapshot.operation?.coverageIndex;
  const coverageRows=Array.isArray(coverage?.rows)?coverage.rows as any[]:[];
  const coverageOffset=coverageRows.length?evidenceCursor%coverageRows.length:0;
  const coverageOrder=[...coverageRows.slice(coverageOffset),...coverageRows.slice(0,coverageOffset)];
  let coverageIncluded=coverageOrder.length;
  let recovered=snapshot.operation?.recoveredContext;
  const plans=Array.isArray(snapshot.alreadyPlanned)?snapshot.alreadyPlanned as any[]:[];
  let plansIncluded=plans.length;
  const focalIds=new Set([snapshot.operation?.focus?.taskId,
    ...ordered.slice(0,1).map(entry=>entry.taskId??entry.task_id)].filter(Boolean));
  const orderedPlans=[...plans.filter(plan=>focalIds.has(plan.task_id)),
    ...plans.filter(plan=>!focalIds.has(plan.task_id))];
  const mandatoryPlans=orderedPlans.filter(plan=>focalIds.has(plan.task_id)).length;
  const transitions=Array.isArray(snapshot.operation?.deliveryTransitions)?snapshot.operation.deliveryTransitions as any[]:[];
  const focalTransition=(entry:any,index:number)=>index===0 || (entry.scope===snapshot.operation?.focus?.scope &&
    ['tracking','executing','waiting_supervision','planning','replanning_required'].includes(entry.state));
  const requiredTransitions=transitions.filter(focalTransition);
  const orderedTransitions=[...requiredTransitions,...transitions.filter((entry,index)=>!focalTransition(entry,index))];
  let transitionsIncluded=transitions.length;
  // Long summaries of auxiliary dependencies are historical observations, not
  // the current case's evidence or the pending task's acceptance contract.
  // Budget those observations before removing a current transport recovery.
  let summaryLimit:number|undefined;
  const sourceTasks=snapshot.operation?.tasks as any[]|undefined;
  const shortenAuxiliarySummaries=()=>{
    if(summaryLimit===0)return false;
    if(!sourceTasks?.some(task=>!focalIds.has(task.id) && typeof task.summary==='string' &&
      task.summary.length>(summaryLimit??120) && !task.live &&
      ['diagnostic','repair','planning'].includes(task.workKind)))return false;
    summaryLimit=summaryLimit===undefined?120:0;return true;
  };
  const windowedTasks=()=>summaryLimit===undefined?sourceTasks:sourceTasks?.map(task=>{
    if(focalIds.has(task.id)||task.live||!['diagnostic','repair','planning'].includes(task.workKind)||
      typeof task.summary!=='string'||task.summary.length<=summaryLimit!)return task;
    return {...task,summary:task.summary.slice(0,summaryLimit),summarySelection:{
      truncated:true,originalCharacters:task.summary.length,prefixCharacters:summaryLimit,
      projectedSha256:managerTextHash(task.summary),fullSourcePreserved:true,
      recovery:{entity:'runs',taskId:task.id,expectedAttempt:task.expectedAttempt,field:'summary',
        tool:'management.contextRequests',request:{kind:'task_metadata',taskId:task.id,
          expectedAttempt:task.expectedAttempt,offset:0}}}};
  });
  const selectedReadPage=()=>{
    const entry=recovered?.results?.[protectedReadIndex];
    if(!protectedRead || !entry)return undefined;
    const offset=entry.request?.offset??0,end=offset+entry.text.length;
    return {request:entry.request,digest:entry.digest,offset,characters:entry.text.length,
      originalCharacters:protectedRead.text.length,minimumCharacters:minimumReadCharacters,
      nextOffset:end<entry.totalCharacters?end:null,sourceComplete:offset===0&&end===entry.totalCharacters,
      pageShortened:entry.text.length<protectedRead.text.length,
      cursor:evidenceCursor%readUnits.length,totalUnits:readUnits.length,nextCursor:(evidenceCursor+1)%readUnits.length};
  };
  const shrinkRecoveredPage=(selected=false)=>{
    let changed=false;
    const results=recovered?.results?.map((entry:any,index:number)=>{
      if(entry.status!=='OK')return entry;
      if((index===protectedReadIndex)!==selected)return entry;
      const minimum=selected?minimumReadCharacters:256;
      if(typeof entry.text==='string' && entry.text.length>minimum && Number.isFinite(entry.totalCharacters) && entry.digest){
        const text=entry.text.slice(0,Math.max(minimum,Math.floor(entry.text.length/2))),end=(entry.request?.offset??0)+text.length;
        changed=true;return {...entry,text,truncated:end<entry.totalCharacters,nextOffset:end<entry.totalCharacters?end:null};
      }
      if(Array.isArray(entry.rows) && entry.rows.length>1 && Number.isFinite(entry.totalRows)){
        const rows=entry.rows.slice(0,Math.max(1,Math.floor(entry.rows.length/2))),end=(entry.request?.offset??0)+rows.length;
        changed=true;return {...entry,rows,truncated:end<entry.totalRows,nextOffset:end<entry.totalRows?end:null};
      }
      return entry;
    });
    if(changed){
      recovered={...recovered,results};
      const page=selectedReadPage();
      recovered={...recovered,selection:{...recovered.selection,pageBudgetAdjusted:true,fullSourcesPreserved:true,
        ...(page?{selectedReadPage:page}:{}),
        recovery:{tool:'management.contextRequests',instruction:'Only the displayed intervals were delivered. Continue at nextOffset with the same source identity and returned digest. A shortened page is not a complete source; the selected page keeps at least 2048 characters unless its original returned text was shorter.'}}};
    }
    return changed;
  };
  // Existing by-ID and digest-bound catalog readers recover optional discovery.
  // Current decision references and unknown row shapes remain complete. Original
  // window cursors are not rewritten into offsets for this local projection.
  const catalogKeys=['planningDemands','deliveryConsumptions'] as const;
  const recoverableCatalogRow=(row:any)=>/^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(row?.id??'');
  // Unknown operation fields are conservatively part of the decision envelope.
  // Known discovery/progress directories remain delivered independently; their
  // mere membership is not a causal reference from the current decision.
  const discoveryFields=new Set(['planningDemands','planningDemandWindow','deliveryConsumptions','deliveryConsumptionWindow',
    'tasks','sourceCatalog','evidence','retainedEvidence','workOpportunities','deferredWorkOpportunities',
    'coverageIndex','frontier','progress','pool','resources','accounts','targets','previous','compositions','deferredDeliveries','scopeRevisions','deliveryTransitions']);
  if(proposals.discoveryDirectoryValidated)discoveryFields.add('caseEvidenceDirectory');
  const fullTaskReferences=sourceTasks?.filter(task=>!task.selection?.detailOmitted);
  const taskContractReferences=fullTaskReferences?.map(task=>{
    const {id,dependencies,...contract}=task;
    // These two fields describe graph membership. Criteria, intent, source
    // bindings and every other (including unknown) field remain references.
    return {...contract,...(focalIds.has(id)?{id}:{}),
      ...(!(Array.isArray(dependencies)&&dependencies.every((entry:any)=>recoverableCatalogRow({id:entry})))?{dependencies}:{})};
  });
  const mandatoryEnvelope=[snapshot.operation?.focus,snapshot.operation?.decisionFeedback,
    snapshot.operation?.recoveredContext,snapshot.operation?.backlog,requiredTransitions,
    snapshot.operation?.previous?.receipts,snapshot.operation?.previous?.followThrough,
    snapshot.operation?.deferredDeliveries,snapshot.operation?.scopeRevisions,
    plans.filter(plan=>focalIds.has(plan.task_id)),fullTaskReferences,
    Object.fromEntries(Object.entries(snapshot.operation??{}).filter(([key])=>!discoveryFields.has(key)))];
  const identityPattern=/\b(?:[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}|[a-f0-9]{64})\b/gi;
  const identities=(value:any)=>new Set((JSON.stringify(value)??'').match(identityPattern)?.map(id=>id.toLowerCase())??[]);
  // Future operation roots have no declared semantics. Retain the previous
  // conservative identity match for them, including inventory relationships.
  const knownControlFields=new Set(['focus','decisionFeedback','recoveredContext','backlog','requiredDecision']);
  const unknownOperationRoots=Object.fromEntries(Object.entries(snapshot.operation??{})
    .filter(([key])=>!discoveryFields.has(key)&&!knownControlFields.has(key)));
  const knownKeys=(value:any,keys:string[])=>value && typeof value==='object' && !Array.isArray(value) &&
    Object.keys(value).every(key=>keys.includes(key));
  const stringLeaf=(value:any)=>value==null||typeof value==='string';
  const integerLeaf=(value:any)=>value==null||Number.isSafeInteger(value)&&value>=0;
  const timeLeaf=(value:any)=>stringLeaf(value)||value instanceof Date&&Number.isFinite(value.getTime());
  const stringList=(value:any)=>value==null||Array.isArray(value)&&value.every((entry:any)=>typeof entry==='string');
  const leaves=(value:any,strings:string[],integers:string[]=[])=>strings.every(key=>stringLeaf(value[key]))&&integers.every(key=>integerLeaf(value[key]));
  const planningFields=['id','scope','lane','state','source_revision','task_id','code','updated_at','createdTaskIds',
    'remainingItems','remainingItemCount','remainingItemsTruncated','remainingItemsRequest','remainingItemRequest','originalPlanRequest'];
  const deliveryFields=['id','goal_id','handoff_id','producer_task_id','producer_run_id','consumer_task_id','scope',
    'consumer_contract_digest','producer_manifest_digest','state','receipt','candidate_run_id','candidate_digest',
    'qa_task_id','qa_run_id','created_at','updated_at','obligationKey','consumptionGeneration','sourceConflictGeneration','deliveryResolutionBinding'];
  const requestKnown=(value:any)=>value==null || knownKeys(value,['kind','requestId','itemIndex','taskId','expectedAttempt','name','offset']) &&
    leaves(value,['kind','requestId','taskId','name'],['itemIndex','expectedAttempt','offset']);
  const planningItemKnown=(item:any)=>knownKeys(item,['reference','status','code','coverageKey','lane','sourceTaskId','consumerTaskId','baseWorkspace','workKind','replacement']) &&
    leaves(item,['status','code','coverageKey','lane','sourceTaskId','consumerTaskId','baseWorkspace','workKind']) &&
    (item.reference==null||knownKeys(item.reference,['requestId','runId','artifactId','artifactDigest','index','proposalDigest']) &&
      leaves(item.reference,['requestId','runId','artifactId','artifactDigest','proposalDigest'],['index'])) &&
    (item.replacement==null||knownKeys(item.replacement,['taskId','reviewRunId','reviewArtifactId','reviewArtifactDigest','proposalDigest']) &&
      leaves(item.replacement,['taskId','reviewRunId','reviewArtifactId','reviewArtifactDigest','proposalDigest']));
  const knownCatalogRow=(key:string,row:any)=>recoverableCatalogRow(row) && typeof row.scope==='string' && typeof row.state==='string' &&
    (key==='planningDemands'?knownKeys(row,planningFields) &&
      leaves(row,['id','scope','lane','state','source_revision','task_id','code'],['remainingItemCount']) && timeLeaf(row.updated_at) &&
      stringList(row.createdTaskIds) && (row.remainingItemsTruncated==null||typeof row.remainingItemsTruncated==='boolean') &&
      (row.remainingItems==null||Array.isArray(row.remainingItems)&&row.remainingItems.every(planningItemKnown)) &&
      [row.remainingItemsRequest,row.remainingItemRequest,row.originalPlanRequest].every(requestKnown):
      knownKeys(row,deliveryFields) && leaves(row,deliveryFields.filter(key=>!['receipt','created_at','updated_at','deliveryResolutionBinding'].includes(key))) &&
      timeLeaf(row.created_at) && timeLeaf(row.updated_at) &&
      (row.receipt==null||knownKeys(row.receipt,['code','owner','generation','nextAction','applied','caseId','blockedTaskIds']) &&
        leaves(row.receipt,['code','owner','generation','nextAction','caseId']) &&
        (row.receipt.applied==null||typeof row.receipt.applied==='boolean') && stringList(row.receipt.blockedTaskIds)) &&
      (row.deliveryResolutionBinding==null||knownKeys(row.deliveryResolutionBinding,['obligationId','generation']) &&
        leaves(row.deliveryResolutionBinding,['obligationId','generation'])));
  const feedbackStrings=['eventId','eventKind','rejectedAt','caseId','taskId','sourceRunId','decisionRunId','code','cause','nextAction'];
  const rejectionStrings=['code','reviewRunId','rejectedAt','sourceTaskId','consumerTaskId','currentConsumerState','nextAction'];
  const rejectionNumbers=['currentConsumerAttempt','itemIndex','occurrencesInHistoryWindow'];
  const flag=(v:any)=>v==null||typeof v==='boolean';
  const feedbackBinding=(v:any)=>knownKeys(v,['obligationId','generation'])&&leaves(v,['obligationId','generation']);
  const knownFeedback=(v:any)=>v==null||knownKeys(v,['version','entries','total','truncated','backlogRejections','guide'])&&v.version===1&&
    integerLeaf(v.total)&&flag(v.truncated)&&stringLeaf(v.guide)&&Array.isArray(v.entries)&&v.entries.every((e:any)=>
      knownKeys(e,[...feedbackStrings,'sourceAttempt','decisionAttempt','rejectedBinding','currentResolutionBindings','currentResolutionBindingsTruncated'])&&
      leaves(e,feedbackStrings,['sourceAttempt','decisionAttempt'])&&flag(e.currentResolutionBindingsTruncated)&&
      (e.currentResolutionBindings==null||Array.isArray(e.currentResolutionBindings)&&e.currentResolutionBindings.every(feedbackBinding))&&
      (e.rejectedBinding==null||knownKeys(e.rejectedBinding,['obligationId','generation','expectedSourceRunId'])&&
        leaves(e.rejectedBinding,['obligationId','generation','expectedSourceRunId'])))&&
    (v.backlogRejections==null||knownKeys(v.backlogRejections,['entries','truncated','historyPlanLimit','guide'])&&
      flag(v.backlogRejections.truncated)&&integerLeaf(v.backlogRejections.historyPlanLimit)&&stringLeaf(v.backlogRejections.guide)&&
      Array.isArray(v.backlogRejections.entries)&&v.backlogRejections.entries.every((e:any)=>
        knownKeys(e,[...rejectionStrings,...rejectionNumbers])&&leaves(e,rejectionStrings,rejectionNumbers)));
  const catalogRowIsRequired=(key:string,row:any,mandatoryIdentities:Set<string>,conservativeIdentities:Set<string>)=>{
    if(!knownCatalogRow(key,row))return true;
    if(row.consumer_task_id===snapshot.operation?.focus?.taskId || row.task_id===snapshot.operation?.focus?.taskId)return true;
    // goal_id is ownership of the entire catalog, not evidence that all rows
    // are cited. Every other exact identity/hash, including nested receipts,
    // is checked conservatively against the complete mandatory envelope.
    const {goal_id:_goal,...bindings}=row;
    if(key==='planningDemands'){
      if([...identities(bindings)].some(id=>conservativeIdentities.has(id)))return true;
      // These three task relationships are inventory membership, not the
      // address of a planning request or proposal. A repair may occur in many
      // demands. Its presence in the focal contract must not pull every demand
      // body into the decision. Keep task_id and every proposal/reader address,
      // artifact/run/hash and replacement reference conservative and complete.
      const {createdTaskIds:_created,remainingItems,...address}=bindings;
      const references=Array.isArray(remainingItems)?remainingItems.map((item:any)=>{
        const {sourceTaskId:_source,consumerTaskId:_consumer,...reference}=item;return reference;
      }):remainingItems;
      return [...identities({...address,remainingItems:references})].some(id=>mandatoryIdentities.has(id));
    }
    return [...identities(bindings)].some(id=>mandatoryIdentities.has(id));
  };
  // Only delivered cases can add references to the current decision. Other
  // roots stay conservative, including explicit requests and unknown fields.
  // At most six distinct windows are classified within this immutable snapshot.
  const caseAuthorityCache=new Map<number,{sourceAuthority:string;directSourceAuthority:string;requiredCatalogRows:Map<string,Set<any>>}>();
  const caseAuthority=()=>{
    const cached=caseAuthorityCache.get(included);if(cached)return cached;
    const envelope=[ordered.slice(0,included),...mandatoryEnvelope];
    const mandatoryIdentities=identities(envelope);
    // Case/evidence schemas can evolve independently of the catalog. Keep
    // their entire references conservative, including nested unknown fields;
    // only inventory relationships from known operation roots are weakened.
    const conservativeIdentities=identities([unknownOperationRoots,ordered.slice(0,included),
      snapshot.operation?.backlog,snapshot.operation?.requiredDecision,
      snapshot.operation?.previous?.receipts,snapshot.operation?.previous?.followThrough,
      ...(!knownFeedback(snapshot.operation?.decisionFeedback)?[snapshot.operation?.decisionFeedback]:[])]);
    const requiredCatalogRows=new Map(catalogKeys.map(key=>[key,new Set<any>(
      (Array.isArray(snapshot.operation?.[key])?snapshot.operation[key]:[])
        .filter((row:any)=>catalogRowIsRequired(key,row,mandatoryIdentities,conservativeIdentities)))]));
    const authority={sourceAuthority:JSON.stringify(envelope),
      directSourceAuthority:JSON.stringify([ordered.slice(0,included),...mandatoryEnvelope.map(root=>
        root===fullTaskReferences?taskContractReferences:root)]),requiredCatalogRows};
    caseAuthorityCache.set(included,authority);return authority;
  };
  const requiredCatalogRow=(key:typeof catalogKeys[number],row:any)=>
    caseAuthority().requiredCatalogRows.get(key)?.has(row)===true;
  const catalogLimits=new Map<string,number>();
  const catalogDirectoriesOmitted=new Set<string>();
  for(const key of catalogKeys)if(Array.isArray(snapshot.operation?.[key]))
    catalogLimits.set(key,snapshot.operation[key].length);
  const catalogProjection=()=>Object.fromEntries(catalogKeys.flatMap(key=>{
    const rows=snapshot.operation?.[key] as any[]|undefined,limit=catalogLimits.get(key);
    if(!rows || limit===undefined || limit>=rows.length)return [];
    const prioritized=rows.map((row,index)=>({row,index})).sort((a,b)=>
      Number(b.row.consumer_task_id===snapshot.operation?.focus?.taskId || b.row.task_id===snapshot.operation?.focus?.taskId)-
      Number(a.row.consumer_task_id===snapshot.operation?.focus?.taskId || a.row.task_id===snapshot.operation?.focus?.taskId) ||
      Number(b.row.scope===snapshot.operation?.focus?.scope)-Number(a.row.scope===snapshot.operation?.focus?.scope) || a.index-b.index);
    const included=new Set(prioritized.filter(entry=>!requiredCatalogRow(key,entry.row)).slice(0,limit).map(entry=>entry.index));
    return [[key,rows.flatMap((row,index)=>requiredCatalogRow(key,row)||included.has(index)?[row]:catalogDirectoriesOmitted.has(key)?[]:
      [{id:row.id,scope:row.scope,state:row.state,detailsProjected:true,sourceRowSha256:managerTextHash(JSON.stringify(row))}])]];
  }));
  const catalogGuide=()=>{
    const entries=catalogKeys.filter(key=>(catalogLimits.get(key)??0)<(snapshot.operation?.[key]?.length??0)).map(key=>
      [key,{fullRows:snapshot.operation[key].filter((row:any)=>requiredCatalogRow(key,row)).length+
          Math.min(catalogLimits.get(key)??0,snapshot.operation[key].filter((row:any)=>!requiredCatalogRow(key,row)).length),
        sourceRowsSha256:managerTextHash(JSON.stringify(snapshot.operation[key])),
        ...(catalogDirectoriesOmitted.has(key)?{discoveryRowsOmitted:true,sourceWindowRows:snapshot.operation[key].length,
          omittedRows:snapshot.operation[key].filter((row:any)=>!requiredCatalogRow(key,row)).length,
          discoveryRequest:{kind:key==='planningDemands'?'planning_demands':'delivery_consumptions',offset:0}}:{}),
        fullSourcesPreserved:true,recovery:{tool:'management.contextRequests',
          kind:key==='planningDemands'?'planning_request':'delivery_consumption',
          idField:key==='planningDemands'?'requestId':'obligationId',idColumn:'id',offset:0}}]);
    return entries.length?{catalogDetailProjection:{catalogs:Object.fromEntries(entries),
      guide:'detailsProjected rows are identities, not full decision evidence. Retrieve with kind/idField, row.id and offset:0 before acting. If discoveryRowsOmitted=true, missing rows are still present in the complete catalog: use discoveryRequest, then its returned nextOffset/snapshotDigest; STALE_CATALOG requires restarting at zero. Original window totals/cursors describe the original read, not a complete delivered list; no local index is a global offset. Verify current identity/generation. sourceRowSha256/sourceRowsSha256 are only observed hashes, never retrieval snapshotDigest or action authority.'}}:{};
  };
  const shrinkCatalog=()=>{
    for(const key of catalogKeys){const limit=catalogLimits.get(key)??0;
      if(limit>0 && snapshot.operation[key].some((row:any)=>!requiredCatalogRow(key,row))){catalogLimits.set(key,Math.floor(limit/2));return true;}}
    for(const key of catalogKeys)if(!catalogDirectoriesOmitted.has(key) && Array.isArray(snapshot.operation?.[key]) &&
      snapshot.operation[key].some((row:any)=>!requiredCatalogRow(key,row))){catalogDirectoriesOmitted.add(key);return true;}
    return false;
  };
  const sourceInputKeys=['workOpportunities','deferredWorkOpportunities'] as const;
  const knownSourceInputs=(row:any)=>knownKeys(row,['taskId','runId','expectedAttempt','acceptancePassed','baseWorkspace','workspace',
    'workKind','covers','inputs','omittedInputs']) && row.acceptancePassed===true &&
    leaves(row,['taskId','runId','baseWorkspace','workspace','workKind'],['expectedAttempt','omittedInputs']) && stringList(row.covers) &&
    Array.isArray(row.inputs)&&row.inputs.every((input:any)=>knownKeys(input,['artifactId','name','digest','size'])&&
      recoverableCatalogRow({id:input.artifactId})&&typeof input.name==='string'&&typeof input.digest==='string'&&
      /^[a-f0-9]{64}$/.test(input.digest)&&Number.isSafeInteger(input.size)&&input.size>=0);
  const canProjectInputs=(row:any)=>recoverableCatalogRow({id:row?.taskId}) && recoverableCatalogRow({id:row?.runId}) &&
    Number.isSafeInteger(row.expectedAttempt) && row.expectedAttempt>=0 && Array.isArray(row.inputs) && row.inputs.length>0 &&
    knownSourceInputs(row) && ![row.taskId,row.runId,...row.inputs.flatMap((input:any)=>[input.artifactId,input.digest])]
      .some(identity=>typeof identity==='string' && identity.length>0 &&
        (row.workKind==='diagnostic'?caseAuthority().directSourceAuthority:caseAuthority().sourceAuthority).includes(identity));
  let sourceInputsProjected=false;
  const shrinkSourceInputs=()=>{
    if(sourceInputsProjected || !sourceInputKeys.some(key=>Array.isArray(snapshot.operation?.[key]?.acceptedSources) && snapshot.operation[key].acceptedSources.some(canProjectInputs)))return false;
    sourceInputsProjected=true;return true;
  };
  const projectSourceInputs=()=>sourceInputsProjected?{
    ...Object.fromEntries(sourceInputKeys.filter(key=>Array.isArray(snapshot.operation?.[key]?.acceptedSources)).map(key=>
      [key,{...snapshot.operation[key],acceptedSources:snapshot.operation[key].acceptedSources.map((row:any)=>
        !canProjectInputs(row)?row:{...row,inputs:{detailsProjected:true,inputCount:row.inputs.length,
          sourceSha256:managerTextHash(JSON.stringify(row.inputs)),fullSourcePreserved:true,
          recovery:{kind:'artifacts',taskId:row.taskId,expectedAttempt:row.expectedAttempt,offset:0}}})}])),
    sourceInputProjectionGuide:'Projected inputs are not an empty artifact list. Recover exact refs with management.contextRequests using the provided request and expectedAttempt before deciding. Verify run/attempt and returned artifact IDs/digests; stale or changed identity is not authority. All other source fields and cited inputs remain complete.',
  }:{};
  let sourceCatalogLimit=snapshot.operation?.recoveredContext?.results?.length?12:24,idleTaskLimit=12;
  const shrinkDirectories=()=>{
    if(Array.isArray(snapshot.operation?.sourceCatalog?.rows) && !snapshot.operation.sourceCatalog.selection?.windowed && sourceCatalogLimit>1){
      sourceCatalogLimit=Math.max(1,Math.floor(sourceCatalogLimit/2));return true;}
    if(snapshot.operation?.tasks?.some((task:any)=>task.dependencySummary && task.selection?.detailOmitted) && idleTaskLimit>0){
      idleTaskLimit=Math.floor(idleTaskLimit/2);return true;}
    return false;
  };
  const headroom=sections?.directory.sections.length?
    Math.min(10000,Math.max(0,Math.floor(maximum/10))):Math.min(7000,Math.max(0,Math.floor(maximum/20)));
  const reserveHeadroom=()=>{
    // Spend optional discovery before shortening any explicitly requested page.
    if(shrinkCatalog())return true;
    if(!protectedRead && !sourceInputsProjected &&
      !catalogKeys.some(key=>(catalogLimits.get(key)??0)<(snapshot.operation?.[key]?.length??0)))return false;
    if(shrinkSourceInputs())return true;
    if(shrinkDirectories())return true;
    // A soft reserve must not evict fresh evidence that already fits. The
    // hard-cap path below still rotates reports when the envelope cannot fit.
    if(shrinkRecoveredPage())return true;
    if(shrinkRecoveredPage(true))return true;
    return false;
  };
  let projected: any, description: string;
  let sharedSourceCharacters=-1,sharedSaving=0;
  do {
    const windowedEvidence = evidence && evidenceIncluded < evidence.length;
    const historyWindow=history && historyIncluded<history.length?{
      ...snapshot.operation.retainedEvidence,entries:historyOrder.slice(0,historyIncluded),
      selection:{windowed:true,total:history.length,included:historyIncluded,omitted:history.length-historyIncluded,
        cursor:historyOffset,nextCursor:history.length?(historyOffset+Math.max(1,historyIncluded))%history.length:0},
      omittedEntries:historyOrder.slice(historyIncluded).map(entry=>({request:entry.request,runId:entry.runId,
        artifactId:entry.artifactId,digest:entry.digest,totalCharacters:entry.totalCharacters,
        pages:entry.pages?.map((page:any)=>({offset:page.offset,characters:page.text.length}))})),
      recovery:{tool:'management.contextRequests',fullSourcesPreserved:true},
    }:null;
    const coverageWindow=coverageIncluded<coverageRows.length?{...coverage,
      rows:coverageOrder.slice(0,coverageIncluded),complete:false,
      nextOffset:coverageOffset+coverageIncluded<coverageRows.length?coverageOffset+coverageIncluded:coverage.nextOffset,
      selection:{windowed:true,totalKnownRows:coverageRows.length,includedRows:coverageIncluded,
        omittedRows:coverageRows.length-coverageIncluded,cursor:coverageOffset,
        nextCursor:(coverageOffset+Math.max(1,coverageIncluded))%coverageRows.length,
        sourceComplete:coverage.complete===true,sourceRowsSha256:managerTextHash(JSON.stringify(coverageRows)),
        fullSourcesPreserved:true},
      recovery:{tool:'management.contextRequests',request:{kind:'coverage',baseWorkspace:coverage.scope,
        query:'',offset:coverageOffset},fullIndexPreserved:true}}:null;
    projected = projectManagerContext({ ...snapshot,
      ...(plansIncluded<plans.length?{alreadyPlanned:orderedPlans.slice(0,plansIncluded),
        plannedWindow:{total:plans.length,included:plansIncluded,omitted:plans.length-plansIncluded,
          sourceSha256:managerTextHash(JSON.stringify(plans)),fullSourcesPreserved:true,
          recovery:{entity:'supervision_cases',goalId:snapshot.operation?.goalId,
            fields:['decision','support_task_ids'],instruction:'Cobertura atual permanece em operation.workOpportunities.existingCoverage; omissão de uma decisão antiga não autoriza duplicar trabalho.'}}}:{}),
      ...(snapshot.operation?{operation:{...snapshot.operation,
        ...catalogProjection(),...catalogGuide(),...projectSourceInputs(),
        ...(sourceTasks?{tasks:windowedTasks()}:{}),
        ...(transitionsIncluded<transitions.length?{deliveryTransitions:orderedTransitions.slice(0,transitionsIncluded),
          deliveryTransitionWindow:{total:transitions.length,included:transitionsIncluded,omitted:transitions.length-transitionsIncluded,
            sourceSha256:managerTextHash(JSON.stringify(transitions)),fullSourcesPreserved:true,
            recovery:{entity:'delivery_transitions',idColumn:'id',fields:['proposal','receipt'],
              instruction:'Decisões anteriores permanecem persistidas; a decisão mais recente e transições ativas do foco estão incluídas.'}}}:{}),
        ...(recovered?{recoveredContext:recovered}:{}),
        ...(historyWindow?{retainedEvidence:historyWindow}:{}),
        ...(coverageWindow?{coverageIndex:coverageWindow}:{}),
        ...(windowedEvidence?{evidence: evidenceOrder.slice(0, evidenceIncluded),
        evidenceWindow: { total: evidence.length, included: evidenceIncluded, remaining: evidence.length - evidenceIncluded,
          cursor: evidenceOffset, nextCursor: evidence.length ? (evidenceOffset + Math.max(1, evidenceIncluded)) % evidence.length : 0,
          omitted: evidenceOrder.slice(evidenceIncluded).map(entry => ({ taskId: entry.taskId, runId: entry.runId,
            expectedAttempt: entry.expectedAttempt, artifactId: entry.artifactId, name: entry.name, digest: entry.digest })),
          recovery: { fullArtifactsPreserved: true, tool: 'management.contextRequests' } }}:{}),
      }}:{}), cases: ordered.slice(0, included), caseWindow: {
      total: cases.length, included, remaining: cases.length - included, cursor: offset,
      nextCursor: cases.length ? (offset + Math.max(1, included)) % cases.length : 0,
      selectedCaseIds: ordered.slice(0, included).map(c => c.id),
      maximumSerializedDescription: maximum,
      recovery: { entity: 'supervision_cases', fullCasesPreserved: true },
    } },{sourceCatalogLimit,idleTaskLimit});
    // Freeze from the original operation before windows/packing, but replace
    // views only after the original snapshot has driven all selection logic.
    if(sections?.directory.sections.length){
      if(!sectionPagingApplied){
        const plain=prefix+JSON.stringify(projected);
        const shared=JSON.stringify(plain).length>maximum?prefix+JSON.stringify(compactSharedManagerContext(projected)):plain;
        const complete=JSON.stringify(shared).length<JSON.stringify(plain).length?shared:plain;
        if(JSON.stringify(complete).length<=maximum)return {description:complete,caseWindow:projected.caseWindow,withinBudget:true};
        usedSectionNames=sections.directory.sections.filter(row=>
          JSON.stringify(projected.operation[row.section]??null).length>JSON.stringify(sections.references[row.section]).length)
          .map(row=>row.section);
        sectionPagingApplied=true;
      }
      if(usedSectionNames.length)projected={...projected,operation:{...projected.operation,
        ...Object.fromEntries(usedSectionNames.map(name=>[name,sections.references[name as keyof typeof sections.references]])),
        contextSectionDirectory:{...sections.directory,sections:sections.directory.sections.filter(row=>usedSectionNames.includes(row.section))}}};
    }
    description = prefix + JSON.stringify(projected);
    if (JSON.stringify(description).length <= maximum){
      if(JSON.stringify(description).length>maximum-headroom && reserveHeadroom())continue;
      break;
    }
    // Exact sharing can make the whole decision fit, including cached pages.
    // Try it before removing history; otherwise valid reusable evidence is
    // always discarded even when its shared representation fits the budget.
    const characters=JSON.stringify(description).length;
    const share=()=>{
      const encoded=prefix+JSON.stringify(compactSharedManagerContext(projected));
      sharedSourceCharacters=characters;
      sharedSaving=Math.max(sharedSaving,characters-JSON.stringify(encoded).length);
      if(JSON.stringify(encoded).length<JSON.stringify(description).length)description=encoded;
      return JSON.stringify(description).length<=maximum;
    };
    if((sharedSourceCharacters<0 || characters<=maximum+sharedSaving+1024) && share()){
      if(JSON.stringify(description).length>maximum-headroom && reserveHeadroom())continue;
      break;
    }
    // A historical report must not exclude the last actionable pending case.
    // Rotate whole auxiliary reports before considering a review without cases.
    if (coverageIncluded > 1) coverageIncluded=Math.max(1,Math.floor(coverageIncluded/2));
    // Current causal evidence must take precedence over operational history.
    // Otherwise the manager repeatedly asks to read a report we just omitted.
    else if(plansIncluded>mandatoryPlans)plansIncluded=Math.max(mandatoryPlans,Math.floor(plansIncluded/2));
    else if(transitionsIncluded>requiredTransitions.length)transitionsIncluded=Math.max(requiredTransitions.length,Math.floor(transitionsIncluded/2));
    // Spend optional discovery before evicting already delivered evidence.
    else if(shrinkCatalog())continue;
    else if(shrinkSourceInputs())continue;
    else if(shrinkDirectories())continue;
    else if (historyIncluded > 0) historyIncluded--;
    else if(shortenAuxiliarySummaries())continue;

    else {
      if (included > 1) included--;
      else if (evidenceIncluded > 0) evidenceIncluded--;
      // Requested source bytes remain recoverable pages with exact identities.
      else if(shrinkRecoveredPage())continue;
      else if(shrinkRecoveredPage(true))continue;
      // The final irreducible envelope always gets an exact sharing attempt;
      // an earlier estimate cannot falsely label a compressible case blocked.
      else {if(sharedSourceCharacters!==characters)share();break;}
    }
  } while (true);
  return { description, caseWindow: projected.caseWindow,
    withinBudget: JSON.stringify(description).length <= maximum,
    ...(usedSectionNames.length?{sectionBlobs:sections!.blobs.filter(blob=>usedSectionNames.includes(blob.section))}:{}) };
}

/** Reproject an existing stopped manager description without discarding its contracts. */
export function projectPersistedManagerDescription(description: string, originalDescription?: any) {
  const start = description.lastIndexOf('\n{"cases":');
  if (start < 0 || !description.includes('MANAGEMENT_V1'))
    throw new Error('MANAGER_SNAPSHOT_MISSING');
  const snapshot = expandSharedManagerContext(JSON.parse(description.slice(start + 1)));
  if (!Array.isArray(snapshot.cases) || !Array.isArray(snapshot.operation?.tasks))
    throw new Error('MANAGER_SNAPSHOT_INVALID');
  const prefix = description.slice(0, start + 1).replace(
    /^MANAGER_CONTEXT_V\d+\r?\n[^\n]*\r?\n/m,
    managerContextVersion + '\n' + managerContextGuide + '\n',
  );
  if (!prefix.includes(managerContextVersion)) throw new Error('MANAGER_CONTRACT_MISSING');
  if (!snapshot.operation.sourceCatalog)
    snapshot.operation = { ...snapshot.operation, ...compactManagementSources(snapshot.operation.tasks) };
  if (originalDescription) snapshot.originalDescription = originalDescription;
  const review=projectManagerReview(snapshot,prefix);
  if(!review.withinBudget)throw Error('MANAGER_PROJECTION_STILL_TOO_LARGE');
  return review.description;
}

export function managerInlineMemoryHashes(description: string): string[] {
  if (!description.includes(managerContextVersion)) return [];
  const start = description.lastIndexOf('\n{"cases":');
  if (start < 0) return [];
  try {
    const snapshot = JSON.parse(description.slice(start + 1));
    // Derive from actual delivered text, never trust a claimed hash from evidence.
    return (snapshot.evidenceTexts ?? [])
      .filter((entry: any) => typeof entry.text === 'string')
      .map((entry: any) => managerTextHash(entry.text));
  } catch {
    return [];
  }
}
