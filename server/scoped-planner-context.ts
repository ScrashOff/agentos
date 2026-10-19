import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import type { PlannerCatalogTransport } from '../scripts/planner-catalog-transport.mjs';
import { packManagerRecords } from './manager-context.js';
import { scopedPlanHandoffGuide } from './scoped-plan-contract.js';

export const scopedPlannerContextVersion = 'SCOPED_PLANNER_V5';
const closed = new Set(['accepted', 'cancelled', 'superseded']);
const hash = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');

// A previous no-work decision is evidence to inspect, never an authority to
// unblock its consumer. Keep its current reconciliation exact and queryable
// without copying every prerequisite into the next planner's inline context.
const previousNoWorkProjection = (snapshot: any) => snapshot.previousNoWork ? {
  previousNoWork: {
    requestId: snapshot.previousNoWork.requestId,
    authority: 'observation_only',
    record: { $catalog: { jsonPath: ['previousNoWork'],
      valueSha256: hash(snapshot.previousNoWork), bytes: Buffer.byteLength(JSON.stringify(snapshot.previousNoWork)) } },
    instruction: 'Read the exact blocker reconciliation from the frozen catalog. Accepted prerequisites prove completion only; inspect their evidence and the existing consumer. This observation does not prove consumption, criterion acceptance or QA readiness.',
  },
} : {};

/** Ship the exact reader with the frozen input bundle: queries never need a
 * service credential, a later manager turn, or access to mutable task state. */
export async function createScopedPlannerCatalogReaderInput(requestId: string, catalogDigest: string, inputRoot = '/home/agent/inputs', transport?: PlannerCatalogTransport) {
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(requestId) ||
    !/^[a-f0-9]{64}$/.test(catalogDigest) || !path.isAbsolute(inputRoot)) throw Error('CATALOG_READER_BINDING');
  const source = await readFile(new URL('../scripts/planner-catalog-query.mjs', import.meta.url), 'utf8');
  const marker = '/* AGENTOS_CATALOG_BINDING */ null';
  if (source.split(marker).length !== 2) throw Error('CATALOG_READER_BINDING');
  const loaderImport = "import { readPlannerCatalog } from './planner-catalog-transport.mjs';";
  if (source.split(loaderImport).length !== 2) throw Error('CATALOG_READER_BINDING');
  const loader = (await readFile(new URL('../scripts/planner-catalog-transport.mjs', import.meta.url), 'utf8'))
    .replace(/^export /gm, '');
  const bytes = Buffer.from(source.replace(loaderImport, loader).replace(marker, JSON.stringify({
    path: path.join(inputRoot, requestId, 'planning-catalog.json'), digest: catalogDigest, ...(transport ? { transport } : {}) })));
  const digest = createHash('sha256').update(bytes).digest('hex');
  // Execution-input identities are unique across names. Keep the query code's
  // identity separate from the request's catalog and stable across preparation.
  const identifier = createHash('sha256').update('scoped-planner-reader:' + requestId + ':' + digest).digest('hex').slice(0, 32);
  const variant = (8 | (Number.parseInt(identifier[16], 16) & 3)).toString(16);
  const id = `${identifier.slice(0, 8)}-${identifier.slice(8, 12)}-5${identifier.slice(13, 16)}-${variant}${identifier.slice(17, 20)}-${identifier.slice(20)}`;
  return { id, name: 'planning-query.mjs', digest, base64: bytes.toString('base64') };
}

function onDemandProjection(snapshot: any, fullCatalog: any, provenance: any[], maximumCharacters: number) {
  const reference = (jsonPath: Array<string | number>, value: unknown) => ({
    $catalog: { jsonPath, valueSha256: hash(value), bytes: Buffer.byteLength(JSON.stringify(value)) },
  });
  const directory = (name: string, values: any[]) => ({ ...reference([name], values),
    recordCount: values.length, representation: 'queryable_directory',
    instruction: 'All records remain in the frozen catalog. Query before claiming absence or authorizing a decision.' });
  // A compact identity row remains available for every actor, even when the
  // actor's instructions, criteria and dependency tree are exceptionally large.
  // Contracts are never shortened; both a record and each field are addressable.
  let tasks = snapshot.tasks.map((task: any, index: number) => ({
    id: task.id, title: task.title, state: task.state, stage: task.stage,
    workKind: task.execution?.workKind ?? null, attempt_count: task.attempt_count,
    last_run_id: task.last_run_id, last_run_state: task.last_run_state,
    last_run_attempt: task.last_run_attempt, has_live_run: task.has_live_run,
    acceptance_passed: task.acceptance_passed,
    contract: reference(['tasks', index], task),
    criteriaCount: Array.isArray(task.criteria) ? task.criteria.length : null,
    dependencyCount: Array.isArray(task.dependencies) ? task.dependencies.length : null,
  }));
  const projection: any = {
    contextProjectionVersion: scopedPlannerContextVersion,
    ...previousNoWorkProjection(snapshot),
    strategy: snapshot.strategy == null ? null : reference(['strategy'], snapshot.strategy),
    ...(snapshot.laneAvailability?{laneAvailability:snapshot.laneAvailability}:{}),
    encoding: 'schemas/rows encode exact identity records. Each $catalog is bound to fullCatalog.digest and its exact jsonPath/valueSha256. Contracts, instructions, criteria, dependencies and proofs are complete in the catalog; query the original before relying on it. Selected pages are not the whole catalog, and omission is not absence. Every live actor is listed below; never use its workspace as an authorized baseline.',
    tasks: packManagerRecords(tasks),
    opportunities: { ...reference(['opportunities'], snapshot.opportunities), representation: 'queryable_directory' },
    artifacts: directory('artifacts', snapshot.artifacts ?? []),
    coverage: directory('coverage', snapshot.coverage ?? []),
    repairConsumption: directory('repairConsumption', snapshot.repairConsumption ?? []),
    deferredDeliveries: directory('deferredDeliveries', snapshot.deferredDeliveries ?? []),
    handoffReservations: scopedPlanHandoffGuide(),
    // Provenance is queryable only when the caller actually mounted it.
    provenance: fullCatalog.provenanceIncluded === true ? directory('provenance', provenance) : packManagerRecords(provenance),
    fullCatalog,
    catalogQueries: {
      version: 1,
      argv: ['node', fullCatalog.readerPath, fullCatalog.path, fullCatalog.digest, '<query JSON>'],
      operations: {
        directory: { operation: 'directory', jsonPath: ['tasks'], offset: 0, limit: 12 },
        task: { operation: 'find', jsonPath: ['tasks'], where: { id: '<exact UUID>' }, offset: 0, limit: 1 },
        read: { operation: 'read', jsonPath: ['tasks', 0, 'criteria'], offset: 0, limit: 12 },
        artifact: { operation: 'find', jsonPath: ['artifacts'], where: { id: '<exact UUID>' }, offset: 0, limit: 12 },
        artifactContent: { operation: 'artifact', artifactId: '<exact mounted input UUID>', jsonPath: [], offset: 0, limit: 12 },
        mountedInputs: { operation: 'directory', jsonPath: ['inputManifest'], offset: 0, limit: 12 },
        omittedInputs: { operation: 'read', jsonPath: ['omitted'], offset: 0, limit: 12 },
        coverage: { operation: 'find', jsonPath: ['coverage'], where: { source_task_id: '<exact UUID>' }, offset: 0, limit: 12 },
        deferredDeliveries: scopedPlanHandoffGuide().query,
      },
      instruction: 'Run the reader with the shell tool during this execution. It verifies the complete catalog digest before every query. Pass valueSha256 from a reference to additionally verify that exact value. Cursor is response.selection.nextOffset, never response.nextOffset. Stop when it is null; otherwise require a safe integer strictly greater than the requested offset. A missing cursor is a contract error, not another page zero. read entries use {key,value}; directory/find use {key,identity,reference}. query_child entries require reading their reference; missing inline value or identity is not absence. Read references recursively when proving coverage. Artifact content is readable only for exact inputManifest IDs; bytes must match the mounted size/digest, and symlinks outside the fixed input path are rejected. A catalog listing does not mean bytes are mounted: inspect inputManifest/omitted and report unavailable evidence explicitly. Keep queries bounded; maxCharacters is at most 16000. A receipt describes a frozen snapshot, not current mutation authority; the controller rechecks source revision and acceptance.' },
    contextSelection: { mode: 'on_demand', fullCatalogPreserved: true, maximumCharacters,
      taskCount: tasks.length, liveActorCount: tasks.filter((task: any) => task.has_live_run).length,
      instruction: 'The identity directory is complete. Fetch only contracts and artifact proofs relevant to this decision; no task criteria, dependency, attempt or evidence hash was changed.' },
  };
  // Unbounded titles can otherwise recreate the very prompt overflow that the
  // contract directory prevents. Preserve a verifiable location instead.
  if (JSON.stringify(projection).length > maximumCharacters) {
    tasks = tasks.map((task: any, index: number) => ({ ...task,
      title: typeof task.title === 'string' && task.title.length > 80
        ? reference(['tasks', index, 'title'], task.title) : task.title }));
    projection.tasks = packManagerRecords(tasks);
  }
  if (JSON.stringify(projection).length > maximumCharacters) {
    // A long-lived scope can contain hundreds of accepted auxiliary records.
    // Their identities also have exact, paginated locations in the catalog;
    // only live/uncertain actors and delivery consumers must stay inline.
    const activeStates = new Set(['queued', 'running', 'cancelling', 'reconciling']);
    const consumers = new Set((snapshot.opportunities?.consumers ?? []).map((v: any) => v.taskId));
    const declaredActors = new Set((snapshot.opportunities?.liveActors ?? []).map((v: any) => v.taskId));
    const mandatory = (task: any) => task.has_live_run === true || activeStates.has(task.state) ||
      activeStates.has(task.last_run_state) || declaredActors.has(task.id) || consumers.has(task.id) ||
      (!closed.has(task.state) && ['integrate', 'verify', 'review'].includes(task.stage));
    const required = new Set(snapshot.tasks.filter(mandatory).map((task: any) => task.id));
    // The small first page is useful context; it is never described as complete.
    const optional = tasks.filter((task: any) => !required.has(task.id)).slice(0, 24);
    const selected = new Set([...required, ...optional.map((task: any) => task.id)]);
    const render = (ids: Set<unknown>) => {
      const inline = tasks.filter((task: any) => ids.has(task.id));
      projection.tasks = packManagerRecords(inline);
      projection.taskDirectory = { ...directory('tasks', snapshot.tasks),
        inlineCount: inline.length, omittedInlineCount: tasks.length - inline.length,
        inlineComplete: inline.length === tasks.length,
        query: { operation: 'directory', jsonPath: ['tasks'], offset: 0, limit: 12 } };
      projection.encoding = 'schemas/rows encode selected exact identity records. Every live or uncertain actor, selected delivery consumer and nonclosed delivery-stage task remains inline; tasks is NOT the complete task directory. taskDirectory points to every original task in the frozen catalog. Each $catalog is bound to fullCatalog.digest and its exact jsonPath/valueSha256. Query the original contract, criteria, dependencies and proofs before a decision; paginate the directory before claiming absence. Never use a live actor workspace as an authorized baseline.';
      Object.assign(projection.contextSelection, { identityDirectoryComplete: inline.length === tasks.length,
        inlineTaskCount: inline.length, omittedInlineTaskCount: tasks.length - inline.length,
        mandatoryInlineCount: required.size,
        instruction: 'Every live/uncertain actor, selected delivery consumer and nonclosed delivery-stage task remains inline. Other tasks may be omitted from this inline page, never from the full catalog. Query taskDirectory from offset zero and follow nextOffset to prove coverage or absence. Original contracts, criteria, dependencies, attempts and hashes remain unchanged.' });
    };
    render(selected);
    // Keep authority-bearing identities even if they alone exceed the budget.
    // The caller still refuses admission; no actors or contracts are truncated.
    if (JSON.stringify(projection).length + 32 > maximumCharacters) render(required);
  }
  projection.contextSelection.withinBudget = JSON.stringify(projection).length + 32 <= maximumCharacters;
  return projection;
}

/** The mounted catalog is authoritative. Projection only shares record keys and
 * replaces historical prose/proofs with explicit, verifiable catalog locations.
 * Delivery/dependency contracts and live actor identities remain inline. Large
 * unrelated auxiliary contracts stay recoverable in the mounted catalog. */
export function projectScopedPlannerContext(snapshot: any, fullCatalog: any, provenance: any[], maximumCharacters=100000) {
  if (typeof fullCatalog.readerPath === 'string' && /^[a-f0-9]{64}$/.test(fullCatalog.readerDigest ?? ''))
    return onDemandProjection(snapshot, fullCatalog, provenance, maximumCharacters);
  const reference = (path: Array<string | number>, value: unknown) => ({
    $catalog: { jsonPath: path,
      valueSha256: hash(value), bytes: Buffer.byteLength(JSON.stringify(value)) },
  });
  let tasks = snapshot.tasks.flatMap((task: any, index: number) => {
    if (closed.has(task.state)) return [];
    const auxiliary = ['diagnostic', 'repair'].includes(task.execution?.workKind);
    return [{ ...task, ...(typeof task.intent === 'string' && (auxiliary || task.intent.length > 800)
      ? { intent: { ...reference(['tasks', index, 'intent'], task.intent),
        ...(!auxiliary ? { preview: task.intent.slice(0, 800), previewOnly: true } : {}) } } : {}) }];
  });
  let opportunities = Object.fromEntries(Object.entries(snapshot.opportunities).map(([key, value]) => [
    key, Array.isArray(value) ? packManagerRecords(value) : value,
  ]));
  let repairConsumption=packManagerRecords(snapshot.repairConsumption),provenanceRecords:any=packManagerRecords(provenance),coverage:any=packManagerRecords(snapshot.coverage);
  const deferredDeliveries = snapshot.deferredDeliveries.map((delivery: any, index: number) => ({
    ...delivery,
    ...(delivery.receipt != null ? { receipt: {
      code: delivery.receipt.code ?? null, detail: reference(['deferredDeliveries', index, 'receipt'], delivery.receipt),
    } } : {}),
    ...(delivery.consumer_proof != null ? {
      consumer_proof: reference(['deferredDeliveries', index, 'consumer_proof'], delivery.consumer_proof),
    } : {}),
  }));
  const projected = () => ({
    strategy:snapshot.strategy??null,
    ...previousNoWorkProjection(snapshot),
    ...(snapshot.laneAvailability?{laneAvailability:snapshot.laneAvailability}:{}),
    contextProjectionVersion: scopedPlannerContextVersion,
    encoding: 'schemas/rows are lossless records: row[0] selects schemas; remaining values match its field names. Reconstruct records before comparing IDs or contracts. Every $catalog uses the shared fullCatalog.path and fullCatalog.digest; jsonPath locates the exact JSON value and valueSha256 binds it. Read the original instructions/proofs needed for a decision there. A preview is not the full instruction. Referenced or unselected content remains available and is not absent. Long prose and handoff proofs are references; current criteria, dependencies and all nonclosed task identities remain exact.',
    opportunities, coverage,
    repairConsumption,
    deferredDeliveries: packManagerRecords(deferredDeliveries),
    handoffReservations: scopedPlanHandoffGuide(),
    tasks: packManagerRecords(tasks), provenance: provenanceRecords, fullCatalog,
  });
  let result:any=projected();
  if(JSON.stringify(result).length<=maximumCharacters)return result;
  // Preserve the delivery contracts and their entire dependency closure. Other
  // auxiliary contracts remain in the mounted, immutable catalog, with exact
  // paths and hashes; their live identity is never removed to fit a prompt.
  const byId=new Map(snapshot.tasks.map((task:any)=>[task.id,task]));
  const pending=snapshot.tasks.filter((task:any)=>!closed.has(task.state) &&
    !['diagnostic','repair'].includes(task.execution?.workKind)).map((task:any)=>task.id);
  const required=new Set<string>();
  while(pending.length){const id=pending.pop();if(!id||required.has(id))continue;
    required.add(id);pending.push(...((byId.get(id) as any)?.dependencies??[]));}
  tasks=tasks.map((task:any)=>{
    if(required.has(task.id)||!['diagnostic','repair'].includes(task.execution?.workKind))return task;
    const index=snapshot.tasks.findIndex((entry:any)=>entry.id===task.id),copy={...task};
    for(const field of ['criteria','dependencies'])
      if(JSON.stringify(task[field]??null).length>500)copy[field]=reference(['tasks',index,field],task[field]);
    return copy;
  });
  // Historical source prose is recoverable from that same catalog. Preserve
  // source IDs, acceptance/attempt metadata, coverage and artifact provenance.
  opportunities=Object.fromEntries(Object.entries(snapshot.opportunities).map(([key,value])=>[
    key,Array.isArray(value)?packManagerRecords(value.map((entry:any,index:number)=>
      Object.fromEntries(Object.entries(entry).map(([field,content])=>[
        field,typeof content==='string'&&content.length>800&&['summary','reason','instruction'].includes(field)
          ?reference(['opportunities',key,index,field],content):content])))):value,
  ]));
  repairConsumption=packManagerRecords(snapshot.repairConsumption.map((entry:any,index:number)=>({...entry,
    ...(JSON.stringify(entry.consumers??[]).length>500?{consumers:reference(['repairConsumption',index,'consumers'],entry.consumers)}:{})})));
  // The caller mounts these exact provenance records alongside the catalog.
  // Identity and digests remain here; bulky proofs are readable at their bound
  // location instead of repeated for every artifact from the same producer.
  if(fullCatalog.provenanceIncluded===true)provenanceRecords=packManagerRecords(provenance.map((entry:any,index:number)=>
    Object.fromEntries(Object.entries(entry).map(([field,value])=>[field,
      JSON.stringify(value??null).length>300?reference(['provenance',index,field],value):value]))));
  result=projected();
  if(JSON.stringify(result).length>maximumCharacters && fullCatalog.provenanceIncluded===true){
    provenanceRecords={...reference(['provenance'],provenance),recordCount:provenance.length,
      representation:'mounted_provenance_directory',lookupFields:['artifactId','runId','taskId','name','digest','role'],
      instruction:'Read exact matching provenance records from fullCatalog before using source authority. This directory is preserved, not absent.'};
    result=projected();
  }
  if(JSON.stringify(result).length>maximumCharacters){
    coverage={...reference(['coverage'],snapshot.coverage),recordCount:snapshot.coverage.length,
      representation:'mounted_coverage_directory',lookupFields:['coverage_key','id','source_task_id'],
      instruction:'Check exact existing coverage in fullCatalog before proposing work; never infer absence from this reference.'};
    result=projected();
  }
  result.contextSelection={budgetAdjusted:true,maximumCharacters,fullCatalogPreserved:true,
    requiredTaskCount:required.size,requiredTaskIdsSha256:hash([...required].sort()),instruction:'Auxiliary contracts, historical prose, consumption detail and mounted provenance proofs may be exact $catalog references. Read those fields before relying on them. Delivery criteria and required dependency contracts remain inline. Omission is not absence.'};
  return result;
}
