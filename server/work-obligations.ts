import {createHash} from 'node:crypto';
import {event,type DB} from './db.js';

const sha=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const digest=/^[a-f0-9]{64}$/;
const live=['queued','running','cancelling','reconciling'];
function canonical(value:any):any {
  if(Array.isArray(value))return value.map(canonical);
  if(value && typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])]));
  return value;
}
export type BacklogObligationInput={
  goalId:string;taskId:string;consumer:any;source:any;workKind:'diagnostic'|'repair';description:string;
  context:{inputs:any[];provenance:any[];omitted?:any[]}|null;
};

/** Exact equivalence only. Neither a label nor a provider route proves a new
 * hypothesis. Different actual instructions or input bytes remain eligible.
 * A replacement consumer needs an explicit lineage before we can equate it. */
export function backlogObligationIdentity(input:Omit<BacklogObligationInput,'taskId'>) {
  const {consumer,source,context}=input;
  if(!source || !consumer || consumer.goal_id!==input.goalId || source.goal_id!==input.goalId ||
    !consumer.execution?.baseWorkspace || consumer.execution.baseWorkspace!==source.execution?.baseWorkspace ||
    !Array.isArray(consumer.criteria) || !consumer.criteria.length || !context ||
    !Array.isArray(context.inputs) || !Array.isArray(context.provenance) || !context.provenance.length ||
    context.omitted?.some(ref=>ref.role==='accepted_source'))return null;
  const refs=[] as Array<{role:string;name:string;digest:string}>;
  for(const ref of context.provenance) {
    const file=context.inputs.find(item=>item.id===ref.artifactId);
    if(!file || !digest.test(ref.digest??'') || file.digest!==ref.digest || file.name!==ref.name ||
      typeof ref.role!=='string' || !ref.role)return null;
    refs.push({role:ref.role,name:ref.name,digest:ref.digest});
  }
  if(!context.provenance.some(ref=>ref.role==='accepted_source' && ref.taskId===source.id))return null;
  // Criteria IDs and labels identify presentation; the entire actual condition
  // (type, field, expected value, proof version, etc.) identifies the obligation.
  const criteria=consumer.criteria.map(({id:_id,label:_label,...condition}:any)=>canonical(condition))
    .sort((a:any,b:any)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
  const obligationKey='work-obligation:'+sha(canonical({version:1,goalId:input.goalId,
    consumerTaskId:consumer.id,scope:consumer.execution.baseWorkspace,stage:consumer.stage,
    instruction:consumer.description,criteria,covers:[...(consumer.covers??[])].sort()}));
  // Preserve interior whitespace/case: it may be literal code or a test input.
  const hypothesisDigest=sha([input.workKind,input.description.replace(/\r\n/g,'\n').trim()]);
  const evidenceDigest=sha([...new Map(refs.map(ref=>[JSON.stringify(canonical(ref)),canonical(ref)])).values()]
    .sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b))));
  return {obligationKey,hypothesisDigest,evidenceDigest,
    reservationKey:'work-obligation-reservation:'+sha([obligationKey,hypothesisDigest,evidenceDigest])};
}

/** Read-only preview: avoid remote workspace preparation for an exact existing
 * obligation. A miss is not admission; the final claim still runs atomically
 * with task creation under the ordinary admission lock. */
export async function findExistingBacklogObligation(db:DB,input:Omit<BacklogObligationInput,'taskId'>) {
  const identity=backlogObligationIdentity(input);
  if(!identity)return null;
  const prior=(await db.query(`SELECT e.payload,t.state,t.goal_id FROM events e
    LEFT JOIN tasks t ON t.id=(e.payload->>'taskId')::uuid
    WHERE e.entity_id=$1 AND e.kind='BACKLOG_OBLIGATION_RESERVED' ORDER BY e.id DESC LIMIT 1`,
    [identity.reservationKey])).rows[0];
  if(prior) {
    if(prior.goal_id!==input.goalId)throw Error('OBLIGATION_BINDING_INVALID');
    return {code:'OBLIGATION_REUSE_REQUIRED' as const,taskId:prior.payload.taskId as string,...identity,
      state:prior.state as string,reason:live.includes(prior.state)?'Equivalent work already has a live owner.':
        prior.state==='accepted'?'Reuse the accepted evidence through its authorized consumer.':
          'Continue or revise the existing work; a new task identity is not new evidence.'};
  }
  return null;
}

/** Caller owns the ordinary backlog transaction/admission lock and all source,
 * actor, cancellation and authority guards. These receipts add no permission.
 * Reservation and task INSERT MUST commit together; no remote effect occurs. */
export async function claimBacklogObligation(db:DB,input:BacklogObligationInput) {
  const identity=backlogObligationIdentity(input);
  if(!identity)return {code:'OBLIGATION_UNBOUND' as const,taskId:input.taskId};
  const prior=await findExistingBacklogObligation(db,input);
  if(prior)return prior;
  const payload={version:1,goalId:input.goalId,taskId:input.taskId,consumerTaskId:input.consumer.id,
    sourceTaskId:input.source.id,sourceAttempt:input.source.attempt_count,workKind:input.workKind,...identity};
  await event(db,'BACKLOG_OBLIGATION_RESERVED',identity.reservationKey,payload);
  await event(db,'BACKLOG_OBLIGATION_BOUND',input.taskId,payload);
  return {code:'OBLIGATION_RESERVED' as const,taskId:input.taskId,...identity};
}

/** Indexed lookup by task. The controller-generated binding, not worker text,
 * provides the cross-task scope used by retry accounting. */
export async function readTaskObligation(db:DB,taskId:string,goalId:string) {
  const row=(await db.query(`SELECT payload FROM events WHERE entity_id=$1
    AND kind='BACKLOG_OBLIGATION_BOUND' ORDER BY id DESC LIMIT 1`,[taskId])).rows[0]?.payload;
  return row?.goalId===goalId && row.taskId===taskId && row.version===1 &&
    typeof row.reservationKey==='string' && row.reservationKey.startsWith('work-obligation-reservation:')?row:null;
}
