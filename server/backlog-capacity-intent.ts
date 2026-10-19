import {createHash} from 'node:crypto';
import {pool,tx,lock,event,type DB} from './db.js';
import {expandSharedManagerContext} from './manager-context-sharing.js';
import {backlogProposalSchema} from './planning-remainder-contract.js';
import {deliveryDigest} from './delivery-supervision.js';
import {readDeliverySourceIdentity,sourceIdentityAccepted,sourceIdentityFrozen} from './delivery-source-resolution.js';
import {checkedExecutionMetadata,executionMetadataColumns} from './task-execution-metadata.js';

type Reader=(id:string)=>Promise<{bytes:Buffer;digest?:string}>;
const uuid=/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
const digest=/^[a-f0-9]{64}$/;
const sha=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
const waiting=['waiting_capacity','preparing'];
export type CapacityIntent={version:1;state:string;code:string;proposal:any;proposalDigest:string;
  reviewRunId:string;artifactId:string;artifactDigest:string;index:number;taskId?:string;
  binding:{mode?:'initial'|'episode';consumer:any;source:any;caseId?:string;generation?:string;obligationId?:string;caseRunId?:string}};
export type CapacityIntentRef={runId:string;index:number;proposalDigest:string};
/** Context projection only. Durable admission still reads the original proposal
 * and binding from the receipt, never this audit envelope. Historical artifacts
 * have a real HTTP audit route; this does not promise a manager retrieval tool. */
export function projectBacklogCapacityReceipts(receipts:any[]):any[]{
  return receipts.map(receipt=>{
    const intent=receipt?.capacityIntent;
    if(!intent||typeof intent!=='object'||!Object.hasOwn(intent,'proposal'))return receipt;
    const {proposal,...metadata}=intent;
    const text=JSON.stringify(proposal);
    return {...receipt,capacityIntent:{...metadata,proposal:{
      detailsProjected:true,characters:text.length,valueSha256:deliveryDigest(proposal),
      summary:{workKind:proposal?.workKind,lane:proposal?.lane,coverageKey:proposal?.coverageKey,
        title:typeof proposal?.title==='string'?proposal.title.slice(0,120):undefined,
        titleProjected:typeof proposal?.title==='string'&&proposal.title.length>120},
      auditSource:uuid.test(intent.artifactId??'')&&Number.isSafeInteger(intent.index)&&intent.index>=0?
        {artifactId:intent.artifactId,artifactDigest:intent.artifactDigest,reviewRunId:intent.reviewRunId,
          path:'/api/artifacts/'+intent.artifactId,jsonPointer:'/backlog/tasks/'+intent.index}:null,
      guide:'Proposal details remain in the immutable coordination artifact and durable receipt. This is an audit reference, not a contextRequests kind. The scheduler revalidates and consumes the original proposal; omitted details are not an empty instruction or relaxed acceptance.',
    }}};
  });
}
const fail=(code:string)=>({ok:false as const,code});
function unpack(value:any):any[]{
  if(Array.isArray(value))return value;
  if(value?.lossless!==true||!Array.isArray(value.schemas)||!Array.isArray(value.rows))return [];
  return value.rows.map((row:any)=>{
    const keys=value.schemas[row?.[0]];
    if(!Array.isArray(keys)||!Array.isArray(row)||keys.length!==row.length-1||keys.some(k=>typeof k!=='string'))throw Error('CAPACITY_CONTEXT_INVALID');
    return Object.fromEntries(keys.map((key,i)=>[key,row[i+1]]));
  });
}
async function authority(db:DB,goalId:string,runId:string,recorded=true){
  return (await db.query(`SELECT r.id,CASE WHEN octet_length(r.instruction_snapshot#>>'{task,description}')<=300000
      THEN r.instruction_snapshot#>>'{task,description}' END description
    FROM runs r JOIN tasks t ON t.id=r.task_id AND t.goal_id=$2 AND t.stage='coordinate'
    JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id AND task_id=t.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON a.passed
    WHERE r.id=$1 AND r.state='submitted' AND (NOT $3::boolean OR EXISTS(SELECT 1 FROM management_reviews review
      WHERE review.run_id=r.id AND review.goal_id=t.goal_id))`,[runId,goalId,recorded])).rows[0];
}
const consumerContractSql=`encode(sha256(convert_to(jsonb_build_object('description',t.description,'criteria',t.criteria,
  'revision',t.instruction_revision_id,'agent',t.agent_id,'resources',t.resources,'memories',t.required_memories,
  'accounts',t.allowed_accounts,'execution',t.execution-'continuation','stage',t.stage,'covers',t.covers)::text,'utf8')),'hex')`;
async function enabled(db:DB,goalId:string){
  return !!(await db.query(`SELECT 1 FROM goals g JOIN settings w ON w.id='workforce:'||g.id
    JOIN settings m ON m.id='management:'||g.id JOIN settings s ON s.id='supervision:'||g.id
    LEFT JOIN settings a ON a.id='autonomy:'||g.project WHERE g.id=$1 AND g.state='active'
    AND w.value->>'enabled'='true' AND m.value->>'enabled'='true' AND s.value->>'enabled'='true'
    AND COALESCE(a.value->>'enabled','true')='true'`,[goalId])).rowCount;
}

/** New proposals are captured at their FIRST canonical admission check. Unlike
 * historical recovery, this proof does not require an incidental supervision
 * case. It records the exact source/consumer that applyBacklog just validated;
 * it may never be substituted for a missing proof on an old refusal. */
export async function captureInitialBacklogCapacityIntent(db:DB,goalId:string,reviewRunId:string,index:number,raw:any,read:Reader):Promise<
  {ok:true;intent:CapacityIntent}|{ok:false;code:string}>{
  const parsed=backlogProposalSchema.safeParse(raw);
  if(!parsed.success||!Number.isSafeInteger(index)||index<0||index>=12)return fail('CAPACITY_PROPOSAL_INVALID');
  // Existing plan receipts, including a historical rejection, cannot acquire a
  // current initial binding. Their only recovery path is frozen historical proof.
  if((await db.query('SELECT 1 FROM workforce_backlog_plans WHERE run_id=$1',[reviewRunId])).rowCount)return fail('CAPACITY_INITIAL_ALREADY_RECORDED');
  if(!await authority(db,goalId,reviewRunId,false))return fail('CAPACITY_REVIEW_AUTHORITY');
  const refs=(await db.query("SELECT id,digest,size FROM artifacts WHERE run_id=$1 AND name='coordination.json' LIMIT 2",[reviewRunId])).rows;
  if(refs.length!==1||Number(refs[0].size)>1000000)return fail('CAPACITY_REVIEW_ARTIFACT');
  const ref=refs[0];
  try{
    const value=await read(ref.id);
    if(value.bytes.length!==Number(ref.size)||sha(value.bytes)!==ref.digest||(value.digest&&value.digest!==ref.digest))return fail('CAPACITY_REVIEW_CHANGED');
    const original=backlogProposalSchema.safeParse(JSON.parse(value.bytes.toString()).backlog?.tasks?.[index]);
    if(!original.success||deliveryDigest(original.data)!==deliveryDigest(parsed.data))return fail('CAPACITY_PROPOSAL_CHANGED');
  }catch{return fail('CAPACITY_REVIEW_ARTIFACT');}
  const p=parsed.data,c=(await db.query(`SELECT t.id,t.state,t.stage,t.attempt_count,t.instruction_revision_id,t.criteria,
    t.execution->>'workspace' workspace,t.execution->>'baseWorkspace' scope,t.execution->'readOnlySource' readonly,
    ${consumerContractSql} contract_digest FROM tasks t WHERE t.id=$1 AND t.goal_id=$2`,[p.deliverToTaskId,goalId])).rows[0];
  if(!c||!c.workspace||!c.scope)return fail('CAPACITY_CONSUMER_PROOF_REQUIRED');
  let source:any=null;
  if(p.sourceTaskId){
    const identity=await readDeliverySourceIdentity(db,p.sourceTaskId);
    if(!identity||identity.goal_id!==goalId||identity.scope!==c.scope||!sourceIdentityAccepted(identity)||!sourceIdentityFrozen(identity))return fail('CAPACITY_SOURCE_CHANGED');
    const manifest=(await db.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[identity.run_id])).rows;
    source={taskId:identity.id,runId:identity.run_id,workspace:identity.workspace,contractDigest:identity.contract_digest,manifestDigest:deliveryDigest(manifest)};
  }
  const intent:CapacityIntent={version:1,state:'waiting_capacity',code:'CAPACITY_WAIT',proposal:p,proposalDigest:deliveryDigest(p),
    reviewRunId,artifactId:ref.id,artifactDigest:ref.digest,index,binding:{mode:'initial',source,
      consumer:{id:c.id,expectedAttempt:c.attempt_count,stage:c.stage,workspace:c.workspace,scope:c.scope,
        instructionRevisionId:c.instruction_revision_id,criteriaDigest:deliveryDigest(c.criteria),
        readOnlySource:c.readonly??false,contractDigest:c.contract_digest}}};
  const checked=await checkBacklogCapacityIntent(db,goalId,intent);
  return checked.ok?{ok:true,intent}:fail(checked.code);
}

/** Capture only a previously accepted semantic proposal. A frozen source
 * identity is not a manifest: both digests must have been visible in the review.
 * Missing historical proof is supervision work, never current recertification. */
export async function captureBacklogCapacityIntent(db:DB,goalId:string,reviewRunId:string,index:number,raw:any,read:Reader):Promise<
  {ok:true;intent:CapacityIntent}|{ok:false;code:string}>{
  const parsed=backlogProposalSchema.safeParse(raw);
  if(!parsed.success||!Number.isSafeInteger(index)||index<0||index>=12)return fail('CAPACITY_PROPOSAL_INVALID');
  const proposal=parsed.data,review=await authority(db,goalId,reviewRunId);
  if(!review)return fail('CAPACITY_REVIEW_AUTHORITY');
  const refs=(await db.query(`SELECT id,digest,size FROM artifacts WHERE run_id=$1 AND name='coordination.json' LIMIT 2`,[reviewRunId])).rows;
  if(refs.length!==1||refs[0].size>1000000)return fail('CAPACITY_REVIEW_ARTIFACT');
  const ref=refs[0];let snapshot:any,advice:any;
  try{
    const value=await read(ref.id);
    if(value.bytes.length!==Number(ref.size)||sha(value.bytes)!==ref.digest||(value.digest&&value.digest!==ref.digest))return fail('CAPACITY_REVIEW_CHANGED');
    advice=JSON.parse(value.bytes.toString());
    const at=typeof review.description==='string'?review.description.lastIndexOf('\n{"cases":'):-1;
    if(at<0)return fail('CAPACITY_CONTEXT_REQUIRED');
    snapshot=expandSharedManagerContext(JSON.parse(review.description.slice(at+1)));
  }catch{return fail('CAPACITY_CONTEXT_INVALID');}
  const original=backlogProposalSchema.safeParse(advice.backlog?.tasks?.[index]);
  if(!original.success||deliveryDigest(original.data)!==deliveryDigest(proposal))return fail('CAPACITY_PROPOSAL_CHANGED');
  let consumer:any;
  try{consumer=[...unpack(snapshot.operation?.focusedTaskDirectory),...unpack(snapshot.operation?.tasks),
    ...unpack(snapshot.operation?.otherTaskDirectory)].find((t:any)=>t.id===proposal.deliverToTaskId);}catch{return fail('CAPACITY_CONTEXT_INVALID');}
  if(!consumer||!Number.isSafeInteger(consumer.expectedAttempt)||!Array.isArray(consumer.criteria)||
    typeof consumer.workspace!=='string'||!('instructionRevisionId' in consumer))return fail('CAPACITY_CONSUMER_PROOF_REQUIRED');
  const matches=(snapshot.cases??[]).filter((c:any)=>c.taskId===consumer.id&&c.scope===proposal.baseWorkspace)
    .flatMap((c:any)=>Array.isArray(c.evidence)?c.evidence.filter((e:any)=>e.kind==='DELIVERY_SOURCE_REVIEW_REQUIRED'&&
      e.sourceConflict?.observed?.taskId===proposal.sourceTaskId).map((e:any)=>({c,e})):[]);
  if(matches.length!==1)return fail('CAPACITY_SOURCE_PROOF_REQUIRED');
  const {c,e}=matches[0],source=e.sourceConflict.observed;
  if(!uuid.test(c.id)||!uuid.test(c.currentRun?.id??'')||c.currentRun.attempt!==consumer.expectedAttempt||
    !uuid.test(e.obligationId??'')||!digest.test(e.sourceConflict.generation??'')||!uuid.test(source.runId??'')||
    !digest.test(source.contractDigest??'')||!digest.test(source.manifestDigest??'')||source.currentFrozen!==true||source.currentAccepted!==true)
    return fail('CAPACITY_SOURCE_PROOF_REQUIRED');
  const intent:CapacityIntent={version:1,state:'waiting_capacity',code:'CAPACITY_WAIT',proposal,
    proposalDigest:deliveryDigest(proposal),reviewRunId,artifactId:ref.id,artifactDigest:ref.digest,index,
    binding:{mode:'episode',consumer:{id:consumer.id,expectedAttempt:consumer.expectedAttempt,stage:consumer.stage,
      workspace:consumer.workspace,scope:proposal.baseWorkspace,instructionRevisionId:consumer.instructionRevisionId,
      criteriaDigest:deliveryDigest(consumer.criteria),readOnlySource:consumer.readOnlySource??false},
      source:{taskId:source.taskId,runId:source.runId,contractDigest:source.contractDigest,manifestDigest:source.manifestDigest},
      caseId:c.id,caseRunId:c.currentRun.id,generation:e.sourceConflict.generation,obligationId:e.obligationId}};
  const checked=await checkBacklogCapacityIntent(db,goalId,intent);
  return checked.ok?{ok:true,intent}:fail(checked.code);
}

/** Current authority is checked again inside admission, after all preparation.
 * No artifact IO or frozen prompt decoding occurs while holding that lock. */
export async function checkBacklogCapacityIntent(db:DB,goalId:string,intent:CapacityIntent):Promise<{ok:boolean;code:string}>{
  if(intent?.version!==1||!uuid.test(intent.reviewRunId??'')||!uuid.test(intent.artifactId??'')||
    !digest.test(intent.artifactDigest??'')||!digest.test(intent.proposalDigest??'')||
    !backlogProposalSchema.safeParse(intent.proposal).success||deliveryDigest(intent.proposal)!==intent.proposalDigest||!intent.binding||
    !Number.isSafeInteger(intent.index)||intent.index<0||intent.index>=12||!['initial','episode',undefined].includes(intent.binding.mode)||
    !uuid.test(intent.binding.consumer?.id??'')||!Number.isSafeInteger(intent.binding.consumer?.expectedAttempt)||
    intent.binding.consumer.expectedAttempt<0||!digest.test(intent.binding.consumer?.criteriaDigest??'')||
    typeof intent.binding.consumer.workspace!=='string'||typeof intent.binding.consumer.scope!=='string')return fail('CAPACITY_INTENT_INVALID');
  const initial=intent.binding.mode==='initial';
  if(initial?!digest.test(intent.binding.consumer.contractDigest??''):
    !uuid.test(intent.binding.caseId??'')||!uuid.test(intent.binding.caseRunId??'')||!uuid.test(intent.binding.obligationId??'')||!digest.test(intent.binding.generation??''))return fail('CAPACITY_INTENT_INVALID');
  if(intent.proposal.sourceTaskId&&(!uuid.test(intent.binding.source?.taskId??'')||!uuid.test(intent.binding.source?.runId??'')||
    !digest.test(intent.binding.source?.contractDigest??'')||!digest.test(intent.binding.source?.manifestDigest??'')))return fail('CAPACITY_INTENT_INVALID');
  if(!intent.proposal.sourceTaskId&&intent.binding.source!==null)return fail('CAPACITY_INTENT_INVALID');
  if(!await enabled(db,goalId))return fail('CAPACITY_AUTHORITY_DISABLED');
  const accepted=(await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id AND t.goal_id=$2 AND t.stage='coordinate'
    JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id AND task_id=t.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON a.passed
    JOIN artifacts ar ON ar.id=$3 AND ar.run_id=r.id AND ar.name='coordination.json' AND ar.digest=$4
    WHERE r.id=$1 AND r.state='submitted'`,[intent.reviewRunId,goalId,intent.artifactId,intent.artifactDigest])).rowCount;
  if(!accepted)return fail('CAPACITY_REVIEW_CHANGED');
  const b=intent.binding,p=intent.proposal;
  if(p.deliverToTaskId!==b.consumer.id||(p.sourceTaskId??null)!==(b.source?.taskId??null)||
    (p.baseWorkspace&&![b.consumer.scope,b.consumer.workspace,b.source?.workspace].includes(p.baseWorkspace)))return fail('CAPACITY_INTENT_BINDING');
  const c=checkedExecutionMetadata((await db.query(`SELECT t.id,t.state,t.stage,t.attempt_count,t.instruction_revision_id,t.criteria,
    metadata.control execution,r.id run_id,${executionMetadataColumns},CASE WHEN $3::boolean THEN ${consumerContractSql} END contract_digest FROM tasks t
    LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    LEFT JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count WHERE t.id=$1 AND t.goal_id=$2`,[b.consumer.id,goalId,initial])).rows)[0];
  if(!c||['accepted','cancelled','superseded'].includes(c.state)||c.attempt_count!==b.consumer.expectedAttempt||
    (!initial&&c.run_id!==b.caseRunId)||c.stage!==b.consumer.stage||c.execution?.workspace!==b.consumer.workspace||
    c.execution?.baseWorkspace!==b.consumer.scope||(c.instruction_revision_id??null)!==b.consumer.instructionRevisionId||
    deliveryDigest(c.criteria)!==b.consumer.criteriaDigest||(c.execution?.readOnlySource??false)!==b.consumer.readOnlySource)return fail('CAPACITY_CONSUMER_CHANGED');
  if(initial&&c.contract_digest!==b.consumer.contractDigest)return fail('CAPACITY_CONSUMER_CONTRACT_CHANGED');
  if(!initial){
    const consumerIdentity=await readDeliverySourceIdentity(db,b.consumer.id);
    if(consumerIdentity?.run_id!==b.caseRunId||!sourceIdentityFrozen(consumerIdentity))return fail('CAPACITY_CONSUMER_CONTRACT_CHANGED');
  const current=(await db.query(`SELECT 1 FROM supervision_cases c JOIN delivery_consumptions d ON d.id=$4
    AND d.goal_id=c.goal_id AND d.consumer_task_id=c.task_id WHERE c.id=$1 AND c.goal_id=$2 AND c.task_id=$3
    AND c.source_run_id=$5 AND c.state='open' AND c.decision_run_id IS NULL
    AND d.state NOT IN('completed','superseded') AND d.receipt#>>'{sourceConflict,generation}'=$6
    AND EXISTS(SELECT 1 FROM jsonb_array_elements(c.evidence) e WHERE e->>'obligationId'=$4::text
      AND e#>>'{sourceConflict,generation}'=$6)`,
    [b.caseId,goalId,b.consumer.id,b.obligationId,b.caseRunId,b.generation])).rowCount;
  if(!current)return fail('CAPACITY_EPISODE_CHANGED');
  }
  if(!b.source)return {ok:true,code:'CAPACITY_VALID'}; // Authorized base workspace, no invented immutable source proof.
  const source=await readDeliverySourceIdentity(db,b.source.taskId);
  if(!source||source.goal_id!==goalId||source.scope!==b.consumer.scope||source.run_id!==b.source.runId||
    !sourceIdentityAccepted(source)||!sourceIdentityFrozen(source)||source.contract_digest!==b.source.contractDigest)return fail('CAPACITY_SOURCE_CHANGED');
  const manifest=(await db.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[source.run_id])).rows;
  if(deliveryDigest(manifest)!==b.source.manifestDigest)return fail('CAPACITY_SOURCE_MANIFEST_CHANGED');
  return {ok:true,code:'CAPACITY_VALID'};
}

/** Receipt updates preserve the original refusal and every sibling item. */
export async function updateCapacityIntent(db:DB,goalId:string,ref:CapacityIntentRef,change:Partial<CapacityIntent>,taskId?:string){
  const row=(await db.query('SELECT receipts,task_ids FROM workforce_backlog_plans WHERE run_id=$1 AND goal_id=$2 FOR UPDATE',[ref.runId,goalId])).rows[0];
  const receipt=row?.receipts?.find((r:any)=>r.index===ref.index),intent=receipt?.capacityIntent;
  if(!intent||intent.proposalDigest!==ref.proposalDigest||(!waiting.includes(intent.state)&&intent.state!==change.state))throw Error('CAPACITY_INTENT_CHANGED');
  if(intent.state==='admitted'&&taskId&&intent.taskId!==taskId)throw Error('CAPACITY_ALREADY_ADMITTED');
  receipt.capacityIntent={...intent,...change,...(taskId?{taskId}:{})};
  const ids=taskId?[...new Set([...row.task_ids,taskId])]:row.task_ids;
  await db.query(`UPDATE workforce_backlog_plans SET receipts=$3,task_ids=$4,
    state=CASE WHEN $5::boolean THEN 'partial' ELSE state END WHERE run_id=$1 AND goal_id=$2`,
    [ref.runId,goalId,JSON.stringify(row.receipts),JSON.stringify(ids),!!taskId]);
  if(taskId)await event(db,'BACKLOG_CAPACITY_ADMITTED',goalId,{reviewRunId:ref.runId,index:ref.index,taskId,proposalDigest:ref.proposalDigest});
}

export async function loadCapacityIntent(db:DB,goalId:string,ref:CapacityIntentRef):Promise<CapacityIntent|null>{
  const row=(await db.query('SELECT receipts FROM workforce_backlog_plans WHERE run_id=$1 AND goal_id=$2',[ref.runId,goalId])).rows[0];
  const intent=row?.receipts?.find((r:any)=>r.index===ref.index)?.capacityIntent;
  return intent?.proposalDigest===ref.proposalDigest&&waiting.includes(intent.state)?intent:null;
}
const enabledSelection=`g.state='active' AND w.value->>'enabled'='true' AND m.value->>'enabled'='true'
  AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'`;
const policyJoins=`JOIN goals g ON g.id=p.goal_id JOIN settings w ON w.id='workforce:'||g.id
  JOIN settings m ON m.id='management:'||g.id JOIN settings s ON s.id='supervision:'||g.id
  LEFT JOIN settings a ON a.id='autonomy:'||g.project`;
export async function pendingCapacityIntents(db:DB,goalId?:string,lane?:string){
  return (await db.query(`SELECT goal_id,run_id,index,intent FROM(SELECT p.goal_id,p.run_id,p.created_at,
    (r->>'index')::int index,r->'capacityIntent' intent,
    row_number() OVER(PARTITION BY p.goal_id,r->>'lane' ORDER BY p.created_at,p.run_id,(r->>'index')::int) ordinal
    FROM workforce_backlog_plans p ${policyJoins},LATERAL jsonb_array_elements(p.receipts) r
    WHERE ${enabledSelection} AND ($1::uuid IS NULL OR p.goal_id=$1) AND ($2::text IS NULL OR r->>'lane'=$2)
    AND (r->>'lane'='glm' OR (r->>'lane'='muse' AND COALESCE((w.value->>'museTarget')::int,0)>0))
    AND r#>>'{capacityIntent,state}' IN('waiting_capacity','preparing')
    ) selected ORDER BY ordinal,created_at,run_id,index LIMIT 16`,[goalId??null,lane??null])).rows;
}
/** Invalid administrative or source waits never reserve an executable slot. */
export async function hasCapacityIntentPriority(db:DB,goalId:string,lane:string,except?:CapacityIntentRef,freeSlots=1){
  let reservations=0;
  for(const row of await pendingCapacityIntents(db,goalId,lane)){
    if(except&&row.run_id===except.runId&&row.index===except.index)return false;
    if((await checkBacklogCapacityIntent(db,goalId,row.intent)).ok&&++reservations>=freeSlots)return true;
  }
  return false;
}

/** Historical PENDING_LIMIT receipts are qualified once, without erasing their
 * original decision. File reads happen before the transactional compare/set. */
export async function capturePendingBacklogCapacityIntents(read:Reader){
  const rows=(await pool.query(`SELECT p.goal_id,p.run_id,p.receipts FROM workforce_backlog_plans p ${policyJoins}
    JOIN management_reviews review ON review.run_id=p.run_id AND review.goal_id=p.goal_id
    WHERE ${enabledSelection} AND EXISTS(SELECT 1 FROM jsonb_array_elements(p.receipts) r
      WHERE r->>'code'='PENDING_LIMIT' AND r->'capacityIntent' IS NULL)
    ORDER BY p.created_at,p.run_id LIMIT 8`)).rows;
  for(const row of rows){
    const refs=(await pool.query("SELECT id,digest,size FROM artifacts WHERE run_id=$1 AND name='coordination.json' LIMIT 2",[row.run_id])).rows;
    let advice:any,value:Awaited<ReturnType<Reader>>|undefined;
    try{if(refs.length===1&&Number(refs[0].size)<=1000000){
      value=await read(refs[0].id);
      if(value.bytes.length===Number(refs[0].size)&&sha(value.bytes)===refs[0].digest)advice=JSON.parse(value.bytes.toString());
    }}catch{}
    for(const receipt of row.receipts){
      if(receipt.code!=='PENDING_LIMIT'||receipt.capacityIntent)continue;
      let captured:Awaited<ReturnType<typeof captureBacklogCapacityIntent>>;
      try{captured=await captureBacklogCapacityIntent(pool,row.goal_id,row.run_id,receipt.index,advice?.backlog?.tasks?.[receipt.index],
        async id=>id===refs[0]?.id&&value?value:read(id));}
      catch{captured=fail('CAPACITY_BINDING_UNAVAILABLE');}
      await tx(async db=>{
        await lock(db);
        const current=(await db.query('SELECT receipts FROM workforce_backlog_plans WHERE run_id=$1 AND goal_id=$2 FOR UPDATE',[row.run_id,row.goal_id])).rows[0];
        const target=current?.receipts?.find((r:any)=>r.index===receipt.index);
        if(!target||target.capacityIntent||deliveryDigest(target)!==deliveryDigest(receipt))return;
        const checked=captured.ok?await checkBacklogCapacityIntent(db,row.goal_id,captured.intent):captured;
        target.capacityIntent=captured.ok&&checked.ok?captured.intent:{version:1,state:'needs_revalidation',code:checked.code,
          reviewRunId:row.run_id,index:receipt.index,nextAction:'Revalidate this exact refused item against the current consumer and frozen source; no capacity is reserved.'};
        await db.query('UPDATE workforce_backlog_plans SET receipts=$2 WHERE run_id=$1',[row.run_id,JSON.stringify(current.receipts)]);
        await event(db,'BACKLOG_CAPACITY_RECORDED',row.goal_id,{reviewRunId:row.run_id,index:receipt.index,state:target.capacityIntent.state,code:target.capacityIntent.code});
      });
    }
  }
}
