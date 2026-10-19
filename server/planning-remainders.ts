import {createHash} from 'node:crypto';
import {isDeepStrictEqual} from 'node:util';
import {pool,tx,lock,event,type DB} from './db.js';
import {scopedPlanSchema} from './scoped-plan-contract.js';
import {backlogProposalSchema,planningProposalDigest,planningRemainderReferenceSchema,type BacklogProposal} from './planning-remainder-contract.js';
import {reconcileNoWorkBlockers} from './planning-no-work.js';
import {reportedPlanningActions} from './planning-action-review.js';

type ArtifactReader=(id:string)=>Promise<{bytes:Buffer}>;
const sha=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
const fail=(code:string):never=>{throw Error(code);};
const active=['queued','running','reconciling','cancelling'];

/** Small identity/receipt read only. No execution inputs or catalog TOAST. */
async function originalState(db:DB,requestId:string){
  return (await db.query(`SELECT p.id,p.goal_id,p.scope,p.source_revision,p.task_id,p.state,p.receipt,
    t.attempt_count,t.state task_state,
    (t.recovery->>'mode'='manual' AND autonomy.value->>'enabled' IS DISTINCT FROM 'true') manual_hold,r.id run_id,r.attempt,r.fence,r.state run_state,
    a.id artifact_id,a.digest artifact_digest,a.size artifact_size,
    (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1) passed,
    g.state goal_state,COALESCE(autonomy.value->>'enabled','true')='true' autonomous,
    EXISTS(SELECT 1 FROM runs x WHERE x.task_id=t.id AND x.state=ANY($2::text[])) live,
    EXISTS(SELECT 1 FROM leases l JOIN runs x ON x.id=l.run_id WHERE x.task_id=t.id) leased,
    EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.entity_id=t.id::text
      AND e.payload->>'runId'=r.id::text) cancelled
    FROM planning_requests p JOIN tasks t ON t.id=p.task_id AND t.goal_id=p.goal_id JOIN goals g ON g.id=p.goal_id
    LEFT JOIN settings autonomy ON autonomy.id='autonomy:'||g.project
    JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    JOIN artifacts a ON a.run_id=r.id AND a.name='scope-plan.json' WHERE p.id=$1`,[requestId,active])).rows[0];
}
function originalAuthorized(row:any){
  return row && row.goal_state==='active' && row.autonomous && !row.manual_hold && !row.live && !row.leased && !row.cancelled &&
    row.task_state==='accepted' && row.run_state==='submitted' && row.passed===true;
}
async function originalPlan(row:any,read:ArtifactReader){
  if(row.artifact_size>8*1024*1024)fail('PLANNING_REMAINDER_ARTIFACT_CHANGED');
  const {bytes}=await read(row.artifact_id);
  if(bytes.length!==Number(row.artifact_size)||sha(bytes)!==row.artifact_digest)fail('PLANNING_REMAINDER_ARTIFACT_CHANGED');
  const plan=scopedPlanSchema.parse(JSON.parse(bytes.toString('utf8')));
  if(plan.requestId!==row.id||plan.sourceRevision!==row.source_revision)fail('PLANNING_REMAINDER_SOURCE_CHANGED');
  return plan;
}
/** A known created task or exact dedup owner is not a refused item. An ignored
 * coverage key without such an owner is deliberately NOT treated as resolved. */
async function remainingReceipt(db:DB,row:any,plan:any,backlog:any){
  const allIds=[...(backlog?.task_ids??[]),...(backlog?.receipts??[]).map((r:any)=>r.taskId)]
    .filter((id:any)=>typeof id==='string'&&uuid.test(id));
  const owned=(await db.query(`SELECT b.coverage_key,b.task_id FROM workforce_backlog_items b JOIN tasks t ON t.id=b.task_id
    WHERE b.goal_id=$1 AND t.goal_id=$1 AND b.task_id=ANY($2::uuid[])`,[row.goal_id,allIds])).rows;
  const byIndex=new Map((backlog?.receipts??[]).map((r:any)=>[r.index,r]));
  const items=plan.tasks.flatMap((proposal:any,index:number)=>{
    const receipt:any=byIndex.get(index);
    if(!receipt)fail('PLANNING_REMAINDER_RECEIPT_MISSING');
    if(receipt.code==='WORKSPACE_PREPARING')return [];
    if(receipt.coverageKey!==proposal.coverageKey || receipt.consumerTaskId!==proposal.deliverToTaskId ||
      receipt.lane!==proposal.lane || receipt.sourceTaskId!==proposal.sourceTaskId)fail('PLANNING_REMAINDER_RECEIPT_CHANGED');
    const known=owned.some(o=>o.coverage_key===proposal.coverageKey &&
      (o.task_id===receipt.taskId || (backlog.task_ids??[]).includes(o.task_id)));
    if(known)return [];
    // Reuse has an exact semantic owner even when it uses another coverage key.
    // Keep that existing owner; downstream follow-through still checks its work.
    if(receipt.code==='OBLIGATION_REUSE_REQUIRED'&&receipt.status==='ignored'&&receipt.taskId&&owned.some(o=>o.task_id===receipt.taskId))return [];
    if(!['rejected','ignored'].includes(receipt.status))fail('PLANNING_REMAINDER_OWNER_MISSING');
    const reference={requestId:row.id,runId:row.run_id,artifactId:row.artifact_id,artifactDigest:row.artifact_digest,
      index,proposalDigest:planningProposalDigest(proposal)};
    const previous=row.receipt?.remaining?.items?.find((item:any)=>item.reference?.index===index &&
      isDeepStrictEqual(item.reference,reference));
    return [{reference,status:previous?.status==='linked'?'linked':'pending',code:receipt.code,
      coverageKey:proposal.coverageKey,lane:proposal.lane,sourceTaskId:proposal.sourceTaskId,
      consumerTaskId:proposal.deliverToTaskId,baseWorkspace:proposal.baseWorkspace,workKind:proposal.workKind,
      ...(previous?.status==='linked'?{replacement:previous.replacement}:{})}];
  });
  return {version:1,plannerTaskId:row.task_id,plannerAttempt:row.attempt,items};
}

/** Called after normal application. CAS rechecks the same accepted planner,
 * run/fence, artifact and receipt; no acceptance, task or retry is changed. */
export async function recordScopedPlanningEffects(request:any,artifact:any,plan:any,baseReceipt:any,state:string){
  return tx(async db=>{
    await lock(db);const current=await originalState(db,request.id);
    if(!originalAuthorized(current)||current.run_id!==request.run_id||current.task_id!==request.task_id||
      current.source_revision!==request.source_revision||current.artifact_id!==artifact.id||current.artifact_digest!==artifact.digest ||
      !['assigned','running','preparing','applied','awaiting_supervision'].includes(current.state) || current.state!==request.state)return false;
    if(request.fence!==undefined&&current.fence!==request.fence)return false;
    if(!isDeepStrictEqual(current.receipt,request.expectedReceipt??request.receipt))return false;
    const backlog=(await db.query('SELECT state,receipts,task_ids FROM workforce_backlog_plans WHERE run_id=$1 AND goal_id=$2',[request.run_id,current.goal_id])).rows[0];
    if(!isDeepStrictEqual(backlog,baseReceipt.backlog))return false;
    const remaining=await remainingReceipt(db,current,plan,baseReceipt.backlog);
    const pending=remaining.items.filter((i:any)=>i.status==='pending');
    const noWork=baseReceipt.code==='EVIDENCED_NO_WORK' && !plan.tasks.length &&
      !(plan.actions??[]).length && !(plan.dependencyChanges??[]).length;
    const noWorkReconciliation=noWork?await reconcileNoWorkBlockers(db,current.goal_id,current.scope,plan.blockers):undefined;
    const next={...baseReceipt,...(noWorkReconciliation?{noWorkReconciliation}:{}),
      ...(remaining.items.length||current.receipt.remaining||baseReceipt.backlog?.state==='partial'?{remaining}:{})};
    // Existing durable preparation retains its owner until it completes. Any
    // terminal refusal stays visible, but never enters whole-plan retry.
    const nextState=pending.length && state!=='preparing'?'awaiting_supervision':state;
    if(pending.length)next.code='PLANNING_REMAINDER_REQUIRED';
    const changed=await db.query(`UPDATE planning_requests SET state=$2,receipt=$3,updated_at=clock_timestamp()
      WHERE id=$1 AND (state IS DISTINCT FROM $2 OR receipt IS DISTINCT FROM $3::jsonb) RETURNING id`,
      [request.id,nextState,JSON.stringify(next)]);
    if(changed.rowCount&&pending.length)await event(db,'SCOPED_PLANNING_REMAINDER_REQUIRED',current.goal_id,
      {requestId:current.id,runId:current.run_id,artifactId:current.artifact_id,artifactDigest:current.artifact_digest,
        indexes:pending.map((i:any)=>i.reference.index),createdTaskIds:baseReceipt.backlog?.task_ids??[]});
    return !!changed.rowCount;
  });
}

/** One-time projection of the legacy false-applied state. Reads immutable
 * accepted bytes, then the same locked writer. It never calls applyBacklog. */
export async function recoverPartialPlanningRemainders(read:ArtifactReader){
  if(!(await pool.query("SELECT to_regclass('public.planning_requests') present")).rows[0].present)return [];
  const candidates=(await pool.query(`SELECT p.id FROM planning_requests p JOIN goals g ON g.id=p.goal_id
    JOIN tasks t ON t.id=p.task_id AND t.goal_id=p.goal_id LEFT JOIN settings a ON a.id='autonomy:'||g.project
    JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    WHERE g.state='active' AND p.state IN('applied','awaiting_supervision') AND p.receipt->'backlog'->>'state'='partial'
    AND NOT(p.receipt ? 'remaining') AND NOT(p.receipt ? 'remainingProjectionFailure')
    AND COALESCE(a.value->>'enabled','true')='true' AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
    AND t.state='accepted' AND r.state='submitted' AND p.receipt->>'evaluatedRunId'=r.id::text
    AND (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)=true
    AND EXISTS(SELECT 1 FROM artifacts WHERE run_id=r.id AND name='scope-plan.json')
    AND NOT EXISTS(SELECT 1 FROM runs x WHERE x.task_id=t.id AND x.state=ANY($1::text[]))
    AND NOT EXISTS(SELECT 1 FROM leases l JOIN runs x ON x.id=l.run_id WHERE x.task_id=t.id)
    AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.entity_id=t.id::text AND e.payload->>'runId'=r.id::text)
    ORDER BY p.updated_at,p.id LIMIT 8`,[active])).rows;
  const recovered:string[]=[];
  for(const candidate of candidates){let observed:any;try{
    const row=await originalState(pool,candidate.id);observed=row;if(!originalAuthorized(row)||row.receipt.evaluatedRunId!==row.run_id)continue;
    const plan=await originalPlan(row,read);
    if(await recordScopedPlanningEffects({...row,expectedReceipt:row.receipt},
      {id:row.artifact_id,digest:row.artifact_digest},plan,row.receipt,row.state))recovered.push(row.id);
  }catch(error:any){
    // Record one typed failure, not an endless retry of unreadable historical
    // bytes. The unresolved partial remains visible to its existing supervisor.
    await tx(async db=>{await lock(db);const row=await originalState(db,candidate.id);
      if(!observed||!originalAuthorized(row)||row.run_id!==observed.run_id||row.fence!==observed.fence||
        row.artifact_id!==observed.artifact_id||row.artifact_digest!==observed.artifact_digest||!isDeepStrictEqual(row.receipt,observed.receipt)||
        !['applied','awaiting_supervision'].includes(row.state)||row.receipt.backlog?.state!=='partial'||row.receipt.remaining)return;
      const code=/^PLANNING_REMAINDER_[A-Z_]+$/.test(error?.message??'')?error.message:'PLANNING_REMAINDER_PROOF_UNAVAILABLE';
      await db.query(`UPDATE planning_requests SET state='awaiting_supervision',receipt=receipt||$2::jsonb,updated_at=clock_timestamp()
        WHERE id=$1 AND receipt=$3::jsonb`,[row.id,JSON.stringify({code,remainingProjectionFailure:{version:1,
          runId:row.run_id,artifactId:row.artifact_id,artifactDigest:row.artifact_digest,code}}),JSON.stringify(row.receipt)]);
    });
  }}
  return recovered;
}

export async function readPlanningDemandPage(db:DB,goalId:string,options:{offset?:number;limit?:number;expectedDigest?:string}={}){
  const offset=options.offset??0,limit=Math.min(15,Math.max(1,options.limit??15));
  if(!(await db.query("SELECT to_regclass('public.planning_requests') present")).rows[0].present)
    return {status:'OK',rows:[],totalRows:0,snapshotDigest:sha(Buffer.from('[]')),nextOffset:null};
  const directory=(await db.query(`SELECT id,state,updated_at::text,
    receipt->>'evaluatedRunId' evaluated_run_id,
    md5(COALESCE((receipt->'deliveryActionReceipts')::text,'')) action_receipt_revision FROM planning_requests
    WHERE goal_id=$1 AND state NOT IN('applied','superseded') ORDER BY id`,[goalId])).rows;
  const snapshotDigest=sha(Buffer.from(JSON.stringify(directory)));
  if(options.expectedDigest&&options.expectedDigest!==snapshotDigest)
    return {status:'STALE_CATALOG',rows:[],totalRows:directory.length,snapshotDigest,nextOffset:0};
  const selected=(await db.query(`SELECT id,scope,lane,state,source_revision,task_id,receipt->>'code' code,updated_at,
    receipt->'remaining' remaining,receipt->'backlog'->'task_ids' created_task_ids,
    CASE WHEN jsonb_typeof(receipt->'deliveryActionReceipts')='array' THEN jsonb_array_length(receipt->'deliveryActionReceipts') ELSE 0 END action_count,
    CASE WHEN jsonb_typeof(receipt->'deliveryActionReceipts')='array' THEN
      (SELECT COALESCE(jsonb_agg(entry.value ORDER BY entry.ordinality),'[]'::jsonb)
       FROM jsonb_array_elements(receipt->'deliveryActionReceipts') WITH ORDINALITY entry(value,ordinality)
       WHERE entry.ordinality<=12) ELSE '[]'::jsonb END action_receipts
    FROM planning_requests WHERE goal_id=$1 AND state NOT IN('applied','superseded')
    ORDER BY CASE WHEN receipt->>'code'='PLANNING_REMAINDER_REQUIRED' THEN 0 ELSE 1 END,updated_at,id LIMIT $2 OFFSET $3`,[goalId,limit,offset])).rows;
  const rows=selected.map(({remaining,created_task_ids,action_count,action_receipts,...row})=>({...row,
    ...(action_count||row.code==='DELIVERY_ACTION_REVIEW_REQUIRED'?{deliveryActionReview:reportedPlanningActions(row.id,action_receipts,action_count)}:{}),
    ...(remaining?.version===1?{createdTaskIds:created_task_ids??[],remainingItems:remaining.items.filter((i:any)=>i.status==='pending').slice(0,1),
      remainingItemCount:remaining.items.filter((i:any)=>i.status==='pending').length,
      remainingItemsTruncated:remaining.items.filter((i:any)=>i.status==='pending').length>1,
      remainingItemsRequest:{kind:'planning_request',requestId:row.id,offset:0},
      remainingItemRequest:{kind:'planning_request',requestId:row.id,itemIndex:remaining.items.find((i:any)=>i.status==='pending')?.reference.index,offset:0},
      originalPlanRequest:{kind:'artifact',taskId:remaining.plannerTaskId,expectedAttempt:remaining.plannerAttempt,name:'scope-plan.json',offset:0}}:{})}));
  return {status:'OK',rows,totalRows:directory.length,snapshotDigest,nextOffset:offset+rows.length<directory.length?offset+rows.length:null};
}
export async function readPlanningDemands(db:DB,goalId:string){
  const page=await readPlanningDemandPage(db,goalId);
  return {rows:page.rows,selection:{total:page.totalRows,included:page.rows.length,truncated:page.nextOffset!==null,
    recovery:{kind:'planning_demands',offset:page.nextOffset??0,snapshotDigest:page.snapshotDigest}}};
}

/** Fetch one exact proposal rather than forcing several pages of unrelated
 * plan JSON. No caller-provided path or digest replaces the stored binding. */
export async function readPlanningRemainderItem(db:DB,goalId:string,requestId:string,index:number,read:ArtifactReader){
  const row=await originalState(db,requestId);
  if(!row||row.goal_id!==goalId)return {status:'NOT_FOUND'};
  if(!originalAuthorized(row))return {status:'STALE_SOURCE'};
  const item=row.receipt.remaining?.items?.find((i:any)=>i.reference?.index===index);
  if(!item)return {status:'NOT_FOUND'};
  try{
    const plan=await originalPlan(row,read),proposal=plan.tasks[index];
    if(!proposal||item.reference.runId!==row.run_id||item.reference.artifactId!==row.artifact_id||
      item.reference.artifactDigest!==row.artifact_digest||planningProposalDigest(proposal)!==item.reference.proposalDigest)
      return {status:'STALE_SOURCE'};
    return {status:'OK',value:{requestId:row.id,scope:row.scope,state:row.state,item,originalProposal:proposal,
      createdTaskIds:row.receipt.backlog?.task_ids??[],readOnly:true}};
  }catch{return {status:'INTEGRITY_FAILED'};}
}

export type RemainderReplacementProof={reference:any;proposalDigest:string;reviewRunId:string;reviewArtifactId:string;reviewArtifactDigest:string;reviewFence:string;originalFence:string};
/** Verify the proposed correction really exists in the accepted coordinator
 * artifact. The model cannot assert a link using another review's authority. */
export async function preparePlanningRemainderReplacement(goalId:string,runId:string,proposal:BacklogProposal,read:ArtifactReader):Promise<RemainderReplacementProof|null>{
  if(!proposal.remainingPlanItem)return null;
  const reference=planningRemainderReferenceSchema.parse(proposal.remainingPlanItem);
  const original=await originalState(pool,reference.requestId);
  if(!originalAuthorized(original)||original.goal_id!==goalId)fail('PLANNING_REMAINDER_SOURCE_CHANGED');
  const plan=await originalPlan(original,read),old=plan.tasks[reference.index];
  if(!old||planningProposalDigest(old)!==reference.proposalDigest||reference.runId!==original.run_id||
    reference.artifactId!==original.artifact_id||reference.artifactDigest!==original.artifact_digest)
    fail('PLANNING_REMAINDER_BINDING_CHANGED');
  for(const key of ['coverageKey','baseWorkspace','workKind','deliverToTaskId'] as const)
    if(old[key]!==proposal[key])fail('PLANNING_REMAINDER_ITEM_CHANGED');
  const review=(await pool.query(`SELECT r.fence,a.id,a.digest,a.size FROM runs r JOIN tasks t ON t.id=r.task_id
    JOIN artifacts a ON a.run_id=r.id AND a.name='coordination.json'
    WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate' AND r.state='submitted'
      AND r.attempt=t.attempt_count AND (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)=true`,
    [runId,goalId])).rows[0];
  if(!review||review.size>8*1024*1024)fail('PLANNING_REMAINDER_REVIEW_INVALID');
  const {bytes}=await read(review.id);
  if(bytes.length!==Number(review.size)||sha(bytes)!==review.digest)fail('PLANNING_REMAINDER_REVIEW_CHANGED');
  const proposed=JSON.parse(bytes.toString('utf8'))?.backlog?.tasks;
  if(!Array.isArray(proposed)||!proposed.some((p:any)=>{
    const parsed=backlogProposalSchema.safeParse(p);
    return parsed.success&&planningProposalDigest(parsed.data)===planningProposalDigest(proposal)&&
      isDeepStrictEqual(parsed.data.remainingPlanItem,reference);
  }))fail('PLANNING_REMAINDER_REVIEW_CHANGED');
  return {reference,proposalDigest:planningProposalDigest(proposal),reviewRunId:runId,reviewArtifactId:review.id,
    reviewArtifactDigest:review.digest,reviewFence:review.fence,originalFence:original.fence};
}

/** Must run under the caller's normal admission transaction/global lock. */
export async function checkPlanningRemainderReplacement(db:DB,goalId:string,proof:RemainderReplacementProof){
  const ref=proof.reference,row=await originalState(db,ref.requestId);
  if(!originalAuthorized(row)||row.goal_id!==goalId||row.run_id!==ref.runId||row.fence!==proof.originalFence||
    row.artifact_id!==ref.artifactId||row.artifact_digest!==ref.artifactDigest||row.receipt.evaluatedRunId!==ref.runId)
    fail('PLANNING_REMAINDER_SOURCE_CHANGED');
  const item=row.receipt.remaining?.items?.find((i:any)=>isDeepStrictEqual(i.reference,ref));
  if(!item||!['awaiting_supervision','preparing','applied'].includes(row.state))fail('PLANNING_REMAINDER_CHANGED');
  if(item.status==='linked')return {row,item,alreadyLinked:true};
  if(item.status!=='pending')fail('PLANNING_REMAINDER_CHANGED');
  const valid=(await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id
    JOIN artifacts a ON a.id=$3 AND a.run_id=r.id AND a.name='coordination.json'
    JOIN settings policy ON policy.id='management:'||t.goal_id
    WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate' AND r.state='submitted' AND r.fence=$4
      AND r.attempt=t.attempt_count AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR EXISTS(SELECT 1 FROM goals g JOIN settings a ON a.id='autonomy:'||g.project WHERE g.id=t.goal_id AND a.value->>'enabled'='true')) AND policy.value->>'enabled'='true'
      AND a.digest=$5 AND (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)=true
      AND NOT EXISTS(SELECT 1 FROM runs x WHERE x.task_id=t.id AND x.state=ANY($6::text[]))
      AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.entity_id=t.id::text AND e.payload->>'runId'=r.id::text)`,
    [proof.reviewRunId,goalId,proof.reviewArtifactId,proof.reviewFence,proof.reviewArtifactDigest,active])).rowCount;
  if(!valid)fail('PLANNING_REMAINDER_REVIEW_CHANGED');
  return {row,item,alreadyLinked:false};
}

export async function linkPlanningRemainderReplacement(db:DB,goalId:string,proof:RemainderReplacementProof,taskId:string){
  const {row,item,alreadyLinked}=await checkPlanningRemainderReplacement(db,goalId,proof);
  if(alreadyLinked)fail('PLANNING_REMAINDER_ALREADY_LINKED');
  if(!(await db.query('SELECT 1 FROM tasks WHERE id=$1 AND goal_id=$2',[taskId,goalId])).rowCount)fail('PLANNING_REMAINDER_TASK_MISSING');
  const replacement={taskId,reviewRunId:proof.reviewRunId,reviewArtifactId:proof.reviewArtifactId,
    reviewArtifactDigest:proof.reviewArtifactDigest,proposalDigest:proof.proposalDigest};
  const items=row.receipt.remaining.items.map((i:any)=>i.reference.index===item.reference.index?{...i,status:'linked',replacement}:i);
  const pending=items.some((i:any)=>i.status==='pending');
  const refusedAction=row.receipt.deliveryActionReceipts?.some((r:any)=>!r.applied&&!r.preparing)===true;
  const receipt={...row.receipt,code:pending?'PLANNING_REMAINDER_REQUIRED':refusedAction?'DELIVERY_ACTION_REVIEW_REQUIRED':'PLANNING_REMAINDER_LINKED',remaining:{...row.receipt.remaining,items}};
  const changed=await db.query(`UPDATE planning_requests SET receipt=$3,state=CASE WHEN NOT $4 AND state='awaiting_supervision' THEN 'applied' ELSE state END,
    updated_at=clock_timestamp() WHERE id=$1 AND receipt=$2::jsonb RETURNING id`,[row.id,JSON.stringify(row.receipt),JSON.stringify(receipt),pending||refusedAction]);
  if(!changed.rowCount)fail('PLANNING_REMAINDER_CHANGED');
  await event(db,'SCOPED_PLANNING_REMAINDER_LINKED',goalId,{...proof.reference,...replacement,deliveryAccepted:false});
}
