import {createHash} from 'node:crypto';
import type {DB} from './db.js';
import {readDeliveryConsumptionPage} from './delivery-consumption-catalog.js';
import {quotaFor} from './accounts.js';
import {readLiveFailureReview} from './manager-live-failure.js';
import {readCanonicalRepairRejections} from './canonical-repair-rejection.js';

/** A wake fingerprint is observation, never authority to admit a worker. It
 * covers all scopes, not only the rotating prompt window. No raw instruction,
 * heartbeat, poll timestamp or growing stagnation counter leaves this read. */
export async function readManagementMateriality(db:DB,goalId:string,workforceFingerprint?:string) {
  const pieces:string[]=[];
  const digest=async(sql:string)=>{
    const rows=(await db.query(sql,[goalId])).rows;
    pieces.push(createHash('sha256').update(JSON.stringify(rows)).digest('hex'));
  };
  await digest(`SELECT t.id,t.state,t.stage,t.attempt_count,t.instruction_revision_id,t.account_id,t.model,
    encode(sha256(convert_to(t.description,'UTF8')),'hex') instruction,
    encode(sha256(convert_to(jsonb_build_array(t.criteria,t.covers,t.resources,t.allowed_accounts,t.required_memories)::text,'UTF8')),'hex') contract,
    m.version,m.control,m.input_digests,r.id run_id,r.state run_state,r.error_code,
    (SELECT jsonb_build_array(a.id,a.passed,encode(sha256(convert_to(a.checks::text,'UTF8')),'hex'))
      FROM acceptances a WHERE a.run_id=r.id ORDER BY a.created_at DESC,a.id DESC LIMIT 1) acceptance,
    (SELECT jsonb_agg(d.depends_on ORDER BY d.depends_on) FROM dependencies d WHERE d.task_id=t.id) dependencies,
    (SELECT jsonb_agg(jsonb_build_array(a.id,a.name,a.digest,a.size) ORDER BY a.id) FROM artifacts a WHERE a.run_id=r.id) artifacts
    FROM tasks t LEFT JOIN task_execution_metadata m ON m.task_id=t.id
    LEFT JOIN LATERAL(SELECT id,state,error_code FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    WHERE t.goal_id=$1 AND t.stage IS DISTINCT FROM 'coordinate' ORDER BY t.id`);
  await digest(`SELECT id,task_id,source_run_id,state,decision_run_id,applied_attempt,support_task_ids,
    encode(sha256(convert_to(evidence::text,'UTF8')),'hex') evidence
    FROM supervision_cases WHERE goal_id=$1 ORDER BY id`);
  if((await db.query("SELECT to_regclass('public.planning_requests') present")).rows[0].present)
    await digest(`SELECT id,scope,state,source_revision,task_id,receipt FROM planning_requests WHERE goal_id=$1 ORDER BY id`);
  const consumption=await readDeliveryConsumptionPage(db,goalId,{activeOnly:true,limit:1});
  const accounts=(await db.query(`SELECT id,provider,model,models,enabled,max_concurrency,allow_unknown,
    connection_status,quota,execution_feedback FROM accounts ORDER BY id`)).rows.map(a=>({id:a.id,
      enabled:a.enabled,maxConcurrency:a.max_concurrency,allowUnknown:a.allow_unknown,
      models:[...new Set<string>([a.model,...(Array.isArray(a.models)?a.models:[])])].sort().map(model=>[model,quotaFor(a,model).status])}));
  pieces.push(createHash('sha256').update(JSON.stringify(accounts)).digest('hex'));
  await digest(`SELECT id,value FROM settings WHERE id IN('capacity','remote-capacity','workforce:'||$1::text,
    'objective-capacity:'||$1::text) ORDER BY id`);
  pieces.push(consumption.snapshotDigest,workforceFingerprint??'',(await readLiveFailureReview(db,goalId)).materiality??'');
  pieces.push(createHash('sha256').update(JSON.stringify((await readCanonicalRepairRejections(db,goalId)).map((row:any)=>
    [row.runId,row.causeDigest,row.ownerCaseId,row.ownerState,row.disposition]))).digest('hex'));
  return createHash('sha256').update(JSON.stringify(pieces)).digest('hex');
}

const qualified=new Set(['MANAGEMENT_WAIT_ACTOR','MANAGEMENT_WAIT_CAPACITY','MANAGEMENT_DISPATCH_OWNED',
  'MANAGEMENT_WAIT_DEPENDENCIES','MANAGEMENT_SUPERVISION_OWNED','MANAGEMENT_PLAN_OWNED',
  'MANAGEMENT_WAIT_PLANNER','MANAGEMENT_SCOPE_WAIT']);

/** Check all scopes even if the current prompt omits the obligation. Only the
 * explicit source-review wait backed by a live consumer is non-actionable. */
export async function hasActionableManagementConsumption(db:DB,goalId:string) {
  if((await db.query("SELECT to_regclass('public.planning_requests') present")).rows[0].present &&
    (await db.query(`SELECT 1 FROM planning_requests WHERE goal_id=$1 AND state='awaiting_supervision'
      AND (receipt->>'code'='PLANNING_REMAINDER_REQUIRED' OR receipt ? 'remainingProjectionFailure') LIMIT 1`,[goalId])).rowCount)return true;
  if(!(await db.query("SELECT to_regclass('public.delivery_consumptions') present")).rows[0].present)return false;
  return !!(await db.query(`SELECT 1 FROM delivery_consumptions c WHERE c.goal_id=$1
    AND c.state IN('blocked','waiting_supervision') AND NOT (
      COALESCE(c.receipt->>'code','')='SOURCE_REVIEW_WAIT_CONSUMER' AND EXISTS(SELECT 1 FROM runs r
        WHERE r.task_id=c.consumer_task_id AND r.state IN('queued','running','cancelling','reconciling')))
    LIMIT 1`,[goalId])).rowCount;
}

/** Only a consumed, still-qualified wait can coalesce timer wakes. An open case,
 * failed review, retrieval request or changed material fact always escapes. */
export function canWaitForManagementFacts(previous:any,currentFingerprint:string,followThrough:any,
  input:{runId?:string;runState?:string;openCases:number;contextRepair:boolean;refill:boolean;actionableObligations?:boolean}) {
  return input.runState==='submitted' && !!input.runId && previous?.runId===input.runId &&
    previous?.wakeMateriality===currentFingerprint && !!previous?.reviewedAt &&
    !(previous.receipts??[]).some((r:any)=>['MANAGEMENT_FORMAT','CAPACITY_ASSESSMENT_REQUIRED','OPERATION_REPLAN_REQUIRED'].includes(r.code)) &&
    !input.contextRepair && !input.refill && !input.actionableObligations && input.openCases===0 &&
    Array.isArray(previous.contextRequests) && previous.contextRequests.length===0 &&
    followThrough?.satisfied===true && qualified.has(followThrough.code);
}
