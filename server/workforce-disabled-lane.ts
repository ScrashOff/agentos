import { isDeepStrictEqual } from 'node:util';
import { pool, tx, lock, event, type DB } from './db.js';
import { readAcceptedAuxiliaryProof } from './accepted-auxiliary.js';
import { assertStopped, inspectStopped } from './continuation.js';
import { deliveryDigest, requestDeliverySupervision } from './delivery-supervision.js';
import { chooseWorkforceRoute } from './workforce-policy.js';
import { checkedExecutionMetadata, executionMetadataColumns, taskExecutionMetadataVersion } from './task-execution-metadata.js';

const idle = ['ready','waiting_resource','waiting_quota','waiting_configuration','blocked_dependencies','blocked_failed','failed','submitted','recovery_blocked'];
const contractKeys = ['id','goal_id','key','title','description','agent_id','stage','criteria','execution',
  'required_memories','resources','allowed_accounts','instruction_revision_id'];
const contract = (task: any) => Object.fromEntries(contractKeys.map(key => [key, task[key]]));
const contractHash = (task: any) => deliveryDigest(contract(task));
function heldRouteHash(task: any, caseId: string, runId: string) {
  const continuation=task.execution?.continuation;
  if(continuation?.owner!=='supervisor'||continuation.caseId!==caseId||continuation.runId!==runId||
    continuation.state!=='supervising'||!Number.isFinite(Date.parse(continuation.updatedAt))||
    !Number.isFinite(Date.parse(continuation.nextAt)))throw Error('SUPERVISION_HANDOFF_CHANGED');
  // Only the two clocks renewed by the existing hold trigger are normalized.
  // This is future-route authority, never the acceptance proof classifier.
  const {updatedAt,nextAt,...stable}=continuation;
  return contractHash({...task,execution:{...task.execution,continuation:stable}});
}
const disabled = (task: any, policy: any) => policy?.enabled === true && policy.museTarget === 0 &&
  task?.stage === 'work' && task.execution?.kind === 'harness' && task.execution.workforceLane === 'muse';

/** All DB facts used by the existing proof classifier, plus authority and actors.
 * This is bounded to one task and its explicit inputs. Bytes/SSH are read only
 * outside 4280001; the full read-set is compared again inside that lock. */
async function readState(db: DB, taskId: string) {
  const task = (await db.query('SELECT * FROM tasks WHERE id=$1',[taskId])).rows[0];
  if (!task) return null;
  const authority = (await db.query(`SELECT g.id,g.state,g.project,w.value policy,s.value supervision,a.value autonomy
    FROM goals g LEFT JOIN settings w ON w.id='workforce:'||g.id
    LEFT JOIN settings s ON s.id='supervision:'||g.id LEFT JOIN settings a ON a.id='autonomy:'||g.project WHERE g.id=$1`,[task.goal_id])).rows[0];
  const run = (await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[taskId])).rows[0];
  const acceptance = run && (await db.query('SELECT id,passed,checks FROM acceptances WHERE task_id=$1 AND run_id=$2 ORDER BY created_at DESC LIMIT 1',[taskId,run.id])).rows[0];
  const context = run?.context_id && (await db.query('SELECT id,run_id,manifest FROM contexts WHERE id=$1',[run.context_id])).rows[0];
  const actors = (await db.query(`SELECT r.id,r.state,EXISTS(SELECT 1 FROM leases l WHERE l.run_id=r.id) leased
    FROM runs r WHERE r.task_id=$1 AND (r.state IN('queued','running','cancelling','reconciling') OR
      EXISTS(SELECT 1 FROM leases l WHERE l.run_id=r.id)) ORDER BY r.id`,[taskId])).rows;
  const cancelled = !!run && !!(await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND entity_id=$1 AND payload->>'runId'=$2 LIMIT 1",[taskId,run.id])).rowCount;
  const pending = (await db.query("SELECT id,state FROM supervision_cases WHERE task_id=$1 AND state IN('open','decided','delegated','waiting_evidence') ORDER BY id",[taskId])).rows;
  const inputIds = Array.isArray(task.execution?.inputs) ? task.execution.inputs.map((i: any) => i?.id).filter((id: any) => typeof id === 'string') : [];
  const inputs = inputIds.length ? (await db.query(`SELECT a.id,a.name,a.digest,a.size,a.run_id,r.task_id,r.attempt,r.state,
    r.instruction_snapshot->'task'->'execution'->>'workspace' source_workspace,t.goal_id,
    (SELECT jsonb_build_object('id',v.id,'passed',v.passed) FROM acceptances v WHERE v.run_id=r.id ORDER BY created_at DESC LIMIT 1) acceptance
    FROM artifacts a JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id WHERE a.id::text=ANY($1::text[]) ORDER BY a.id`,[inputIds])).rows : [];
  const outputs = run ? (await db.query('SELECT id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[run.id])).rows : [];
  return {task,authority,run,acceptance,context,actors,cancelled,pending,inputs,outputs};
}
function eligible(state: any, allowPending = false) {
  return state && disabled(state.task,state.authority?.policy) && idle.includes(state.task.state) &&
    state.authority.state === 'active' && state.authority.supervision?.enabled === true &&
    state.authority.autonomy?.enabled !== false &&
    (state.task.recovery?.mode !== 'manual' || state.authority.autonomy?.enabled === true) &&
    state.run?.state === 'submitted' && state.run.attempt === state.task.attempt_count &&
    state.acceptance?.passed === true && !state.cancelled && !state.actors.length &&
    (allowPending || !state.pending.length);
}
function failureKind(state: any, proof: any) {
  if (state.run?.instruction_snapshot?.schema !== 1) return 'LEGACY_SNAPSHOT_MISSING';
  if (!isDeepStrictEqual(contract(state.task),contract(state.run.instruction_snapshot.task ?? {})))
    return 'FROZEN_CONTRACT_CHANGED';
  return proof.reason;
}

/** Restore only the projection of an already proved, stopped auxiliary result.
 * Otherwise create an actionable case for an explicit future-route revision.
 * No quota inference, automatic retry, acceptance rewrite or model call. */
export async function reconcileDisabledWorkforceLanes(inspect = inspectStopped) {
  const cursorKey='disabled-workforce-lane-cursor';
  const previous=(await pool.query('SELECT value FROM settings WHERE id=$1',[cursorKey])).rows[0]?.value;
  const cursor=typeof previous?.taskId==='string' && /^[a-f0-9-]{36}$/.test(previous.taskId) ? previous.taskId : null;
  const rows=checkedExecutionMetadata((await pool.query(`SELECT t.id,${executionMetadataColumns}
    FROM tasks t JOIN goals g ON g.id=t.goal_id JOIN settings w ON w.id='workforce:'||g.id
    LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    WHERE g.state='active' AND w.value->>'enabled'='true' AND w.value->'museTarget'='0'::jsonb
      AND t.stage='work' AND t.state=ANY($1::text[]) AND r.state='submitted'
      AND (SELECT passed FROM acceptances v WHERE v.task_id=t.id AND v.run_id=r.id ORDER BY created_at DESC LIMIT 1)=true
      AND NOT EXISTS(SELECT 1 FROM supervision_cases c WHERE c.task_id=t.id AND c.state IN('open','decided','delegated','waiting_evidence'))
      AND (metadata.task_id IS NULL OR metadata.version<>$2 OR metadata.goal_id<>t.goal_id OR
        (metadata.control->>'kind'='harness' AND metadata.control->>'workforceLane'='muse'))
    ORDER BY CASE WHEN $3::uuid IS NULL OR t.id>$3::uuid THEN 0 ELSE 1 END,t.id LIMIT 8`,
    [idle,taskExecutionMetadataVersion,cursor])).rows);
  const results: {taskId:string;code:string;caseId?:string}[]=[];
  for (const row of rows) {
    try {
      // The proof sees one immutable DB snapshot. It may read retained files,
      // but never holds the scheduler/global advisory lock while doing so.
      const observed=await tx(async db=>{
        await db.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
        const state=await readState(db,row.id);
        if(!eligible(state))return null;
        return {state};
      });
      if(!observed){results.push({taskId:row.id,code:'TARGET_UNAVAILABLE'});continue;}
      const stop=await inspect(observed.state!.run.id,false);
      const verified=await tx(async db=>{
        await db.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
        const state=await readState(db,row.id);
        if(!eligible(state)||deliveryDigest(state)!==deliveryDigest(observed.state))return null;
        return {state,proof:await readAcceptedAuxiliaryProof(db,row.id)};
      });
      if(!verified){results.push({taskId:row.id,code:'TARGET_CHANGED'});continue;}
      const result=await tx(async db=>{
        await lock(db);
        const state=await readState(db,row.id);
        if(!eligible(state) || deliveryDigest(state)!==deliveryDigest(verified.state))return {taskId:row.id,code:'TARGET_CHANGED'};
        const {task,run,authority,acceptance}=state!;
        assertStopped(stop,run,task);
        if(verified.proof.eligible){
          await db.query("UPDATE tasks SET state='accepted',reason='Aceite anterior comprovado; projeção recuperada sem nova execução.',updated_at=now() WHERE id=$1",[task.id]);
          await event(db,'ACCEPTED_AUXILIARY_PROJECTION_RESTORED',task.id,{runId:run.id,attempt:run.attempt,
            acceptanceId:acceptance.id,proof:verified.proof,stopCheckedAt:stop.checkedAt,lane:'muse'});
          return {taskId:task.id,code:'ACCEPTED_PROJECTION_RESTORED'};
        }
        const generation=deliveryDigest({runId:run.id,fence:run.fence,contractHash:contractHash(task),
          acceptanceId:acceptance.id,policy:authority.policy});
        const evidence={kind:'WORKFORCE_LANE_DISABLED',version:1,code:'WORKFORCE_LANE_RECERTIFICATION_REQUIRED',
          obligationKey:'disabled-lane:'+task.id,generation,taskId:task.id,runId:run.id,runFence:run.fence,
          taskContractHash:contractHash(task),acceptanceId:acceptance.id,lane:'muse',target:0,
          proofFailure:failureKind(state,verified.proof),proofReason:verified.proof.reason,
          requiredAction:{kind:'revise',lane:'glm',instruction:'Recertifique os critérios originais em uma nova tentativa Harness pela rota GLM autorizada. Confira fonte, entradas, arquivos e limites; preserve os testes e relate qualquer ausência. O aceite histórico não comprova o contrato atual.'},
          preserved:{criteriaHash:deliveryDigest(task.criteria),inputsHash:deliveryDigest(task.execution.inputs??[]),workspace:task.execution.workspace}};
        const requested=await requestDeliverySupervision(db,authority,task,run,evidence,{stopProof:stop});
        if(requested.applied){
          await db.query("UPDATE tasks SET state='recovery_blocked',reason='Rota Muse desativada; supervisor deve recertificar pela rota autorizada.',updated_at=now() WHERE id=$1",[task.id]);
          // The existing supervision-hold trigger adds its continuation envelope
          // on this state transition. Pin that exact result separately instead
          // of stripping technical fields or rewriting the original evidence.
          const held=(await db.query('SELECT * FROM tasks WHERE id=$1',[task.id])).rows[0];
          await event(db,'WORKFORCE_LANE_RECOVERY_REQUESTED',task.id,{caseId:requested.caseId,runId:run.id,
            runFence:run.fence,generation,proofFailure:evidence.proofFailure,
            originalContractHash:evidence.taskContractHash,heldContractHash:contractHash(held),
            heldRouteHash:heldRouteHash(held,requested.caseId!,run.id)});
        }
        return {taskId:task.id,code:requested.code,...('caseId' in requested?{caseId:requested.caseId}:{})};
      });
      results.push(result);
    } catch(error:any) {
      const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(error.message??'')?error.message:'DISABLED_LANE_INSPECTION_FAILED';
      results.push({taskId:row.id,code});
    }
  }
  if(rows.length){
    const next=JSON.stringify({version:1,taskId:rows[rows.length-1].id});
    if(previous===undefined)await pool.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO NOTHING',[cursorKey,next]);
    else await pool.query('UPDATE settings SET value=$2 WHERE id=$1 AND value=$3::jsonb',[cursorKey,next,JSON.stringify(previous)]);
  }
  return {examined:results.length,restored:results.filter(r=>r.code==='ACCEPTED_PROJECTION_RESTORED').length,
    requested:results.filter(r=>r.code==='SUPERVISION_REQUESTED').length,results};
}

/** Called inside the normal applied-revision transaction, before its new
 * instructions are frozen. Only this typed case + explicit accepted lane choice
 * can replace an old Muse allowlist. The runtime and task contract remain Harness. */
export async function applyDisabledLaneRevision(db: DB, caseId: string, action: any, stop: any) {
  await lock(db);
  const c=(await db.query('SELECT * FROM supervision_cases WHERE id=$1 FOR UPDATE',[caseId])).rows[0];
  const evidence=c?.evidence?.find((e:any)=>e.kind==='WORKFORCE_LANE_DISABLED'&&e.version===1);
  if(!evidence)return null;
  if(action.kind!=='revise'||action.lane!=='glm'||action.caseId!==c.id||c.state!=='applied'||!c.decision_run_id||
    !c.decision?.actions?.some((a:any)=>isDeepStrictEqual(a,action)))throw Error('SUPERVISION_ROUTE_UNAVAILABLE');
  const hold=(await db.query(`SELECT payload FROM events WHERE entity_id=$1 AND kind='WORKFORCE_LANE_RECOVERY_REQUESTED'
    AND payload->>'caseId'=$2 AND payload->>'generation'=$3 ORDER BY id DESC LIMIT 1`,
    [c.task_id,c.id,evidence.generation])).rows[0]?.payload;
  const state=await readState(db,c.task_id);
  if(!eligible(state) || c.goal_id!==state!.task.goal_id || c.source_run_id!==state!.run.id || c.applied_attempt!==state!.run.attempt ||
    evidence.runId!==state!.run.id || evidence.runFence!==state!.run.fence ||
    hold?.originalContractHash!==evidence.taskContractHash || hold?.runId!==state!.run.id || hold?.runFence!==state!.run.fence ||
    hold?.heldRouteHash!==heldRouteHash(state!.task,c.id,state!.run.id)||evidence.acceptanceId!==state!.acceptance.id)
    throw Error('SUPERVISION_HANDOFF_CHANGED');
  const {task,run,authority}=state!;
  assertStopped(stop,run,task);
  const acceptedDecision=(await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id
    WHERE r.id=$1 AND r.state='submitted' AND t.goal_id=$2 AND t.stage='coordinate'
      AND (SELECT passed FROM acceptances v WHERE v.run_id=r.id ORDER BY created_at DESC LIMIT 1)=true`,[c.decision_run_id,task.goal_id])).rowCount;
  const policy=authority.policy;
  if(!acceptedDecision||policy.glmHarnessEnabled!==true||policy.detachedHarnessEnabled!==true||!(policy.glmTarget>0))
    throw Error('SUPERVISION_ROUTE_UNAVAILABLE');
  const account=(await db.query('SELECT * FROM accounts WHERE id=$1',[policy.glmAccountId])).rows[0];
  const allowed_accounts=[policy.glmAccountId],execution={...task.execution,workforceLane:'glm'};
  const route=chooseWorkforceRoute({...task,execution,allowed_accounts},account?[account]:[],policy);
  if(!route)throw Error('SUPERVISION_ROUTE_UNAVAILABLE');
  const result={execution,allowed_accounts,account_id:route.accountId,model:route.model};
  await db.query('UPDATE tasks SET execution=$2,allowed_accounts=$3,account_id=$4,model=$5 WHERE id=$1',
    [task.id,JSON.stringify(execution),JSON.stringify(allowed_accounts),route.accountId,route.model]);
  await event(db,'WORKFORCE_DISABLED_LANE_REVISED',task.id,{caseId:c.id,sourceRunId:run.id,sourceFence:run.fence,
    decisionRunId:c.decision_run_id,generation:evidence.generation,stopCheckedAt:stop.checkedAt,
    before:{lane:'muse',accountId:task.account_id,model:task.model,allowedAccounts:task.allowed_accounts},
    after:{lane:'glm',accountId:route.accountId,model:route.model,allowedAccounts:allowed_accounts},
    criteriaHash:deliveryDigest(task.criteria),inputsHash:deliveryDigest(task.execution.inputs??[]),workspace:task.execution.workspace});
  return result;
}
