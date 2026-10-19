/** Runtime composition over the unchanged schema-authority module.
 * Installers and guards remain in planning-demand.ts; this entrypoint adds
 * no-work reconciliation without changing the persisted migration closure. */
import {randomUUID,createHash} from 'node:crypto';
import {pool,tx,lock,event,type DB} from './db.js';
import {readArtifact} from './service.js';
import {readWorkforceHealth} from './workforce-health.js';
import {readObjectiveFrontier} from './work-frontier.js';
import {applyBacklog} from './backlog.js';
import {recordScopedPlanningEffects,recoverPartialPlanningRemainders} from './planning-remainders.js';
import {scopedSourceRevision,validateScopedPlan,scopedPlanningRejection} from './scoped-plan-contract.js';
import {inspectStopped} from './continuation.js';
import {applyPlanningDependencyChange} from './planning-dependencies.js';
import {retireSupersededPlanningTasks} from './planning-retirement.js';
import {applyScopedPlanAction} from './scoped-plan-actions.js';
import {checkedExecutionMetadata,executionMetadataColumns,taskExecutionMetadataVersion} from './task-execution-metadata.js';
import {noWorkDecisionUnchanged,planningDecisionMateriality,reconcileNoWorkBlockers,readLatestPlanningDecision,evidencedNoWorkDecision,observedNoWorkRevision} from './planning-no-work.js';
import {scopeSnapshot,scopeSourceRevision,planningSourceFence,observeRunningScopedPlanner,
  refreshScopedPlanningFeedback,retryRejectedScopedPlan,rejectScopedPlanningResult,
  recoverScopedPlannerContexts,reconcilePendingStrategyIntents,assignPlanningRequest,readScopedPlannerCapacity} from './planning-demand.js';
import {scopedPlannerContextVersion} from './scoped-planner-context.js';
import {plannerCatalogInputVersion} from '../scripts/planner-catalog-transport.mjs';
import {plannerPreparationBindingVersion} from './planner-preparation-binding.js';
import {keepOperatorOpenCodePlannerInSupervision,operatorOpenCodeRecoveryOwnsScope} from './operator-opencode-recovery.js';
import {reconcileOperatorOpenCodePlannerHandoffs} from './operator-opencode-planner-handoff.js';

async function scopeTaskIds(db:DB,goalId:string,scope:string){
  // Keep missing rows in the selection so an incomplete projection cannot make
  // a scope look empty. The normal path reads only indexed scalar metadata.
  return checkedExecutionMetadata((await db.query(`SELECT t.id,${executionMetadataColumns}
    FROM tasks t LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    WHERE t.goal_id=$1 AND (metadata.base_workspace=$2 OR metadata.task_id IS NULL OR metadata.version<>${taskExecutionMetadataVersion} OR metadata.goal_id<>t.goal_id)
    ORDER BY t.id`,[goalId,scope])).rows,{objectRequired:false}).map(row=>row.id);
}

async function scopeDeliverySignal(db:DB,goalId:string,scope:string){
  const scopeIds=await scopeTaskIds(db,goalId,scope);
  const rows=(await db.query(`SELECT DISTINCT ON(t.id,e.kind) e.id event_id,e.kind,t.id task_id,r.id run_id,a.checks
    FROM tasks t JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count AND r.state='submitted'
    JOIN LATERAL(SELECT passed,checks FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
    JOIN events e ON e.entity_id=t.id::text AND e.payload->>'runId'=r.id::text
    WHERE t.goal_id=$1 AND t.id=ANY($2::uuid[])
      AND metadata.work_kind IS DISTINCT FROM 'planning'
      AND ((e.kind='TASK_ACCEPTED' AND a.passed AND t.state='accepted' AND (
        (t.stage IN('work','integrate','verify') AND COALESCE(metadata.work_kind,'') NOT IN('diagnostic','repair')) OR
        (metadata.work_kind='repair' AND EXISTS(SELECT 1 FROM artifacts x WHERE x.run_id=r.id AND x.name='implementation.patch')))) OR
      (e.kind='VALIDATION_PENDING' AND NOT a.passed AND t.stage IN('work','integrate','verify')
        AND COALESCE(metadata.work_kind,'') NOT IN('diagnostic','repair')))
    ORDER BY t.id,e.kind,e.id DESC`,[goalId,scopeIds])).rows;
  if(!rows.length)return null;
  const facts=rows.map(row=>({kind:row.kind,taskId:row.task_id,
    ...(row.kind==='TASK_ACCEPTED'?{runId:row.run_id}:{failedCriteria:(row.checks??[])
      .filter((check:any)=>check.passed===false).map((check:any)=>[check.id,check.type]).sort()})}));
  return {key:createHash('sha256').update(JSON.stringify(facts)).digest('hex'),
    events:rows.map(({checks,...row})=>row),facts};
}

export async function createPlanningDemands(goalId:string,snapshotReader:typeof scopeSnapshot=scopeSnapshot){
  const prepared=await tx(async db=>{
    await db.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    if(!(await db.query("SELECT 1 FROM goals WHERE id=$1 AND state='active'",[goalId])).rowCount)return [];
    const policy=(await db.query('SELECT value FROM settings WHERE id=$1',['workforce:'+goalId])).rows[0]?.value;
    if(!policy?.enabled || !policy.glmAccountId)return [];
    const health=await readWorkforceHealth(db,goalId,policy);
    const allLanes=[{lane:'glm',ready:health.glm.admittedReady,target:policy.glmTarget??12},
      {lane:'muse',ready:health.muse.ready,target:policy.museTarget??0}].filter(l=>l.target>0);
    const deficientLanes=allLanes.filter(l=>l.ready<Math.max(1,Math.ceil(l.target/2)));
    const frontier=await readObjectiveFrontier(db,goalId);
    if(frontier.complete)return [];
    const proposals:any[]=[];
    for(const entry of frontier.scopeSummary.filter(s=>s.pending>0)){
      if(!/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(entry.scope))continue;
      if(await operatorOpenCodeRecoveryOwnsScope(db,goalId,entry.scope))continue;
      if((await db.query("SELECT 1 FROM delivery_transitions WHERE goal_id=$1 AND scope=$2 AND state='awaiting_planner' LIMIT 1",[goalId,entry.scope])).rowCount)continue;
      if((await db.query("SELECT 1 FROM planning_requests WHERE goal_id=$1 AND scope=$2 AND state IN('pending','assigned','running','preparing','replanning_required')",[goalId,entry.scope])).rowCount)continue;
      if((await db.query(`SELECT 1 FROM tasks t JOIN task_execution_metadata metadata ON metadata.task_id=t.id
        JOIN runs r ON r.task_id=t.id WHERE t.goal_id=$1
        AND metadata.base_workspace=$2 AND metadata.work_kind='planning'
        AND r.state IN('queued','running','cancelling','reconciling') LIMIT 1`,[goalId,entry.scope])).rowCount)continue;
      const sourceFence=await planningSourceFence(db,goalId,entry.scope);
      const revision=await scopeSourceRevision(db,goalId,entry.scope);
      const demandSignal=await scopeDeliverySignal(db,goalId,entry.scope);
      if(!deficientLanes.length && !demandSignal)continue;
      const lanes=deficientLanes.length?deficientLanes:allLanes.filter(l=>l.lane==='glm');
      // A rejected/no_work plan is retained until actual inputs/candidate change,
      // rather than spinning every timer tick to inflate session counts.
      if((await db.query(`SELECT 1 FROM planning_requests WHERE goal_id=$1 AND scope=$2 AND source_revision=$3
        AND ($4::text IS NULL OR snapshot->'demandSignal'->>'key'=$4)`,[goalId,entry.scope,revision,demandSignal?.key??null])).rowCount)continue;
      const priorMetadata=await readLatestPlanningDecision(db,goalId,entry.scope,{catalog:false});
      if(observedNoWorkRevision(priorMetadata,revision,demandSignal?.key??null))continue;
      const snapshot=await snapshotReader(db,goalId,entry.scope);
      // Catalog and revision come from the same repeatable read. Admission
      // below rejects source changes by cooperating or independent writers.
      if(scopedSourceRevision(snapshot.tasks,snapshot.artifacts,snapshot.deferredDeliveries)!==revision)continue;
      const prior=await readLatestPlanningDecision(db,goalId,entry.scope);
      // Source authority still changes and remains fenced. Only a previously
      // evidenced no_work with identical decision inputs can coalesce metadata.
      const previousNoWork=evidencedNoWorkDecision(prior)?{requestId:prior.id,
          reconciliation:await reconcileNoWorkBlockers(db,goalId,entry.scope,prior.receipt.blockers)}:undefined;
      if(noWorkDecisionUnchanged(prior,snapshot,demandSignal?.key??null)){
        proposals.push({suppressed:true,scope:entry.scope,revision,sourceFence,policy,demandSignal,
          priorDecision:{id:prior.id,rowVersion:prior.row_version},noWorkReconciliation:previousNoWork!.reconciliation,
          decisionDemandKey:planningDecisionMateriality(snapshot,demandSignal?.key??null)});
        continue;
      }
      proposals.push({scope:entry.scope,revision,sourceFence,priorDecision:prior?{id:prior.id,rowVersion:prior.row_version}:null,snapshot:{...snapshot,
        decisionDemandKey:planningDecisionMateriality(snapshot,demandSignal?.key??null),
        ...(previousNoWork?{previousNoWork}:{}),...(demandSignal?{demandSignal}:{})},lanes,policy,demandSignal});
    }
    return proposals;
  });
  const created:string[]=[];
  for(const proposal of prepared)await tx(async db=>{
    await lock(db);
    const {scope,revision,sourceFence,snapshot,lanes,policy,demandSignal,priorDecision}=proposal;
    const current=(await db.query(`SELECT g.state,
      (SELECT value FROM settings WHERE id='workforce:'||g.id) policy,
      (SELECT value FROM settings WHERE id='autonomy:'||g.project) autonomy
      FROM goals g WHERE g.id=$1`,[goalId])).rows[0];
    if(current?.state!=='active' || current.autonomy?.enabled===false ||
      JSON.stringify(current.policy)!==JSON.stringify(policy) ||
      await planningSourceFence(db,goalId,scope)!==sourceFence)return;
    if(await operatorOpenCodeRecoveryOwnsScope(db,goalId,scope))return;
    if((await db.query("SELECT 1 FROM delivery_transitions WHERE goal_id=$1 AND scope=$2 AND state='awaiting_planner' LIMIT 1",[goalId,scope])).rowCount)return;
    if((await scopeDeliverySignal(db,goalId,scope))?.key!==demandSignal?.key)return;
    // Recheck the decision identity without detoasting a historical catalog
    // while holding the admission lock. Changed evidence gets a fresh read.
    const currentDecision=(await db.query(`SELECT id,xmin::text row_version FROM planning_requests
      WHERE goal_id=$1 AND scope=$2 ORDER BY created_at DESC,id DESC LIMIT 1`,[goalId,scope])).rows[0];
    if((currentDecision?.id??null)!==(priorDecision?.id??null) ||
      (currentDecision?.row_version??null)!==(priorDecision?.rowVersion??null))return;
    if(proposal.suppressed){
      const receipt={noWorkDecision:{version:1,sourceRevision:revision,signalKey:demandSignal?.key??null,
        decisionDemandKey:proposal.decisionDemandKey},noWorkReconciliation:proposal.noWorkReconciliation};
      await db.query(`UPDATE planning_requests SET receipt=receipt||$2::jsonb,updated_at=clock_timestamp()
        WHERE id=$1 AND state='blocked_external'`,[priorDecision!.id,JSON.stringify(receipt)]);
      await event(db,'SCOPED_NO_WORK_RECONCILED',goalId,{requestId:priorDecision!.id,scope,
        sourceRevision:revision,decisionDemandKey:proposal.decisionDemandKey,authority:'observation_only'});
      return;
    }
    if((await db.query(`SELECT 1 FROM planning_requests WHERE goal_id=$1 AND scope=$2 AND
      ((source_revision=$3 AND ($4::text IS NULL OR snapshot->'demandSignal'->>'key'=$4)) OR
        state IN('pending','assigned','running','preparing','replanning_required'))`,
      [goalId,scope,revision,demandSignal?.key??null])).rowCount)return;
    const demandCounts=(await db.query(`SELECT lane,count(*)::int n FROM planning_requests WHERE goal_id=$1
        AND state IN('pending','assigned','running','preparing','replanning_required') GROUP BY lane`,[goalId])).rows;
      const id=randomUUID(),lane=[...lanes].sort((a,b)=>
        (a.ready+(demandCounts.find(c=>c.lane===a.lane)?.n??0)*Math.max(1,a.target/2))/a.target-
        (b.ready+(demandCounts.find(c=>c.lane===b.lane)?.n??0)*Math.max(1,b.target/2))/b.target)[0].lane;
      const epoch=Number((await db.query('SELECT COALESCE(max(demand_epoch),0)+1 epoch FROM planning_requests WHERE goal_id=$1 AND scope=$2 AND source_revision=$3',[goalId,scope,revision])).rows[0].epoch);
      const reason=demandSignal?'DELIVERY_EVIDENCE_CHANGED':'READY_RESERVE_DEFICIT';
      await db.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,demand_epoch,reason,priority,snapshot)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,[id,goalId,scope,lane,revision,epoch,reason,demandSignal?75:65,JSON.stringify(snapshot)]);
      await event(db,'PLANNING_DEMAND_CREATED',goalId,{requestId:id,scope,lane,sourceRevision:revision,reason,demandSignalKey:demandSignal?.key});
      created.push(id);
  });
  return created;
}

export async function reconcilePlanningDemand(observe?:(phase:{name:string;durationMs:number})=>void){
  let phaseStarted=Date.now();
  const phase=(name:string)=>{const now=Date.now();try{observe?.({name,durationMs:now-phaseStarted});}catch{/* metrics cannot change admission */}phaseStarted=now;};
  await reconcileOperatorOpenCodePlannerHandoffs();
  phase('operator_opencode_planner_handoffs');
  await recoverPartialPlanningRemainders(readArtifact);
  phase('recover_partial_plans');
  await retireSupersededPlanningTasks();
  phase('retire_superseded');
  await recoverScopedPlannerContexts();
  phase('recover_contexts');
  const completed=(await pool.query(`SELECT p.*,t.state task_state,r.id run_id,r.fence,r.error_code,a.id artifact_id,a.digest artifact_digest FROM planning_requests p
    JOIN goals g ON g.id=p.goal_id AND g.state='active'
    LEFT JOIN settings autonomy ON autonomy.id='autonomy:'||g.project
    JOIN tasks t ON t.id=p.task_id LEFT JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    LEFT JOIN artifacts a ON a.run_id=r.id AND a.name='scope-plan.json'
    WHERE COALESCE(autonomy.value->>'enabled','true')='true' AND (p.state IN('assigned','running','preparing','replanning_required') OR
      (p.state='awaiting_supervision' AND (t.state IN('queued','running','cancelling','reconciling') OR
        (t.state='accepted' AND p.receipt->>'evaluatedRunId' IS DISTINCT FROM r.id::text))))
    ORDER BY p.created_at`)).rows;
  for(const request of completed){
    if(request.run_id&&await keepOperatorOpenCodePlannerInSupervision(request.id,request.run_id))continue;
    if(['queued','running','cancelling','reconciling'].includes(request.task_state)){
      await observeRunningScopedPlanner(request);continue;}
    if(request.state==='replanning_required'){
      await refreshScopedPlanningFeedback(request);
      try{await retryRejectedScopedPlan(request.id,await inspectStopped(request.run_id,true));}
      catch(e:any){
        const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(e.code??e.message??'')?(e.code??e.message):'SCOPED_RETRY_FAILED';
        const detail={code,sourceRunId:request.run_id};
        await tx(async db=>{
          await lock(db);
          const changed=await db.query(`UPDATE planning_requests SET receipt=receipt||$2::jsonb,updated_at=now()
            WHERE id=$1 AND state='replanning_required' AND receipt->'retryBlocked' IS DISTINCT FROM $3::jsonb RETURNING id`,
            [request.id,JSON.stringify({retryBlocked:detail}),JSON.stringify(detail)]);
          if(changed.rowCount)await event(db,'SCOPED_PLANNING_RETRY_BLOCKED',request.goal_id,{requestId:request.id,...detail});
        });
      }
      continue;
    }
    if(['failed','recovery_blocked'].includes(request.task_state)){
      request.receipt={code:request.error_code??'PLANNER_EXECUTION_FAILED'};
      await refreshScopedPlanningFeedback(request);
      await rejectScopedPlanningResult(request,request.receipt);continue;
    }
    if(request.task_state!=='accepted' || !request.artifact_id)continue;
    try{
      const bytes=(await readArtifact(request.artifact_id)).bytes;
      if(createHash('sha256').update(bytes).digest('hex')!==request.artifact_digest)throw Error('ARTIFACT_CORRUPT');
      const plan=validateScopedPlan(JSON.parse(bytes.toString()),request);
      const dependencyReceipts=[];
      for(const change of plan.dependencyChanges)dependencyReceipts.push(await applyPlanningDependencyChange(request,change));
      const deliveryActionReceipts=[];
      for(const action of plan.actions)deliveryActionReceipts.push(await applyScopedPlanAction(request,action));
      const created=await applyBacklog(request.goal_id,request.run_id,{reason:plan.reason,tasks:plan.tasks},{durablePreparation:true});
      const receipt=(await pool.query('SELECT state,receipts,task_ids FROM workforce_backlog_plans WHERE run_id=$1',[request.run_id])).rows[0];
      const preparing=receipt?.receipts?.some((r:any)=>r.code==='WORKSPACE_PREPARING') || dependencyReceipts.some(r=>r.preparing) || deliveryActionReceipts.some(r=>r.preparing);
      const refusedAction=deliveryActionReceipts.some(r=>!r.applied && !r.preparing);
      const state=preparing?'preparing':refusedAction?'awaiting_supervision':created.length || receipt?.task_ids?.length || dependencyReceipts.some(r=>r.applied) || deliveryActionReceipts.some(r=>r.applied)?'applied':!plan.tasks.length?'blocked_external':'replanning_required';
      await recordScopedPlanningEffects(request,{id:request.artifact_id,digest:request.artifact_digest},plan,
        {code:refusedAction?'DELIVERY_ACTION_REVIEW_REQUIRED':!plan.tasks.length && !plan.dependencyChanges.length && !plan.actions.length?'EVIDENCED_NO_WORK':plan.actions.length?'DELIVERY_ACTIONS_RECORDED':receipt?.state,
          evaluatedRunId:request.run_id,blockers:plan.blockers,backlog:receipt,dependencyReceipts,deliveryActionReceipts},state);
    }catch(e:any){
      if(['WORKFORCE_DISABLED','GOAL_NOT_ACTIVE'].includes(e.message))continue;
      const receipt=Array.isArray(e.issues)||e instanceof SyntaxError?scopedPlanningRejection(e):
        {code:/^[A-Z][A-Z0-9_]{2,80}$/.test(e.message??'')?e.message:'SCOPED_PLAN_INVALID'};
      await rejectScopedPlanningResult(request,receipt);
    }
  }
  phase('apply_completed_plans');
  await reconcilePendingStrategyIntents();
  phase('pending_strategies');
  const goals=(await pool.query("SELECT g.id FROM goals g JOIN settings s ON s.id='workforce:'||g.id WHERE g.state='active' AND s.value->>'enabled'='true'")).rows;
  for(const goal of goals)await createPlanningDemands(goal.id);
  phase('create_demands');
  const pending=(await pool.query("SELECT id FROM planning_requests WHERE state='pending' ORDER BY priority DESC,created_at LIMIT 8")).rows;
  for(const request of pending){
    try{await assignPlanningRequest(request.id);}
    catch(e:any){
      const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(e.code??e.message??'')?(e.code??e.message):'PLANNING_PREPARATION_FAILED';
      await pool.query("UPDATE planning_requests SET state='awaiting_supervision',receipt=$2,updated_at=now() WHERE id=$1 AND state='pending'",[
        request.id,JSON.stringify({code,contextProjectionVersion:scopedPlannerContextVersion,catalogInputVersion:plannerCatalogInputVersion,preparationBindingVersion:plannerPreparationBindingVersion,nextAction:'Gestor deve resolver a preparação/contexto deste escopo; outros escopos continuam.'})]);
      await event(pool,'SCOPED_PLANNING_PREPARATION_BLOCKED',request.id,{code,requestId:request.id});
    }
  }
  phase('assign_planners');
  for(const goal of goals){
    const requests=(await pool.query(`SELECT id,scope,lane,state,task_id,jsonb_build_object('code',receipt->>'code',
      'blockers',COALESCE(receipt->'blockers','[]')) receipt,created_at,updated_at
      FROM planning_requests WHERE goal_id=$1 ORDER BY created_at DESC LIMIT 20`,[goal.id])).rows;
    const preparations=(await pool.query("SELECT state,count(*)::int count FROM workspace_preparations WHERE goal_id=$1 GROUP BY state",[goal.id])).rows;
    const compactRequests=requests.map(r=>({...r,receipt:{code:r.receipt?.code,blockers:r.receipt?.blockers?.slice(0,3).map((b:any)=>({
      taskId:b.taskId,criterionId:b.criterionId,reason:b.reason?.slice(0,160),nextAction:b.nextAction?.slice(0,160),
      blockedByTaskIds:b.blockedByTaskIds?.slice(0,3),artifactIds:b.artifactIds?.slice(0,3)})),
      omittedBlockers:Math.max(0,(r.receipt?.blockers?.length??0)-3)}}));
    await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',[
      'planning-demand:'+goal.id,JSON.stringify({checkedAt:new Date().toISOString(),
        ...await readScopedPlannerCapacity(pool,goal.id),requests:compactRequests,preparations})]);
  }
  phase('publish_status');
}
