import {managerCaseEvidenceSource} from './manager-case-evidence.js';
import {protocolReportDiagnostic} from '../bridge/protocol-report-contract.mjs';
import type {DB} from './db.js';
import {deliveryDigest} from './delivery-supervision.js';
import {taskExecutionMetadataVersion} from './task-execution-metadata.js';
import {buildRetryEffectEvidence} from './retry-effect-evidence.js';
import {attachAdministrativeRecoveryReviews} from './manager-administrative-recovery.js';
import {managerReviewOwnerReleased} from './manager-review-owner.js';

/** The same case query used by wakeSupervisors. Enrichment supplies a review
 * hint only; action/stop/source authority remains in the existing appliers. */
export async function readManagerCases(db:DB,goalId:string,materialReview=true) {
 const cases=(await db.query(`SELECT c.*,r.id current_run_id,r.attempt current_attempt,r.state current_run_state,t.state current_task_state,
          r.fence current_run_fence,r.context_id current_context_version,
          decision_review.id decision_review_id,decision_review.state decision_review_state,
          decision_task.goal_id decision_review_goal_id,decision_task.stage decision_review_stage,
          t.attempt_count task_attempt_count,t.stage current_stage,
          left(r.summary,1000) current_summary,r.error_code current_error_code,
          EXISTS(SELECT 1 FROM runs live_case WHERE live_case.task_id=t.id
            AND live_case.state IN('queued','running','cancelling','reconciling')) has_live_run,
          t.execution->>'testFile' assigned_test_file,t.execution->>'baseWorkspace' base_workspace,
          t.execution->>'workKind' work_kind,
          (SELECT count(*)::int FROM dependencies d JOIN tasks p ON p.id=d.depends_on
            WHERE d.task_id=t.id AND p.state<>'accepted') pending_dependencies,
          (SELECT count(*)::int FROM deferred_handoffs h WHERE h.consumer_task_id=t.id AND h.state='applied'
            AND h.applied_at>COALESCE(r.started_at,r.created_at)) fresh_handoffs,
          COALESCE((SELECT jsonb_agg(jsonb_build_object('id',a.id,'name',a.name,'digest',a.digest,'size',a.size))
            FROM (SELECT id,name,digest,size FROM artifacts WHERE run_id=r.id
              ORDER BY CASE name WHEN 'missing-inputs.json' THEN 0 WHEN 'repair-report.json' THEN 1
                WHEN 'protocol-report.json' THEN 2 WHEN 'verification.json' THEN 3 ELSE 4 END,size,id LIMIT 8)a),'[]'::jsonb) current_artifacts
          FROM supervision_cases c JOIN tasks t ON t.id=c.task_id
          LEFT JOIN runs decision_review ON decision_review.id=c.decision_run_id
          LEFT JOIN tasks decision_task ON decision_task.id=decision_review.task_id
          LEFT JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
          WHERE c.goal_id=$1 AND c.state='open' ORDER BY c.created_at,c.id`,[goalId])).rows;
 const reviewed=materialReview?await attachMaterialConsumptionReviews(db,goalId,cases):cases;
 return attachAdministrativeRecoveryReviews(db,goalId,await attachRetryEffectEvidence(db,goalId,reviewed));
}

/** Compare only the latest run to the exact immediately preceding run named by
 * its applied supervisor revision. Metadata-only, bounded query; no workspace,
 * artifact-byte, provider, case-state, or acceptance writes. */
async function attachRetryEffectEvidence(db:DB,goalId:string,cases:any[]) {
 const eligible=cases.filter(c=>c.state==='open'&&c.current_stage==='integrate'&&
  (c.work_kind==null||c.work_kind==='repair')&&
  c.source_run_id===c.current_run_id&&c.current_run_state==='submitted'&&c.has_live_run===false&&
  Number.isSafeInteger(c.current_attempt)&&c.current_attempt===c.task_attempt_count).map(c=>c.id);
 if(!eligible.length)return cases;
 const rows=(await db.query(`SELECT c.id case_id,t.id task_id,t.stage,t.execution->>'workKind' work_kind,
   cur.id current_run_id,cur.attempt current_attempt,cur.state current_run_state,cur.instruction_snapshot current_snapshot,
   current_acceptance.passed current_passed,current_acceptance.checks current_checks,current_context.manifest current_manifest,
   COALESCE(current_diff.items,'[]'::jsonb) current_diffs,
   revision.provenance revision_provenance,
   prev.id previous_run_id,prev.attempt previous_attempt,prev.state previous_run_state,prev.instruction_snapshot previous_snapshot,
   previous_acceptance.passed previous_passed,previous_acceptance.checks previous_checks,previous_context.manifest previous_manifest,
   COALESCE(previous_diff.items,'[]'::jsonb) previous_diffs
   FROM supervision_cases c JOIN tasks t ON t.id=c.task_id AND t.goal_id=c.goal_id
   JOIN runs cur ON cur.id=c.source_run_id AND cur.task_id=t.id
   JOIN task_instruction_revisions revision ON revision.id::text=cur.instruction_snapshot->>'revisionId'
     AND revision.task_id=t.id AND revision.goal_id=t.goal_id AND revision.provenance->>'kind'='revise'
   JOIN runs prev ON prev.id::text=revision.provenance->>'sourceRunId' AND prev.task_id=t.id
     AND revision.provenance->>'sourceAttempt'=prev.attempt::text
   JOIN supervision_cases prior_case ON prior_case.id::text=revision.provenance->>'caseId'
     AND prior_case.task_id=t.id AND prior_case.goal_id=t.goal_id AND prior_case.state='applied'
     AND prior_case.applied_attempt=prev.attempt AND prior_case.decision_run_id::text=revision.provenance->>'decisionRunId'
     AND (prior_case.decision->>'kind'='revise' OR prior_case.decision->'actions' @> '[{"kind":"revise"}]'::jsonb)
   JOIN contexts current_context ON current_context.id=cur.context_id AND current_context.run_id=cur.id
   JOIN contexts previous_context ON previous_context.id=prev.context_id AND previous_context.run_id=prev.id
   JOIN LATERAL(SELECT passed,checks FROM acceptances WHERE task_id=t.id AND run_id=cur.id
     ORDER BY created_at DESC,id DESC LIMIT 1) current_acceptance ON true
   JOIN LATERAL(SELECT passed,checks FROM acceptances WHERE task_id=t.id AND run_id=prev.id
     ORDER BY created_at DESC,id DESC LIMIT 1) previous_acceptance ON true
   LEFT JOIN LATERAL(SELECT jsonb_agg(jsonb_build_object('digest',digest,'size',size) ORDER BY id) items
     FROM artifacts WHERE run_id=cur.id AND name='change.diff') current_diff ON true
   LEFT JOIN LATERAL(SELECT jsonb_agg(jsonb_build_object('digest',digest,'size',size) ORDER BY id) items
     FROM artifacts WHERE run_id=prev.id AND name='change.diff') previous_diff ON true
   WHERE c.id=ANY($1::uuid[]) AND c.goal_id=$2 AND c.state='open' AND c.source_run_id=cur.id
     AND t.stage='integrate' AND t.execution->>'workKind' IS DISTINCT FROM 'diagnostic'
     AND cur.id=(SELECT id FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)
     AND cur.attempt=t.attempt_count AND cur.state='submitted' AND prev.state='submitted'
     AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id
       AND live.state IN('queued','running','cancelling','reconciling'))
     AND current_acceptance.passed=false AND previous_acceptance.passed=false`,[eligible,goalId])).rows;
 const byCase=new Map<string,any>();
 for(const row of rows){
  if(row.revision_provenance?.caseId==null||row.revision_provenance?.decisionRunId==null)continue;
  const item=buildRetryEffectEvidence({id:row.current_run_id,attempt:row.current_attempt,state:row.current_run_state,
    instructionSnapshot:row.current_snapshot,manifest:row.current_manifest,acceptancePassed:row.current_passed,
    checks:row.current_checks,changeDiffs:row.current_diffs},
   {id:row.previous_run_id,attempt:row.previous_attempt,state:row.previous_run_state,
    instructionSnapshot:row.previous_snapshot,manifest:row.previous_manifest,acceptancePassed:row.previous_passed,
    checks:row.previous_checks,changeDiffs:row.previous_diffs},
   {stage:row.stage,workKind:row.work_kind,supervisedRevise:row.revision_provenance.kind==='revise'});
  if(item)byCase.set(row.case_id,item);
 }
 return cases.map(c=>byCase.has(c.id)?{...c,retryEffectEvidence:byCase.get(c.id)}:c);
}

const uuid=/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const digest=/^[a-f0-9]{64}$/;
const live=['queued','running','cancelling','reconciling'];
const reviewStates=['failed','submitted','recovery_blocked'];
const maxManifestEntries=128;

/** Bounded batches of current control metadata, never artifact bytes, frozen
 * prompts or source contract hashes. This is a priority hint, NOT a claim that
 * today's source is still semantically compatible. The authoritative source,
 * contract and physical-stop checks are repeated by the unchanged applier. */
async function attachMaterialConsumptionReviews(db:DB,goalId:string,cases:any[]) {
 const candidates=new Map<string,{entry:any,evidence:any}[]>();
 for(const entry of cases){
  if(!managerReviewOwnerReleased(entry)||
    !reviewStates.includes(entry.current_task_state)||!['failed','submitted'].includes(entry.current_run_state)||
    entry.source_run_id!==entry.current_run_id||!Number.isSafeInteger(entry.current_attempt)||
    entry.current_attempt!==entry.task_attempt_count)continue;
  for(const evidence of Array.isArray(entry.evidence)?entry.evidence:[]){
   const id=typeof evidence?.obligationKey==='string'?evidence.obligationKey.replace(/^delivery-consumption:/,''):'';
   if(evidence?.kind!=='DELIVERY_CONSUMPTION_REQUIRED'||evidence.obligationKey!=='delivery-consumption:'+id||
     !uuid.test(id)||!digest.test(evidence.generation??'')||evidence.consumerTaskId!==entry.task_id||
     evidence.consumerRunId!==entry.current_run_id||!uuid.test(evidence.producerTaskId??'')||
     !uuid.test(evidence.producerRunId??'')||!digest.test(evidence.producerManifestDigest??''))continue;
   const values=candidates.get(id)??[];values.push({entry,evidence});candidates.set(id,values);
  }
 }
 const selected=new Map<string,any[]>(),ids=[...candidates.keys()];
 for(let offset=0;offset<ids.length;offset+=64){
  const rows=(await db.query(`SELECT d.id,d.goal_id,d.handoff_id,d.consumer_task_id,d.scope,d.state,
   d.producer_task_id,d.producer_run_id,d.producer_manifest_digest,
   d.receipt->>'generation' generation,d.receipt->>'caseId' case_id,d.receipt->>'code' code,
   d.receipt->>'owner' owner,d.receipt->'sourceConflict' source_conflict,
   h.state handoff_state,h.producer_task_id handoff_producer,h.consumer_task_id handoff_consumer,
   cr.id consumer_run_id,cr.attempt consumer_attempt,cr.state consumer_run_state,cr.fence consumer_run_fence,
   cr.context_id consumer_context_version,ct.attempt_count consumer_attempt_count,
   current_case.state current_case_state,current_case.source_run_id current_case_source_run_id,
   current_case.decision_run_id current_case_decision_run_id,
   current_review.id current_decision_review_id,current_review.state current_decision_review_state,
   current_review_task.goal_id current_decision_review_goal_id,current_review_task.stage current_decision_review_stage,
   ct.state consumer_state,ct.goal_id consumer_goal,ct.stage consumer_stage,
   cm.base_workspace consumer_scope,cm.control->>'readOnlySource' consumer_readonly,
   cm.version consumer_metadata_version,cm.goal_id consumer_metadata_goal,cm.execution_type consumer_metadata_type,
   p.id source_task_id,p.goal_id source_goal,p.state source_state,p.attempt_count source_attempt_count,
   pm.base_workspace source_scope,pm.work_kind source_work_kind,pm.version source_metadata_version,pm.goal_id source_metadata_goal,pm.execution_type source_metadata_type,
   pr.id source_run_id,pr.attempt source_attempt,pr.state source_run_state,
   (SELECT passed FROM acceptances WHERE task_id=p.id AND run_id=pr.id ORDER BY created_at DESC,id DESC LIMIT 1) source_passed,
   EXISTS(SELECT 1 FROM runs active WHERE active.task_id IN(ct.id,p.id) AND active.state=ANY($3::text[])) actor_live,
   EXISTS(SELECT 1 FROM leases l JOIN runs leased ON leased.id=l.run_id WHERE leased.task_id IN(ct.id,p.id)) actor_leased,
   EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.payload->>'runId' IN(cr.id::text,pr.id::text)) actor_cancelled,
   EXISTS(SELECT 1 FROM supervision_cases owned WHERE owned.task_id=ct.id AND owned.id::text IS DISTINCT FROM d.receipt->>'caseId'
     AND owned.state IN('decided','delegated','waiting_evidence')) decision_owned,
   sr.id::text resolution_event_id,sr.payload->>'version' resolution_version,
   sr.payload->>'obligationId' resolution_obligation,sr.payload->>'consumerTaskId' resolution_consumer,
   sr.payload->'original' resolution_original,
   sr.payload#>>'{replacement,producerRunId}' replacement_run_id,
   sr.payload#>>'{replacement,producerManifestDigest}' replacement_manifest,
   COALESCE(sr.payload#>'{replacement,repairArtifacts}',d.binding->'repairArtifacts') repair_artifacts,
   COALESCE(a.refs,'[]'::jsonb) artifacts
   FROM delivery_consumptions d JOIN deferred_handoffs h ON h.id=d.handoff_id
   JOIN tasks ct ON ct.id=d.consumer_task_id
   JOIN supervision_cases current_case ON current_case.id=CASE
    WHEN d.receipt->>'caseId' ~ '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'
    THEN (d.receipt->>'caseId')::uuid END
    AND current_case.task_id=ct.id AND current_case.goal_id=d.goal_id
   LEFT JOIN runs current_review ON current_review.id=current_case.decision_run_id
   LEFT JOIN tasks current_review_task ON current_review_task.id=current_review.task_id
   JOIN LATERAL(SELECT id,attempt,state,fence,context_id FROM runs WHERE task_id=ct.id ORDER BY attempt DESC LIMIT 1)cr ON true
   LEFT JOIN task_execution_metadata cm ON cm.task_id=ct.id
   LEFT JOIN LATERAL(SELECT id,payload FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=d.id::text ORDER BY id DESC LIMIT 1)sr ON true
   JOIN tasks p ON p.id=CASE WHEN sr.id IS NULL THEN d.producer_task_id
    WHEN sr.payload#>>'{replacement,producerTaskId}' ~ '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'
    THEN (sr.payload#>>'{replacement,producerTaskId}')::uuid END
   LEFT JOIN task_execution_metadata pm ON pm.task_id=p.id
   JOIN LATERAL(SELECT id,attempt,state FROM runs WHERE task_id=p.id ORDER BY attempt DESC LIMIT 1)pr ON true
   LEFT JOIN LATERAL(SELECT jsonb_agg(ref ORDER BY ref.id) refs FROM
     (SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=pr.id ORDER BY id LIMIT $4)ref)a ON true
   WHERE d.goal_id=$1 AND d.id=ANY($2::uuid[])`,[goalId,ids.slice(offset,offset+64),live,maxManifestEntries+1])).rows;
  for(const row of rows){
   const sourceRun=row.resolution_event_id?row.replacement_run_id:row.producer_run_id;
   const sourceManifest=row.resolution_event_id?row.replacement_manifest:row.producer_manifest_digest;
   if(row.resolution_event_id&&(row.resolution_version!=='1'||row.resolution_obligation!==row.id||
     row.resolution_consumer!==row.consumer_task_id||row.resolution_original?.producerTaskId!==row.producer_task_id||
     row.resolution_original?.producerRunId!==row.producer_run_id||row.resolution_original?.producerManifestDigest!==row.producer_manifest_digest))continue;
   // Current receipt, not append-only evidence order, owns conflict state. A
   // restored source can reuse an earlier requestKey without appending it again.
   if(row.state!=='waiting_supervision'||row.code!=='SUPERVISION_REQUESTED'||row.owner!=='supervisor'||row.source_conflict!=null||
     row.handoff_state!=='applied'||row.handoff_producer!==row.producer_task_id||row.handoff_consumer!==row.consumer_task_id||
     !reviewStates.includes(row.consumer_state)||!['work','integrate'].includes(row.consumer_stage)||row.consumer_readonly==='true'||
     !['failed','submitted'].includes(row.consumer_run_state)||row.consumer_attempt!==row.consumer_attempt_count||
     row.source_state!=='accepted'||row.source_run_state!=='submitted'||row.source_attempt!==row.source_attempt_count||
     row.source_passed!==true||row.source_work_kind!=='repair'||row.source_run_id!==sourceRun||
     row.source_goal!==goalId||row.consumer_goal!==goalId||typeof row.scope!=='string'||!row.scope||
     row.source_scope!==row.scope||row.consumer_scope!==row.scope||
     row.source_metadata_version!==taskExecutionMetadataVersion||row.consumer_metadata_version!==taskExecutionMetadataVersion||
     row.source_metadata_type!=='object'||row.consumer_metadata_type!=='object'||
     row.source_metadata_goal!==goalId||row.consumer_metadata_goal!==goalId||
     row.actor_live||row.actor_leased||row.actor_cancelled||row.decision_owned||!digest.test(sourceManifest??'')||
     row.artifacts.length>maxManifestEntries||deliveryDigest(row.artifacts)!==sourceManifest)continue;
   const refs=row.repair_artifacts;
   if(!Array.isArray(refs)||refs.length!==2||
     !['implementation.patch','repair-report.json'].every(name=>refs.filter(a=>a?.name===name).length===1)||
     refs.some(ref=>!row.artifacts.some((a:any)=>a.id===ref.id&&a.run_id===sourceRun&&a.name===ref.name&&a.digest===ref.digest&&a.size===ref.size)))continue;
   for(const {entry,evidence} of candidates.get(row.id)??[]){
    if(row.case_id!==entry.id||row.consumer_task_id!==entry.task_id||row.consumer_run_id!==entry.current_run_id||
      row.consumer_attempt!==entry.current_attempt||row.consumer_run_fence!==entry.current_run_fence||
      row.consumer_context_version!==entry.current_context_version||
      row.current_case_source_run_id!==entry.source_run_id||row.current_case_decision_run_id!==entry.decision_run_id||
      !managerReviewOwnerReleased({...entry,state:row.current_case_state,
       decision_run_id:row.current_case_decision_run_id,decision_review_id:row.current_decision_review_id,
       decision_review_state:row.current_decision_review_state,decision_review_goal_id:row.current_decision_review_goal_id,
       decision_review_stage:row.current_decision_review_stage,current_run_state:row.consumer_run_state,
       has_live_run:row.actor_live})||row.generation!==evidence.generation||
      row.source_task_id!==evidence.producerTaskId||sourceRun!==evidence.producerRunId||
      sourceManifest!==evidence.producerManifestDigest||row.handoff_id!==evidence.handoffId||
      deliveryDigest(refs)!==deliveryDigest(evidence.repairArtifacts??null))continue;
    const values=selected.get(entry.id)??[];
    const item={obligationId:row.id,generation:row.generation,handoffId:row.handoff_id,
     producerTaskId:row.source_task_id,producerRunId:sourceRun,producerManifestDigest:sourceManifest,
     ...(row.resolution_event_id?{sourceResolutionEventId:row.resolution_event_id}:{}),repairArtifacts:refs};
    if(!values.some(value=>value.obligationId===item.obligationId))values.push(item);
    selected.set(entry.id,values);
   }
  }
 }
 return cases.map(entry=>{
  const obligations=selected.get(entry.id)?.sort((a,b)=>a.obligationId.localeCompare(b.obligationId));
  if(!obligations?.length)return entry;
  const identity={goalId,caseId:entry.id,consumerTaskId:entry.task_id,consumerRunId:entry.current_run_id,
   consumerAttempt:entry.current_attempt,obligations};
  return {...entry,materialConsumptionReview:{version:1,authority:'review_only',...identity,
   identityDigest:deliveryDigest(identity),actionValidationRequired:true}};
 });
}

/** The actual case envelope passed to projectManagerReview. */
export function projectManagerCases(selectedCases:any[]) {
 return selectedCases.map((c) => {
  const evidenceSource=managerCaseEvidenceSource(c);
  return ({
          id: c.id,
          taskId: c.task_id,
          reason: c.reason,
          scope:c.base_workspace,workKind:c.work_kind,
          ...(c.operationalRecovery?{operationalRecovery:true}:{}),
          ...(c.materialConsumptionReview?{materialConsumptionReview:c.materialConsumptionReview}:{}),
          ...(c.administrativeRecoveryReview?{administrativeRecoveryReview:c.administrativeRecoveryReview}:{}),
          ...(c.retryEffectEvidence?{retryEffectEvidence:c.retryEffectEvidence}:{}),
          currentRun: {
            id: c.current_run_id,
            attempt: c.current_attempt,
            state: c.current_run_state,
            errorCode:c.current_error_code,
            summary: c.current_summary,
            artifacts: c.current_artifacts,
          },
          deliveryDiagnostic: protocolReportDiagnostic(c.current_error_code, c.assigned_test_file),
          ...(evidenceSource?{evidenceSource}:{}),
          evidence: c.evidence.map((e: any) => ({ ...e, summary: e.summary?.slice(0, 1800) })),
        });});
}
