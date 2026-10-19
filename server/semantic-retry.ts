import { createHash, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { config } from './config.js';
import { pool, tx, lock, event, type DB } from './db.js';
import { acceptedSupervisionActions } from './supervision-actions.js';
import { supervisionGenerationReady } from './supervision-generations.js';
import {canonicalRepairProofRejection,canonicalRepairRejectionKey,canonicalRepairCause,recordCanonicalRepairRejection} from './canonical-repair-rejection.js';
export {readCanonicalRepairRejection,readCanonicalRepairRejections} from './canonical-repair-rejection.js';

const sha = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const live = ['queued', 'running', 'cancelling', 'reconciling'];
export const semanticRetryCode = 'SEMANTIC_RETRY_REQUIRES_SUPERVISION';
export const semanticEvidenceCode = 'SEMANTIC_RETRY_EVIDENCE_INVALID';

async function artifactJson(ref: any) {
  try {
    if (
      typeof ref.id !== 'string' ||
      !uuid.test(ref.id) ||
      !Number.isSafeInteger(ref.size) ||
      ref.size < 0 ||
      ref.size > 8 * 1024 * 1024
    )
      throw Error('INVALID_ARTIFACT');
    const bytes = await readFile(path.join(config.dataDir, 'artifacts', ref.id));
    if (bytes.length !== ref.size || sha(bytes) !== ref.digest) throw Error('INVALID_ARTIFACT');
    try {
      return JSON.parse(bytes.toString('utf8'));
    } catch {
      return null;
    }
  } catch {
    throw Error(semanticEvidenceCode);
  }
}

/** A valid negative repair delivery is planning evidence, never an interrupted executor. */
export async function semanticRepairRejection(db: DB, task: any, run: any) {
  if (run.state !== 'submitted') return null;
  const acceptance = (
    await db.query(
      'SELECT id,passed,checks FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1',
      [run.id],
    )
  ).rows[0];
  if (acceptance?.passed !== false)return null;
  if (
    !acceptance.checks?.some(
      (check: any) => check.id === 'repair_verified' && check.passed === false,
    )
  )
    return canonicalRepairProofRejection(db,task,run,acceptance);
  const frozen = run.instruction_snapshot?.task;
  if (frozen && (frozen.id !== task.id || frozen.goal_id !== task.goal_id))
    throw Error('SEMANTIC_RETRY_EVIDENCE_INVALID');
  const criterion = (frozen?.criteria ?? task.criteria ?? []).find(
    (entry: any) =>
      entry.id === 'repair_verified' &&
      entry.type === 'json_field' &&
      entry.name === 'repair-report.json' &&
      entry.field === 'fixVerified' &&
      entry.equals === true,
  );
  if (!criterion) return canonicalRepairProofRejection(db,task,run,acceptance);
  const ref = (
    await db.query(
      'SELECT id,name,digest,size FROM artifacts WHERE run_id=$1 AND name=$2 ORDER BY created_at DESC LIMIT 1',
      [run.id, criterion.name],
    )
  ).rows[0];
  // Missing/malformed deliveries retain the existing packaging-recovery path.
  if (!ref) return null;
  let report: any,
    invalidEvidence = false;
  try {
    report = await artifactJson(ref);
  } catch {
    invalidEvidence = true;
  }
  if (!invalidEvidence && (!report || typeof report !== 'object' || report.fixVerified !== false))
    return canonicalRepairProofRejection(db,task,run,acceptance);
  return {
    version: 1,
    kind: invalidEvidence ? 'semantic_repair_evidence_invalid' : 'semantic_repair_rejected',
    taskId: task.id,
    goalId: task.goal_id,
    sourceRunId: run.id,
    sourceRunFence: run.fence,
    sourceContextVersion: run.context_id??null,
    sourceAttempt: run.attempt,
    acceptanceId: acceptance.id,
    report: ref,
    failedCriterion: {
      id: criterion.id,
      name: criterion.name,
      field: criterion.field,
      expected: true,
      actual: invalidEvidence ? null : false,
    },
    note: invalidEvidence
      ? 'Integridade da evidência não conferida; não inferir o resultado do reparo.'
      : 'Entrega diagnóstica negativa recuperável; reparo e jogo permanecem sem aceite.',
  };
}

/** Match the applied decision to an accepted, byte-verified coordinator artifact. */
export async function currentSemanticDecision(db: DB, task: any, run: any, c: any) {
  const directive =
    task.execution?.supervisionHandoff?.caseId === c?.id
      ? task.execution.supervisionHandoff
      : task.execution?.supervision;
  if (
    !c ||
    c.state !== 'applied' ||
    c.task_id !== task.id ||
    c.goal_id !== task.goal_id ||
    directive?.caseId !== c.id ||
    c.applied_attempt !== run.attempt ||
    (directive.sourceRunId ?? c.source_run_id) !== run.id ||
    task.attempt_count !== run.attempt
  )
    return false;
  const latest = (
    await db.query(
      'SELECT id FROM supervision_cases WHERE task_id=$1 AND goal_id=$2 ORDER BY created_at DESC,id DESC LIMIT 1',
      [task.id, task.goal_id],
    )
  ).rows[0];
  if (latest?.id !== c.id) return false;
  const pendingFact=(await db.query('SELECT value FROM settings WHERE id=$1',
    ['canonical-repair-rejection:'+run.id])).rows[0]?.value;
  if(pendingFact?.ownerCaseId===c.id&&pendingFact.runFence===run.fence&&
    pendingFact.contextVersion===(run.context_id??null)&&pendingFact.expectedAttempt===run.attempt&&
    String(pendingFact.disposition).startsWith('deferred_'))return false;
  const rawActions = c.decision?.actions ?? [c.decision];
  if (!Array.isArray(rawActions)) return false;
  const actions = acceptedSupervisionActions(rawActions);
  if (
    !actions.length ||
    actions.some((a: any) => a.caseId !== c.id || a.kind === 'wait_for_evidence')
  )
    return false;
  const refs = (
    await db.query(
      `SELECT a.id,a.name,a.digest,a.size FROM artifacts a
      JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id
      WHERE r.id=$1 AND r.state='submitted' AND t.goal_id=$2 AND t.stage='coordinate' AND a.name='coordination.json'
      AND (SELECT v.passed FROM acceptances v WHERE v.run_id=r.id ORDER BY v.created_at DESC LIMIT 1)=true`,
      [c.decision_run_id, task.goal_id],
    )
  ).rows;
  for (const ref of refs) {
    try {
      const raw = await artifactJson(ref);
      if (!Array.isArray(raw?.supervision?.actions)) continue;
      const declared = acceptedSupervisionActions(raw.supervision.actions);
      if (
        actions.every((a: any) =>
          declared.some((b: any) => JSON.stringify(a) === JSON.stringify(b)),
        )
      )
        return true;
    } catch {}
  }
  return false;
}

export function semanticRetryError(rejection: any) {
  return Object.assign(
    new Error(
      rejection.kind === 'semantic_repair_evidence_invalid'
        ? semanticEvidenceCode
        : semanticRetryCode,
    ),
    { semanticRejection: rejection },
  );
}

/** Called after admission rolled back. Re-read the fence; never mutate a newer/live attempt. */
export async function requestSemanticReplan(taskId: string, runId: string, prepared?:{
  runFence:string;contextId:string|null;attempt:number;criteriaDigest:string;rejection:any;
}) {
  return tx(async (db) => {
    await lock(db);
    const task = (
      await db.query(
        `SELECT t.* FROM tasks t JOIN goals g ON g.id=t.goal_id
        JOIN settings s ON s.id='supervision:'||g.id LEFT JOIN settings a ON a.id='autonomy:'||g.project
        WHERE t.id=$1 AND g.state='active' AND s.value->>'enabled'='true'
        AND COALESCE(a.value->>'enabled','true')='true'
        AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true') FOR UPDATE OF t`,
        [taskId],
      )
    ).rows[0];
    if (!task || [...live, 'accepted', 'cancelled', 'superseded','paused'].includes(task.state))
      return false;
    const run = (
      await db.query(
        'SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1 FOR UPDATE',
        [taskId],
      )
    ).rows[0];
    if (run?.id !== runId || run.state !== 'submitted' || task.attempt_count !== run.attempt)
      return false;
    if (
      (
        await db.query(
          `SELECT 1 FROM runs WHERE task_id=$1 AND state=ANY($3::text[])
      UNION ALL SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$2 LIMIT 1`,
          [taskId, runId, live],
        )
      ).rowCount
    )
      return false;
    let rejection:any;
    if(prepared){
      if(run.fence!==prepared.runFence||(run.context_id??null)!==prepared.contextId||run.attempt!==prepared.attempt||
        sha(JSON.stringify(task.criteria))!==prepared.criteriaDigest)return false;
      const acceptance=(await db.query('SELECT id,passed FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1',[runId])).rows[0];
      if(acceptance?.id!==prepared.rejection.acceptanceId||acceptance.passed!==false)return false;
      const expected=[prepared.rejection.report,prepared.rejection.patch].filter(Boolean);
      const refs=(await db.query('SELECT id,name,digest,size FROM artifacts WHERE run_id=$1 AND id=ANY($2::uuid[]) ORDER BY id',
        [runId,expected.map(ref=>ref.id)])).rows;
      if(refs.length!==expected.length||expected.some(ref=>!refs.some(row=>
        row.id===ref.id&&row.name===ref.name&&row.digest===ref.digest&&row.size===ref.size)))return false;
      rejection=prepared.rejection;
    }else rejection=await semanticRepairRejection(db, task, run);
    if (!rejection) return false;
    const ids = [
      task.execution?.supervisionHandoff?.caseId,
      task.execution?.supervision?.caseId,
    ].filter((id) => typeof id === 'string' && uuid.test(id));
    const requestKey = canonicalRepairRejectionKey(rejection);
    const previous = (
      await db.query(
        'SELECT * FROM supervision_cases WHERE task_id=$1 AND source_run_id=$2 ORDER BY created_at DESC,id DESC FOR UPDATE',
        [taskId, runId],
      )
    ).rows;
    const matchesRejection = (c: any) => c?.request_key === requestKey ||
      (Array.isArray(c?.evidence) && c.evidence.some((e: any) =>
        e.kind === rejection.kind &&
        e.sourceRunId === runId &&
        e.report?.digest === rejection.report.digest&&
        (rejection.kind!=='repair_source_proof_rejected'||
          (e.patch?.digest===rejection.patch.digest&&e.code===rejection.code))));
    const existing = previous.find(matchesRejection);
    if (ids.length) {
      const applied = (
        await db.query(
          "SELECT * FROM supervision_cases WHERE id=ANY($1::uuid[]) AND task_id=$2 AND goal_id=$3 AND state='applied' ORDER BY updated_at DESC LIMIT 1",
          [ids, taskId, task.goal_id],
        )
      ).rows[0];
      // The supervised dispatcher owns an already-applied, current handoff.
      if (
        (rejection.kind === 'semantic_repair_rejected' || matchesRejection(applied)) &&
        (await currentSemanticDecision(db, task, run, applied))
      )
        return true;
    }
    const pending=(await db.query(`SELECT * FROM supervision_cases WHERE task_id=$1
      AND state IN('open','decided','delegated','waiting_evidence') ORDER BY created_at DESC,id DESC LIMIT 1 FOR UPDATE`,[taskId])).rows[0];
    if(pending){
      await recordCanonicalRepairRejection(db,task,run,rejection,pending);
      return true; // Existing ownership is not a claim that the cause was routed.
    }
    // An old semantic decision remains history. A repeated poll is not a new
    // cause and must not reopen an applied generation or replace a pending one.
    if (existing) return false;
    if (!(await supervisionGenerationReady(db))) return false;
    const caseId = randomUUID(), parentCaseId = previous[0]?.id ?? null;
    const evidence = [{...rejection,requestKey,causeDigest:canonicalRepairCause(rejection,run),
      authority:'review_only',observedAt:new Date().toISOString()}];
    const reason = rejection.kind==='repair_source_proof_rejected'
      ? rejection.code+': corrigir a prova estrutural a partir dos bytes preservados; manter a evidência histórica, integração e QA original.'
      : rejection.kind === 'semantic_repair_evidence_invalid'
        ? 'Integridade da evidência de reparo inválida. Diagnostique a origem e recupere insumo íntegro antes de retomar; não inferir fixVerified nem alterar o aceite.'
        : 'Reparo não comprovado: fixVerified=false. Use a evidência diagnóstica para decidir nova hipótese ou insumo antes de retomar; preserve os critérios e o aceite final.';
    await db.query(
      `INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence,request_key,parent_case_id)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
      [caseId, task.goal_id, taskId, runId, reason, JSON.stringify(evidence),requestKey,parentCaseId],
    );
    if (task.state !== 'recovery_blocked')
      await db.query("UPDATE tasks SET state='recovery_blocked',reason=$2 WHERE id=$1", [
        taskId,
        reason,
      ]);
    await event(db, 'SEMANTIC_RETRY_SUPERVISION_REQUESTED', taskId, {caseId,...rejection,requestKey,parentCaseId});
    await recordCanonicalRepairRejection(db,task,run,rejection,{id:caseId,state:'open',source_run_id:run.id,
      decision:null,decision_run_id:null,evidence});
    return true;
  });
}

let rejectionCursor:string|null=null;
/** Bounded review facts for actors already owned by supervision. Artifact
 * inspection is outside the admission transaction; requestSemanticReplan
 * revalidates every identity before attaching or retaining a deferred fact. */
export async function reconcileCanonicalRepairRejections(){
  const rows=(await pool.query(`SELECT c.id case_id,t.id,t.goal_id,t.criteria,t.state,t.attempt_count,
      r.id run_id,r.state run_state,r.attempt,r.fence,r.context_id,
      r.instruction_snapshot->'task'->>'id' frozen_task_id,
      r.instruction_snapshot->'task'->>'goal_id' frozen_goal_id,
      r.instruction_snapshot->'task'->'criteria' frozen_criteria,
      r.instruction_snapshot->'task' IS NOT NULL has_snapshot
    FROM supervision_cases c JOIN tasks t ON t.id=c.task_id AND t.goal_id=c.goal_id
    JOIN goals g ON g.id=t.goal_id
    JOIN settings s ON s.id='supervision:'||g.id AND s.value->>'enabled'='true'
    LEFT JOIN settings autonomy ON autonomy.id='autonomy:'||g.project
    JOIN LATERAL(SELECT id,state,attempt,fence,context_id,instruction_snapshot FROM runs
      WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
    LEFT JOIN settings observed ON observed.id='canonical-repair-rejection:'||r.id::text
    WHERE g.state='active' AND COALESCE(autonomy.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR autonomy.value->>'enabled'='true')
      AND t.state IN('submitted','recovery_blocked','failed') AND r.state='submitted'
      AND r.attempt=t.attempt_count AND a.passed=false
      AND (c.state IN('open','decided','delegated','waiting_evidence') OR
        (c.state='applied' AND c.source_run_id=r.id AND observed.value->>'ownerCaseId'=c.id::text
          AND observed.value->>'disposition' LIKE 'deferred_%'))
      AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id AND live.state IN('queued','running','reconciling','cancelling'))
    ORDER BY CASE WHEN $1::uuid IS NULL OR c.id>$1 THEN 0 ELSE 1 END,c.id LIMIT 8`,[rejectionCursor])).rows;
  const result:{caseId:string;taskId:string;runId:string;observed:boolean;code?:string}[]=[];
  for(const row of rows){
    rejectionCursor=row.case_id;
    const run={id:row.run_id,state:row.run_state,attempt:row.attempt,fence:row.fence,context_id:row.context_id,
      instruction_snapshot:row.has_snapshot?{task:{id:row.frozen_task_id,goal_id:row.frozen_goal_id,criteria:row.frozen_criteria}}:null};
    try{
      const rejection=await semanticRepairRejection(pool,row,run);
      const observed=rejection?await requestSemanticReplan(row.id,row.run_id,{runFence:row.fence,
        contextId:row.context_id??null,attempt:row.attempt,criteriaDigest:sha(JSON.stringify(row.criteria)),rejection}):false;
      result.push({caseId:row.case_id,taskId:row.id,runId:row.run_id,observed});
    }catch(error:any){
      if(error?.message!==semanticEvidenceCode)throw error;
      result.push({caseId:row.case_id,taskId:row.id,runId:row.run_id,observed:false,code:semanticEvidenceCode});
    }
  }
  return result;
}
