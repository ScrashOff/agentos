import { createHash, randomUUID } from 'node:crypto';
import { event, lock, type DB } from './db.js';
import { assertStopped } from './continuation.js';
import { supervisionGenerationReady } from './supervision-generations.js';

const canonical = (value: any): any => Array.isArray(value) ? value.map(canonical) :
  value && typeof value === 'object' ? Object.fromEntries(Object.keys(value).sort().map(k => [k, canonical(value[k])])) : value;
export const deliveryDigest = (value: unknown): string => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');

/** A new decision is bound to new evidence, not the planner's wording or UUID.
 * The caller has already proved the artifact/source authority. This requester
 * never retries a task, changes its instructions or overwrites a prior decision. */
export async function requestDeliverySupervision(db: DB, goal: any, target: any, latest: any, evidence: any,
  options: { stopProof?: any } = {}) {
  await lock(db);
  const key = 'delivery:' + deliveryDigest(evidence.obligationKey ? {
    obligationKey: evidence.obligationKey, generation: evidence.generation, code: evidence.code,
  } : {
    kind: evidence.action?.kind, sourceProof: evidence.sourceProof,
    consumerTaskId: target.id, consumerRunId: latest?.id ?? null,
    artifactIds: [...(evidence.action?.artifactIds ?? [])].sort(), conflict: evidence.conflict ?? null,
  });
  const authority = (await db.query(`SELECT g.state, s.value->>'enabled' enabled, a.value->>'enabled' autonomy,
    t.state task_state,t.attempt_count,t.recovery FROM goals g JOIN tasks t ON t.goal_id=g.id
    LEFT JOIN settings s ON s.id='supervision:'||g.id LEFT JOIN settings a ON a.id='autonomy:'||g.project
    WHERE g.id=$1 AND t.id=$2`, [goal.id, target.id])).rows[0];
  if (!authority || authority.state !== 'active' || authority.enabled !== 'true' || authority.autonomy === 'false' ||
    (authority.recovery?.mode === 'manual' && authority.autonomy !== 'true')) return { applied: false, code: 'SUPERVISION_UNAVAILABLE' };
  if (['accepted', 'cancelled', 'superseded', 'cancelling','paused'].includes(authority.task_state) ||
    authority.attempt_count !== target.attempt_count) return { applied: false, code: 'SUPERVISION_TARGET_CHANGED' };
  const currentRun = (await db.query('SELECT id,attempt,fence,state FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [target.id])).rows[0];
  if ((currentRun?.id ?? null) !== (latest?.id ?? null) || (currentRun?.fence ?? null) !== (latest?.fence ?? null))
    return { applied: false, code: 'SUPERVISION_TARGET_CHANGED' };
  if ((await db.query(`SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling')
    UNION ALL SELECT 1 FROM leases l JOIN runs r ON r.id=l.run_id WHERE r.task_id=$1
    UNION ALL SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$2`, [target.id, latest?.id ?? null])).rowCount ||
    currentRun?.state === 'cancelled') return { applied: false, code: 'SUPERVISION_ACTOR_UNAVAILABLE' };
  const pending = (await db.query(`SELECT * FROM supervision_cases WHERE task_id=$1
    AND state IN('open','decided','delegated','waiting_evidence') ORDER BY created_at DESC,id DESC LIMIT 1 FOR UPDATE`, [target.id])).rows[0];
  const item = { ...evidence, requestKey: key };
  if (pending) {
    if (pending.state !== 'open') return { applied: false, code: 'SUPERVISION_DECISION_PENDING', caseId: pending.id };
    if (!Array.isArray(pending.evidence)) return { applied: false, code: 'SUPERVISION_EVIDENCE_INVALID', caseId: pending.id };
    if (!pending.evidence.some((e: any) => e.requestKey === key)) {
      await db.query("UPDATE supervision_cases SET evidence=evidence||$2::jsonb,updated_at=now() WHERE id=$1 AND state='open'", [pending.id, JSON.stringify([item])]);
      await event(db, 'DELIVERY_SUPERVISION_EVIDENCE_ATTACHED', target.id, { caseId: pending.id, requestKey: key });
    }
    return { applied: true, code: 'SUPERVISION_REQUESTED', caseId: pending.id };
  }
  if(evidence.kind==='DELIVERY_SOURCE_REVIEW_REQUIRED'){
    if(!latest||!options.stopProof)return {applied:false,code:'SUPERVISION_STOP_PROOF_REQUIRED'};
    assertStopped(options.stopProof,latest,target);
  }
  // A fulfilled generation is never reopened merely because another poll ran.
  const previous = (await db.query(`SELECT id,state,request_key,evidence FROM supervision_cases WHERE task_id=$1
    AND source_run_id IS NOT DISTINCT FROM $2::uuid ORDER BY created_at DESC,id DESC FOR UPDATE`, [target.id, latest?.id ?? null])).rows;
  const same = previous.find(c => c.request_key === key || (Array.isArray(c.evidence) && c.evidence.some((e: any) => e.requestKey === key)));
  if (same) return { applied: false, code: 'SUPERVISION_GENERATION_RECORDED', caseId: same.id };
  const parent = previous[0];
  if (parent) {
    if (parent.state !== 'applied') return { applied: false, code: 'SUPERVISION_DECISION_PENDING', caseId: parent.id };
    if (!latest || !options.stopProof) return { applied: false, code: 'SUPERVISION_STOP_PROOF_REQUIRED', caseId: parent.id };
    assertStopped(options.stopProof, latest, target);
  }
  if (!await supervisionGenerationReady(db)) return { applied: false, code: 'SUPERVISION_GENERATION_MIGRATION_PENDING' };
  const id = randomUUID();
  await db.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,request_key,parent_case_id,reason,evidence)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, [id, goal.id, target.id, latest?.id ?? null, key, parent?.id ?? null,
    'Consumir evidência aceita e encaminhar a entrega original; preserve critérios, fonte e histórico.', JSON.stringify([item])]);
  await event(db, 'SUPERVISION_REQUESTED', target.id, { caseId: id, sourceRunId: latest?.id ?? null,
    parentCaseId: parent?.id ?? null, requestKey: key, reason: 'DELIVERY_CONSUMPTION_REQUIRED' });
  return { applied: true, code: 'SUPERVISION_REQUESTED', caseId: id };
}
