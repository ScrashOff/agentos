import { createHash, randomUUID } from 'node:crypto';
import { pool, tx, lock, event, type DB } from './db.js';
import { readArtifact } from './service.js';
import { acceptedSnapshotProof, sameSnapshotProof, stageDeferredHandoff } from './deferred-handoffs.js';
import { scopedPlanActionSchema, validateScopedPlan, type ScopedPlanAction } from './scoped-plan-contract.js';
import type { BacklogHandoff } from './backlog-handoffs.js';
import { requestDeliverySupervision } from './delivery-supervision.js';

export type ScopedActionReceipt = { applied: boolean; preparing: boolean; code: string;
  existing?: boolean; handoffId?: string; caseId?: string; actionKey?: string };
const result = (code: string, extra: Partial<ScopedActionReceipt> = {}): ScopedActionReceipt => ({ applied: false, preparing: false, code, ...extra });
const digest = (value: unknown): string => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const sorted = (value: any[]): any[] => [...value].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
const validRequestStates = ['assigned', 'running', 'preparing', 'applied'];
const hasUnappliedAcceptedPlan=(request:any)=>validRequestStates.includes(request.state) ||
  (request.state==='awaiting_supervision' && request.receipt?.evaluatedRunId!==request.plan_run_id);

async function authority(db: DB, id: string) {
  return (await db.query(`SELECT p.*,r.id plan_run_id,a.id plan_artifact_id,a.digest plan_digest,a.size plan_size
    FROM planning_requests p JOIN tasks t ON t.id=p.task_id AND t.goal_id=p.goal_id
    JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    JOIN artifacts a ON a.run_id=r.id AND a.name='scope-plan.json'
    WHERE p.id=$1 AND t.state='accepted' AND r.state='submitted'
      AND t.execution->>'workKind'='planning' AND t.execution->>'planningRequestId'=p.id::text
      AND t.execution->>'baseWorkspace'=p.scope
      AND (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)=true
      AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id
        AND live.state IN('queued','running','cancelling','reconciling'))`, [id])).rows[0];
}

/** Semantic requests are evidence, never counterfeit Sol decisions. An existing
 * decision or applied case remains byte-for-byte intact; the conflict receipt
 * carries the evidence to the next review instead of resetting authority. */
async function requestSupervision(db: DB, goal: any, target: any, latest: any, evidence: any) {
  if (!(await db.query("SELECT to_regclass('public.supervision_cases') present")).rows[0].present)
    return result('SUPERVISION_UNAVAILABLE');
  const receipt = await requestDeliverySupervision(db, goal, target, latest, evidence);
  if (receipt.code === 'SUPERVISION_DECISION_PENDING') await event(db, 'SCOPED_ACTION_SUPERVISION_CONFLICT', target.id,
    { ...evidence, caseId: receipt.caseId, nextAction: 'Considere a evidência sem substituir a decisão existente.' });
  return { ...result('SUPERVISION_UNAVAILABLE'), ...receipt };
}

/** Apply only an action actually published by the current accepted planner.
 * The ordinary deferred-handoff reconciler alone mutates a stopped consumer,
 * after fresh stop/source proofs. This function never launches, retries or
 * accepts a worker and never replaces an existing supervision decision. */
export async function applyScopedPlanAction(request: any, raw: unknown): Promise<ScopedActionReceipt> {
  const parsed = scopedPlanActionSchema.safeParse(raw);
  if (!parsed.success) return result('PLANNING_ACTION_INVALID');
  const action: ScopedPlanAction = parsed.data;
  const auth = await authority(pool, request.id);
  if (!auth || (request.run_id && request.run_id !== auth.plan_run_id) ||
    (request.goal_id && request.goal_id !== auth.goal_id) || !hasUnappliedAcceptedPlan(auth))
    return result('PLANNING_ACTION_AUTHORITY');
  let plan: ReturnType<typeof validateScopedPlan>;
  const verifiedArtifacts = new Map<string, any>();
  try {
    const artifact = await readArtifact(auth.plan_artifact_id);
    if (artifact.digest !== auth.plan_digest || artifact.bytes.length !== auth.plan_size) return result('PLANNING_ACTION_ARTIFACT_CHANGED');
    plan = validateScopedPlan(JSON.parse(artifact.bytes.toString()), auth);
    if (!plan.actions.some(candidate => digest(candidate) === digest(action))) return result('PLANNING_ACTION_NOT_PUBLISHED');
    for (const id of action.artifactIds) {
      const sourceArtifact = await readArtifact(id);
      if (sourceArtifact.bytes.length !== sourceArtifact.size) return result('PLANNING_ACTION_ARTIFACT_CHANGED');
      verifiedArtifacts.set(id, { id, run_id: sourceArtifact.run_id, name: sourceArtifact.name,
        digest: sourceArtifact.digest, size: sourceArtifact.size });
    }
  } catch { return result('PLANNING_ACTION_ARTIFACT_INVALID'); }
  const actionKey = 'scoped-action:' + digest([auth.id, auth.plan_run_id, action]);
  return tx(async db => {
    await lock(db);
    const currentAuth = await authority(db, auth.id);
    if (!currentAuth || !hasUnappliedAcceptedPlan(currentAuth) ||
      currentAuth.plan_run_id !== auth.plan_run_id || currentAuth.plan_artifact_id !== auth.plan_artifact_id ||
      currentAuth.plan_digest !== auth.plan_digest || currentAuth.source_revision !== auth.source_revision ||
      digest(currentAuth.snapshot) !== digest(auth.snapshot)) return result('PLANNING_ACTION_AUTHORITY_CHANGED');
    const previous = (await db.query(`SELECT payload->'receipt' receipt FROM events
      WHERE kind='SCOPED_DELIVERY_ACTION_RECORDED' AND payload->>'actionKey'=$1 ORDER BY id DESC LIMIT 1`, [actionKey])).rows[0];
    if (previous) return { ...previous.receipt, existing: true };
    const record = async (receipt: ScopedActionReceipt) => {
      const value = { ...receipt, actionKey };
      await event(db, 'SCOPED_DELIVERY_ACTION_RECORDED', auth.goal_id, { actionKey, requestId: auth.id,
        planRunId: auth.plan_run_id, planArtifactId: auth.plan_artifact_id, planDigest: auth.plan_digest, action, receipt: value });
      return value;
    };
    const goal = (await db.query(`SELECT g.id,g.state,
      (SELECT value FROM settings WHERE id='autonomy:'||g.project) autonomy,
      (SELECT value FROM settings WHERE id='workforce:'||g.id) workforce FROM goals g WHERE g.id=$1`, [auth.goal_id])).rows[0];
    if (goal?.state !== 'active' || goal.autonomy?.enabled === false || goal.workforce?.enabled !== true)
      return result('PLANNING_ACTION_DISABLED', { preparing: true });
    if ((await db.query(`SELECT 1 FROM planning_requests WHERE goal_id=$1 AND scope=$2 AND id<>$3
      AND state IN('pending','assigned','running','preparing','replanning_required')`, [auth.goal_id, auth.scope, auth.id])).rowCount)
      return record(result('SCOPED_DEMAND_RESERVED'));
    const tasks = (await db.query('SELECT * FROM tasks WHERE id=ANY($1::uuid[]) FOR UPDATE', [[action.taskId, action.sourceTaskId]])).rows;
    const target = tasks.find(task => task.id === action.taskId), source = tasks.find(task => task.id === action.sourceTaskId);
    const frozenTarget = auth.snapshot.tasks.find((task: any) => task.id === action.taskId);
    const frozenSource = auth.snapshot.tasks.find((task: any) => task.id === action.sourceTaskId);
    if (!target || !source || !frozenTarget || !frozenSource || target.id === source.id ||
      target.goal_id !== auth.goal_id || source.goal_id !== auth.goal_id ||
      target.execution?.baseWorkspace !== auth.scope || source.execution?.baseWorkspace !== auth.scope)
      return record(result('PLANNING_ACTION_SCOPE_CHANGED'));
    if (['accepted', 'cancelled', 'superseded', 'cancelling'].includes(target.state) ||
      target.attempt_count !== action.expectedAttempt || target.execution?.workspace !== frozenTarget.execution?.workspace ||
      target.description !== frozenTarget.intent ||
      digest(target.criteria) !== digest(frozenTarget.criteria) || target.stage !== frozenTarget.stage ||
      (target.recovery?.mode === 'manual' && goal.autonomy?.enabled !== true)) return record(result('PLANNING_ACTION_TARGET_CHANGED'));
    const latest = (await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [target.id])).rows[0];
    if ((latest?.id??null)!==(frozenTarget.last_run_id??null) ||
      (latest && latest.attempt!==target.attempt_count)) return record(result('PLANNING_ACTION_TARGET_CHANGED'));
    if (latest && (latest.state === 'cancelled' || latest.state === 'cancelling' ||
      (await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1", [latest.id])).rowCount))
      return record(result('PLANNING_ACTION_CANCELLED'));
    const dependencies = (await db.query('SELECT depends_on FROM dependencies WHERE task_id=$1 ORDER BY depends_on', [target.id])).rows.map(row => row.depends_on);
    if (digest(sorted(dependencies)) !== digest(sorted(frozenTarget.dependencies ?? []))) return record(result('PLANNING_ACTION_DEPENDENCIES_CHANGED'));
    let sourceProof: Awaited<ReturnType<typeof acceptedSnapshotProof>>;
    try { sourceProof = await acceptedSnapshotProof(db, source); }
    catch { return record(result('SOURCE_NOT_CURRENT_ACCEPTED')); }
    if (sourceProof.runId !== action.expectedSourceRunId || sourceProof.runId !== frozenSource.last_run_id ||
      sourceProof.attempt !== frozenSource.attempt_count || sourceProof.workspace !== frozenSource.execution?.workspace ||
      source.description !== frozenSource.intent || digest(source.criteria) !== digest(frozenSource.criteria)) return record(result('PLANNING_ACTION_SOURCE_CHANGED'));
    const manifest = (await db.query("SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 AND name<>'task-instructions-history.json' ORDER BY id", [sourceProof.runId])).rows;
    const frozenManifest = auth.snapshot.artifacts.filter((entry: any) => entry.task_id === source.id)
      .map(({ id, run_id, name, digest, size }: any) => ({ id, run_id, name, digest, size }));
    if (digest(sorted(manifest)) !== digest(sorted(frozenManifest)) || action.artifactIds.some(id =>
      !manifest.some(row => row.id === id && digest(row) === digest(verifiedArtifacts.get(id))))) return record(result('SOURCE_MANIFEST_CHANGED'));
    const cycle = (await db.query(`WITH RECURSIVE parents(id) AS(SELECT depends_on FROM dependencies WHERE task_id=$1
      UNION SELECT d.depends_on FROM dependencies d JOIN parents p ON d.task_id=p.id)
      SELECT 1 FROM parents WHERE id=$2`, [source.id, target.id])).rowCount;
    if (cycle) return record(result('DEPENDENCY_CYCLE'));
    const liveTarget = (await db.query("SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling')", [target.id])).rowCount;
    const evidence = { kind: 'SCOPED_DELIVERY_ACTION', actionKey, requestId: auth.id, sourcePlanRunId: auth.plan_run_id,
      action, sourceProof, consumerAttempt: target.attempt_count, consumerRunId: latest?.id ?? null };
    if (action.kind === 'request_verification') {
      if (target.stage !== 'verify' || target.execution.readOnlySource !== true || !dependencies.includes(source.id))
        return record(result('VERIFICATION_CONTRACT_CHANGED'));
      if (liveTarget) return record(result('VERIFICATION_ACTIVE', { applied: true }));
      if (['failed', 'recovery_blocked', 'submitted'].includes(target.state)) return record(await requestSupervision(db, goal, target, latest, evidence));
      const pending = (await db.query("SELECT 1 FROM tasks WHERE id=ANY($1::uuid[]) AND state<>'accepted'", [dependencies])).rowCount;
      return record(result(pending ? 'VERIFICATION_WAITING_DEPENDENCIES' : target.state === 'ready' ? 'VERIFICATION_READY' : 'VERIFICATION_WAITING_SCHEDULER', { applied: true }));
    }
    if (!['diagnostic', 'repair'].includes(source.execution?.workKind) || !['work', 'integrate', 'verify'].includes(target.stage) ||
      (source.execution.workKind === 'repair' && (target.execution.readOnlySource === true || target.stage === 'verify')) ||
      (action.kind === 'prepare_integration' && (source.execution.workKind !== 'repair' || target.stage !== 'integrate' || target.execution.readOnlySource === true)))
      return record(result('HANDOFF_CONTRACT_CHANGED'));
    const currentHandoff = (await db.query('SELECT * FROM deferred_handoffs WHERE producer_task_id=$1 FOR UPDATE', [source.id])).rows[0];
    let existingSourceVerified = !currentHandoff;
    if (currentHandoff) {
      // A backlog producer normally started from an earlier accepted baseline.
      // Keep that reservation/proof; the producer's own accepted output was
      // independently verified above. Never substitute one source for another.
      const baseline=(await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',[currentHandoff.source_task_id])).rows[0];
      if (baseline?.goal_id===auth.goal_id && baseline.execution?.baseWorkspace===auth.scope) {
        try { existingSourceVerified=sameSnapshotProof(currentHandoff.source_proof,await acceptedSnapshotProof(db,baseline)); }
        catch { existingSourceVerified=false; }
      }
    }
    if (currentHandoff && (currentHandoff.goal_id !== auth.goal_id || currentHandoff.consumer_task_id !== target.id ||
      !existingSourceVerified ||
      !['waiting_result', 'staged', 'applied'].includes(currentHandoff.state))) {
      const supervisor = await requestSupervision(db, goal, target, latest, { ...evidence, conflict: 'EXISTING_HANDOFF', handoffId: currentHandoff.id });
      return record({ ...supervisor, code: supervisor.applied ? 'HANDOFF_CONFLICT_SUPERVISION_REQUESTED' : 'HANDOFF_CONFLICT', handoffId: currentHandoff.id });
    }
    let handoffId = currentHandoff?.id;
    if (!currentHandoff) {
      const lane = source.execution.workforceLane ?? (source.execution.kind === 'opencode' ? 'glm' : null);
      if (!['glm', 'muse'].includes(lane)) return record(result('HANDOFF_LANE_UNSUPPORTED'));
      const coverage = (await db.query('SELECT coverage_key FROM workforce_backlog_items WHERE goal_id=$1 AND task_id=$2', [auth.goal_id, source.id])).rows[0]?.coverage_key;
      const handoff: BacklogHandoff = { taskId: source.id, plannerRunId: auth.plan_run_id,
        coverageKey: coverage ?? 'reuse-' + source.id.replaceAll('-', ''), lane, workKind: source.execution.workKind };
      await stageDeferredHandoff(db, auth.goal_id, source.id, source, target, handoff);
      handoffId = (await db.query('SELECT id FROM deferred_handoffs WHERE producer_task_id=$1', [source.id])).rows[0].id;
      await db.query("UPDATE deferred_handoffs SET state='staged' WHERE id=$1 AND state='waiting_result'", [handoffId]);
    }
    if (!liveTarget && ['failed', 'recovery_blocked', 'submitted'].includes(target.state)) {
      const supervisor = await requestSupervision(db, goal, target, latest, { ...evidence, handoffId });
      return record({ ...supervisor, applied: true, code: supervisor.applied ? 'HANDOFF_STAGED_SUPERVISION_REQUESTED' : 'HANDOFF_STAGED_SUPERVISION_PENDING', handoffId });
    }
    return record(result(currentHandoff?.state === 'applied' ? 'HANDOFF_ALREADY_APPLIED' : 'HANDOFF_STAGED', { applied: true, handoffId }));
  });
}
