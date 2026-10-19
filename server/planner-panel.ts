import { pool, type DB } from './db.js';
import { DomainError } from './contracts.js';
import { readScopedPlannerCapacity } from './planning-demand.js';
import { checkedExecutionMetadata, executionMetadataColumns } from './task-execution-metadata.js';

type Row = Record<string, any>;
const closed = new Set(['accepted', 'cancelled', 'superseded']);
const live = new Set(['queued', 'running', 'cancelling', 'reconciling']);
const auxiliary = new Set(['planning', 'diagnostic', 'repair']);
const pendingPlans = new Set(['pending', 'assigned', 'running', 'preparing', 'replanning_required']);
const brief = (value: unknown, limit = 220) => typeof value === 'string' ? value.slice(0, limit) : null;
/** Only display metadata is read here: never snapshots, prompts, input bytes or
 * provider secrets. This request cannot enqueue work or wake a planner. */
export async function readPlannerPanel(db: DB, goalId: string) {
  const goal = (await db.query('SELECT id,state FROM goals WHERE id=$1', [goalId])).rows[0];
  if (!goal) throw new DomainError('NOT_FOUND', 'Objetivo não encontrado.', 404);
  const policy = (await db.query(`SELECT jsonb_build_object('enabled',value->'enabled',
    'plannerMaxActive',value->'plannerMaxActive') value FROM settings WHERE id=$1`, ['workforce:' + goalId])).rows[0]?.value;
  const tasks = checkedExecutionMetadata((await db.query(`SELECT t.id,t.title,t.state,t.stage,t.priority,t.attempt_count,
    ${executionMetadataColumns},COALESCE(metadata.base_workspace,metadata.workspace) scope,metadata.work_kind,
    left(t.reason,280) reason,t.next_attempt_at,t.updated_at,
    current_run.id current_run_id,current_run.state current_run_state,
    active.id active_run_id,active.state active_run_state,active.model active_model,
    a.passed acceptance_passed,a.created_at accepted_at,
    NOT EXISTS(SELECT 1 FROM dependencies d JOIN tasks parent ON parent.id=d.depends_on
      WHERE d.task_id=t.id AND parent.state<>'accepted') dependencies_ready
    FROM tasks t LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    LEFT JOIN runs current_run ON current_run.task_id=t.id AND current_run.attempt=t.attempt_count
    LEFT JOIN LATERAL(SELECT r.id,r.state,r.model FROM runs r WHERE r.task_id=t.id
      AND r.state IN('queued','running','cancelling','reconciling') ORDER BY r.attempt DESC LIMIT 1)active ON true
    LEFT JOIN LATERAL(SELECT passed,created_at FROM acceptances WHERE run_id=current_run.id
      ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
    WHERE t.goal_id=$1 AND t.stage IS DISTINCT FROM 'coordinate' ORDER BY t.id`, [goalId])).rows);
  const installed = (await db.query("SELECT to_regclass('public.planning_requests') present")).rows[0].present;
  const requests = installed ? (await db.query(`SELECT DISTINCT ON(p.scope) p.id,p.scope,p.state,p.task_id,
    p.source_revision,p.updated_at,left(p.reason,280) reason,
    receipt.code,receipt.blockers->0 blocker,
    r.id run_id,r.state run_state,r.model,account.label account_label
    FROM planning_requests p LEFT JOIN tasks t ON t.id=p.task_id
    LEFT JOIN LATERAL(SELECT id,state,model,account_id FROM runs WHERE task_id=t.id
      ORDER BY (state IN('queued','running','cancelling','reconciling')) DESC,attempt DESC LIMIT 1)r ON true
    LEFT JOIN accounts account ON account.id=COALESCE(r.account_id,t.account_id)
    LEFT JOIN LATERAL jsonb_to_record(p.receipt) AS receipt(code text,blockers jsonb) ON true
    WHERE p.goal_id=$1 ORDER BY p.scope,(COALESCE(r.state,'') IN('queued','running','cancelling','reconciling')) DESC,
      (p.state IN('pending','assigned','running','preparing','replanning_required')) DESC,p.updated_at DESC,p.id DESC`, [goalId])).rows : [];
  const capacity = installed ? await readScopedPlannerCapacity(db, goalId) : null;
  return { ...buildPlannerPanel(goal, policy, tasks, requests),
    ...(capacity ? { maxActive: capacity.maxScopePlanners, globalMaxActive: capacity.globalMaxScopePlanners,
      reserved: capacity.occupiedScopePlanners, globalReserved: capacity.occupiedGlobalPlanners } : {}) };
}

export function buildPlannerPanel(goal: Row, policy: Row | undefined, tasks: Row[], requests: Row[], now = Date.now()) {
  const scopes = [...new Set([...tasks.filter(task => !closed.has(task.state) || task.state === 'accepted').map(task => task.scope),
    ...requests.map(request => request.scope)].filter((scope): scope is string => typeof scope === 'string' && !!scope))].sort();
  const rows = scopes.map(scope => {
    const group = tasks.filter(task => task.scope === scope), workers = group.filter(task => task.work_kind !== 'planning');
    const request = requests.find(item => item.scope === scope);
    const canonical = workers.filter(task => !auxiliary.has(task.work_kind));
    const stageOrder: Record<string, number> = { work: 0, integrate: 1, verify: 2 };
    const next = canonical.filter(task => !closed.has(task.state)).sort((a, b) =>
      Number(b.active_run_state === 'running') - Number(a.active_run_state === 'running') ||
      (stageOrder[a.stage] ?? 3) - (stageOrder[b.stage] ?? 3) ||
      Number(b.priority ?? 0) - Number(a.priority ?? 0) || String(a.id).localeCompare(String(b.id)))[0];
    const accepted = workers.filter(task => task.state === 'accepted' && task.acceptance_passed === true &&
      task.current_run_state === 'submitted').sort((a, b) => Date.parse(b.accepted_at) - Date.parse(a.accepted_at))[0];
    const activePlanner = group.find(task => task.work_kind === 'planning' && live.has(task.active_run_state));
    const ownerTaskId = activePlanner?.id ?? request?.task_id ?? null;
    const ownerState = activePlanner?.active_run_state ?? (live.has(request?.run_state) ? request?.run_state : null);
    const blocked = next && !next.active_run_id && ['blocked_dependencies', 'recovery_blocked', 'failed', 'waiting_quota', 'waiting_resource'].includes(next.state);
    const blocker = request?.blocker;
    const reason = blocked ? brief(next.reason) ?? (next.state === 'blocked_dependencies' ? 'Aguardando dependências da entrega.' : 'O gestor precisa encaminhar a recuperação.') :
      ['awaiting_supervision', 'blocked_external', 'replanning_required'].includes(request?.state) ? brief(blocker?.reason) ?? 'Planejamento aguardando encaminhamento.' : null;
    return {
      scope, label: scope.split('/').at(-1)!.replace(/[-_]+/g, ' '),
      planner: { taskId: ownerTaskId, requestId: request?.id ?? null,
        state: ownerState ?? request?.state ?? 'idle', occupied: !!ownerState,
        model: activePlanner?.active_model ?? request?.model ?? null,
        account: request?.task_id === ownerTaskId ? request?.account_label ?? null : null },
      nextDelivery: next ? { taskId: next.id, title: next.title, state: next.state, stage: next.stage } : null,
      blocker: reason ? { reason, code: brief(request?.code, 80), taskId: blocked ? next.id : blocker?.taskId ?? ownerTaskId,
        nextAction: brief(blocker?.nextAction) } : null,
      workers: {
        running: workers.filter(task => task.active_run_state === 'running').length,
        queued: workers.filter(task => task.active_run_state === 'queued').length,
        confirming: workers.filter(task => ['cancelling', 'reconciling'].includes(task.active_run_state)).length,
        ready: workers.filter(task => task.state === 'ready' && !task.active_run_id && task.dependencies_ready &&
          (!task.next_attempt_at || Date.parse(task.next_attempt_at) <= now)).length,
      },
      lastAdvance: accepted ? { taskId: accepted.id, runId: accepted.current_run_id, title: accepted.title,
        at: accepted.accepted_at, kind: auxiliary.has(accepted.work_kind) ? 'auxiliary' : 'delivery', stage: accepted.stage } : null,
      details: { sourceRevision: request?.source_revision ?? null, planningUpdatedAt: request?.updated_at ?? null,
        planningPending: pendingPlans.has(request?.state), canonicalRemaining: canonical.filter(task => !closed.has(task.state)).length },
    };
  });
  return { goalId: goal.id, goalState: goal.state, enabled: policy?.enabled === true,
    maxActive: Number.isInteger(policy?.plannerMaxActive) && policy!.plannerMaxActive > 0 ? policy!.plannerMaxActive : 5,
    observedAt: new Date(now).toISOString(), scopes: rows };
}

export async function plannerPanel(goalId: string) {
  const db = await pool.connect();
  try {
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    await db.query("SET LOCAL statement_timeout='8s'");
    const result = await readPlannerPanel(db, goalId);
    await db.query('COMMIT');
    return result;
  } catch (error) { await db.query('ROLLBACK'); throw error; }
  finally { db.release(); }
}
