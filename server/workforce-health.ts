import { createHash } from 'node:crypto';
import { checkedExecutionMetadata, executionMetadataColumns } from './task-execution-metadata.js';
import type { DB } from './db.js';
import { config } from './config.js';
import { quotaFor } from './accounts.js';
import type { WorkforcePolicy } from './workforce-policy.js';
import { buildMuseWorkforceHealth } from './muse-workforce-health.js';
import { workerRunAdmission, workerSchedulerMaintenance } from './worker-rollout.js';
import { reservesWorkforceBacklog } from './workforce-reserve.js';

const live = new Set(['queued', 'running', 'cancelling', 'reconciling']);
const closed = new Set(['accepted', 'cancelled', 'superseded']);
// Match schedule(): waiting_quota/configuration can become eligible after a
// fresh account observation, and recovered dependency waits must be reconsidered.
const schedulable = new Set([
  'ready',
  'blocked_dependencies',
  'blocked_failed',
  'waiting_quota',
  'waiting_resource',
  'waiting_configuration',
  'retry_scheduled',
]);

type Task = {
  id: string;
  state: string;
  stage?: string;
  attempt_count?: number;
  account_id?: string;
  model?: string;
  allowed_accounts?: string[];
  resources?: string[];
  next_attempt_at?: string | Date | null;
  execution?: { kind?: string; workforceLane?: string; workKind?: string; testFile?: string; protocolDelivery?: string; backlogContext?: Record<string, unknown> | null };
  dependency_blocked?: boolean;
  has_live_run?: boolean;
  live_state?: string | null;
};
export type WorkforceHealthInput = {
  goalState: string;
  tasks: Task[];
  policy: WorkforcePolicy;
  account?: any;
  accountOccupied: number;
  museAccounts?: { account: any; occupied: number }[];
  admission?: {glm:{allowed:boolean;service:string};glmHarness?:{allowed:boolean;service:string};muse:{allowed:boolean;service:string}};
  globalOccupied: number;
  goalOccupied: number;
  limits: { maxRuns: number; remoteSlots: number; browserSlots: number; objectiveSlots?: number };
  leases: Record<string, number>;
  remoteExecution: boolean;
  bridgeConfigured: boolean;
};
export type WorkforceHealth = {
  version: 1;
  observedAt: string;
  fingerprint: string;
  glm: {
    target: number;
    requestLimit: number;
    running: number;
    queued: number;
    confirming: number;
    occupied: number;
    ready: number;
    admittedReady: number;
    admissionBlocked: number;
    executors: {kind:string;service:string;allowed:boolean;ready:number;occupied:number;available:number}[];
    available: number;
    blocked: number;
    parked: number;
    delayed: number;
    deficit: number;
    pending: number;
    totalPending: number;
    pendingLimit: number;
    reserveTarget: number;
    reserveDeficit: number;
    freeSlots: number;
    reason: string;
    admission: {allowed:boolean;service:string};
    planning: {running:number;queued:number;occupied:number};
  };
  muse: ReturnType<typeof buildMuseWorkforceHealth>;
  status: string;
  reason: string;
  needsPlanning: boolean;
  refillPossible: boolean;
  replenishment: 'scheduler' | 'supervisor' | 'none';
  bottlenecks: { code: string; count: number }[];
  limits: WorkforceHealthInput['limits'];
};

/** Read model only: never creates tasks, retries, sessions or provider calls. */
export function buildWorkforceHealth(
  input: WorkforceHealthInput,
  now = Date.now(),
): WorkforceHealth {
  const { tasks, policy, account, leases, limits } = input;
  const target = Math.max(1, policy.glmTarget ?? 12);
  const requestLimit = Math.max(1, policy.glmRequestLimit ?? 6);
  const workers = tasks.filter((t) => !['coordinate', 'review'].includes(t.stage ?? ''));
  const pending = workers.filter((t) => (t.execution?.kind === 'opencode' ||
    (policy.glmHarnessEnabled === true && t.execution?.kind === 'harness' &&
      t.execution.workforceLane === 'glm' && t.account_id === policy.glmAccountId)) && !closed.has(t.state));
  const occupiedTasks = pending.filter((t) => t.has_live_run || live.has(t.state));
  const occupied = occupiedTasks.length;
  const countLive = (states: string[]) =>
    occupiedTasks.filter((t) => states.includes(t.live_state ?? t.state)).length;
  const blocked = pending.filter(
    (t) => !t.has_live_run && !live.has(t.state) && t.dependency_blocked,
  ).length;
  const due = (t: Task) => !t.next_attempt_at || new Date(t.next_attempt_at).getTime() <= now;
  const ready = pending.filter(
    (t) =>
      !t.has_live_run &&
      !live.has(t.state) &&
      !t.dependency_blocked &&
      schedulable.has(t.state) &&
      t.execution?.workKind!=='planning' &&
      due(t),
  );
  const delayed = pending.filter(
    (t) =>
      !t.has_live_run &&
      !live.has(t.state) &&
      !t.dependency_blocked &&
      schedulable.has(t.state) &&
      !due(t),
  ).length;
  const parked = pending.length - occupied - blocked - ready.length - delayed;
  const issues = new Map<string, number>();
  const issue = (code: string, count = 1) => issues.set(code, (issues.get(code) ?? 0) + count);
  if (blocked) issue('DEPENDENCIES', blocked);
  if (parked) issue('SUPERVISION_OR_EVIDENCE', parked);
  if (delayed) issue('RETRY_DELAY', delayed);
  const incomplete = workers.some((t) => !closed.has(t.state));
  const enabled = policy.enabled && input.goalState === 'active';
  let accountBlocked: string | undefined;
  if (!account?.enabled || !account?.bridge_id) accountBlocked = 'ACCOUNT_CONFIGURATION';
  else {
    const quota = quotaFor(account, account.model);
    const stale = account.observed_at && now - new Date(account.observed_at).getTime() > 600000;
    if (quota.status === 'auth_required') accountBlocked = 'AUTH_REQUIRED';
    else if (quota.status === 'exhausted' || (quota.remaining !== null && quota.remaining <= 0))
      accountBlocked = 'QUOTA_EXHAUSTED';
    else if ((quota.status === 'unknown' || stale) && !account.allow_unknown)
      accountBlocked = 'QUOTA_UNKNOWN';
  }
  if (!input.remoteExecution || !input.bridgeConfigured) accountBlocked = 'EXECUTOR_CONFIGURATION';
  const nativeAdmission=input.admission?.glm??{allowed:true,service:'agentos-opencode'};
  const harnessAdmission=input.admission?.glmHarness??input.admission?.muse??{allowed:true,service:'agentos-workers'};
  const admissionFor=(t:Task)=>t.execution?.kind==='harness'?harnessAdmission:nativeAdmission;
  const executors=[['opencode',nativeAdmission],['harness',harnessAdmission]].map(([kind,a])=>{
    const route=a as typeof nativeAdmission;
    return {kind:kind as string,...route,ready:ready.filter(t=>t.execution?.kind===kind).length,
      occupied:occupiedTasks.filter(t=>t.execution?.kind===kind).length,available:0};
  });
  const admittedReady=ready.filter(t=>admissionFor(t).allowed);
  const admissionBlocked=ready.length-admittedReady.length;
  const admission={allowed:nativeAdmission.allowed || (policy.glmHarnessEnabled===true && harnessAdmission.allowed),
    service:executors.filter(e=>e.kind==='opencode' || policy.glmHarnessEnabled===true).map(e=>e.service).join(',')};
  if(admissionBlocked || !admission.allowed)issue('ROLLOUT_DRAIN',Math.max(1,admissionBlocked));
  if (accountBlocked) issue(accountBlocked);
  const headroom = {
    workforce: target - occupied,
    account: account ? account.max_concurrency - input.accountOccupied : 0,
    global: limits.maxRuns - input.globalOccupied,
    objective: (limits.objectiveSlots ?? Number.MAX_SAFE_INTEGER) - input.goalOccupied,
    remote: leases['host:agentos-remote'] ? 0 : limits.remoteSlots - (leases['remote-slot'] ?? 0),
  };
  for (const [key, value] of Object.entries(headroom))
    if (value <= 0)
      issue(
        key === 'remote' && leases['host:agentos-remote']
          ? 'HOST_EXCLUSIVE'
          : key.toUpperCase() + '_CAPACITY',
      );
  const freeSlots =
    enabled && !accountBlocked && admission.allowed ? Math.max(0, Math.min(...Object.values(headroom))) : 0;
  // Estimate admissions using the scheduler's exact lease capacities and ordering.
  // Availability is a snapshot, never a reservation or admission authority.
  const simulated = { ...leases };
  let available = 0;
  for (const t of ready) {
    if(!admissionFor(t).allowed)continue;
    if (
      (t.account_id && t.account_id !== policy.glmAccountId) ||
      (t.allowed_accounts?.length && !t.allowed_accounts.includes(policy.glmAccountId))
    ) {
      issue('ACCOUNT_SCOPE');
      continue;
    }
    if (t.model && !(account?.models ?? []).includes(t.model)) {
      issue('MODEL_UNAVAILABLE');
      continue;
    }
    if (available >= freeSlots) continue;
    const resources = t.resources ?? [];
    const resourceBlocked = resources.some((resource) => {
      if (resource === 'remote-slot' && simulated['host:agentos-remote']) return true;
      if (resource === 'host:agentos-remote' && simulated['remote-slot']) return true;
      const cap =
        resource === 'browser'
          ? limits.browserSlots
          : resource === 'remote-slot'
            ? limits.remoteSlots
            : 1;
      return (simulated[resource] ?? 0) >= cap;
    });
    if (resourceBlocked) {
      issue('RESOURCE_LEASE');
      continue;
    }
    available++;
    executors.find(e=>e.kind===t.execution?.kind)!.available++;
    for (const resource of resources) simulated[resource] = (simulated[resource] ?? 0) + 1;
  }
  const reserveTarget = target;
  const reserveDeficit = Math.max(0, reserveTarget - admittedReady.length);
  const muse=buildMuseWorkforceHealth(input,simulated,available,now);
  const glmNeedsPlanning=enabled&&incomplete&&admission.allowed&&reserveDeficit>0;
  const glmRefillPossible=enabled&&available>0;
  const needsPlanning = glmNeedsPlanning || muse.needsPlanning;
  const refillPossible = glmRefillPossible || muse.refillPossible;
  let status = 'backlog_missing',
    reason = 'Sem tarefa GLM pronta; o supervisor precisa delimitar o próximo trabalho útil.';
  if (!incomplete) {
    status = 'complete';
    reason = 'Não há trabalho pendente neste objetivo.';
  } else if (!enabled) {
    status = 'inactive';
    reason = 'Objetivo ou política de workforce não está ativo.';
  } else if (!admission.allowed || (ready.length>0 && admittedReady.length===0)) {
    status = 'rollout';
    reason = 'GLM aguarda atualização confirmada do executor; tarefas prontas permanecem na fila.';
  } else if (accountBlocked) {
    status = 'configuration';
    reason = 'Executor ou conta GLM indisponível: ' + accountBlocked + '.';
  } else if (glmRefillPossible) {
    status = 'refilling';
    reason = `${available} tarefa(s) GLM podem ocupar vagas pelo scheduler, sem chamar o modelo para repor uma sessão.`;
  } else if (ready.length) {
    status = 'capacity';
    reason = 'Há tarefas GLM prontas, aguardando limite de sessões, conta ou recursos.';
  } else if (occupied >= target) {
    status = 'full';
    reason = 'Meta de sessões GLM ocupada; falta reserva útil para as próximas vagas.';
  } else if (blocked || parked || delayed) {
    status = 'blocked';
    reason = `Sem tarefa GLM pronta: ${blocked} aguardam dependências, ${parked} supervisão/insumos e ${delayed} o prazo de retomada.`;
  }
  const fingerprint = createHash('sha256')
    .update(
      JSON.stringify({
        goalState: input.goalState,
        target,
        requestLimit,
        museTarget: muse.target,
        admission: [nativeAdmission.allowed,harnessAdmission.allowed,muse.admission.allowed],
        tasks: workers
          .map((t) => [
            t.id,
            t.has_live_run || live.has(t.state)
              ? 'live'
              : schedulable.has(t.state)
                ? 'dispatchable'
                : t.state,
            // A new accepted source is semantic evidence; a failed retry counter is not.
            t.state === 'accepted' ? (t.attempt_count ?? 0) : 0,
            !!t.dependency_blocked,
            due(t),
            t.execution?.kind,
          ])
          .sort((a, b) => String(a[0]).localeCompare(String(b[0]))),
      }),
    )
    .digest('hex');
  const glmReason=reason;
  if(muse.refillPossible&&!glmRefillPossible){status='refilling';reason=muse.reason;}
  return {
    version: 1,
    observedAt: new Date(now).toISOString(),
    fingerprint,
    glm: {
      target,
      requestLimit,
      planning:{running:occupiedTasks.filter(t=>t.execution?.workKind==='planning' && (t.live_state??t.state)==='running').length,
        queued:occupiedTasks.filter(t=>t.execution?.workKind==='planning' && (t.live_state??t.state)==='queued').length,
        occupied:occupiedTasks.filter(t=>t.execution?.workKind==='planning').length},
      running: countLive(['running']),
      queued: countLive(['queued']),
      confirming: countLive(['cancelling', 'reconciling']),
      occupied,
      ready: ready.length,
      admittedReady:admittedReady.length,
      admissionBlocked,
      executors,
      available,
      blocked,
      parked,
      delayed,
      deficit: Math.max(0, target - occupied),
      pending: pending.filter(reservesWorkforceBacklog).length,
      totalPending: pending.length,
      pendingLimit: 2 * target,
      reserveTarget,
      reserveDeficit,
      freeSlots,
      reason: glmReason,
      admission,
    },
    muse,
    status,
    reason,
    needsPlanning,
    refillPossible,
    replenishment: refillPossible ? 'scheduler' : needsPlanning ? 'supervisor' : 'none',
    bottlenecks: [...issues].map(([code, count]) => ({ code, count })),
    limits,
  };
}

/** Coalesce semantic planning demand; the existing supervisor owns actual wake/admission. */
export function workforcePlanningWake(
  health: Pick<WorkforceHealth, 'needsPlanning' | 'fingerprint'>,
  managerState: { workforceFingerprint?: string; requestedAt?: string } | undefined,
  now = Date.now(),
) {
  if (!health.needsPlanning || managerState?.workforceFingerprint === health.fingerprint)
    return false;
  const requested = Date.parse(managerState?.requestedAt ?? '');
  return !Number.isFinite(requested) || now - requested >= 120000;
}

export async function readWorkforceHealth(
  db: DB,
  goalId: string,
  policy: WorkforcePolicy,
  now = Date.now(),
) {
  const goal = (await db.query('SELECT state FROM goals WHERE id=$1', [goalId])).rows[0];
  const tasks = checkedExecutionMetadata((
    await db.query(
      `SELECT t.id,t.state,t.stage,t.attempt_count,t.account_id,t.model,t.allowed_accounts,t.resources,t.next_attempt_at,
    ${executionMetadataColumns},
    jsonb_build_object('kind',control.kind,'workforceLane',control."workforceLane",
      'workKind',control."workKind",'testFile',control."testFile",
      'protocolDelivery',control."protocolDelivery",'backlogContext',
      CASE WHEN jsonb_typeof(control."backlogContext")='object' THEN '{}'::jsonb ELSE NULL END) execution,
    EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=t.id AND p.state<>'accepted') dependency_blocked,
    r.state live_state,r.id IS NOT NULL has_live_run FROM tasks t
    LEFT JOIN runs r ON r.task_id=t.id AND r.state IN('queued','running','cancelling','reconciling')
    LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    LEFT JOIN LATERAL jsonb_to_record(metadata.control)
      AS control(kind text,"workforceLane" text,"workKind" text,"testFile" text,"protocolDelivery" text,"backlogContext" jsonb) ON true
    WHERE t.goal_id=$1 AND (t.state NOT IN('accepted','cancelled','superseded') OR r.id IS NOT NULL)
    ORDER BY t.priority+CASE WHEN $2::boolean THEN
      CASE WHEN t.stage IN('integrate','verify','review') THEN 40
        WHEN control."workKind"='repair' THEN 20 ELSE 0 END ELSE 0 END DESC,t.created_at`,
      [goalId,policy.deliveryFirstEnabled===true],
    )
  ).rows, { objectRequired: false });
  const account = (await db.query('SELECT * FROM accounts WHERE id=$1', [policy.glmAccountId]))
    .rows[0];
  const museAccounts=(await db.query(
    `SELECT a.*,
     (SELECT count(*) FROM runs WHERE account_id=a.id AND state IN('queued','running','cancelling','reconciling'))+
     (SELECT count(*) FROM cycle_attempts WHERE account_id=a.id AND state IN('running','cancelling','reconciling'))+
     (SELECT count(*) FROM chat_messages WHERE account_id=a.id AND state IN('queued','running','cancelling'))+
     (SELECT count(*) FROM planning_runs WHERE account_id=a.id AND state='running' AND output IS NULL) occupied
     FROM accounts a WHERE provider='muse' AND id=ANY($1::text[])`,
    [policy.workerAccounts??[]],
  )).rows.map(({occupied,...account})=>({account,occupied:Number(occupied)}));
  const maintenance=await workerSchedulerMaintenance(db);
  const admission={
    glm:workerRunAdmission(maintenance,{execution:{kind:'opencode'},workforceEnabled:policy.enabled}),
    glmHarness:workerRunAdmission(maintenance,{execution:{kind:'harness'},workforceEnabled:policy.enabled,
      detachedHarnessEnabled:policy.detachedHarnessEnabled}),
    muse:workerRunAdmission(maintenance,{execution:{kind:'harness'},workforceEnabled:policy.enabled,
      detachedHarnessEnabled:policy.detachedHarnessEnabled}),
  };
  const settings = (
    await db.query("SELECT id,value FROM settings WHERE id IN('capacity','remote-capacity')")
  ).rows;
  const global = settings.find((s) => s.id === 'capacity')?.value ?? {
    maxRuns: 4,
    browserSlots: 2,
  };
  const remote = settings.find((s) => s.id === 'remote-capacity')?.value ?? { maxRuns: 1 };
  const objective = (await db.query('SELECT request FROM objectives WHERE id=$1', [goalId]))
    .rows[0];
  const objectiveCapacity=(await db.query('SELECT value FROM settings WHERE id=$1',['objective-capacity:'+goalId])).rows[0]?.value?.maxRuns;
  const counts = (
    await db.query(
      `SELECT
    (SELECT count(*) FROM runs WHERE state IN('queued','running','cancelling','reconciling'))+
    (SELECT count(*) FROM cycle_attempts WHERE account_id IS NOT NULL AND state IN('running','cancelling','reconciling')) global,
    (SELECT count(*) FROM runs WHERE account_id=$1 AND state IN('queued','running','cancelling','reconciling'))+
    (SELECT count(*) FROM cycle_attempts WHERE account_id=$1 AND state IN('running','cancelling','reconciling'))+
    (SELECT count(*) FROM chat_messages WHERE account_id=$1 AND state IN('queued','running','cancelling'))+
    (SELECT count(*) FROM planning_runs WHERE account_id=$1 AND state='running' AND output IS NULL) account,
    (SELECT count(*) FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$2 AND r.state IN('queued','running','cancelling','reconciling')) goal`,
      [policy.glmAccountId, goalId],
    )
  ).rows[0];
  const leases = Object.fromEntries(
    (await db.query('SELECT resource,count(*)::int count FROM leases GROUP BY resource')).rows.map(
      (r) => [r.resource, r.count],
    ),
  );
  return buildWorkforceHealth(
    {
      goalState: goal?.state ?? 'missing',
      tasks,
      policy,
      account,
      museAccounts,
      admission,
      accountOccupied: Number(counts.account),
      globalOccupied: Number(counts.global),
      goalOccupied: Number(counts.goal),
      limits: {
        maxRuns: global.maxRuns,
        browserSlots: global.browserSlots,
        remoteSlots: remote.maxRuns,
        ...(objectiveCapacity??objective?.request?.requestedCapacity
          ? { objectiveSlots: objectiveCapacity??objective.request.requestedCapacity }
          : {}),
      },
      leases,
      remoteExecution: config.remoteExecution,
      bridgeConfigured: !!config.bridgeHost,
    },
    now,
  );
}

export async function persistWorkforceHealth(
  db: DB,
  goalId: string,
  policy: WorkforcePolicy,
  now = Date.now(),
) {
  const value = await readWorkforceHealth(db, goalId, policy, now);
  await db.query(
    'INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    ['workforce-health:' + goalId, JSON.stringify(value)],
  );
  return value;
}
