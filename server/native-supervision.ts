import { createHash, randomUUID } from 'node:crypto';
import { event, lock, pool, tx, type DB } from './db.js';
import { allocateRuntimeMemory, canReserveHarnessMemory, validRuntimeMemory, sameRuntimeMemory, type RuntimeMemoryReservation } from './runtime-memory.js';
import { hasRuntimeProgressStop, runtimeProgressCode } from './runtime-progress-contract.js';
import {hasOperatorRecoveryIntent,operatorRecoveryCode} from './operator-recovery-contract.js';

export const nativeSupervisorLockNamespace = 4280041;
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const releasePath = /^\/opt\/agentos\/releases\/managed-[0-9a-f-]{36}$/;
export type NativeRelease = { candidate: string; digest: string };
export type NativeReservation = NativeRelease & {
  version: 1;
  mode: 'detached';
  runtime?: 'opencode' | 'harness';
  runId: string;
  runFence: string;
  launchToken: string;
  unit: string;
  launchAttempt: number;
  reservedAt: string;
  resources?: RuntimeMemoryReservation;
};
export type NativeUnitProof = {
  unit: string;
  pid: number;
  cwd: string;
  cgroup: string;
};
export type NativeUnitObservation = {
  unit: string;
  checkedAt: string;
  activeState: string;
  loadState: string;
  mainPid: number;
  pendingJob: boolean;
  cgroupEmpty: boolean;
};

export function nativeSupervisorUnit(runId: string) {
  if (!uuid.test(runId)) throw Error('NATIVE_RUN_ID_INVALID');
  return 'agentos-native-supervisor-' + runId;
}
export function nativeSupervisorCgroup(runId: string) {
  // An underscore avoids systemd's implicit parent slices for hyphenated names.
  return '/agentos_native_supervisors.slice/' + nativeSupervisorUnit(runId) + '.service';
}
export function nativeSupervisorLockKey(runId: string) {
  if (!uuid.test(runId)) throw Error('NATIVE_RUN_ID_INVALID');
  return createHash('sha256').update(runId).digest().readUInt32BE(0) & 0x7fffffff;
}
export function assertNativeRelease(release: NativeRelease) {
  if (!releasePath.test(release.candidate) || !/^[0-9a-f]{64}$/.test(release.digest))
    throw Error('NATIVE_RELEASE_INVALID');
}
export async function installNativeSupervision(db: DB) {
  await db.query(`CREATE TABLE IF NOT EXISTS opencode_jobs (
    run_id uuid PRIMARY KEY REFERENCES runs(id),state text NOT NULL DEFAULT 'queued',
    created_at timestamptz NOT NULL DEFAULT now(),started_at timestamptz,finished_at timestamptz
  )`);
  await db.query('ALTER TABLE opencode_jobs ADD COLUMN IF NOT EXISTS supervision jsonb');
}

/** Caller holds the normal controller transaction. Only a never-started queued
 * run is reservable. Reserving a launch does not start/retry the actual agent. */
export async function reserveNativeSupervisor(db: DB, runId: string, release: NativeRelease) {
  assertNativeRelease(release);
  nativeSupervisorUnit(runId);
  await lock(db);
  const row = (
    await db.query(
      `SELECT r.id,r.fence,r.task_id,r.runtime,r.attempt,r.checkpoint,
        t.execution->>'workspace' workspace,j.supervision,a.provider,a.bridge_id FROM runs r
      JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id JOIN opencode_jobs j ON j.run_id=r.id
      LEFT JOIN accounts a ON a.id=r.account_id
      WHERE r.id=$1 AND r.state='queued' AND t.state='queued' AND
        ((r.runtime='opencode' AND t.execution->>'kind'='opencode') OR
         (r.runtime='harness' AND t.execution->>'kind'='harness' AND
           EXISTS(SELECT 1 FROM settings WHERE id='workforce:'||g.id
             AND value->>'enabled'='true' AND value->>'detachedHarnessEnabled'='true')))
      AND g.state='active' AND r.worker_id IS NULL AND r.started_at IS NULL AND r.context_id IS NULL
      AND j.state='queued' AND (j.supervision->>'nextLaunchAt' IS NULL OR
        (j.supervision->>'nextLaunchAt')::timestamptz<=now())
      AND r.attempt=t.attempt_count
      AND NOT EXISTS(SELECT 1 FROM events e WHERE e.payload->>'runId'=r.id::text
        AND e.kind IN('RUN_STARTED','OPENCODE_LAUNCH_ATTEMPTED','HARNESS_LAUNCH_ATTEMPTED'))
      FOR UPDATE OF r,j`,
      [runId],
    )
  ).rows[0];
  if (!row) return null;
  const reservation: NativeReservation = {
    version: 1,
    mode: 'detached',
    ...(row.runtime==='harness'?{runtime:'harness' as const}:{}),
    runId,
    runFence: row.fence,
    launchToken: randomUUID(),
    unit: nativeSupervisorUnit(runId),
    ...release,
    launchAttempt: Number(row.supervision?.launchAttempt ?? 0) + 1,
    reservedAt: new Date().toISOString(),
  };
  // Muse keeps its native runner and conservative legacy charge; it does not
  // implement the Harness cgroup profile/receipt contract.
  if (row.runtime === 'opencode' || (row.provider!=='muse' && !row.bridge_id?.startsWith('agentos:muse:'))) {
    const allocation = await allocateRuntimeMemory(db,reservation,row,row.workspace);
    if (!allocation) return null;
    reservation.resources = allocation;
  } else if (!(await canReserveHarnessMemory(db))) return null;
  await db.query("UPDATE opencode_jobs SET state='launching',supervision=$2 WHERE run_id=$1", [
    runId,
    JSON.stringify(reservation),
  ]);
  await event(db, 'OPENCODE_SUPERVISOR_RESERVED', row.task_id, {
    runId,
    unit: reservation.unit,
    launchToken: reservation.launchToken,
    candidate: reservation.candidate,
    digest: reservation.digest,
    memoryMiB: reservation.resources?.memoryMiB,
    memoryReservationId: reservation.resources?.id,
  });
  return reservation;
}

function sameReservation(actual: any, expected: NativeReservation) {
  return (
    actual?.version === 1 &&
    actual.mode === 'detached' &&
    (actual.runtime??'opencode')===(expected.runtime??'opencode') &&
    ((!actual.resources && !expected.resources) ||
       (validRuntimeMemory(actual.resources,expected) && sameRuntimeMemory(actual.resources,expected.resources))) &&
    ['runId', 'runFence', 'launchToken', 'unit', 'candidate', 'digest'].every(
      (key) => actual[key] === (expected as any)[key],
    )
  );
}

/** The dedicated connection holds the run lock for the entire gateway/SSH
 * lifetime. The self proof is checked by the systemd entry point before here. */
export async function registerNativeSupervisor(
  db: DB,
  reservation: NativeReservation,
  proof: NativeUnitProof,
  backendPid: number,
) {
  assertNativeRelease(reservation);
  if (
    proof.unit !== nativeSupervisorUnit(reservation.runId) ||
    proof.cwd !== reservation.candidate ||
    proof.cgroup !== nativeSupervisorCgroup(reservation.runId) ||
    !Number.isSafeInteger(proof.pid) ||
    proof.pid <= 0
  )
    throw Error('NATIVE_SUPERVISOR_IDENTITY_INVALID');
  await lock(db);
  const key = nativeSupervisorLockKey(reservation.runId);
  const owns = (
    await db.query(
      `SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objsubid=2
    AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
    AND classid=$1::oid AND objid=$2::oid AND pid=$3 AND granted`,
      [nativeSupervisorLockNamespace, key, backendPid],
    )
  ).rowCount;
  if (!owns) throw Error('NATIVE_SUPERVISOR_OWNER_MISSING');
  const row = (
    await db.query(
      `SELECT j.state,j.supervision,r.fence,r.task_id,r.runtime,r.state run_state,t.state task_state,g.state goal_state
    FROM opencode_jobs j JOIN runs r ON r.id=j.run_id JOIN tasks t ON t.id=r.task_id
      JOIN goals g ON g.id=t.goal_id WHERE r.id=$1 FOR UPDATE OF r,j`,
      [reservation.runId],
    )
  ).rows[0];
  if (
    !row ||
    row.run_state !== 'queued' ||
    row.task_state !== 'queued' ||
    row.goal_state !== 'active' ||
    row.fence !== reservation.runFence ||
    row.runtime !== (reservation.runtime??'opencode') ||
    row.state !== 'launching' ||
    ((row.runtime === 'opencode' || row.supervision?.resources) && row.supervision?.resources?.state !== 'reserved') ||
    !sameReservation(row.supervision, reservation)
  )
    return false;
  const ready = {
    ...proof,
    backendPid,
    lockNamespace: nativeSupervisorLockNamespace,
    lockKey: key,
    runFence: reservation.runFence,
    launchToken: reservation.launchToken,
    readyAt: new Date().toISOString(),
  };
  await db.query(
    "UPDATE opencode_jobs SET state='running',started_at=now(),supervision=$2 WHERE run_id=$1",
    [reservation.runId, JSON.stringify({ ...row.supervision, owner: ready })],
  );
  await event(db, 'OPENCODE_SUPERVISOR_READY', row.task_id, {
    runId: reservation.runId,
    unit: proof.unit,
    pid: proof.pid,
    candidate: reservation.candidate,
    digest: reservation.digest,
    launchToken: reservation.launchToken,
  });
  return true;
}

/** Registration and start are separate transactions. Recheck pause/cancel and
 * the exact detached owner under the scheduler lock at the actual start, so a
 * goal paused between those phases cannot launch remote work. */
export async function startDetachedNativeRun(
  db: DB,
  reservation: NativeReservation,
  workerId: string,
) {
  const prefix=reservation.runtime==='harness'?'harness-native-':'opencode-native-';
  if (!new RegExp('^'+prefix+'\\d+$').test(workerId)) throw Error('NATIVE_WORKER_ID_INVALID');
  await lock(db);
  if (((reservation.runtime??'opencode') === 'opencode' || reservation.resources) &&
      (!validRuntimeMemory(reservation.resources,reservation) || reservation.resources?.state !== 'reserved')) return null;
  const run = (
    await db.query(
      `UPDATE runs r SET state='running',worker_id=$4,started_at=now(),heartbeat_at=now()
    FROM tasks t,goals g,opencode_jobs j
    WHERE r.id=$1 AND r.fence=$2 AND r.state='queued' AND r.runtime=$7
    AND t.id=r.task_id AND t.state='queued' AND t.execution->>'kind'=$7 AND r.attempt=t.attempt_count
    AND ($7='opencode' OR EXISTS(SELECT 1 FROM settings WHERE id='workforce:'||t.goal_id
      AND value->>'enabled'='true' AND value->>'detachedHarnessEnabled'='true'))
    AND g.id=t.goal_id AND g.state='active' AND j.run_id=r.id AND j.state='running'
    AND j.supervision->>'launchToken'=$3 AND j.supervision->>'candidate'=$5 AND j.supervision->>'digest'=$6
    AND j.supervision->'owner'->>'pid'=$8 AND ${independentNativeSupervisorSql()}
    AND (($7='harness' AND $9::jsonb IS NULL AND NOT(j.supervision ? 'resources')) OR
      (j.supervision->'resources'=$9::jsonb AND
      j.supervision->'resources'->>'state'='reserved' AND
      j.supervision->'resources'->>'workspace'=t.execution->>'workspace'))
    RETURNING r.*`,
      [
        reservation.runId,
        reservation.runFence,
        reservation.launchToken,
        workerId,
        reservation.candidate,
        reservation.digest,
        reservation.runtime??'opencode',
        workerId.slice(prefix.length),
        reservation.resources ? JSON.stringify(reservation.resources) : null,
      ],
    )
  ).rows[0];
  if (!run) return null;
  await db.query("UPDATE tasks SET state='running',updated_at=now() WHERE id=$1", [run.task_id]);
  await event(db, 'RUN_STARTED', run.task_id, { runId: run.id, workerId });
  // Ephemeral owner envelope, not a task/model field or a persisted rewrite.
  // Its exact allocation was revalidated atomically by the UPDATE above.
  return {...run,...(reservation.runtime==='harness' && reservation.resources ?
    {memoryReservation:reservation.resources,launchToken:reservation.launchToken}:{})};
}

/** A supervisor disappearing is not proof that its remote sandbox stopped.
 * Preserve leases/context and put active work into reconciliation. Never launch
 * another agent for that run. Rotation only fences genuinely never-started work. */
export async function reconcileNativeSupervisor(
  db: DB,
  reservation: NativeReservation,
  observation: NativeUnitObservation,
) {
  await lock(db);
  const row = (
    await db.query(
      `SELECT r.*,j.state job_state,j.supervision FROM runs r
    JOIN opencode_jobs j ON j.run_id=r.id WHERE r.id=$1 FOR UPDATE OF r,j`,
      [reservation.runId],
    )
  ).rows[0];
  if (!row || row.fence !== reservation.runFence || !sameReservation(row.supervision, reservation))
    return 'changed';
  const age = Date.now() - Date.parse(observation.checkedAt);
  const stopped =
    observation.unit === reservation.unit &&
    Number.isFinite(age) &&
    age >= -5000 &&
    age <= 30000 &&
    ['inactive', 'failed'].includes(observation.activeState) &&
    ['loaded', 'not-found'].includes(observation.loadState) &&
    observation.mainPid === 0 &&
    observation.pendingJob === false &&
    observation.cgroupEmpty === true;
  if (!stopped) return 'observing';
  const locked = (
    await db.query(
      `SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objsubid=2
    AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
    AND classid=$1::oid AND objid=$2::oid AND granted`,
      [nativeSupervisorLockNamespace, nativeSupervisorLockKey(row.id)],
    )
  ).rowCount;
  if (locked) return 'observing';
  if (['submitted', 'failed', 'cancelled'].includes(row.state)) {
    // A cancellation before start has no sandbox state file to inspect. Only
    // this fresh supervisor-stop + absent owner + absent start/launch evidence
    // proves that no sandbox could have been launched. Terminal state alone
    // remains insufficient for every started execution.
    if (row.supervision.resources?.state === 'reserved' && row.worker_id === null &&
        row.started_at === null && row.context_id === null &&
        !(await db.query("SELECT 1 FROM events WHERE payload->>'runId'=$1 AND kind IN ('RUN_STARTED','OPENCODE_LAUNCH_ATTEMPTED','HARNESS_LAUNCH_ATTEMPTED')",[row.id])).rowCount) {
      const allocation={...row.supervision.resources,state:'released',releasedAt:new Date().toISOString(),
        releaseProofHash:createHash('sha256').update(JSON.stringify(observation)).digest('hex'),releaseReason:'never_started_fenced'};
      await db.query("UPDATE opencode_jobs SET supervision=jsonb_set(jsonb_set(supervision,'{resources}',$2::jsonb),'{launchToken}',$3::jsonb) WHERE run_id=$1",
        [row.id,JSON.stringify(allocation),JSON.stringify(randomUUID())]);
      await event(db,'RUNTIME_MEMORY_RELEASED',row.task_id,{runId:row.id,reservationId:allocation.id,
        runFence:row.fence,memoryMiB:allocation.memoryMiB,reason:'never_started_fenced',proof:observation});
    }
    await db.query(
      "UPDATE opencode_jobs SET state='finished',finished_at=COALESCE(finished_at,now()) WHERE run_id=$1",
      [row.id],
    );
    return 'finished';
  }
  const neverStarted =
    row.state === 'queued' &&
    row.worker_id === null &&
    row.started_at === null &&
    row.context_id === null &&
    !(
      await db.query(
        `SELECT 1 FROM events WHERE payload->>'runId'=$1 AND
      kind IN('RUN_STARTED','OPENCODE_LAUNCH_ATTEMPTED','HARNESS_LAUNCH_ATTEMPTED')`,
        [row.id],
      )
    ).rowCount;
  if (neverStarted) {
    // The next reservation gets a different token. A delayed old unit cannot
    // register/start after this invalidation even if systemd later starts it.
    const next = {
      ...row.supervision,
      launchToken: randomUUID(),
      owner: null,
      nextLaunchAt: new Date(Date.now() + 5000).toISOString(),
      lastStoppedProof: observation,
      ...(row.supervision.resources ? { resources: { ...row.supervision.resources, state: 'released',
        releasedAt: new Date().toISOString(), releaseProofHash: createHash('sha256').update(JSON.stringify(observation)).digest('hex'),
        releaseReason: 'never_started_fenced' } } : {}),
    };
    await db.query(
      "UPDATE opencode_jobs SET state='queued',started_at=NULL,supervision=$2 WHERE run_id=$1",
      [row.id, JSON.stringify(next)],
    );
    await event(db, 'OPENCODE_SUPERVISOR_NOT_STARTED', row.task_id, {
      runId: row.id,
      launchToken: reservation.launchToken,
      confirmed: true,
      runFence: row.fence,
      proof: observation,
      memoryReservationId: row.supervision.resources?.id,
      memoryReleased: Boolean(row.supervision.resources),
    });
    return 'queued';
  }
  if (['running', 'cancelling', 'reconciling'].includes(row.state)) {
    const progressStop = row.error_code === runtimeProgressCode && hasRuntimeProgressStop(row.checkpoint,row.id,row.fence);
    const operatorStop=row.error_code===operatorRecoveryCode && await hasOperatorRecoveryIntent(db,row);
    await db.query(
      "UPDATE runs SET state='reconciling',error_code=$3 WHERE id=$1 AND fence=$2",
      [row.id, row.fence, operatorStop?operatorRecoveryCode:progressStop ? runtimeProgressCode : 'SUPERVISOR_LOST'],
    );
    await db.query(
      "UPDATE tasks SET state='reconciling',reason='Supervisor nativo encerrado; conferir o sandbox remoto antes de retomar.' WHERE id=$1",
      [row.task_id],
    );
    await db.query("UPDATE opencode_jobs SET state='orphaned' WHERE run_id=$1", [row.id]);
    if (row.job_state !== 'orphaned')
      await event(db, 'OPENCODE_SUPERVISOR_LOST', row.task_id, {
        runId: row.id,
        launchToken: reservation.launchToken,
        remoteStopped: false,
        proof: observation,
      });
    return 'reconciling';
  }
  return 'observing';
}

export async function finishNativeSupervisor(db: DB, reservation: NativeReservation) {
  await lock(db);
  const row = (
    await db.query(
      `SELECT r.state,r.fence,j.supervision FROM runs r
    JOIN opencode_jobs j ON j.run_id=r.id WHERE r.id=$1 FOR UPDATE OF j`,
      [reservation.runId],
    )
  ).rows[0];
  if (!row || row.fence !== reservation.runFence || !sameReservation(row.supervision, reservation))
    return false;
  // A STOP_UNCONFIRMED path must never become a replayable queued job.
  const terminal = ['submitted', 'failed', 'cancelled'].includes(row.state);
  await db.query(
    'UPDATE opencode_jobs SET state=$2,finished_at=CASE WHEN $3 THEN now() ELSE NULL END WHERE run_id=$1',
    [reservation.runId, terminal ? 'finished' : 'orphaned', terminal],
  );
  return terminal;
}

export async function superviseNativeRun(
  runId: string,
  launchToken: string,
  prove: (reservation: NativeReservation) => Promise<NativeUnitProof>,
  execute: (runId: string, reservation: NativeReservation) => Promise<void>,
) {
  const key = nativeSupervisorLockKey(runId),
    owner = await pool.connect();
  let owns = false,
    registered = false,
    reservation: NativeReservation | undefined;
  try {
    owns = (
      await owner.query('SELECT pg_try_advisory_lock($1::int,$2::int) owned', [
        nativeSupervisorLockNamespace,
        key,
      ])
    ).rows[0].owned;
    if (!owns) return 'duplicate_owner';
    const row = (
      await owner.query('SELECT supervision FROM opencode_jobs WHERE run_id=$1', [runId])
    ).rows[0];
    if (
      !row?.supervision ||
      row.supervision.launchToken !== launchToken ||
      row.supervision.runId !== runId
    )
      return 'stale_launch';
    reservation = row.supervision as NativeReservation;
    const proof = await prove(reservation);
    const backendPid = (await owner.query('SELECT pg_backend_pid() pid')).rows[0].pid;
    registered = await tx((db) => registerNativeSupervisor(db, reservation!, proof, backendPid));
    if (!registered) return 'not_admitted';
    await execute(runId, reservation);
    return 'finished';
  } finally {
    try {
      if (registered && reservation) await tx((db) => finishNativeSupervisor(db, reservation!));
    } finally {
      if (owns)
        await owner
          .query('SELECT pg_advisory_unlock($1::int,$2::int)', [nativeSupervisorLockNamespace, key])
          .catch(() => {});
      owner.release();
    }
  }
}

/** Aliases are fixed source code, not request parameters. to_jsonb keeps the
 * rollout compatible with legacy tables that do not yet have this column. A
 * metadata label alone cannot exempt an actor: a live per-run lock and the
 * registered independent cgroup/fence/token are mandatory. */
export function independentNativeSupervisorSql(runAlias = 'r', jobAlias = 'j') {
  const s = `(to_jsonb(${jobAlias})->'supervision')`,
    o = `(${s}->'owner')`;
  return `COALESCE((${s}->>'version'='1' AND ${s}->>'mode'='detached'
    AND COALESCE(${s}->>'runtime','opencode')=${runAlias}.runtime
    AND ${s}->>'runId'=${runAlias}.id::text AND ${s}->>'runFence'=${runAlias}.fence::text
    AND ${s}->>'unit'='agentos-native-supervisor-'||${runAlias}.id::text
    AND ${s}->>'candidate' ~ '^/opt/agentos/releases/managed-[0-9a-f-]{36}$'
    AND ${s}->>'digest' ~ '^[0-9a-f]{64}$'
    AND ${o}->>'unit'=${s}->>'unit' AND ${o}->>'cwd'=${s}->>'candidate'
    AND ${o}->>'cgroup'='/agentos_native_supervisors.slice/'||(${s}->>'unit')||'.service'
    AND ${o}->>'pid' ~ '^[1-9][0-9]{0,8}$'
    AND ${o}->>'runFence'=${runAlias}.fence::text
    AND ${o}->>'launchToken'=${s}->>'launchToken'
    AND ${o}->>'lockNamespace'='${nativeSupervisorLockNamespace}'
    AND EXISTS(SELECT 1 FROM pg_locks nl WHERE nl.locktype='advisory' AND nl.objsubid=2
      AND nl.database=(SELECT oid FROM pg_database WHERE datname=current_database())
      AND nl.classid=${nativeSupervisorLockNamespace}::oid AND nl.granted
      AND nl.objid=(('x'||substr(encode(sha256(convert_to(${runAlias}.id::text,'UTF8')),'hex'),1,8))::bit(32)::bigint & 2147483647)::oid
      AND nl.pid=CASE WHEN ${o}->>'backendPid' ~ '^[1-9][0-9]{0,8}$' THEN (${o}->>'backendPid')::int ELSE -1 END)),false)`;
}
