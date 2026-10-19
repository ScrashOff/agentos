import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { accountBridge } from './bridge.js';
import { event, lock, pool, tx, type DB } from './db.js';

const live = ['queued', 'running', 'cancelling', 'reconciling'];
const evidenceRef = z.string().refine((value) =>
  /^event:[1-9]\d{0,17}$/.test(value) || /^artifact:[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(value),
  'evidenceRef must be event:<positive integer> or artifact:<UUID>',
);

export const guidanceSchema = z.object({
  kind: z.literal('guide'),
  taskId: z.uuid(),
  expectedAttempt: z.number().int().positive(),
  runId: z.uuid(),
  runFence: z.uuid(),
  contextVersion: z.uuid(),
  instruction: z.string().min(40).max(2400),
  reason: z.string().min(20).max(2000),
  facts: z.array(z.object({ evidenceRef, statement: z.string().min(1).max(500) })).min(1).max(8),
}).strict();

type Guidance = z.infer<typeof guidanceSchema>;
type BridgeRequest = (method: string, params?: unknown, options?: { timeout?: number }) => Promise<any>;

function containsSecret(value: string) {
  return /-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~+/-]{12,}|\b(?:sk|rk|ghp|github_pat)-[A-Za-z0-9_-]{16,}|\b(?:api[_-]?key|password|client_secret)\s*[:=]\s*\S+/i.test(value);
}

function guidanceSafe(value: Guidance) {
  return ![value.instruction, value.reason, ...value.facts.map((fact) => fact.statement)].some(containsSecret);
}

async function authorizedManager(db: DB, goalId: string, managerRunId: string) {
  return (await db.query(`SELECT g.id goal_id,g.state goal_state,m.value management,w.value workforce,a.value autonomy,
      t.stage manager_stage,r.state manager_state
    FROM goals g JOIN settings m ON m.id='management:'||g.id
    JOIN settings w ON w.id='workforce:'||g.id
    LEFT JOIN settings a ON a.id='autonomy:'||g.project
    JOIN runs r ON r.id=$2 JOIN tasks t ON t.id=r.task_id AND t.goal_id=g.id
    WHERE g.id=$1 AND t.stage='coordinate' AND r.state='submitted'
      AND EXISTS(SELECT 1 FROM acceptances ac WHERE ac.run_id=r.id AND ac.passed)
      AND EXISTS(SELECT 1 FROM artifacts ar WHERE ar.run_id=r.id AND ar.name='coordination.json')`,
  [goalId, managerRunId])).rows[0];
}

async function targetBinding(db: DB, goalId: string, value: Guidance) {
  return (await db.query(`SELECT t.id task_id,t.goal_id,t.state task_state,t.attempt_count,
      t.execution->>'kind' execution_kind,t.stage,t.recovery,r.id run_id,r.fence run_fence,r.attempt,r.state run_state,
      r.runtime,r.context_id run_context_id,c.id persisted_context_id
    FROM tasks t JOIN runs r ON r.task_id=t.id AND r.id=$3
    LEFT JOIN contexts c ON c.id=r.context_id AND c.run_id=r.id
    WHERE t.id=$1 AND t.goal_id=$2`, [value.taskId, goalId, value.runId])).rows[0];
}

async function validateProposal(db: DB, goalId: string, managerRunId: string, value: Guidance) {
  const manager = await authorizedManager(db, goalId, managerRunId);
  if (!manager || manager.goal_state !== 'active' || manager.management?.enabled !== true ||
      manager.workforce?.enabled !== true || manager.autonomy?.enabled === false)
    return 'MANAGEMENT_DISABLED';
  if (!guidanceSafe(value)) return 'GUIDANCE_SECRET_REJECTED';
  const target = await targetBinding(db, goalId, value);
  if (!target || target.task_state !== 'running' || target.run_state !== 'running' ||
      target.execution_kind !== 'harness' || target.runtime !== 'harness' ||
      target.attempt_count !== value.expectedAttempt || target.attempt !== value.expectedAttempt ||
      target.run_fence !== value.runFence || target.run_context_id !== value.contextVersion ||
      target.persisted_context_id !== value.contextVersion || target.stage === 'coordinate')
    return 'GUIDANCE_BINDING_STALE';
  if (target.recovery?.mode==='manual' && manager.autonomy?.enabled!==true) return 'MANUAL_RECOVERY';
  if ((await db.query(`SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED'
    AND (payload->>'runId'=$1 OR entity_id IN($1,$2)) LIMIT 1`,
  [value.runId, value.taskId])).rowCount) return 'CANCEL_REQUESTED';
  for (const fact of value.facts) {
    if (fact.evidenceRef.startsWith('artifact:')) {
      const id = fact.evidenceRef.slice('artifact:'.length);
      const found = await db.query(`SELECT 1 FROM artifacts ar JOIN runs r ON r.id=ar.run_id
        JOIN tasks t ON t.id=r.task_id WHERE ar.id=$1 AND t.goal_id=$2`, [id, goalId]);
      if (!found.rowCount) return 'GUIDANCE_EVIDENCE_INVALID';
    } else {
      const id = fact.evidenceRef.slice('event:'.length);
      const found = await db.query(`SELECT 1 FROM events e WHERE e.id=$1::bigint AND
        (e.entity_id=$2 OR e.entity_id IN(SELECT t.id::text FROM tasks t WHERE t.goal_id=$2::uuid)
         OR e.entity_id IN(SELECT r.id::text FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$2::uuid))`,
      [id, goalId]);
      if (!found.rowCount) return 'GUIDANCE_EVIDENCE_INVALID';
    }
  }
  return null;
}

/** Store typed guidance as a durable proposal. No remote I/O occurs in this transaction. */
export async function proposeRuntimeGuidance(goalId: string, managerRunId: string, index: number, raw: unknown) {
  const parsed = guidanceSchema.safeParse(raw);
  return tx(async (db) => {
    await lock(db);
    const existing = (await db.query('SELECT receipt FROM management_actions WHERE run_id=$1 AND action_index=$2',
      [managerRunId, index])).rows[0];
    if (existing) return existing.receipt;
    const actionId = randomUUID();
    let code: string | null = parsed.success ? 'PROPOSED' : 'GUIDANCE_FORMAT';
    let receipt: Record<string, unknown> = { kind: 'guide', index, actionId, code,
      taskId: parsed.success ? parsed.data.taskId : null };
    if (!Number.isSafeInteger(index) || index < 0) code = 'GUIDANCE_INDEX_INVALID';
    else if (parsed.success) {
      code = await validateProposal(db, goalId, managerRunId, parsed.data);
      receipt.code = code ?? 'PROPOSED';
      if (!code) {
        const value = { version: 1, actionId, managerRunId, goalId, ...parsed.data,
          state: 'proposed', proposedAt: new Date().toISOString() };
        await db.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO NOTHING',
          ['runtime-guidance:' + actionId, JSON.stringify(value)]);
        await event(db, 'RUNTIME_GUIDANCE_PROPOSED', goalId,
          { actionId, managerRunId, taskId: parsed.data.taskId, runId: parsed.data.runId,
            expectedAttempt: parsed.data.expectedAttempt, contextVersion: parsed.data.contextVersion });
      }
    }
    receipt.code = code ?? 'PROPOSED';
    await db.query('INSERT INTO management_actions(run_id,action_index,receipt) VALUES($1,$2,$3)',
      [managerRunId, index, JSON.stringify(receipt)]);
    await event(db, 'MANAGEMENT_ACTION', goalId, { runId: managerRunId, ...receipt });
    return { ...receipt, code: code || 'PROPOSED' };
  });
}

function receiptMatches(receipt: any, action: any) {
  return receipt && receipt.actionId === action.actionId && receipt.runId === action.runId &&
    receipt.runFence === action.runFence && receipt.goalId === action.goalId &&
    receipt.taskId === action.taskId && receipt.contextVersion === action.contextVersion &&
    (!z.uuid().safeParse(action.remoteMessageId).success || receipt.messageId === action.remoteMessageId) &&
    (['uncertain','dispatching'].includes(receipt.state) || z.uuid().safeParse(receipt.messageId).success) &&
    ['queued_next_step', 'consumed', 'uncertain', 'dispatching'].includes(receipt.state);
}

async function updateAction(db: DB, action: any, state: string, extra: Record<string, unknown> = {}) {
  const next = { ...action, ...extra, state, updatedAt: new Date().toISOString() };
  await db.query('UPDATE settings SET value=$2 WHERE id=$1',
    ['runtime-guidance:' + action.actionId, JSON.stringify(next)]);
  await event(db, 'RUNTIME_GUIDANCE_' + state.toUpperCase(), action.goalId,
    { actionId: action.actionId, taskId: action.taskId, runId: action.runId, state });
  return next;
}

async function revalidateStored(db: DB, action: any) {
  const raw = {
    kind: action.kind, taskId: action.taskId, expectedAttempt: action.expectedAttempt,
    runId: action.runId, runFence: action.runFence, contextVersion: action.contextVersion,
    instruction: action.instruction, reason: action.reason, facts: action.facts,
  };
  const parsed = guidanceSchema.safeParse(raw);
  if (!parsed.success) return 'GUIDANCE_RECORD_INVALID';
  return validateProposal(db, action.goalId, action.managerRunId, parsed.data);
}

/** Dispatch pending guidance. The claim transaction commits before SSH; a crash in dispatching only probes the remote receipt and never repeats the effect. */
export async function reconcileRuntimeGuidance(dispatch: BridgeRequest = accountBridge.request.bind(accountBridge)) {
  const rows = (await pool.query(`SELECT id,value FROM settings WHERE id LIKE 'runtime-guidance:%'
    AND value->>'state' IN('proposed','dispatching','queued_next_step','consumed','uncertain') ORDER BY id`)).rows;
  let dispatched = 0, queued = 0, uncertain = 0, rejected = 0, effectVerified = 0;
  for (const row of rows) {
    const claim = await tx(async (db) => {
      await lock(db);
      const current = (await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [row.id])).rows[0]?.value;
      if (!current || !['proposed', 'dispatching', 'queued_next_step', 'consumed', 'uncertain'].includes(current.state)) return null;
      // An ambiguous send owns its durable intent. Only inspect its receipt; never send again.
      if (current.state === 'queued_next_step' || current.state === 'consumed' || current.state === 'uncertain') return { action: current, mode: 'receipt' };
      // A claimed send may already have crossed the RPC boundary. A later
      // binding change cannot erase that unknown effect as a rejected proposal.
      // Let its owner finish; after lease expiry inspect only the durable receipt.
      if (current.state === 'dispatching') return Date.now()-Date.parse(current.dispatchClaimAt ?? '')<30000
        ? null : { action: current, mode: 'receipt' };
      const invalid = await revalidateStored(db, current);
      if (invalid) {
        const updated = await updateAction(db, current, 'rejected', { rejection: invalid });
        return { action: updated, mode: 'done' };
      }
      const updated = await updateAction(db, current, 'dispatching', { dispatchClaimAt:new Date().toISOString(),dispatchClaimId:randomUUID() });
      return { action: updated, mode: 'dispatch' };
    });
    if (!claim) continue;
    if (claim.mode==='done') { rejected++; continue; }
    let remote: any = null;
    try {
      if (claim.mode === 'dispatch') {
        remote = await dispatch('worker.guide', {
          actionId: claim.action.actionId, goalId: claim.action.goalId, taskId: claim.action.taskId,
          expectedAttempt: claim.action.expectedAttempt, runId: claim.action.runId,
          runFence: claim.action.runFence, contextVersion: claim.action.contextVersion,
          instruction: claim.action.instruction, reason: claim.action.reason, facts: claim.action.facts,
        }, { timeout: 20_000 });
        dispatched++;
      } else {
        remote = await dispatch('worker.guidanceReceipt', {
          operation: 'receipt', actionId: claim.action.actionId, goalId: claim.action.goalId,
          taskId: claim.action.taskId, runId: claim.action.runId, runFence: claim.action.runFence,
          contextVersion: claim.action.contextVersion,
        }, { timeout: 10_000 });
      }
    } catch (error: any) {
      const state = error?.code === 'GUIDANCE_CHANNEL_UNAVAILABLE' ? 'rejected' : 'uncertain';
      await tx(async (db) => { await lock(db); const current = (await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [row.id])).rows[0]?.value;
        if (current?.state === 'dispatching' && current.dispatchClaimId===claim.action.dispatchClaimId) await updateAction(db, current, state, { rejection: state === 'rejected' ? 'GUIDANCE_CHANNEL_UNAVAILABLE' : undefined }); });
      if (state === 'rejected') rejected++; else uncertain++;
      continue;
    }
    if (!receiptMatches(remote, claim.action)) {
      await tx(async (db) => { await lock(db); const current = (await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [row.id])).rows[0]?.value;
        // A later invalid read cannot erase an already bound queue or consumption receipt.
        if (current?.state === 'dispatching')
          await updateAction(db, current, 'uncertain', { remoteReceiptInvalid: true }); });
      uncertain++;
      continue;
    }
    await tx(async (db) => {
      await lock(db);
      const current = (await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [row.id])).rows[0]?.value;
      if (!current || ['effectverified','rejected'].includes(current.state)) return;
      // A parallel reader may have advanced the message identity during this RPC.
      if (!receiptMatches(remote,current)) { uncertain++; return; }
      if (['queued_next_step','consumed'].includes(current.state) && ['dispatching','uncertain'].includes(remote.state)) { uncertain++; return; }
      if (remote.state === 'uncertain') { if(current.state!=='uncertain') await updateAction(db, current, 'uncertain', { remoteMessageId: remote.messageId }); uncertain++; return; }
      if (remote.state === 'dispatching') { if(current.state!=='uncertain') await updateAction(db,current,'uncertain',{reason:'CLAIM_WITHOUT_CONFIRMED_RPC_RECEIPT'}); uncertain++; return; }
      if (remote.state === 'queued_next_step') {
        if (current.state!=='consumed' && current.state!=='queued_next_step') await updateAction(db, current, 'queued_next_step', { remoteMessageId: remote.messageId });
        queued++; return;
      }
      if (current.state!=='consumed') await updateAction(db, current, 'consumed', { consumedAt: new Date().toISOString(), remoteMessageId: remote.messageId });
      const accepted = await db.query(`SELECT 1 FROM tasks t JOIN runs r ON r.id=$2 AND r.task_id=t.id
        JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id AND task_id=t.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON a.passed
        WHERE t.id=$1 AND t.goal_id=$3 AND t.state='accepted' AND t.attempt_count=$4
        AND r.attempt=$4 AND r.fence=$5 AND r.context_id=$6`,
      [current.taskId, current.runId, current.goalId, current.expectedAttempt, current.runFence, current.contextVersion]);
      if (accepted.rowCount) { await updateAction(db, { ...current, state: 'consumed' }, 'effectverified'); effectVerified++; }
      else queued++;
    });
  }
  return { examined: rows.length, dispatched, queued, uncertain, rejected, effectVerified };
}
