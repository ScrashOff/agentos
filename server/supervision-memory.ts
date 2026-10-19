import { createHash } from 'node:crypto';
import type { DB } from './db.js';
import { DomainError } from './contracts.js';

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const digest = (text: string) => createHash('sha256').update(text).digest('hex');

/** Read only structured references in the current, persisted supervisor handoff. */
export async function appliedSupervisionMemoryReferences(
  db: DB,
  run: any,
  task: any,
  project: string,
) {
  const current = (
    await db.query(
      `SELECT t.execution,t.attempt_count,t.created_at task_created_at,r.attempt,r.created_at,
       to_regclass('public.supervision_cases') supervision_table
     FROM tasks t JOIN goals g ON g.id=t.goal_id JOIN runs r ON r.task_id=t.id
     WHERE t.id=$1 AND t.goal_id=$2 AND g.project=$3 AND r.id=$4 AND r.attempt=$5`,
      [task.id, task.goal_id, project, run.id, run.attempt],
    )
  ).rows[0];
  if (!current || current.attempt !== current.attempt_count || !current.supervision_table)
    return null;
  const directives = [current.execution?.supervision, current.execution?.supervisionHandoff].filter(
    (entry) => typeof entry?.caseId === 'string' && uuid.test(entry.caseId),
  );
  // Source-run/attempt and newer cases prevent an old repair handoff from
  // making its evidence mandatory for a different revision or another task.
  let relation = 'applied_task';
  let applied = directives.length
    ? (
        await db.query(
          `SELECT c.*,source.id handoff_source_run_id FROM supervision_cases c
     JOIN jsonb_array_elements($1::jsonb) directive ON directive->>'caseId'=c.id::text
     JOIN runs source ON source.id::text=COALESCE(directive->>'sourceRunId',c.source_run_id::text)
       AND source.task_id=c.task_id AND source.attempt=c.applied_attempt
     WHERE c.task_id=$2 AND c.goal_id=$3 AND c.state='applied'
       AND c.applied_attempt<$4 AND c.updated_at<=$5
       AND NOT EXISTS(SELECT 1 FROM supervision_cases newer WHERE newer.task_id=c.task_id
         AND newer.goal_id=c.goal_id AND newer.id<>c.id
         AND (newer.created_at>c.created_at OR newer.applied_attempt>c.applied_attempt)
         AND newer.state IN('open','decided','delegated','waiting_evidence','applied','forwarded'))
     ORDER BY c.applied_attempt DESC,c.updated_at DESC,c.id LIMIT 1`,
          [JSON.stringify(directives), task.id, task.goal_id, run.attempt, current.created_at],
        )
      ).rows[0]
    : null;
  // A child receives only evidence from the persisted case that explicitly
  // lists it. Sharing a project or mentioning another case in text is not ACL.
  if (!applied) {
    relation = 'delegated_child';
    applied = (
      await db.query(
        `SELECT c.*,source.id handoff_source_run_id FROM supervision_cases c
       JOIN tasks parent ON parent.id=c.task_id AND parent.goal_id=c.goal_id
       JOIN runs source ON source.task_id=parent.id AND source.attempt=c.applied_attempt
         AND (c.source_run_id IS NULL OR c.source_run_id=source.id)
       JOIN runs decision ON decision.id=c.decision_run_id AND decision.state='submitted'
       JOIN tasks reviewer ON reviewer.id=decision.task_id AND reviewer.goal_id=c.goal_id AND reviewer.stage='coordinate'
       WHERE c.goal_id=$1 AND c.state IN('delegated','waiting_evidence')
         AND (c.support_task_id=$2 OR c.support_task_ids @> $3::jsonb)
         AND c.created_at<=$4 AND c.updated_at<=$5
         AND parent.state NOT IN('accepted','cancelled','superseded')
         AND NOT EXISTS(SELECT 1 FROM supervision_cases newer WHERE newer.task_id=c.task_id
           AND newer.goal_id=c.goal_id AND newer.id<>c.id AND newer.created_at>c.created_at
           AND newer.state IN('open','decided','delegated','waiting_evidence','applied','forwarded'))
       ORDER BY c.updated_at DESC,c.id LIMIT 1`,
        [
          task.goal_id,
          task.id,
          JSON.stringify([task.id]),
          current.task_created_at,
          current.created_at,
        ],
      )
    ).rows[0];
  }
  if (!applied) return null;
  const references = new Map<
    string,
    { id: string; expectedVersion?: number; expectedHash?: string }
  >();
  for (const entry of Array.isArray(applied.evidence) ? applied.evidence : []) {
    if (!entry || typeof entry !== 'object' || !Object.hasOwn(entry, 'memoryId')) continue;
    if (
      typeof entry.memoryId !== 'string' ||
      !uuid.test(entry.memoryId) ||
      (entry.memoryVersion !== undefined &&
        (!Number.isInteger(entry.memoryVersion) || entry.memoryVersion < 1)) ||
      (entry.memoryHash !== undefined &&
        (typeof entry.memoryHash !== 'string' || !/^[0-9a-f]{64}$/.test(entry.memoryHash)))
    )
      throw new DomainError(
        'SUPERVISION_CONTEXT_INVALID',
        'Referência de memória da supervisão inválida.',
      );
    const ref = {
      id: entry.memoryId.toLowerCase(),
      ...(entry.memoryVersion !== undefined ? { expectedVersion: entry.memoryVersion } : {}),
      ...(entry.memoryHash !== undefined ? { expectedHash: entry.memoryHash } : {}),
    };
    const prior = references.get(ref.id);
    if (
      prior &&
      ((prior.expectedVersion !== undefined &&
        ref.expectedVersion !== undefined &&
        prior.expectedVersion !== ref.expectedVersion) ||
        (prior.expectedHash !== undefined &&
          ref.expectedHash !== undefined &&
          prior.expectedHash !== ref.expectedHash))
    )
      throw new DomainError(
        'SUPERVISION_CONTEXT_INVALID',
        'Versões conflitantes na evidência da supervisão.',
      );
    references.set(ref.id, { ...prior, ...ref });
  }
  return {
    caseId: applied.id,
    relation,
    authorityTaskId: applied.task_id,
    taskId: task.id,
    goalId: task.goal_id,
    sourceRunId: applied.handoff_source_run_id,
    decisionRunId: applied.decision_run_id,
    appliedAttempt: applied.applied_attempt,
    caseUpdatedAt: applied.updated_at.toISOString(),
    references: [...references.values()],
  };
}

/** Snapshot versions and provenance; required delivery does not confirm an observation. */
export function supervisionMemorySnapshot(
  authority: Awaited<ReturnType<typeof appliedSupervisionMemoryReferences>>,
  rows: any[],
) {
  if (!authority) return null;
  const memories = authority.references.map((ref) => {
    const memory = rows.find((row) => row.id === ref.id);
    if (
      !memory ||
      (ref.expectedVersion !== undefined && ref.expectedVersion !== memory.version) ||
      (ref.expectedHash !== undefined && ref.expectedHash !== digest(memory.content))
    )
      throw new DomainError(
        'REQUIRED_CONTEXT_MISSING',
        'Uma memória da supervisão foi removida, substituída, expirou ou não pertence ao projeto.',
      );
    return {
      id: memory.id,
      version: memory.version,
      hash: digest(memory.content),
      kind: memory.kind,
      verification: memory.verification,
      source: memory.source,
    };
  });
  return { ...authority, memories };
}

export async function assertSupervisionMemorySnapshot(
  db: DB,
  run: any,
  task: any,
  project: string,
  snapshot: ReturnType<typeof supervisionMemorySnapshot>,
) {
  const authority = await appliedSupervisionMemoryReferences(db, run, task, project);
  if (
    JSON.stringify(authority) !==
    JSON.stringify(snapshot ? (({ memories, ...value }) => value)(snapshot) : null)
  )
    throw new DomainError(
      'SUPERVISION_CONTEXT_CHANGED',
      'A revisão da supervisão mudou durante a montagem do contexto.',
    );
  if (!snapshot?.memories.length) return;
  const rows = (
    await db.query(
      `SELECT m.* FROM memories m WHERE m.project=$1 AND m.id=ANY($2::uuid[])
       AND (m.expires_at IS NULL OR m.expires_at>now())
       AND NOT EXISTS(SELECT 1 FROM memories newer WHERE newer.supersedes=m.id)`,
      [project, snapshot.memories.map((memory) => memory.id)],
    )
  ).rows;
  if (
    JSON.stringify(supervisionMemorySnapshot(authority, rows)?.memories) !==
    JSON.stringify(snapshot.memories)
  )
    throw new DomainError(
      'SUPERVISION_CONTEXT_CHANGED',
      'Uma memória da supervisão mudou durante a montagem do contexto.',
    );
}
