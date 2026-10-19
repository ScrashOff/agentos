import type { DB } from './db.js';
import { hash } from './memory.js';

/** New work needs new relevant evidence; clocks and fresh heartbeats do not count. */
export async function supervisionEvidenceFingerprint(db: DB, task: any) {
  const accepted = (
    await db.query(
      `SELECT t.id,r.id run_id,a.id artifact_id,a.digest FROM dependencies d
       JOIN tasks t ON t.id=d.depends_on JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
       LEFT JOIN artifacts a ON a.run_id=r.id
       WHERE d.task_id=$1 AND t.state='accepted'
       AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=r.id AND v.passed)
       ORDER BY t.id,r.id,a.id`,
      [task.id],
    )
  ).rows;
  const inputs = (task.execution?.inputs ?? [])
    .map((input: any) => [input.id, input.name, input.digest])
    .sort((a: any[], b: any[]) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  return hash(JSON.stringify({ inputs, accepted, memories: [...(task.required_memories ?? [])].sort() }));
}
