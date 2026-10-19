import type { DB } from './db.js';
import { checkedExecutionMetadata, executionMetadataColumns } from './task-execution-metadata.js';

/** The trigger-maintained projection preserves execution->>'baseWorkspace'
 * without unpacking every task's embedded execution inputs for each plan.
 * Keep SQL NULL execution excluded, JSON null/nonobjects included as null,
 * and fail closed on absent/obsolete/cross-goal metadata instead of hiding a
 * scope. This read is discovery only; all existing admission fences remain.
 */
export async function readBacklogScopes(db: DB, goalId: string): Promise<Array<string | null>> {
  const rows = checkedExecutionMetadata((await db.query(`SELECT t.id,
    ${executionMetadataColumns},metadata.execution_present,metadata.base_workspace workspace
    FROM tasks t LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    WHERE t.goal_id=$1`, [goalId])).rows, { objectRequired: false });
  return [...new Set<string | null>(rows.filter(row => row.execution_present).map(row => row.workspace))];
}
