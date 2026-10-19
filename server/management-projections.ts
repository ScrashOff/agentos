import type { DB } from './db.js';
import { checkedExecutionMetadata, executionMetadataColumns, taskExecutionMetadataVersion } from './task-execution-metadata.js';

/** Scope selection must precede pagination without hiding a missing projection.
 * Only identities and persisted scalar metadata are read; authority and frozen
 * attempt contracts remain in the existing consumers. Null means all scopes. */
export async function readManagementScopeTaskIds(db: DB, goalId: string, scope: string | null) {
  const rows = (await db.query(`SELECT t.id,${executionMetadataColumns}
    FROM tasks t LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    WHERE t.goal_id=$1 AND ($2::text IS NULL OR metadata.base_workspace=$2
      OR metadata.task_id IS NULL OR metadata.version<>$3 OR metadata.goal_id<>t.goal_id)
    ORDER BY t.id`, [goalId, scope, taskExecutionMetadataVersion])).rows;
  // The old ->> predicate permits non-object JSON and simply finds no key.
  // Do not add an unrelated execution-shape restriction to this read path.
  return checkedExecutionMetadata(rows, { objectRequired: false }).map(row => row.id as string);
}
