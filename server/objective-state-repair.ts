import {event,type DB} from './db.js';

/** Repair only the derived-state defect: an older cancelled task contaminated
 * an entirely accepted current revision. Current cancellation, pause and live
 * or uncertain executions cannot pass this proof. No task/run is rewritten. */
export async function reconcileInheritedCancellation(db:DB,goalId?:string){
  const rows=(await db.query(`SELECT g.id FROM goals g JOIN objectives o ON o.id=g.id
    JOIN plan_revisions p ON p.objective_id=o.id AND p.revision=o.revision
    WHERE g.state='cancelled' AND o.status='cancelled' AND o.mode='execute'
      AND ($1::uuid IS NULL OR g.id=$1)
      AND p.plan ? 'incremental'
      AND (jsonb_array_length(COALESCE(p.plan#>'{incremental,pendingRequirementIds}','[]'))>0 OR
        EXISTS(SELECT 1 FROM objective_feedback f WHERE f.objective_id=g.id AND NOT EXISTS(
          SELECT 1 FROM planning_runs pr WHERE pr.objective_id=g.id AND pr.state='completed'
            AND pr.feedback->'feedbackIds' ? f.id::text)))
      AND EXISTS(SELECT 1 FROM tasks t WHERE t.goal_id=g.id AND t.plan_revision=o.revision AND t.state='accepted')
      AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.goal_id=g.id AND t.plan_revision=o.revision AND t.state<>'accepted')
      AND EXISTS(SELECT 1 FROM tasks t WHERE t.goal_id=g.id AND t.plan_revision<o.revision AND t.state='cancelled')
      AND NOT EXISTS(SELECT 1 FROM tasks t JOIN runs r ON r.task_id=t.id WHERE t.goal_id=g.id
        AND r.state IN('queued','running','cancelling','reconciling')) ORDER BY g.id LIMIT 20`,[goalId??null])).rows;
  for(const row of rows){
    await db.query("UPDATE goals SET state='active' WHERE id=$1 AND state='cancelled'",[row.id]);
    await db.query("UPDATE objectives SET status='replanning',reason=NULL,updated_at=now() WHERE id=$1",[row.id]);
    await event(db,'OBJECTIVE_STATE_RECONCILED',row.id,{reason:'historical_task_cancellation',
      acceptedCurrentRevision:true,pendingWork:true,tasksChanged:false,runsChanged:false});
  }
  return rows.length;
}
