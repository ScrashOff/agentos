import type {DB} from './db.js';
import {checkedExecutionMetadata,executionMetadataColumns} from './task-execution-metadata.js';
import {isMuseBacklogExecution} from './muse-backlog-delivery.js';
const closed = new Set(['accepted','cancelled','superseded']);
const reservable = new Set(['ready','blocked_dependencies','blocked_failed','waiting_quota',
  'waiting_resource','waiting_configuration','retry_scheduled','queued','running','cancelling','reconciling',
  'submitted','recovering_artifacts']);

/** A parked failed task stays in the objective, but does not consume the
 * runnable reserve. Any actual live actor still counts, regardless of status. */
export function reservesWorkforceBacklog(task: {state:string;has_live_run?:boolean;dependency_blocked?:boolean}) {
  return !closed.has(task.state) && (!!task.has_live_run || (!task.dependency_blocked && reservable.has(task.state)));
}
/** The caller's task alias is t; live-run confirmation matches the read model. */
export const workforceReserveSql = `t.state NOT IN('accepted','cancelled','superseded') AND (
  (t.state IN('ready','blocked_dependencies','blocked_failed','waiting_quota','waiting_resource',
    'waiting_configuration','retry_scheduled','queued','running','cancelling','reconciling','submitted','recovering_artifacts')
    AND NOT EXISTS(SELECT 1 FROM dependencies reserve_dependency JOIN tasks reserve_parent ON reserve_parent.id=reserve_dependency.depends_on
      WHERE reserve_dependency.task_id=t.id AND reserve_parent.state<>'accepted'))
  OR EXISTS(SELECT 1 FROM runs reserve_run WHERE reserve_run.task_id=t.id
    AND reserve_run.state IN('queued','running','cancelling','reconciling')))`;

/** Shared admission count for planners and backlog workers. The caller must
 * hold the ordinary admission lock when using this observation to insert work.
 * No prompt/input bundles are read and missing metadata fails closed. */
export async function readWorkforceReserve(db:DB,goalId:string,lane:'glm'|'muse',policy:any){
  const rows=checkedExecutionMetadata((await db.query(`SELECT t.id,t.model,metadata.control execution,
    ${executionMetadataColumns} FROM tasks t LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    WHERE t.goal_id=$1 AND (${workforceReserveSql})`,[goalId])).rows,{objectRequired:false});
  const pending=rows.filter(t=>lane==='glm'?t.execution?.kind==='opencode':
    isMuseBacklogExecution(t.execution)&&(t.execution?.workforceLane==='muse'||/^muse-/i.test(t.model??''))).length;
  const limit=lane==='glm'?2*Math.max(1,policy?.glmTarget??12):2*Math.max(0,policy?.museTarget??0);
  return {pending,limit,available:policy?.enabled===true&&pending<limit};
}
