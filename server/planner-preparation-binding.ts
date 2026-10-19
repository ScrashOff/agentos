import { isCurrentAcceptedWorkSource } from './work-opportunities.js';

export const plannerPreparationBindingVersion = 'PLANNER_PREPARATION_BINDING_V1';
export type PlannerPreparationSource = {
  taskId: string; runId: string; expectedAttempt: number; workspace: string; baseWorkspace: string;
};
export function plannerWorkspace(request: any) {
  return request.scope.split('/')[0] + '/o-plan-' + request.id;
}
const identity = (task: any): PlannerPreparationSource => ({
  taskId: task.id, runId: task.last_run_id, expectedAttempt: task.attempt_count,
  workspace: task.execution.workspace, baseWorkspace: task.execution.baseWorkspace,
});
export function matchesPlannerPreparationSource(task: any, source: PlannerPreparationSource) {
  return !!task && isCurrentAcceptedWorkSource(task) && task.id === source.taskId &&
    task.last_run_id === source.runId && task.attempt_count === source.expectedAttempt &&
    task.execution.workspace === source.workspace && task.execution.baseWorkspace === source.baseWorkspace;
}
/** A catalog window is discovery, not authority. Use the complete frozen scope
 * and the same accepted-source predicate as discovery. Never retarget a folder.
 * Ambiguous workspace ownership is refused even if one candidate is eligible. */
export function selectPlannerPreparationSource(request: any, preparation?: any): PlannerPreparationSource | null {
  const tasks: any[] = request.snapshot.tasks;
  if (preparation && (preparation.goal_id !== request.goal_id || preparation.workspace !== plannerWorkspace(request)))
    throw Error('PREPARATION_SOURCE_CHANGED');
  const ownership = new Map<string, any[]>();
  for (const task of tasks) {
    const path = task.execution?.workspace;
    if (typeof path === 'string') ownership.set(path, [...(ownership.get(path) ?? []), task]);
  }
  const owns = (workspace: string) => ownership.get(workspace) ?? [];
  if (preparation) {
    const bound = owns(preparation.source_workspace);
    if (bound.length !== 1 || bound[0].execution?.baseWorkspace !== request.scope || !isCurrentAcceptedWorkSource(bound[0]))
      throw Error('PLANNER_PREPARATION_SOURCE_INELIGIBLE');
    return identity(bound[0]);
  }
  const candidates = tasks.filter(t => t.execution?.baseWorkspace === request.scope && isCurrentAcceptedWorkSource(t) &&
    owns(t.execution.workspace).length === 1);
  candidates.sort((a, b) => String(a.id).localeCompare(String(b.id)) ||
    String(a.last_run_id).localeCompare(String(b.last_run_id)) || String(a.execution.workspace).localeCompare(String(b.execution.workspace)));
  return candidates.length ? identity(candidates[0]) : null;
}

/** Compare the prepared folder with the chosen source again immediately before
 * admission. Its owner/state may change while artifact inputs are collected. */
export function matchesPreparedPlannerWorkspace(request: any, preparation: any, source: PlannerPreparationSource, preparationId?: string) {
  return !!preparation && preparation.goal_id === request.goal_id && preparation.workspace === plannerWorkspace(request) &&
    preparation.source_workspace === source.workspace && preparation.state === 'prepared' &&
    (!preparationId || preparation.id === preparationId);
}
