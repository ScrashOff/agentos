import type { DB } from './db.js';

const live = new Set(['queued', 'running', 'cancelling', 'reconciling']);
const closed = new Set(['accepted', 'cancelled', 'superseded']);
export const workOpportunityCharacterLimit = 18000;
export const isCurrentAcceptedWorkSource = (task: any) =>
  task.execution?.workKind!=='planning' &&
  task.state === 'accepted' &&
  !task.has_live_run &&
  !live.has(task.last_run_state) &&
  task.last_run_state === 'submitted' &&
  task.last_run_attempt === task.attempt_count &&
  task.acceptance_passed === true &&
  !!task.execution?.workspace;

/** Planning metadata only: admission still rechecks the consumer and accepted inputs. */
export function projectWorkOpportunities(
  tasks: any[],
  coverage: any[],
  inputs: any[],
  maxCharacters = workOpportunityCharacterLimit,
  cursor = 0,
  options:{deferred?:boolean}={},
) {
  const byId = new Map(tasks.map((task) => [task.id, task]));
  const ancestors = (task: any) => {
    if (Array.isArray(task.ancestor_ids)) return new Set<string>(task.ancestor_ids);
    const found = new Set<string>();
    const pending = [...(task.dependencies ?? [])];
    while (pending.length) {
      const id = pending.pop();
      if (found.has(id)) continue;
      found.add(id);
      pending.push(...(byId.get(id)?.dependencies ?? []));
    }
    return found;
  };
  const consumers = tasks
    .filter(
      (task) =>
        ['work', 'integrate', 'verify'].includes(task.stage) &&
        !closed.has(task.state) &&
        (options.deferred || (!live.has(task.state) && !task.has_live_run)) &&
        task.execution?.workKind!=='planning' &&
        typeof task.execution?.baseWorkspace === 'string' &&
        !!task.execution.baseWorkspace &&
        !!task.execution.workspace,
    )
    .sort(
      (a, b) =>
        Number(['diagnostic', 'repair'].includes(a.execution.workKind)) -
          Number(['diagnostic', 'repair'].includes(b.execution.workKind)) ||
        Number(b.stage === 'integrate') - Number(a.stage === 'integrate') ||
        Number(b.stage === 'verify') - Number(a.stage === 'verify') ||
        String(a.id).localeCompare(String(b.id)),
    );
  const bases = new Set(consumers.map((task) => task.execution.baseWorkspace));
  const sourceCandidates = tasks.filter(
    (task) => isCurrentAcceptedWorkSource(task) && bases.has(task.execution?.baseWorkspace),
  );
  const sourceOptions = new Map(
    consumers.map((consumer) => {
      const upstream = ancestors(consumer);
      return [
        consumer.id,
        sourceCandidates
          .filter(
            (source) =>
              source.id !== consumer.id &&
              source.execution.baseWorkspace === consumer.execution.baseWorkspace &&
              !ancestors(source).has(consumer.id),
          )
          .sort(
            (a, b) =>
              Number(!!b.pending_handoff) - Number(!!a.pending_handoff) ||
              Number(upstream.has(b.id)) - Number(upstream.has(a.id)) ||
              Number(b.stage === 'integrate') - Number(a.stage === 'integrate') ||
              (new Date(b.updated_at).getTime() || 0) - (new Date(a.updated_at).getTime() || 0) ||
              String(a.id).localeCompare(String(b.id)),
          ),
      ];
    }),
  );
  const sources = sourceCandidates.filter((source) =>
    [...sourceOptions.values()].some((options) =>
      options.some((option) => option.id === source.id),
    ),
  );
  const actors = tasks.filter(
    (task) =>
      bases.has(task.execution?.baseWorkspace) && (task.has_live_run || live.has(task.state)),
  );
  const coverageKeys = new Map(coverage.map((item) => [item.task_id, item.coverage_key]));
  const coveredTasks = tasks.filter(
    (task) =>
      bases.has(task.execution?.baseWorkspace) &&
      (coverageKeys.has(task.id) || ['diagnostic', 'repair'].includes(task.execution?.workKind)),
  );
  const catalog = {
    version: 1,
    characterLimit: maxCharacters,
    // Full criteria/covers and the consumer's exact base are in operation.tasks.
    consumers: [] as Array<{
      taskId: string;
      workKinds: string[];
      sourceTaskIds: string[];
      sourceCount: number;
      delivery?:string;live?:boolean;
    }>,
    acceptedSources: [] as any[],
    liveActors: [] as any[],
    existingCoverage: [] as any[],
    selection: {
      cursor:Math.max(0,Math.trunc(cursor)),
      nextCursor:Math.max(0,Math.trunc(cursor))+1,
      total: {
        consumers: consumers.length,
        acceptedSources: sources.length,
        liveActors: actors.length,
        existingCoverage: coveredTasks.length,
      },
      omitted: { consumers: 0, acceptedSources: 0, liveActors: 0, existingCoverage: 0 },
      truncated: false,
    },
  };
  const fits = () => JSON.stringify(catalog).length <= maxCharacters - 200;
  const append = (entries: any[], entry: any) => {
    entries.push(entry);
    if (fits()) return true;
    entries.pop();
    return false;
  };
  const offset = Math.max(0, Math.trunc(cursor));
  const window = consumers.length ? [...consumers.slice(offset % consumers.length),
    ...consumers.slice(0, offset % consumers.length)] : [];
  for (const consumer of window.slice(0, 12))
    append(catalog.consumers, {
      taskId: consumer.id,
      ...(options.deferred?{delivery:'deferred',live:consumer.has_live_run || live.has(consumer.state)}:{}),
      workKinds:
        consumer.stage === 'verify' ||
        consumer.execution.readOnlySource ||
        consumer.execution.workKind === 'diagnostic'
          ? ['diagnostic']
          : ['diagnostic', 'repair'],
      sourceTaskIds: [],
      sourceCount: sourceOptions.get(consumer.id)!.length,
    });
  for (const actor of actors)
    append(catalog.liveActors, {
      taskId: actor.id,
      baseWorkspace: actor.execution.baseWorkspace,
      workspace: actor.execution.workspace,
      stage: actor.stage,
      workKind: actor.execution.workKind,
      expectedAttempt: actor.attempt_count,
      runId: actor.last_run_id,
      covers: actor.covers ?? [],
    });
  // Round-robin across consumers keeps a large historical game from hiding other scopes.
  const included = new Set<string>();
  const addSourceRound = (index: number) => {
    for (const consumer of catalog.consumers) {
      const options = sourceOptions.get(consumer.taskId) ?? [];
      const source = index < options.length ? options[(index + offset) % options.length] : undefined;
      if (!source) continue;
      if (!included.has(source.id)) {
        const refs = inputs.filter((input) => input.run_id === source.last_run_id);
        if (
          !append(catalog.acceptedSources, {
            taskId: source.id,
            expectedAttempt: source.attempt_count,
            runId: source.last_run_id,
            acceptancePassed: true,
            baseWorkspace: source.execution.baseWorkspace,
            workspace: source.execution.workspace,
            workKind: source.execution.workKind,
            covers: source.covers ?? [],
            inputs: refs.slice(0, 8).map((ref) => ({
              artifactId: ref.id,
              name: ref.name,
              digest: ref.digest,
              size: ref.size,
            })),
            omittedInputs: Math.max(0, refs.length - 8),
          })
        )
          continue;
        included.add(source.id);
      }
      consumer.sourceTaskIds.push(source.id);
      if (!fits()) consumer.sourceTaskIds.pop();
    }
  };
  // At least one exact source per scope precedes potentially large coverage history.
  addSourceRound(0);
  // Reserve room for a second source before historical coverage consumes the
  // entire context. A persisted review cursor makes omitted sources discoverable.
  addSourceRound(1);
  // Show current work and selected-source coverage per scope before old history.
  // The complete closed-source title catalog remains in operation.sourceCatalog;
  // omitting historical coverage here is never evidence of novelty.
  const coverageByScope = new Map<string, any[]>();
  for (const task of [...coveredTasks].sort(
    (a, b) =>
      Number(closed.has(a.state)) - Number(closed.has(b.state)) ||
      Number(included.has(b.id)) - Number(included.has(a.id)) ||
      (new Date(b.updated_at).getTime() || 0) - (new Date(a.updated_at).getTime() || 0) ||
      String(a.id).localeCompare(String(b.id)),
  )) {
    const base = task.execution.baseWorkspace;
    coverageByScope.set(base, [...(coverageByScope.get(base) ?? []), task]);
  }
  const orderedCoverage: any[] = [];
  for (
    let index = 0;
    index < Math.max(0, ...[...coverageByScope.values()].map((items) => items.length));
    index++
  )
    for (const items of coverageByScope.values())
      if (items[index]) orderedCoverage.push(items[index]);
  // Include accepted and failed investigations too; renaming a coverage key is not novelty.
  for (const task of orderedCoverage)
    append(catalog.existingCoverage, {
      taskId: task.id,
      coverageKey: coverageKeys.get(task.id) ?? null,
      title: task.title?.slice(0, 180),
      state: task.state,
      workKind: task.execution.workKind,
      baseWorkspace: task.execution.baseWorkspace,
      sourceTaskId: task.execution.backlogContext?.sourceTaskId ?? null,
      consumerTaskIds: tasks
        .filter((consumer) => consumer.dependencies?.includes(task.id))
        .map((consumer) => consumer.id),
      covers: task.covers ?? [],
    });
  for (let index = 2; index < 6; index++) addSourceRound(index);
  for (const key of ['consumers', 'acceptedSources', 'liveActors', 'existingCoverage'] as const) {
    catalog.selection.omitted[key] = catalog.selection.total[key] - catalog[key].length;
    catalog.selection.truncated ||= catalog.selection.omitted[key] > 0;
  }
  return catalog;
}

export async function readWorkOpportunities(db: DB, goalId: string, tasks: any[],options:{deferred?:boolean}={}) {
  if((await db.query("SELECT to_regclass('public.deferred_handoffs') present")).rows[0].present){
    const staged=(await db.query(`SELECT producer_task_id FROM deferred_handoffs WHERE goal_id=$1
      AND state IN('staged','replanning_required')`,[goalId])).rows;
    const pending=new Set(staged.map(r=>r.producer_task_id));
    tasks=tasks.map(t=>pending.has(t.id)?{...t,pending_handoff:true}:t);
  }
  const cursor = Number((await db.query('SELECT count(*)::int n FROM management_reviews WHERE goal_id=$1',[goalId])).rows[0].n);
  const coverage = (
    await db.query(
      'SELECT task_id,coverage_key FROM workforce_backlog_items WHERE goal_id=$1 ORDER BY coverage_key',
      [goalId],
    )
  ).rows;
  const runIds = tasks.filter(isCurrentAcceptedWorkSource).map((task) => task.last_run_id);
  // A source can depend through a coordinator or a task outside the compact snapshot.
  // Read persisted ancestry before advertising any safe source/consumer pair.
  const lineage = runIds.length
    ? (
        await db.query(
          `WITH RECURSIVE parents(source_id,id) AS (
            SELECT task_id,depends_on FROM dependencies WHERE task_id=ANY($1::uuid[])
            UNION SELECT p.source_id,d.depends_on FROM parents p JOIN dependencies d ON d.task_id=p.id
          ) SELECT source_id,array_agg(id) ancestor_ids FROM parents GROUP BY source_id`,
          [tasks.filter(isCurrentAcceptedWorkSource).map((task) => task.id)],
        )
      ).rows
    : [];
  const sourceParents = new Map(lineage.map((row) => [row.source_id, row.ancestor_ids]));
  const inputs = runIds.length
    ? (
        await db.query(
          `SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=ANY($1::uuid[])
          AND name<>'task-instructions-history.json'
          ORDER BY CASE name WHEN 'candidate.json' THEN 0 WHEN 'implementation.patch' THEN 1
            WHEN 'changes.patch' THEN 2 WHEN 'change.diff' THEN 3 ELSE 4 END,name,id`,
          [runIds],
        )
      ).rows
    : [];
  return projectWorkOpportunities(
    tasks.map((task) =>
      isCurrentAcceptedWorkSource(task) ? { ...task, ancestor_ids: sourceParents.get(task.id) ?? [] } : task,
    ),
    coverage,
    inputs,
    workOpportunityCharacterLimit,
    cursor,
    options,
  );
}
