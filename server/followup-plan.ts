import { objectiveSchema, planSchema, validatePlan } from './objective-contracts.js';
import { isDeepStrictEqual } from 'node:util';

/** PostgreSQL JSONB omits optional undefined keys and does not retain object key order. */
export function followupPlansEqual(left: unknown, right: unknown) {
  return isDeepStrictEqual(JSON.parse(JSON.stringify(left)), JSON.parse(JSON.stringify(right)));
}

/** Validate an operator-authored DAG by the same contract used for generated plans. */
export function validateFollowupPlan(
  requestRaw: unknown,
  planRaw: any,
  tasks: any[],
  agents: any[],
  accounts: any[],
) {
  const request = objectiveSchema.parse(requestRaw);
  const work = tasks.filter((t) => t.stage !== 'coordinate' && t.state !== 'superseded');
  const plan = planSchema.parse({
    ...planRaw,
    tasks: work.map((t) => ({
      key: t.key,
      title: t.title,
      description: t.description,
      agentId: t.agentId ?? t.agent_id,
      accountId: t.accountId ?? t.account_id,
      model: t.model,
      stage: t.stage,
      baseWorkspace: t.execution.baseWorkspace,
      covers: planRaw.tasks.find((p: any) => p.key === t.key)?.covers ?? [],
      dependencies: (t.dependencies ?? []).map((id: string) => {
        const parent = work.find((p) => p.id === id);
        if (!parent) throw Error('FOLLOWUP_DEPENDENCY_MISSING');
        return parent.key;
      }),
      sourceTask:
        t.stage === 'verify'
          ? work.find((p) => p.execution.workspace === t.execution.sourceWorkspace)?.key
          : undefined,
      writePaths: t.execution.writePaths ?? [],
      browser: (t.resources ?? []).includes('browser'),
      criteria: t.criteria,
    })),
  });
  validatePlan(plan, request, agents, accounts);
  return { request, plan };
}
