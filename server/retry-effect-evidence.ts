import { createHash } from 'node:crypto';

const digestPattern = /^[a-f0-9]{64}$/;
const sha = (value: string) => createHash('sha256').update(value).digest('hex');
const canonical = (value: any): any => Array.isArray(value)
  ? value.map(canonical)
  : value && typeof value === 'object'
    ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])]))
    : value;
const stable = (value: any) => JSON.stringify(canonical(value));

type RetryEffectRun = {
  id: string;
  attempt: number;
  state: string;
  instructionSnapshot: any;
  manifest: any;
  acceptancePassed: boolean;
  checks: any[];
  changeDiffs: Array<{ digest: string; size: number }>;
};

type InputFingerprints = { material: string; selectedMemoryContext: string; controlMemoryContext: string; complete: string };

function frozenInputFingerprints(run: RetryEffectRun): InputFingerprints | null {
  const executionInputs = run.instructionSnapshot?.executionInputs;
  const execution = run.instructionSnapshot?.task?.execution;
  const dependencies = run.manifest?.dependencyArtifacts;
  const recovery = run.manifest?.recoveryArtifacts;
  const selectedMemories = run.manifest?.selected;
  const supervisionMemory = run.manifest?.supervisionMemory;
  const recoveryAliases = run.manifest?.recoveryArtifactAliases;
  if (!Array.isArray(executionInputs) || !Array.isArray(dependencies) || !Array.isArray(recovery) ||
      !Array.isArray(selectedMemories) || !Array.isArray(recoveryAliases) || !execution ||
      !Object.hasOwn(run.manifest, 'supervisionMemory')) return null;
  const controlMemories = supervisionMemory == null ? [] : supervisionMemory.memories;
  if (!Array.isArray(controlMemories)) return null;
  const validRef = (ref: any) => ref && typeof ref.id === 'string' && typeof ref.name === 'string' &&
    digestPattern.test(ref.digest ?? '');
  if (executionInputs.some((ref: any) => !validRef(ref)) || dependencies.some((ref: any) => !validRef(ref)) ||
      recovery.some((ref: any) => !validRef(ref)) ||
      selectedMemories.some((memory: any) => typeof memory?.id !== 'string' ||
        !Number.isSafeInteger(memory.version) || !digestPattern.test(memory.hash ?? '')) ||
      controlMemories.some((memory: any) => typeof memory?.id !== 'string' ||
        !Number.isSafeInteger(memory.version) || !digestPattern.test(memory.hash ?? ''))) return null;
  const selectedById = new Map(selectedMemories.map((memory: any) => [memory.id, memory]));
  if (controlMemories.some((memory: any) => {
    const selected = selectedById.get(memory.id) as any;
    return !selected || selected.version !== memory.version || selected.hash !== memory.hash;
  })) return null;
  const refs = (values: any[]) => values.map((ref) => ({
    id: ref.id, name: ref.name, digest: ref.digest,
    ...(Number.isSafeInteger(ref.size) ? { size: ref.size } : {}),
    ...(typeof ref.task_id === 'string' ? { taskId: ref.task_id } : {}),
    ...(typeof ref.run_id === 'string' ? { runId: ref.run_id } : {}),
  })).sort((a, b) => a.id.localeCompare(b.id));
  const material = sha(stable({
    executionInputs: refs(executionInputs),
    dependencyArtifacts: refs(dependencies),
    recoveryArtifacts: refs(recovery),
    recoveryArtifactAliases: canonical(recoveryAliases),
    workspaceIdentity: {
      baseWorkspace: execution.baseWorkspace ?? null,
      sourceWorkspace: execution.sourceWorkspace ?? null,
      workspace: execution.workspace ?? null,
      readOnlySource: execution.readOnlySource ?? null,
    },
  }));
  const selectedMemoryContext = sha(stable(selectedMemories.map((memory: any) => ({
    id: memory.id, version: memory.version, hash: memory.hash,
  })).sort((a: any, b: any) => a.id.localeCompare(b.id))));
  const controlMemoryContext = sha(stable(controlMemories.map((memory: any) => ({
    id: memory.id, version: memory.version, hash: memory.hash,
  })).sort((a: any, b: any) => a.id.localeCompare(b.id))));
  return { material, selectedMemoryContext, controlMemoryContext,
    complete: sha(stable({ material, selectedMemoryContext, controlMemoryContext })) };
}

function failedCriterionIds(run: RetryEffectRun): string[] | null {
  if (run.acceptancePassed !== false || !Array.isArray(run.checks)) return null;
  const criteria = run.instructionSnapshot?.task?.criteria;
  if (!Array.isArray(criteria) || !criteria.length || criteria.some((criterion: any) => typeof criterion?.id !== 'string' || !criterion.id)) return null;
  const ids = [...new Set(criteria.map((criterion: any) => criterion.id))].sort();
  const byId = new Map<string, boolean>();
  for (const check of run.checks) {
    if (ids.includes(check?.id) && typeof check.passed === 'boolean') byId.set(check.id, check.passed);
  }
  if (ids.some((id) => !byId.has(id))) return null;
  const failed = ids.filter((id) => byId.get(id) === false);
  return failed.length ? failed : null;
}

/** A read-only comparison hint; it never changes case state or acceptance. */
export function buildRetryEffectEvidence(current: RetryEffectRun, previous: RetryEffectRun, context: {
  stage: string;
  workKind: string | null;
  supervisedRevise: boolean;
}): any | null {
  // Diagnostics and test-only work intentionally do not participate in an
  // implementation-no-change signal.
  if (context.stage !== 'integrate' || (context.workKind !== null && context.workKind !== 'repair') ||
      context.supervisedRevise !== true ||
      !current.id || !previous.id || current.state !== 'submitted' || previous.state !== 'submitted' ||
      !Number.isSafeInteger(current.attempt) || current.attempt !== previous.attempt + 1 ||
      !Array.isArray(current.changeDiffs) || current.changeDiffs.length !== 1 ||
      !Array.isArray(previous.changeDiffs) || previous.changeDiffs.length !== 1 ||
      !digestPattern.test(current.changeDiffs[0]?.digest ?? '') || !digestPattern.test(previous.changeDiffs[0]?.digest ?? '') ||
      !Number.isSafeInteger(current.changeDiffs[0]?.size) || !Number.isSafeInteger(previous.changeDiffs[0]?.size)) return null;

  const currentCriteria = current.instructionSnapshot?.task?.criteria;
  const previousCriteria = previous.instructionSnapshot?.task?.criteria;
  if (!Array.isArray(currentCriteria) || !Array.isArray(previousCriteria)) return null;
  const currentCriteriaDigest = sha(stable(currentCriteria));
  const previousCriteriaDigest = sha(stable(previousCriteria));
  const currentFailureIds = failedCriterionIds(current);
  const previousFailureIds = failedCriterionIds(previous);
  const currentInputs = frozenInputFingerprints(current);
  const previousInputs = frozenInputFingerprints(previous);
  if (!currentFailureIds || !previousFailureIds || !currentInputs || !previousInputs) return null;

  const sameChangeDiff = current.changeDiffs[0].digest === previous.changeDiffs[0].digest;
  const sameCorePatch = sameChangeDiff;
  const sameMaterialInputs = currentInputs.material === previousInputs.material;
  const sameSelectedMemoryContext = currentInputs.selectedMemoryContext === previousInputs.selectedMemoryContext;
  const sameControlMemoryContext = currentInputs.controlMemoryContext === previousInputs.controlMemoryContext;
  const sameMemoryContext = sameSelectedMemoryContext && sameControlMemoryContext;
  const sameFrozenInputs = currentInputs.complete === previousInputs.complete;
  const sameCriteria = currentCriteriaDigest === previousCriteriaDigest;
  const sameFailedCriteria = stable(currentFailureIds) === stable(previousFailureIds);
  let status = 'observed_change_or_outcome';
  if (sameCorePatch && sameMaterialInputs && sameMemoryContext && sameCriteria && sameFailedCriteria)
    status = 'same_change_same_inputs_same_failed_criteria';
  else if (sameCorePatch && sameMaterialInputs && sameMemoryContext && sameCriteria)
    status = 'same_change_same_inputs_failure_set_changed';
  else if (sameCorePatch && sameMaterialInputs && !sameMemoryContext && sameCriteria && sameFailedCriteria)
    status = 'same_change_same_material_inputs_memory_context_changed_same_failed_criteria';
  else if (sameCorePatch && sameMaterialInputs && !sameMemoryContext && sameCriteria)
    status = 'same_change_same_material_inputs_memory_context_changed_failure_set_changed';
  else if (!sameCorePatch) status = 'change_diff_changed';
  else if (!sameMaterialInputs) status = 'material_inputs_changed';
  else if (!sameMemoryContext) status = 'memory_context_changed';
  else if (!sameCriteria) status = 'criteria_snapshot_changed';

  return {
    version: 2,
    authority: 'read_only_retry_effect_comparison',
    status,
    priorRunId: previous.id,
    priorAttempt: previous.attempt,
    currentRunId: current.id,
    currentAttempt: current.attempt,
    sameChangeDiff,
    sameCorePatch,
    priorChangeDiffSha256: previous.changeDiffs[0].digest,
    currentChangeDiffSha256: current.changeDiffs[0].digest,
    sameFrozenInputs,
    priorFrozenInputSha256: previousInputs.complete,
    currentFrozenInputSha256: currentInputs.complete,
    sameMaterialInputs,
    priorMaterialInputSha256: previousInputs.material,
    currentMaterialInputSha256: currentInputs.material,
    sameSelectedMemoryContext,
    priorSelectedMemoryContextSha256: previousInputs.selectedMemoryContext,
    currentSelectedMemoryContextSha256: currentInputs.selectedMemoryContext,
    sameControlMemoryContext,
    priorControlMemoryContextSha256: previousInputs.controlMemoryContext,
    currentControlMemoryContextSha256: currentInputs.controlMemoryContext,
    sameCriteria,
    sameFailedCriteria,
    priorFailedCriteria: previousFailureIds,
    currentFailedCriteria: currentFailureIds,
    acceptanceRemainsFalse: true,
    decisionRequired: true,
  };
}
