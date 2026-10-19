export const analyticalPromptCharacterLimit = 180000;

export function promptCheckpoint(checkpoint: any) {
  if (checkpoint?.kind !== 'server_migration') return checkpoint ?? null;
  const { proof, ...rest } = checkpoint;
  return {
    ...rest,
    proof: proof ? {
      runId: proof.runId, workspace: proof.workspace, checkedAt: proof.checkedAt,
      process: proof.process, candidateDigest: proof.candidate?.candidateDigest,
      baseDigest: proof.candidate?.baseDigest,
    } : undefined,
  };
}

/** Shared by assembly and execution: JSON escaping is part of the budget. */
export function analyticalPrompt(run: any, task: any, context: {content: string; manifest: any}, dependencies: any[]) {
  return JSON.stringify({
    role: task.agent_id, task: task.title, description: task.description,
    instructionManifest: context.manifest.taskInstructions ?? null,
    executionInputs: context.manifest.executionInputs ?? [],
    criteria: task.criteria, checkpoint: promptCheckpoint(run.checkpoint),
    context: context.content, dependencies,
  });
}

/** Only optional memory can be excluded. All non-memory inputs are immutable. */
export function fitManagerOptionalMemory<T extends {id: string; required: boolean; content: string}>(
  memories: T[], measure: (selected: T[]) => number,
) {
  const selected = memories.filter(m => m.required);
  const requiredPayloadCharacters = measure(selected);
  const omitted: Array<{id: string; characters: number; reason: string}> = [];
  for (const memory of memories.filter(m => !m.required)) {
    if (measure([...selected, memory]) <= analyticalPromptCharacterLimit) selected.push(memory);
    else omitted.push({id: memory.id, characters: memory.content.length, reason: 'serialized_transport_budget'});
  }
  return {selected, omitted, requiredPayloadCharacters, payloadCharacters: measure(selected),
    maximumCharacters: analyticalPromptCharacterLimit,
    requiredFits: requiredPayloadCharacters <= analyticalPromptCharacterLimit};
}
