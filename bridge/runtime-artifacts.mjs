// Called only after artifact paths, size, uniqueness and content hashes are checked.
// Preserve worker claims separately; the runtime computes canonical evidence itself.
export function preserveRuntimeArtifactClaims(artifacts) {
  const reserved = new Set(['candidate.json', 'changes.patch']);
  const renamed = [];
  for (const artifact of artifacts) {
    if (!reserved.has(artifact.name)) continue;
    const name = 'worker-' + artifact.name;
    if (artifacts.some(a => a.name === name))
      throw Object.assign(Error('ARTIFACT_NAME_COLLISION'), { code: 'ARTIFACT_NAME_COLLISION' });
    renamed.push({ originalName: artifact.name, name, digest: artifact.digest });
  }
  for (const rename of renamed) artifacts.find(a => a.name === rename.originalName).name = rename.name;
  return renamed;
}

export const runtimeArtifactInstruction = '\nRUNTIME_EVIDENCE_V1: candidate.json and changes.patch are computed and published by the runtime after stopping the executor. Do not declare them in artifacts or fabricate them to satisfy criteria. Deliver your own diagnostic, implementation.patch and real test outputs. A submitted file under a reserved name is preserved as worker-candidate.json or worker-changes.patch; it never becomes canonical runtime evidence.';
