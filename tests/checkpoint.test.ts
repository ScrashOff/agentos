import { it, expect } from 'vitest';
import { promptCheckpoint } from '../server/runtime.js';
it('keeps migration identity and recovery instructions without injecting the full file inventory or diff', () => {
  const source = {
    kind: 'server_migration',
    workspacePreserved: true,
    instruction: 'Resume the existing files',
    proof: {
      runId: 'previous',
      workspace: 'project/workspace',
      process: { stopped: true },
      candidate: {
        files: Array(10000).fill({ path: 'file' }),
        patch: 'x'.repeat(200000),
        candidateDigest: 'verified',
      },
    },
  };
  const result = promptCheckpoint(source);
  expect(JSON.stringify(result).length).toBeLessThan(1000);
  expect(result.proof.candidateDigest).toBe('verified');
  expect(result.instruction).toBe(source.instruction);
  expect(source.proof.candidate.files).toHaveLength(10000);
});
