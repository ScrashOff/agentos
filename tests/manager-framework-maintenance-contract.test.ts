import { expect, it } from 'vitest';
import { managerInstructions } from '../server/manager-instructions.js';
import { backlogMigrationModules } from '../server/backlog-schema-readiness.js';

it('exposes both exact maintenance action contracts in the effective manager prompt', () => {
  const prompt = managerInstructions();
  expect(prompt).toContain('kind:"request_validation"');
  expect(prompt).toContain(
    'candidateArtifactId:UUID,candidateArtifactDigest:sha256,candidateDigest:sha256',
  );
  expect(prompt).toContain('kind:"request_rebase"');
  expect(prompt).toContain('baseline:{kind:"current_published_runtime",expectedDigest?:sha256}');
  expect(prompt).toContain('candidateValidations');
  expect(prompt).toContain('publishedRebases');
  expect(prompt).toContain('readyForValidation, critérios nem aceite');
  expect(prompt).toContain('RUNTIME_GUIDANCE_V1');
  expect(prompt).toContain('FRAMEWORK_CANDIDATE_IDENTITY_V1');
  expect(prompt).toContain('task_metadata');
  expect(prompt).toContain('runId/runFence/contextVersion');
  expect(prompt).toContain('candidate.json');
  expect(prompt).toContain('EXTERNAL_CI_EVIDENCE_V1');
  expect(prompt).toContain('validationActionId?:UUID');
  expect(prompt).toContain('readyForValidation ou aceite');
  expect(prompt).not.toContain('observa??o');
});

it('pins both maintenance installers in the guarded backlog schema source closure', () => {
  expect(backlogMigrationModules).toContain('server/candidate-validation.ts');
  expect(backlogMigrationModules).toContain('server/published-rebase.ts');
});

it('advertises an optional supervised worker route only for revise while retaining Sol reservation', () => {
  const prompt = managerInstructions();
  expect(prompt).toContain('route?:{accountId:string,model:string}');
  expect(prompt).toContain('somente em supervision.actions kind:"revise"');
  expect(prompt).toContain('WORKFORCE_SOL_RESERVED');
  expect(prompt).toContain('allowlist, modelos, cota/permissões e política workforce atuais');
});
