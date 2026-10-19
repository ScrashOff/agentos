import { DomainError } from './contracts.js';
import { managerTextHash } from './manager-context.js';

type Candidate = { id: string; content: string; required: boolean; score: number };

/** Required facts stay intact; optional retrieval must not prevent execution. */
export function selectMemoryContext<T extends Candidate>(
  candidates: T[],
  rankedOptional: T[],
  stage?: string,
  inlineHashes: string[] = [],
) {
  const selected = candidates.filter((m) => m.required);
  const requiredCharacters = selected.reduce((n, m) => n + m.content.length, 0);
  if (requiredCharacters > 60000)
    throw new DomainError(
      'CONTEXT_TOO_LARGE',
      'Contexto obrigatório excede a janela local; decomponha a tarefa.',
    );
  // Keep room for relevant optional facts when mandatory decisions fill a manager's
  // ordinary retrieval window. Required facts and the absolute ceiling stay intact.
  const nominalRetrievalBudget = stage === 'coordinate' ? 18000 : 60000;
  const retrievalBudget = stage === 'coordinate'
    ? Math.min(60000, Math.max(nominalRetrievalBudget, requiredCharacters + 8000))
    : nominalRetrievalBudget;
  const optionalBudgetCharacters = retrievalBudget - requiredCharacters;
  const omitted: Array<{ id: string; characters: number; reason: string; hash?: string }> = [];
  const alreadyDelivered = new Set(stage === 'coordinate' ? inlineHashes : []);
  const seen = new Set(selected.map((m) => m.id));
  let used = requiredCharacters,
    optionalCount = 0;
  for (const candidate of rankedOptional
    .filter((m) => m.score > 0)
    .sort((a, b) => b.score - a.score)) {
    if (seen.has(candidate.id)) continue;
    seen.add(candidate.id);
    const digest = managerTextHash(candidate.content);
    if (alreadyDelivered.has(digest)) {
      omitted.push({ id: candidate.id, characters: candidate.content.length,
        reason: 'already_in_manager_snapshot', hash: digest });
      continue;
    }
    if (used + candidate.content.length > retrievalBudget || optionalCount >= 8) {
      omitted.push({
        id: candidate.id,
        characters: candidate.content.length,
        reason: optionalCount >= 8 ? 'optional_count' : 'optional_context_budget',
      });
      continue;
    }
    selected.push(candidate);
    used += candidate.content.length;
    optionalCount++;
  }
  return { selected, omitted, retrievalBudget, nominalRetrievalBudget,
    optionalBudgetCharacters, requiredCharacters };
}
