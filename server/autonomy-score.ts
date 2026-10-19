import { createHash } from 'node:crypto';

export type Qualification = {
  id: string; startedAt: string; trials: string[]; minimumPasses: number;
  minimumHours: number; requiredRepairs: string[];
};
export type TrialResult = { id: string; passed: boolean; humanIntervention: boolean;
  evidenceSha256: string; checkedBy: 'deterministic'; finishedAt: string };

/** Fixed denominator. Repeated reports, pending cases and self-ratings earn no credit. */
export function scoreQualification(spec: Qualification, results: TrialResult[], repairs: string[],
  observation: { now: number; uninterruptedSeconds: number; criticalViolations: number;
    coverageComplete: boolean }) {
  if (new Set(spec.trials).size !== spec.trials.length || spec.trials.length !== 60 ||
      spec.minimumPasses !== 54 || spec.minimumHours !== 24 || spec.requiredRepairs.length !== 3)
    throw Error('QUALIFICATION_CONTRACT_INVALID');
  const admitted = new Map<string, TrialResult[]>();
  let rejected = 0;
  for (const result of results) {
    if (!spec.trials.includes(result.id) || result.checkedBy !== 'deterministic' ||
        !/^[a-f0-9]{64}$/.test(result.evidenceSha256) ||
        !(Date.parse(result.finishedAt) >= Date.parse(spec.startedAt)) ||
        Date.parse(result.finishedAt) > observation.now ||
        typeof result.passed !== 'boolean' || typeof result.humanIntervention !== 'boolean') { rejected++; continue; }
    const existing = admitted.get(result.id) ?? [];
    if (!existing.some(r => JSON.stringify(r) === JSON.stringify(result))) existing.push(result);
    admitted.set(result.id, existing);
  }
  const ambiguous = [...admitted.values()].filter(rows => rows.length !== 1).length;
  const evaluated = [...admitted.values()].filter(rows => rows.length === 1).map(rows => rows[0]);
  const passed = evaluated.filter(r => r.passed && !r.humanIntervention).length;
  const repairsPassed = spec.requiredRepairs.filter(id => repairs.includes(id)).length;
  const complete = evaluated.length === spec.trials.length;
  const qualified = complete && passed >= spec.minimumPasses && repairsPassed === 3 &&
    rejected === 0 && ambiguous === 0 && observation.coverageComplete &&
    observation.criticalViolations === 0 && observation.uninterruptedSeconds >= 86400;
  return { contractSha256: createHash('sha256').update(JSON.stringify(spec)).digest('hex'),
    total: spec.trials.length, evaluated: evaluated.length, pending: spec.trials.length-evaluated.length,
    autonomousPassed: passed, failedOrAssisted: evaluated.length-passed, ambiguous, rejected,
    percent: passed / spec.trials.length * 100, repairsPassed, qualified,
    observationHours: observation.uninterruptedSeconds / 3600 };
}
