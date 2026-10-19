import { createHash } from 'node:crypto';
import { z } from 'zod';

const digest = z.string().regex(/^[a-f0-9]{64}$/);
const timestamp = z.string().datetime({ offset: true });
const integer = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
export const runtimeProgressCode = 'RUNTIME_PROGRESS_STALLED';
export const runtimeProgressProofSchema = z.object({
  version: z.literal(1), policy: z.literal('failed-evidence-poll-v1'),
  runId: z.uuid(), workspace: z.string().regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/),
  checkedAt: timestamp, stalled: z.literal(true), reason: z.string().max(160),
  evidence: z.object({
    completedPolls: z.number().int().min(4).max(10000),
    firstStartedAt: timestamp, firstCompletedAt: timestamp, lastCompletedAt: timestamp,
    firstSequence: integer, lastSequence: integer,
    callIds: z.array(z.string().min(1).max(180)).min(4).max(128),
    pendingPollId: z.string().min(1).max(180).nullable().optional(),
    pendingPollSequence: integer.nullable().optional(),
    commandDigest: digest, resultDigest: digest,
    file: z.object({path: z.string().min(1).max(600), digest, size: integer,
      mtimeMs: z.number().finite().nonnegative(), ctimeMs: z.number().finite().nonnegative(),
      dev: z.union([integer, z.string().regex(/^\d+$/)]), ino: z.union([integer, z.string().regex(/^\d+$/)])}),
    failure: z.object({ kind: z.literal('explicit-test-failure'), matches: z.number().int().positive(), digest }),
    trace: z.object({ digest, size: integer, mtimeMs: z.number().finite().nonnegative() }),
  }),
  process: z.object({ unit: z.string().max(120), mainPid: z.number().int().positive(),
    activeState: z.literal('active'), subState: z.literal('running'), startTicks: z.union([integer, z.string().regex(/^\d+$/)]),
    controlGroup: z.string().max(200) }),
  state: z.object({ runtime: z.literal('harness'), state: z.literal('running'), runFence: z.uuid().nullable(), digest }),
});
export type RuntimeProgressProof = z.infer<typeof runtimeProgressProofSchema>;
export const runtimeProgressDigest = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');

/** Observations can justify a checkpoint/stop request, never release a lease or
 * admit another run. Duration alone cannot satisfy any of these conditions. */
export function checkedRuntimeProgressProof(value: unknown, now = Date.now()): RuntimeProgressProof {
  const proof = runtimeProgressProofSchema.parse(value);
  const evidence = proof.evidence;
  const first = Date.parse(evidence.firstCompletedAt), last = Date.parse(evidence.lastCompletedAt);
  const started = Date.parse(evidence.firstStartedAt);
  const checked = Date.parse(proof.checkedAt);
  if (checked > now + 5000 || now - checked > 10000 || last > checked + 5000 ||
      checked - last > 10 * 60_000 || last - first < 60_000 ||
      evidence.firstSequence >= evidence.lastSequence ||
      evidence.completedPolls !== evidence.callIds.length || new Set(evidence.callIds).size !== evidence.callIds.length ||
      evidence.pendingPollId && evidence.callIds.includes(evidence.pendingPollId) ||
      started > first || evidence.file.mtimeMs > started || evidence.file.ctimeMs > started ||
      evidence.file.path.includes('..') || !/^(?:evidence|data)\/[a-zA-Z0-9_./-]+$/.test(evidence.file.path) ||
      proof.process.unit !== 'agentos-run-' + proof.runId + '.service' ||
      proof.process.controlGroup !== '/system.slice/agentos-run-' + proof.runId + '.service')
    throw Object.assign(Error('RUNTIME_PROGRESS_PROOF_INVALID'), {code: 'RUNTIME_PROGRESS_PROOF_INVALID'});
  return proof;
}

export function sameRuntimeProgressEpisode(a: RuntimeProgressProof, b: RuntimeProgressProof) {
  return a.runId === b.runId && a.workspace === b.workspace &&
    runtimeProgressDigest(a.process) === runtimeProgressDigest(b.process) &&
    runtimeProgressDigest(a.state) === runtimeProgressDigest(b.state) &&
    a.evidence.commandDigest === b.evidence.commandDigest && a.evidence.resultDigest === b.evidence.resultDigest &&
    runtimeProgressDigest(a.evidence.file) === runtimeProgressDigest(b.evidence.file) &&
    runtimeProgressDigest(a.evidence.failure) === runtimeProgressDigest(b.evidence.failure) &&
    b.evidence.lastSequence >= a.evidence.lastSequence;
}

export function hasRuntimeProgressStop(checkpoint: any, runId: string, runFence: string) {
  const receipt = checkpoint?.runtimeProgressStop;
  const parsed = runtimeProgressProofSchema.safeParse(receipt?.proof);
  return receipt?.version === 1 && receipt.runId === runId && receipt.runFence === runFence &&
    parsed.success && parsed.data.runId === runId && receipt.proofDigest === runtimeProgressDigest(parsed.data);
}
