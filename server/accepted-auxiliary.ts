import { isDeepStrictEqual } from 'node:util';
import { createHash } from 'node:crypto';
import { open } from 'node:fs/promises';
import { constants } from 'node:fs';
import path from 'node:path';
import { config } from './config.js';
import type { DB } from './db.js';

const sha = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const live = ['queued', 'running', 'cancelling', 'reconciling'];
const contractKeys = ['id', 'goal_id', 'key', 'title', 'description', 'agent_id', 'stage',
  'criteria', 'execution', 'required_memories', 'resources', 'allowed_accounts', 'instruction_revision_id'];
const contract = (task: any) => Object.fromEntries(contractKeys.map(key => [key, task[key]]));

/** Evidence-only classifier. It never restores a task or grants acceptance.
 * An auxiliary result remains a fact about its frozen source, not proof that a
 * later integrated candidate passes QA. Callers must retain their own locks,
 * cancellation/authority checks and current-state compare-and-swap. */
export async function readAcceptedAuxiliaryProof(db: DB, taskId: string) {
  const task = (await db.query('SELECT * FROM tasks WHERE id=$1', [taskId])).rows[0];
  const no = (reason: string) => ({ eligible: false as const, taskId, reason });
  const execution = task?.execution;
  if (task?.stage !== 'work' || !['diagnostic', 'repair'].includes(execution?.workKind) ||
    execution.managed !== true || !execution.workspace || !execution.sourceWorkspace ||
    execution.workspace === execution.sourceWorkspace || execution.workspace === execution.baseWorkspace ||
    !(execution.kind === 'opencode' || (execution.kind === 'harness' &&
      execution.readOnlySource === true && execution.protocolDelivery === 'backlog-v1')))
    return no('NOT_FROZEN_AUXILIARY');
  const run = (await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [taskId])).rows[0];
  if (!run || run.state !== 'submitted' || run.attempt !== task.attempt_count ||
    (await db.query('SELECT 1 FROM runs WHERE task_id=$1 AND state=ANY($2::text[]) LIMIT 1', [taskId, live])).rowCount ||
    (await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND entity_id=$1 AND payload->>'runId'=$2 LIMIT 1", [taskId, run.id])).rowCount)
    return no('ATTEMPT_NOT_FINAL');
  const acceptance = (await db.query('SELECT id,passed,checks FROM acceptances WHERE task_id=$1 AND run_id=$2 ORDER BY created_at DESC LIMIT 1', [taskId, run.id])).rows[0];
  if (acceptance?.passed !== true || !Array.isArray(acceptance.checks) || !task.criteria?.length ||
    task.criteria.some((criterion: any) => !acceptance.checks.some((check: any) => check.id === criterion.id && check.passed === true)))
    return no('ACCEPTANCE_UNPROVEN');
  const snapshot = run.instruction_snapshot;
  if (snapshot?.schema !== 1 || !isDeepStrictEqual(contract(task), contract(snapshot.task ?? {})))
    return no('CONTRACT_CHANGED');
  const context = (await db.query('SELECT manifest FROM contexts WHERE id=$1 AND run_id=$2', [run.context_id, run.id])).rows[0]?.manifest;
  if (context?.taskInstructions?.taskId !== taskId || context.taskInstructions.goalId !== task.goal_id ||
    context.taskInstructions.revisionId !== snapshot.revisionId || context.taskInstructions.descriptionHash !== sha(task.description))
    return no('CONTEXT_UNBOUND');
  const inputs = execution.inputs;
  if (!Array.isArray(inputs) || !inputs.length || new Set(inputs.map((input: any) => input.id)).size !== inputs.length ||
    snapshot.executionInputs?.length !== inputs.length || (context.dependencyArtifacts?.length ?? 0) !== 0 ||
    !isDeepStrictEqual(snapshot.executionInputs, context.executionInputs)) return no('INPUTS_UNBOUND');
  const artifacts = (await db.query(`SELECT a.id,a.name,a.digest,a.size,a.run_id,r.task_id,r.attempt,r.state source_state,
    r.instruction_snapshot->'task'->'execution'->>'workspace' source_workspace,
    t.goal_id,(SELECT passed FROM acceptances v WHERE v.run_id=r.id ORDER BY created_at DESC LIMIT 1) accepted
    FROM artifacts a JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id WHERE a.id::text=ANY($1::text[])`, [inputs.map((input: any) => input.id)])).rows;
  let acceptedSource = false;
  for (const input of inputs) {
    const artifact = artifacts.find(a => a.id === input.id);
    const ref = snapshot.executionInputs?.find((entry: any) => entry.id === input.id);
    if (!artifact || artifact.goal_id !== task.goal_id || artifact.name !== input.name || artifact.digest !== input.digest ||
      ref?.name !== input.name || ref?.digest !== input.digest || ref?.size !== artifact.size || typeof input.base64 !== 'string')
      return no('INPUT_IDENTITY_CHANGED');
    const bytes = Buffer.from(input.base64, 'base64');
    if (bytes.length !== artifact.size || sha(bytes) !== input.digest) return no('INPUT_BYTES_CHANGED');
    // Older source runs predate instruction snapshots. Their exact identity is
    // still provable from the auxiliary's frozen, typed backlog provenance.
    // Never infer that identity from a source task's current workspace path.
    const pinned = execution.backlogContext;
    const legacySource = pinned?.version === 1 && pinned.sourceTaskId === artifact.task_id &&
      pinned.sourceAttempt === artifact.attempt && Array.isArray(pinned.provenance) &&
      pinned.provenance.some((entry: any) => entry.role === 'accepted_source' &&
        entry.taskId === artifact.task_id && entry.runId === artifact.run_id &&
        entry.artifactId === artifact.id && entry.name === input.name && entry.digest === input.digest);
    if (artifact.accepted === true && artifact.source_state === 'submitted' &&
      (artifact.source_workspace === execution.sourceWorkspace || (!artifact.source_workspace && legacySource)) && input.name === 'candidate.json') {
      try {
        const candidate = JSON.parse(bytes.toString());
        acceptedSource ||= /^[a-f0-9]{64}$/.test(candidate.baseDigest) && /^[a-f0-9]{64}$/.test(candidate.candidateDigest);
      } catch { return no('SOURCE_CANDIDATE_INVALID'); }
    }
  }
  if (!acceptedSource) return no('ACCEPTED_SOURCE_UNBOUND');
  // Acceptance refers to retained immutable output bytes. Missing/corrupt output
  // must not be silently preserved as a usable handoff.
  const outputs = (await db.query('SELECT id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id', [run.id])).rows;
  const checkedOutputs = new Map<string, { id: string; digest: string }>();
  for (const criterion of task.criteria) {
    const names = criterion.type === 'repair_source_proof' ? [criterion.name, criterion.patch] : [criterion.name];
    for (const name of names) {
      const matching = outputs.filter(a => a.name === name);
      if (matching.length !== 1) return no('OUTPUT_UNPROVEN');
      const artifact = matching[0];
      try {
        const file = await open(path.join(config.dataDir, 'artifacts', artifact.id), constants.O_RDONLY | constants.O_NOFOLLOW);
        try {
          const before = await file.stat();
          if (!before.isFile() || before.size !== artifact.size) return no('OUTPUT_BYTES_CHANGED');
          const bytes = await file.readFile();
          const after = await file.stat();
          if (before.size !== after.size || before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs ||
            bytes.length !== artifact.size || sha(bytes) !== artifact.digest) return no('OUTPUT_BYTES_CHANGED');
          checkedOutputs.set(artifact.id, { id: artifact.id, digest: artifact.digest });
        } finally { await file.close(); }
      } catch { return no('OUTPUT_UNAVAILABLE'); }
    }
  }
  return { eligible: true as const, taskId, runId: run.id, attempt: run.attempt,
    acceptanceId: acceptance.id, contractHash: sha(JSON.stringify(contract(task))),
    inputHash: sha(JSON.stringify(snapshot.executionInputs)), contextId: run.context_id,
    sourceWorkspace: execution.sourceWorkspace, outputHashes: [...checkedOutputs.values()].sort((a, b) => a.id.localeCompare(b.id)) };
}

export async function preservedAcceptedAuxiliaryIds(db: DB, branch: any[]) {
  const ids: string[] = [];
  for (const task of branch) {
    if (task.state !== 'accepted') continue;
    if ((await readAcceptedAuxiliaryProof(db, task.id)).eligible) ids.push(task.id);
  }
  return ids;
}
