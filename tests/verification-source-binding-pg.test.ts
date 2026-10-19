import { afterAll, beforeAll, expect, it } from 'vitest';
import { createHash, randomUUID } from 'node:crypto';
import { pool, migrate, tx } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { writeArtifact } from '../server/service.js';
import { installVerificationSourceBinding, bindVerificationSourceForAdmission, readVerificationSourceBindingCandidates } from '../server/verification-source-binding.js';

const sha = (v: string | Buffer) => createHash('sha256').update(v).digest('hex');
const canonical = (v: any): any => Array.isArray(v) ? v.map(canonical) : v && typeof v === 'object'
  ? Object.fromEntries(Object.keys(v).sort().map(k => [k, canonical(v[k])])) : v;
const digest = (v: any) => sha(JSON.stringify(canonical(v)));

beforeAll(async () => { await migrate(); await seedRegistry(); await installVerificationSourceBinding(); });
afterAll(async () => { await pool.end(); });

async function fixture(includeBinding = true) {
  const goal = randomUUID(), parentId = randomUUID(), qaId = randomUUID(), parentRun = randomUUID();
  const parentFence = randomUUID(), contextId = randomUUID(), rebaseAction = randomUUID(), bindingAction = randomUUID();
  const oldSource = `agentos/src-${randomUUID().replaceAll('-', '')}`;
  const parentWorkspace = `agentos/o-rb-${randomUUID().replaceAll('-', '')}`;
  const baselineWorkspace = `agentos/pub-${randomUUID().replaceAll('-', '')}`;
  const baseWorkspace = `agentos/base-${randomUUID().replaceAll('-', '')}`;
  const targetWorkspace = `agentos/o-vb-${bindingAction.replaceAll('-', '')}`;
  const rebaseRequest = 'a'.repeat(64), rebaseProof = 'b'.repeat(64);
  const candidateDigest = sha('[]');
  const candidateBytes = Buffer.from(JSON.stringify({ baseDigest: candidateDigest, candidateDigest, files: [] }));
  const candidateArtifactDigest = sha(candidateBytes);
  const criteria = [{ id: 'same', type: 'json_field', name: 'implementation.json', field: 'readyForValidation', equals: true }];
  const originalExecution = { kind: 'harness', workKind: 'verification', readOnlySource: true,
    workspace: `agentos/o-qa-${randomUUID().replaceAll('-', '')}`, sourceWorkspace: oldSource, baseWorkspace };
  const parentExecution = { kind: 'harness', workKind: 'integration', workspace: parentWorkspace,
    sourceWorkspace: baselineWorkspace, baseWorkspace, publishedRebase: { version: 1, actionId: rebaseAction,
      requestDigest: rebaseRequest, publishedBaselineDigest: 'c'.repeat(64), preparedReceiptDigest: rebaseProof,
      priorWorkspace: oldSource, baselineWorkspace, workspace: parentWorkspace } };
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    await c.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'QA source binding','Fixture','agentos',$1::text)", [goal]);
    await c.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,max_attempts,resources,required_memories,allowed_accounts)
      VALUES($1,$3,'parent','Parent','Accepted parent','qa.backend','accepted','integrate','[]',$4,1,3,'["cpu"]','["memory"]','["account"]'),
             ($2,$3,'qa','QA','Original QA contract','qa.backend','blocked_dependencies','verify',$5,$6,0,3,'["cpu"]','["memory"]','["account"]')`,
    [parentId, qaId, goal, JSON.stringify(parentExecution), JSON.stringify(criteria), JSON.stringify(originalExecution)]);
    await c.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)', [qaId, parentId]);
    await c.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,context_id) VALUES($1,$2,1,'running',$3,'harness',1,$4)",
      [parentRun, parentId, parentFence, contextId]);
    await c.query('COMMIT');
  } catch (e) { await c.query('ROLLBACK'); c.release(); throw e; }
  c.release();
  const artifact = await writeArtifact(parentRun, parentFence, 'candidate.json', candidateBytes);
  const c2 = await pool.connect();
  try {
    await c2.query('BEGIN');
    await c2.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1", [parentRun]);
    await c2.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')", [randomUUID(), parentId, parentRun]);
    const rebaseReceipt = { actionId: rebaseAction, taskId: parentId, goalId: goal, state: 'proposed',
      requestDigest: rebaseRequest, targetWorkspace: parentWorkspace, currentWorkspace: oldSource,
      baselineWorkspace, publishedBaselineDigest: 'c'.repeat(64), sourceManifestDigest: candidateDigest,
      workspaceManifestDigest: candidateDigest };
    await c2.query('INSERT INTO settings(id,value) VALUES($1,$2)', [`published-rebase:${rebaseAction}`, JSON.stringify(rebaseReceipt)]);
    await c2.query("UPDATE settings SET value=value||'{\"state\":\"preparing\"}'::jsonb WHERE id=$1", [`published-rebase:${rebaseAction}`]);
    await c2.query('UPDATE settings SET value=value||$2::jsonb WHERE id=$1', [`published-rebase:${rebaseAction}`,
      JSON.stringify({ state: 'prepared', prepared: { sourceChanged: true, receiptDigest: rebaseProof }, preparedAt: new Date().toISOString() })]);
    await c2.query("UPDATE settings SET value=value||'{\"state\":\"applying\"}'::jsonb WHERE id=$1", [`published-rebase:${rebaseAction}`]);
    await c2.query("UPDATE settings SET value=value||jsonb_build_object('state','applied','appliedAt',now()) WHERE id=$1", [`published-rebase:${rebaseAction}`]);

    const preparedProof: any = { version: 1, actionId: bindingAction, requestDigest: 'd'.repeat(64), taskId: qaId, goalId: goal,
      project: 'agentos', parentTaskId: parentId, parentRunId: parentRun, parentFence, parentAttempt: 1,
      candidateArtifactId: artifact.id, candidateArtifactDigest: artifact.digest, candidateDigest,
      sourceWorkspace: parentWorkspace, targetWorkspace, baseWorkspace, sourceManifestDigest: candidateDigest,
      sourceChanged: true, rebaseActionId: rebaseAction, rebaseReceiptDigest: rebaseProof };
    const request: any = { version: 1, actionId: bindingAction, taskId: qaId, goalId: goal, project: 'agentos',
      authorization: { kind: 'registered_agentos_source', project: 'agentos', goalId: goal },
      expectedTask: { attempt: 0, state: 'blocked_dependencies', workspace: originalExecution.workspace,
        sourceWorkspace: oldSource, baseWorkspace, criteriaDigest: digest(criteria), dependencies: [parentId],
        dependenciesDigest: digest([parentId]) },
      parent: { taskId: parentId, attempt: 1, runId: parentRun, fence: parentFence, candidateArtifactId: artifact.id,
        candidateArtifactDigest: artifact.digest, candidateArtifactSize: candidateBytes.length, candidateDigest,
        rebaseActionId: rebaseAction, rebaseRequestDigest: rebaseRequest, rebaseReceiptDigest: rebaseProof,
        workspace: parentWorkspace, baselineWorkspace, priorWorkspace: oldSource, proofManifestDigest: 'e'.repeat(64) },
      targetWorkspace, sourceWorkspace: parentWorkspace, originalWorkspace: originalExecution.workspace,
      originalSourceWorkspace: oldSource, baseWorkspace, state: 'proposed' };
    const requestBody = { ...request }; delete requestBody.state;
    const requestDigest = digest(requestBody);
    request.requestDigest = requestDigest;
    preparedProof.requestDigest = requestDigest;
    const preparedReceiptDigest = digest(preparedProof);
    if (includeBinding) {
      const row = { ...request, state: 'proposed' };
      await c2.query('INSERT INTO settings(id,value) VALUES($1,$2)', [`verification-source-binding:${qaId}`, JSON.stringify(row)]);
      const claim = await c2.query(`UPDATE settings SET value=value||'{"state":"preparing"}'::jsonb
        WHERE id=$1 AND value->>'state'='proposed' AND value->>'actionId'=$2 AND value->>'requestDigest'=$3 RETURNING id`,
        [`verification-source-binding:${qaId}`, bindingAction, requestDigest]);
      expect(claim.rowCount).toBe(1);
      const staleClaim = await c2.query(`UPDATE settings SET value=value||'{"state":"preparing"}'::jsonb
        WHERE id=$1 AND value->>'state'='proposed' AND value->>'actionId'=$2 AND value->>'requestDigest'=$3 RETURNING id`,
        [`verification-source-binding:${qaId}`, bindingAction, requestDigest]);
      expect(staleClaim.rowCount).toBe(0);
      const busyBackoff = await c2.query(`UPDATE settings SET value=value||'{"state":"proposed","errorCode":"VERIFICATION_BINDING_SOURCE_BUSY"}'::jsonb
        WHERE id=$1 AND value->>'state'='preparing' AND value->>'actionId'=$2 AND value->>'requestDigest'=$3 RETURNING id`,
        [`verification-source-binding:${qaId}`, bindingAction, requestDigest]);
      expect(busyBackoff.rowCount).toBe(1);
      const staleBackoff = await c2.query(`UPDATE settings SET value=value||'{"state":"proposed","errorCode":"VERIFICATION_BINDING_SOURCE_BUSY"}'::jsonb
        WHERE id=$1 AND value->>'state'='preparing' AND value->>'actionId'=$2 AND value->>'requestDigest'=$3 RETURNING id`,
        [`verification-source-binding:${qaId}`, bindingAction, requestDigest]);
      expect(staleBackoff.rowCount).toBe(0);
      await c2.query("UPDATE settings SET value=(value-'errorCode')||'{\"state\":\"preparing\"}'::jsonb WHERE id=$1", [`verification-source-binding:${qaId}`]);
      await c2.query('UPDATE settings SET value=value||$2::jsonb WHERE id=$1', [`verification-source-binding:${qaId}`,
        JSON.stringify({ state: 'prepared', prepared: { ...preparedProof, receiptDigest: preparedReceiptDigest } })]);
    }
    await c2.query('COMMIT');
    return { goal, parentId, qaId, parentRun, artifact, targetWorkspace, parentWorkspace, oldSource,
      originalExecution, criteria, candidateDigest, preparedProof, preparedReceiptDigest, rebaseAction };
  } catch (e) { await c2.query('ROLLBACK'); throw e; }
  finally { c2.release(); }
}

async function cleanup(f: any) {
  // The fixture deliberately exercises immutable acceptance, dependency, artifact,
  // and binding evidence. Preserve that graph in the isolated test DB; pausing
  // its uniquely-created goal prevents later admission without rewriting proof.
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1 AND state IN('active','paused')", [f.goal]);
}

it('binds only a stopped zero-attempt QA to its accepted rebased parent while preserving the task contract', async () => {
  const f = await fixture();
  try {
    const before = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.qaId])).rows[0];
    expect((await readVerificationSourceBindingCandidates(pool)).map((candidate: any) => candidate.id)).toContain(f.qaId);
    const result = await tx(db => bindVerificationSourceForAdmission(db, f.qaId));
    expect(result).toEqual({ required: true, applied: true });
    const after = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.qaId])).rows[0];
    expect(after.execution.workspace).toBe(f.targetWorkspace);
    expect(after.execution.sourceWorkspace).toBe(f.parentWorkspace);
    expect(after.execution.verificationSourceBinding.acceptance).toBe(false);
    expect(after.execution.verificationSourceBinding.originalSourceWorkspace).toBe(f.oldSource);
    expect(after.attempt_count).toBe(0);
    expect(after.max_attempts).toBe(before.max_attempts);
    expect(after.criteria).toEqual(before.criteria);
    expect(after.description).toBe(before.description);
    expect(after.resources).toEqual(before.resources);
    expect(after.required_memories).toEqual(before.required_memories);
    expect(after.allowed_accounts).toEqual(before.allowed_accounts);
    expect(after.instruction_revision_id).toBe(before.instruction_revision_id);
    expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.qaId])).rows[0].n).toBe(0);
    const staleWorkerWrite = await pool.query(`UPDATE settings SET value=jsonb_set(value,'{state}','"prepared"'::jsonb)
      WHERE id=$1 AND value->>'state'=ANY(ARRAY['preparing','uncertain','prepared'])
        AND value->>'actionId'=$2 AND value->>'requestDigest'=$3`,
      [`verification-source-binding:${f.qaId}`, after.execution.verificationSourceBinding.actionId,
        after.execution.verificationSourceBinding.requestDigest]);
    expect(staleWorkerWrite.rowCount).toBe(0);
    expect((await pool.query('SELECT value->>\'state\' state FROM settings WHERE id=$1', [`verification-source-binding:${f.qaId}`])).rows[0].state).toBe('applied');
    await expect(pool.query('UPDATE tasks SET criteria=$2 WHERE id=$1', [f.qaId, JSON.stringify([])]))
      .rejects.toThrow('VERIFICATION_SOURCE_BINDING_CONTRACT_FROZEN');

    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,1,$3,'harness',1)",
      [randomUUID(), f.qaId, randomUUID()]);
  } finally { await cleanup(f); }
});

it('refuses an unbound run and direct source mutation after a rebase has made binding mandatory', async () => {
  const f = await fixture(false);
  try {
    await expect(pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,1,$3,'harness',1)",
      [randomUUID(), f.qaId, randomUUID()])).rejects.toThrow('VERIFICATION_SOURCE_BINDING_REQUIRED');
    await expect(pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{sourceWorkspace}',$2::jsonb) WHERE id=$1",
      [f.qaId, JSON.stringify(f.parentWorkspace)])).rejects.toThrow('VERIFICATION_SOURCE_BINDING_REQUIRED');
  } finally { await cleanup(f); }
});
