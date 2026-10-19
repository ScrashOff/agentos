import { pool, tx, lock, event } from './db.js';
import { inspectStopped } from './continuation.js';
import { continuationSignature } from './supervision-handoff.js';
import { recoverOpenCodeRun } from './opencode-recovery.js';
import {
  adoptRecoveredArtifacts,
  collectStoppedArtifacts,
  packagingFailure,
  packagingFailureCodes,
} from './artifact-recovery.js';

// Runs independently of active worker processes; no model or job is started.
export async function recoverMissingOpenCodeArtifacts(
  collect = collectStoppedArtifacts,
  inspect = inspectStopped,
) {
  const candidates = (
    // Filter eligible latest attempts before bounding the batch. Otherwise a
    // prefix of permanent non-packaging failures can starve every later result.
    // Claims below still recheck the authoritative contract and stop ownership.
    await pool.query(`SELECT t.id FROM tasks t
    JOIN goals g ON g.id=t.goal_id LEFT JOIN settings s ON s.id='autonomy:'||g.project
    JOIN LATERAL(SELECT id,state,error_code,runtime,attempt FROM runs
      WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1) r ON true
    LEFT JOIN LATERAL(SELECT checks FROM acceptances WHERE run_id=r.id
      ORDER BY created_at DESC LIMIT 1) v ON true
    WHERE t.state IN('submitted','failed','recovering_artifacts','recovery_blocked') AND t.execution->>'kind'='opencode' AND g.state='active'
    AND COALESCE(s.value->>'enabled','true')='true'
    AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR s.value->>'enabled'='true')
    AND r.runtime='opencode' AND r.attempt=t.attempt_count
    AND (COALESCE(t.execution->>'artifactCollectionAt','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{3}Z$'
      OR t.execution->>'artifactCollectionAt'<=$2)
    AND (r.state='failed' AND r.error_code=ANY($1::text[]) OR
      r.state='submitted' AND
      EXISTS(SELECT 1 FROM jsonb_array_elements(COALESCE(v.checks,'[]'::jsonb)) c
        WHERE c->>'passed' IS DISTINCT FROM 'true') AND
      NOT EXISTS(SELECT 1 FROM jsonb_array_elements(COALESCE(v.checks,'[]'::jsonb)) c
        WHERE c->>'passed' IS DISTINCT FROM 'true' AND
        (c->>'type'='review' OR NOT EXISTS(
          SELECT 1 FROM jsonb_array_elements(t.criteria) criterion
          WHERE criterion->>'id'=c->>'id' AND NULLIF(criterion->>'name','') IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.run_id=r.id AND a.name=criterion->>'name')))))
    AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.payload->>'runId'=r.id::text)
    AND NOT EXISTS(SELECT 1 FROM runs active JOIN tasks worker ON worker.id=active.task_id
      WHERE active.state IN('queued','running','reconciling','cancelling')
      AND (worker.id=t.id OR worker.execution->>'workspace'=t.execution->>'workspace'))
    ORDER BY (t.execution->>'workKind'='planning') DESC NULLS LAST,
      t.execution->>'artifactCollectionAt' ASC NULLS FIRST,t.updated_at LIMIT 30`,
      [packagingFailureCodes,new Date(Date.now()-60000).toISOString()])
  ).rows;
  const recovered = [];
  for (const candidate of candidates) {
    const claim = await tx(async (db) => {
      await lock(db);
      const t = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [candidate.id]))
        .rows[0];
      if (
        !t || t.execution?.kind !== 'opencode' ||
        !['submitted', 'failed', 'recovering_artifacts', 'recovery_blocked'].includes(t.state)
      )
        return;
      const authority=(await db.query(`SELECT g.state,s.value autonomy FROM goals g
        LEFT JOIN settings s ON s.id='autonomy:'||g.project WHERE g.id=$1`,[t.goal_id])).rows[0];
      if(authority?.state!=='active' || authority.autonomy?.enabled===false ||
        (t.recovery?.mode==='manual' && authority.autonomy?.enabled!==true))return;
      if (Date.parse(t.execution.artifactCollectionAt ?? '') > Date.now() - 60000) return;
      const r = (
        await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [t.id])
      ).rows[0];
      if (
        !r ||
        !(r.state === 'submitted' || (r.state === 'failed' && packagingFailure(r.error_code))) ||
        r.runtime !== 'opencode' ||
        r.attempt !== t.attempt_count
      )
        return;
      if (
        (
          await db.query(
            `SELECT 1 FROM runs active JOIN tasks worker ON worker.id=active.task_id
        WHERE active.state IN('queued','running','reconciling','cancelling')
        AND (worker.id=$1 OR worker.execution->>'workspace'=$2) LIMIT 1`,
            [t.id, t.execution.workspace],
          )
        ).rowCount
      )
        return;
      if (
        (
          await db.query(
            "SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",
            [r.id],
          )
        ).rowCount
      )
        return;
      const checks =
        (
          await db.query(
            'SELECT checks FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC LIMIT 1',
            [r.id],
          )
        ).rows[0]?.checks ?? [];
      const artifacts = (await db.query('SELECT name FROM artifacts WHERE run_id=$1', [r.id])).rows;
      const failed = checks.filter((c: any) => !c.passed);
      if (
        r.state === 'submitted' &&
        (!failed.length ||
          !failed.every((c: any) => {
            const criterion = t.criteria.find((v: any) => v.id === c.id);
            return (
              criterion?.name &&
              c.type !== 'review' &&
              !artifacts.some((a) => a.name === criterion.name)
            );
          }))
      )
        return;
      const now = new Date().toISOString();
      const execution = {
        ...t.execution,
        artifactCollectionAt: now,
        continuation: {
          runId: r.id,
          state: 'collecting',
          message: 'Recuperando anexo ausente dos arquivos já produzidos.',
          updatedAt: now,
          nextAt: new Date(Date.now() + 60000).toISOString(),
        },
      };
      await db.query('UPDATE tasks SET execution=$2 WHERE id=$1', [
        t.id,
        JSON.stringify(execution),
      ]);
      return { task: t, run: r };
    });
    if (!claim) continue;
    const { task: t, run: r } = claim;
    try {
      const delivery = await collect(
        r.id,
        Boolean(t.execution.readOnlySource),
        t.execution.testFile,
      );
      recovered.push(await adoptRecoveredArtifacts(r.id, r.fence, delivery));
    } catch (e: any) {
      if (['STALE_RUN', 'TASK_CHANGED'].includes(e.message)) continue;
      if(t.execution.workKind==='planning' && r.state==='failed' &&
        ['RESULT_FORMAT','DELIVERY_NOT_RECOVERABLE','ARTIFACT_MISSING','ARTIFACT_NAME','ARTIFACT_PATH'].includes(e.code??e.message)){
        // The scoped controller owns model retries. Generic protocol recovery
        // must never replace its plan contract with a regression-suite contract.
        await tx(async db=>{await lock(db);
          const current=(await db.query('SELECT state,attempt_count FROM tasks WHERE id=$1 FOR UPDATE',[t.id])).rows[0];
          if(current.attempt_count!==r.attempt || !['failed','recovering_artifacts','recovery_blocked'].includes(current.state))return;
          await db.query("UPDATE tasks SET state='failed',reason='Recuperação do plano exige entrega do arquivo estruturado.' WHERE id=$1",[t.id]);
          await db.query("UPDATE planning_requests SET state='replanning_required',receipt=$2,updated_at=now() WHERE id=$1",[
            t.execution.planningRequestId,JSON.stringify({code:'PLANNING_DELIVERY_MISSING',collectionCode:e.code??e.message,
              nextAction:'Preserve scope-plan.json existente; entregue evidence/scope-plan.json no JSON final com summary e artifacts. Não produza protocol-report.json.'})]);
        });continue;
      }
      // A definitive file/format failure may need a targeted model repair.
      // SSH timeouts and uncertain/live processes never authorize that fallback.
      if (
        r.state === 'failed' &&
        [
          'RESULT_FORMAT',
          'DELIVERY_NOT_RECOVERABLE',
          'ARTIFACT_MISSING',
          'ARTIFACT_NAME',
          'ARTIFACT_NAME_COLLISION',
          'ARTIFACT_PATH',
          'ARTIFACT_TOO_LARGE',
        ].includes(e.code ?? e.message)
      ) {
        try {
          const proof = await inspect(r.id, true);
          proof.packagingFallback = true;
          proof.collectionFailure = e.code ?? e.message;
          proof.continuationSignature = await continuationSignature(
            pool,
            t,
            r.error_code,
            proof.candidate?.candidateDigest ?? null,
          );
          const next = await recoverOpenCodeRun(r.id, r.fence, proof);
          await event(pool, 'ARTIFACT_COLLECTION_FALLBACK', t.id, {
            sourceRunId: r.id,
            nextRunId: next.runId,
            code: proof.collectionFailure,
            stopConfirmed: true,
          });
          continue;
        } catch (fallback: any) {
          await event(pool, 'ARTIFACT_COLLECTION_DEFERRED', t.id, {
            runId: r.id,
            code: fallback.code ?? fallback.message,
          });
          continue;
        }
      }
      const continuation = {
        runId: r.id,
        state: 'checking',
        message: 'A coleta não completou a entrega; verificando a recuperação da execução.',
        updatedAt: new Date().toISOString(),
        nextAt: new Date(Date.now() + 15000).toISOString(),
      };
      await pool.query(
        `UPDATE tasks SET execution=jsonb_set(execution,'{continuation}',$3::jsonb)
        WHERE id=$1 AND attempt_count=$2 AND state IN('submitted','failed','recovering_artifacts','recovery_blocked')`,
        [t.id, r.attempt, JSON.stringify(continuation)],
      );
      await event(pool, 'ARTIFACT_COLLECTION_DEFERRED', t.id, {
        runId: r.id,
        code: e.code ?? e.message,
      });
    }
  }
  return recovered;
}
