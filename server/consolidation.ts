import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { tx, lock, event } from './db.js';
import { config } from './config.js';
import { hash } from './memory.js';

export async function consolidateGoals(targetId: string, sourceId: string) {
  if (targetId === sourceId) throw Error('SAME_GOAL');
  return tx(async (db) => {
    await lock(db);
    const rows = (
      await db.query('SELECT * FROM goals WHERE id=ANY($1::uuid[]) FOR UPDATE', [
        [targetId, sourceId],
      ])
    ).rows;
    const target = rows.find((g) => g.id === targetId),
      source = rows.find((g) => g.id === sourceId);
    if (!target || !source || target.project !== source.project)
      throw Error('GOAL_PROJECT_MISMATCH');
    if (source.merged_into === targetId) return { merged: true, alreadyMerged: true };
    if (
      target.state !== 'paused' ||
      source.state !== 'paused' ||
      source.merged_into ||
      target.merged_into
    )
      throw Error('PAUSE_ADMISSION_FIRST');
    const tasks = (
      await db.query('SELECT * FROM tasks WHERE goal_id=ANY($1::uuid[])', [[targetId, sourceId]])
    ).rows;
    if (new Set(tasks.map((t) => t.key)).size !== tasks.length) throw Error('TASK_KEY_CONFLICT');
    if (tasks.some((t) => !t.execution?.baseWorkspace)) throw Error('WORKSTREAM_MISSING');
    const live = (
      await db.query(
        "SELECT r.*,t.execution FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id WHERE g.project=$1 AND r.state IN('queued','running','cancelling','reconciling')",
        [target.project],
      )
    ).rows;
    const bases = live.map((r) => r.execution?.baseWorkspace ?? r.execution?.workspace);
    if (new Set(bases).size !== bases.length) throw Error('OVERLAPPING_RUNS_REMAIN');
    await db.query(
      `INSERT INTO settings(id,value) VALUES($1,'{"enabled":true}') ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value`,
      ['exclusive-workstreams:' + target.project],
    );
    // Trigger attaches exclusivity to every current task; future plan revisions inherit it too.
    await db.query('UPDATE tasks SET resources=resources WHERE goal_id=ANY($1::uuid[])', [
      [targetId, sourceId],
    ]);
    for (const r of live)
      await db.query('INSERT INTO leases(resource,run_id) VALUES($1,$2) ON CONFLICT DO NOTHING', [
        'workstream:' + (r.execution.baseWorkspace ?? r.execution.workspace),
        r.id,
      ]);
    const moved = (
      await db.query('UPDATE tasks SET goal_id=$1,updated_at=now() WHERE goal_id=$2 RETURNING id', [
        targetId,
        sourceId,
      ])
    ).rows.map((t) => t.id);
    await db.query(
      "UPDATE goals SET state='cancelled',merged_into=$1,description=description || $3 WHERE id=$2",
      [
        targetId,
        sourceId,
        '\nConsolidado no objetivo ' +
          targetId +
          '. Tarefas, tentativas e evidências preservadas no destino.',
      ],
    );
    await db.query(
      "UPDATE objectives SET status='cancelled',reason='Objetivo consolidado',updated_at=now() WHERE id=$1",
      [sourceId],
    );
    const o = (await db.query('SELECT request FROM objectives WHERE id=$1', [targetId])).rows[0];
    if (o) {
      const request = o.request;
      request.message =
        'ATUALIZAÇÃO MAIS RECENTE: uma única operação para estes cinco jogos; no máximo uma execução por jogo. Etapas complementares do mesmo jogo devem executar em sequência. Preservar os critérios de aceite.\n\n' +
        request.requirements.join('\n\n');
      request.constraints = [
        ...request.constraints.filter((c: string) => !c.startsWith('Autorização atual:')),
        'Autorização atual: cinco jogos em paralelo, no máximo um executor por jogo, com etapas complementares em sequência e todos os critérios originais mantidos.',
      ];
      request.requestedCapacity = new Set(tasks.map((t) => t.execution.baseWorkspace)).size;
      await db.query(
        'UPDATE objectives SET request=$2,request_hash=$3,updated_at=now() WHERE id=$1',
        [targetId, JSON.stringify(request), hash(JSON.stringify(request))],
      );
    }
    await event(db, 'GOALS_CONSOLIDATED', targetId, {
      sourceGoalId: sourceId,
      taskIds: moved,
      maxActivePerWorkstream: 1,
    });
    return { merged: true, movedTasks: moved.length, activeRuns: live.length };
  });
}

// Explicit operator reorganization, not failure retry. Only after authenticated stop proof.
export async function resumeConsolidatedRun(runId: string, fence: string, inspection: any) {
  const p = inspection?.process,
    age = Date.now() - Date.parse(inspection?.checkedAt);
  if (
    inspection?.runId !== runId ||
    p?.unit !== `agentos-run-${runId}` ||
    !p.stopped ||
    p.mainPid !== 0 ||
    p.populated !== false ||
    !['inactive', 'failed'].includes(p.activeState) ||
    !Number.isFinite(age) ||
    age < -5000 ||
    age > 60000
  )
    throw Error('INVALID_STOP_PROOF');
  return tx(async (db) => {
    await lock(db);
    const r = (
      await db.query('SELECT * FROM runs WHERE id=$1 AND fence=$2 FOR UPDATE', [runId, fence])
    ).rows[0];
    if (
      !r ||
      r.runtime !== 'harness' ||
      !['cancelled', 'cancelling', 'reconciling'].includes(r.state)
    )
      throw Error('STALE_RUN');
    const t = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [r.task_id])).rows[0];
    const g = (await db.query('SELECT state FROM goals WHERE id=$1', [t.goal_id])).rows[0];
    if (
      g.state !== 'paused' ||
      t.attempt_count !== r.attempt ||
      t.execution?.workspace !== inspection.workspace
    )
      throw Error('TASK_CHANGED');
    if (r.checkpoint?.kind === 'operator_consolidation' && t.state === 'waiting_resource')
      return { taskId: t.id, runId, artifacts: r.checkpoint.artifacts };
    if (!['cancelled', 'cancelling', 'reconciling'].includes(t.state)) throw Error('TASK_CHANGED');
    if (
      !(
        await db.query(
          "SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND entity_id=$1 AND payload->>'runId'=$2",
          [t.id, runId],
        )
      ).rowCount
    )
      throw Error('CANCELLATION_NOT_REQUESTED');
    const files = [
      {
        name: 'consolidation-changes.patch',
        mime: 'text/plain',
        bytes: Buffer.from(inspection.candidate.patch),
      },
      {
        name: 'consolidation-checkpoint.json',
        mime: 'application/json',
        bytes: Buffer.from(
          JSON.stringify(
            { ...inspection, candidate: { ...inspection.candidate, patch: undefined } },
            null,
            2,
          ),
        ),
      },
    ].map((f) => ({ ...f, id: randomUUID(), digest: hash(f.bytes) }));
    if (files.some((f) => f.bytes.length > 8 * 1024 * 1024)) throw Error('CHECKPOINT_TOO_LARGE');
    await mkdir(path.join(config.dataDir, 'artifacts'), { recursive: true });
    for (const f of files) {
      await writeFile(path.join(config.dataDir, 'artifacts', f.id), f.bytes, { flag: 'wx' });
      await db.query(
        'INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
        [f.id, runId, f.name, f.mime, f.digest, f.bytes.length],
      );
      await event(db, 'ARTIFACT_CREATED', t.id, {
        runId,
        artifactId: f.id,
        name: f.name,
        digest: f.digest,
      });
    }
    const artifacts = files.map((f) => ({ id: f.id, name: f.name, digest: f.digest }));
    const checkpoint = {
      kind: 'operator_consolidation',
      previousRunId: runId,
      preservedWorkspace: inspection.workspace,
      artifacts,
      summary:
        'Execução interrompida para eliminar sobreposição. Retomar os arquivos existentes e os testes pendentes; não começar do zero.',
      effectsReviewed: true,
    };
    await db.query(
      "UPDATE runs SET state='cancelled',finished_at=COALESCE(finished_at,now()),checkpoint=$2,summary=$3 WHERE id=$1",
      [runId, JSON.stringify(checkpoint), checkpoint.summary],
    );
    await db.query('DELETE FROM leases WHERE run_id=$1', [runId]);
    const execution = {
      ...t.execution,
      revisionFeedback: {
        ...t.execution.revisionFeedback,
        artifacts: [...(t.execution.revisionFeedback?.artifacts ?? []), ...artifacts],
      },
    };
    await db.query(
      "UPDATE tasks SET state='waiting_resource',reason='Etapa preservada na fila: uma execução por jogo.',next_attempt_at=NULL,max_attempts=GREATEST(max_attempts,attempt_count+1),execution=$2,updated_at=now() WHERE id=$1",
      [t.id, JSON.stringify(execution)],
    );
    await event(db, 'CONSOLIDATION_QUEUED', t.id, { runId, checkpoint, stopProof: p });
    return { taskId: t.id, runId, artifacts };
  });
}
