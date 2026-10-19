import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pool, tx, lock, event } from './db.js';
import { config } from './config.js';
import { readArtifact, validateTask } from './service.js';
import { hash } from './memory.js';

export function recoverCoordinationArtifact(
  artifacts: Array<{ name: string; content: string | Buffer; mime?: string }>,
) {
  if (artifacts.some((a) => a.name === 'coordination.json')) return artifacts;
  const matching = artifacts.filter((a) => a.name === 'report.md');
  if (matching.length !== 1) return artifacts;
  const text = matching[0].content.toString();
  // Only an explicitly named, unambiguous JSON block is recoverable. Never infer advice from prose.
  if (
    !/^#\s+coordination\.json\s*\r?\n/i.test(text.trim()) ||
    (text.match(/```/g) ?? []).length !== 2
  )
    return artifacts;
  const body = /```json\s*\r?\n([\s\S]*?)\r?\n```/i.exec(text)?.[1];
  if (!body) return artifacts;
  try {
    const value = JSON.parse(body);
    if (
      typeof value.summary !== 'string' ||
      !Array.isArray(value.priorities) ||
      !Array.isArray(value.recommendations)
    )
      return artifacts;
    return [...artifacts, { name: 'coordination.json', content: body, mime: 'application/json' }];
  } catch {
    return artifacts;
  }
}

export async function recoverAnalyticalDeliveries() {
  const rows = (
    await pool.query(`SELECT t.id,t.attempt_count,r.id run_id,r.fence FROM tasks t
    JOIN goals g ON g.id=t.goal_id JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    LEFT JOIN settings s ON s.id='autonomy:'||g.project
    WHERE t.stage='coordinate' AND t.execution IS NULL AND t.state='submitted' AND r.state='submitted'
    AND g.state='active' AND COALESCE(s.value->>'enabled','true')='true'
    AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR s.value->>'enabled'='true')
    AND NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.run_id=r.id AND a.name='coordination.json') LIMIT 10`)
  ).rows;
  for (const row of rows) {
    const refs = (
      await pool.query('SELECT id,name,mime,digest FROM artifacts WHERE run_id=$1', [row.run_id])
    ).rows;
    const artifacts = await Promise.all(
      refs.map(async (a) => ({
        name: a.name,
        mime: a.mime,
        content: (await readArtifact(a.id)).bytes,
      })),
    );
    const recovered = recoverCoordinationArtifact(artifacts);
    if (recovered.length === artifacts.length) continue;
    const id = await tx(async (db) => {
      await lock(db);
      const t = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [row.id])).rows[0];
      const r = (
        await db.query('SELECT * FROM runs WHERE id=$1 AND fence=$2', [row.run_id, row.fence])
      ).rows[0];
      const g = (await db.query('SELECT state FROM goals WHERE id=$1', [t.goal_id])).rows[0];
      if (
        t.state !== 'submitted' ||
        t.attempt_count !== row.attempt_count ||
        r?.state !== 'submitted' ||
        g.state !== 'active'
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
      const id = randomUUID(),
        attempt = r.attempt + 1;
      await db.query(
        `INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,worker_id,started_at,finished_at,summary,usage,checkpoint)
        VALUES($1,$2,$3,$4,$5,$6,'submitted','analytical-collector',now(),now(),$7,$8,$9)`,
        [
          id,
          t.id,
          attempt,
          randomUUID(),
          r.runtime,
          r.agent_version,
          r.summary,
          JSON.stringify({ input: 0, output: 0, totalTokens: 0, cost: 0 }),
          JSON.stringify({
            kind: 'analytical_artifact_recovery',
            sourceRunId: r.id,
            modelInvoked: false,
          }),
        ],
      );
      await mkdir(path.join(config.dataDir, 'artifacts'), { recursive: true });
      for (const a of recovered) {
        const artifactId = randomUUID(),
          bytes = Buffer.from(a.content);
        await writeFile(path.join(config.dataDir, 'artifacts', artifactId), bytes, { flag: 'wx' });
        await db.query(
          'INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
          [artifactId, id, a.name, a.mime, hash(bytes), bytes.length],
        );
      }
      await db.query(
        "UPDATE tasks SET attempt_count=$2,max_attempts=GREATEST(max_attempts,$2),reason='Documento estruturado recuperado; conferindo critérios originais.' WHERE id=$1",
        [t.id, attempt],
      );
      await event(db, 'ARTIFACTS_RECOVERED', t.id, {
        runId: id,
        sourceRunId: r.id,
        modelInvoked: false,
        artifactCount: recovered.length,
      });
      return id;
    });
    if (id) await validateTask(row.id);
  }
}
