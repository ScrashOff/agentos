import { config } from './config.js';
import { pool, tx, event } from './db.js';
import { accountBridge } from './bridge.js';
import { hash, saveMemory } from './memory.js';
export async function importRecall(project: string, query: string) {
  if (!config.bridgeHost) return { imported: 0, status: 'not_configured' };
  try {
    const result = await accountBridge.request(
      'memory.search',
      { project, query },
      { timeout: 10000 },
    );
    let imported = 0;
    for (const item of (result.items ?? []).slice(0, 8)) {
      if (
        item.project !== project ||
        typeof item.id !== 'string' ||
        typeof item.body !== 'string' ||
        item.body.length > 20000
      )
        continue;
      const version = hash(item.description + '\n' + item.body),
        sourceId = 'harness-recall:' + item.id;
      await tx(async (db) => {
        await db.query('SELECT pg_advisory_xact_lock(4280026)');
        const prior = (
          await db.query('SELECT * FROM recall_imports WHERE source_id=$1', [sourceId])
        ).rows[0];
        if (prior?.version === version) return;
        const memory = await saveMemory(
          {
            project,
            title: item.description,
            content: item.body,
            kind: 'observation',
            verification: 'unverified',
            source: sourceId,
            supersedes: prior?.memory_id,
          },
          db,
        );
        // Imported records retain provenance; do not export them back as newly learned facts.
        await db.query('DELETE FROM memory_outbox WHERE memory_id=$1', [memory.id]);
        await db.query(
          'INSERT INTO recall_imports(source_id,version,memory_id,project) VALUES($1,$2,$3,$4) ON CONFLICT(source_id) DO UPDATE SET version=$2,memory_id=$3',
          [sourceId, version, memory.id, project],
        );
        imported++;
      });
    }
    return { imported, status: 'read', source: result.source };
  } catch {
    return { imported: 0, status: 'unavailable' };
  }
}
/** Recall notes-hub protocol. Stable IDs make retries of /shared/push idempotent. */
export async function syncRecallOnce() {
  if ((!config.recallUrl || !config.recallKey) && !config.bridgeHost) return 0;
  if (!/^agentos[-_]/.test(config.recallNamespace))
    throw Error('Recall requires a dedicated AgentOS namespace.');
  let synced = 0;
  for (let i = 0; i < 10; i++) {
    const hadWork = await tx(async (db) => {
      const row = (
        await db.query(
          `SELECT o.*,m.title,m.content,m.project,m.kind,m.source,m.version,m.supersedes,m.created_at FROM memory_outbox o JOIN memories m ON m.id=o.memory_id WHERE o.state IN('pending','retry','not_configured') AND (o.next_attempt_at IS NULL OR o.next_attempt_at<=now()) ORDER BY m.created_at FOR UPDATE OF o SKIP LOCKED LIMIT 1`,
        )
      ).rows[0];
      if (!row) return false;
      try {
        const created = new Date(row.created_at).getTime();
        const body = {
          id: 'agentos-' + row.memory_id,
          name: '',
          description: row.title,
          body: row.content,
          project: row.project,
          type: row.kind === 'decision' ? 'decision' : 'reference',
          scope: 'private',
          space: config.recallNamespace,
          author: 'agentos/development',
          createdAt: created,
          updatedAt: created,
          vector: [],
          embedModel: '',
          supersedes: row.supersedes ? ['agentos-' + row.supersedes] : [],
          links: [],
        };
        let ack: { cursor?: unknown };
        if (config.recallUrl && config.recallKey) {
          const res = await fetch(new URL('/shared/push', config.recallUrl), {
            method: 'POST',
            headers: {
              Authorization: 'Bearer ' + config.recallKey,
              'Content-Type': 'application/json',
            },
            body: JSON.stringify(body),
            redirect: 'error',
            signal: AbortSignal.timeout(4000),
          });
          if (!res.ok) throw Error('HTTP ' + res.status);
          ack = (await res.json()) as { cursor?: unknown };
        } else ack = await accountBridge.request('memory.push', body, { timeout: 12000 });
        if (!Number.isSafeInteger(ack.cursor)) throw Error('Acknowledgement missing');
        await db.query(
          "UPDATE memory_outbox SET state='synced',attempts=attempts+1,synced_at=now(),error=NULL WHERE memory_id=$1",
          [row.memory_id],
        );
        await event(db, 'MEMORY_SYNCED', row.memory_id, {
          space: config.recallNamespace,
          cursor: ack.cursor,
        });
        synced++;
      } catch {
        await db.query(
          "UPDATE memory_outbox SET state='retry',attempts=attempts+1,next_attempt_at=now()+make_interval(secs=>$2),error='Recall sem confirmação de persistência' WHERE memory_id=$1",
          [row.memory_id, Math.min(300, 2 ** Math.min(row.attempts + 1, 8))],
        );
      }
      return true;
    });
    if (!hadWork) break;
  }
  return synced;
}
export async function recallStatus() {
  return {
    configured: (!!config.recallUrl && !!config.recallKey) || !!config.bridgeHost,
    namespace: config.recallNamespace,
    ...(
      await pool.query(
        "SELECT count(*) FILTER(WHERE state='synced')::int synced,count(*) FILTER(WHERE state<>'synced')::int pending,max(synced_at) last_synced FROM memory_outbox",
      )
    ).rows[0],
  };
}
