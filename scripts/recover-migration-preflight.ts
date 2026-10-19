import { pool, tx, lock, event } from '../server/db.js';
import { readFile, access } from 'node:fs/promises';
const runId = '85d8121b-7dec-4f85-a6d7-477001c05d5a';
try {
  const bridgeModule = '/opt/agentos-bridge/process-stop.mjs';
  const { inspectUnit } = await import(bridgeModule);
  const r = (
    await pool.query(
      'SELECT r.*,t.description,t.execution,t.id task FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1',
      [runId],
    )
  ).rows[0];
  if (r?.state !== 'failed') throw Error('RUN_CHANGED');
  const prior = JSON.parse(await readFile(r.checkpoint.proofArchive.file, 'utf8'));
  if (JSON.stringify(prior).length + r.description.length <= 120000)
    throw Error('PREFLIGHT_CAUSE_NOT_CONFIRMED');
  if (
    await access('/var/lib/agentos-bridge/worker-runs/' + runId + '/state.json').then(
      () => true,
      () => false,
    )
  )
    throw Error('STATE_EXISTS');
  const proof = await inspectUnit('agentos-run-' + runId);
  if (
    !proof.stopped ||
    proof.mainPid !== 0 ||
    proof.populated !== false ||
    proof.loadState !== 'not-found'
  )
    throw Error('PROCESS_NOT_CONFIRMED_ABSENT');
  await tx(async (db) => {
    await lock(db);
    if (
      (
        await db.query(
          "SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','reconciling','cancelling')",
          [r.task],
        )
      ).rowCount
    )
      throw Error('LIVE_RUN');
    await db.query(
      "UPDATE tasks SET state='ready',reason='Checkpoint de transferência compactado; retomar arquivos preservados.',next_attempt_at=NULL WHERE id=$1 AND state='failed'",
      [r.task],
    );
    await event(db, 'PREFLIGHT_REPAIRED', r.task, {
      runId,
      reason:
        'Migration checkpoint exceeded 120000 characters before remote spawn; proof archived and compacted.',
      proof,
    });
  });
  console.log(
    JSON.stringify({
      requeued: r.task,
      reason: 'oversized-migration-checkpoint',
      stopped: proof.stopped,
    }),
  );
} finally {
  await pool.end();
}
