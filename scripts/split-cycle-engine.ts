import { pool, tx, lock, event } from '../server/db.js';
try {
  await tx(async (db) => {
    await lock(db);
    if (
      (
        await db.query(
          "SELECT 1 FROM cycle_attempts WHERE state IN('running','cancelling','reconciling')",
        )
      ).rowCount
    )
      throw Error('CYCLE_EXECUTION_ACTIVE');
    const owners = (
      await db.query(
        "SELECT pid FROM pg_locks WHERE locktype='advisory' AND classid=0 AND objid=4280088 AND granted",
      )
    ).rows;
    for (const owner of owners) await db.query('SELECT pg_terminate_backend($1)', [owner.pid]);
    await event(db, 'CYCLE_ENGINE_HANDOFF', null, {
      from: 'embedded-worker',
      to: 'agentos-cycles',
      activeAttempts: 0,
    });
  });
  console.log('Cycle engine handed off without interrupting any execution.');
} finally {
  await pool.end();
}
