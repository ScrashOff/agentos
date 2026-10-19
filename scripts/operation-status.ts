import { pool } from '../server/db.js';
const id = '3ccc273d-e790-43a8-ba86-82c1287907c5';
try {
  console.log(
    JSON.stringify({
      at: new Date().toISOString(),
      backlog: (
        await pool.query('SELECT value FROM settings WHERE id=$1', ['backlog-status:' + id])
      ).rows[0]?.value,
      coordinators: (
        await pool.query(
          `SELECT t.state,t.attempt_count,t.description LIKE '%BACKLOG_PLANNER_V1%' contract,r.id,r.started_at,r.finished_at,r.state run_state,r.summary FROM tasks t LEFT JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true WHERE t.goal_id=$1 AND t.stage='coordinate'`,
          [id],
        )
      ).rows,
      plans: (
        await pool.query(
          'SELECT state,reason,task_ids,created_at FROM workforce_backlog_plans WHERE goal_id=$1 ORDER BY created_at DESC LIMIT 3',
          [id],
        )
      ).rows,
      active: (
        await pool.query(
          `SELECT r.model,count(*)::int n FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 AND r.state IN('queued','running','reconciling','cancelling') GROUP BY 1`,
          [id],
        )
      ).rows,
      cycles: (await pool.query('SELECT id,name,state FROM cycles')).rows,
      failed: (
        await pool.query(
          `SELECT t.key,t.state,t.reason,t.attempt_count,t.max_attempts,t.execution->'continuation' continuation FROM tasks t WHERE goal_id=$1 AND t.state IN('failed','recovery_blocked')`,
          [id],
        )
      ).rows,
    }),
  );
} finally {
  await pool.end();
}
