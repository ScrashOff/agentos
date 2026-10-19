import { randomUUID } from 'node:crypto';
import { pool, tx, lock, event } from '../server/db.js';
import { hash } from '../server/memory.js';
import { validateFollowupPlan, followupPlansEqual } from '../server/followup-plan.js';

const goal = process.argv[2];
if (!/^[0-9a-f-]{36}$/.test(goal ?? '')) throw Error('FOLLOWUP_ID_REQUIRED');
try {
  console.log(
    JSON.stringify(
      await tx(async (db) => {
        await lock(db);
        const row = (
          await db.query(
            `SELECT g.state,o.request,o.revision,p.plan FROM goals g JOIN objectives o ON o.id=g.id
      JOIN LATERAL(SELECT plan FROM plan_revisions WHERE objective_id=g.id ORDER BY revision DESC LIMIT 1)p ON true
      JOIN goal_followups f ON f.goal_id=g.id AND f.state='waiting' WHERE g.id=$1 FOR UPDATE OF g,o`,
            [goal],
          )
        ).rows[0];
        if (row?.state !== 'waiting_predecessor') throw Error('FOLLOWUP_MUST_STILL_BE_WAITING');
        if (
          (
            await db.query(
              'SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 LIMIT 1',
              [goal],
            )
          ).rowCount
        )
          throw Error('FOLLOWUP_ALREADY_EXECUTED');
        // RTP is an independent verification of the integration, not a new code producer.
        await db.query(
          `UPDATE tasks SET stage='verify' WHERE goal_id=$1 AND key LIKE '%-rtp'
      AND execution->>'kind'='opencode' AND execution->>'workKind' IS DISTINCT FROM 'repair'`,
          [goal],
        );
        const tasks = (
          await db.query(
            `SELECT t.*,COALESCE((SELECT jsonb_agg(d.depends_on) FROM dependencies d WHERE d.task_id=t.id),'[]') dependencies
      FROM tasks t WHERE t.goal_id=$1 ORDER BY t.created_at,t.key`,
            [goal],
          )
        ).rows;
        const validated = validateFollowupPlan(
          row.request,
          row.plan,
          tasks,
          (await db.query('SELECT * FROM agents')).rows,
          (await db.query('SELECT * FROM accounts')).rows,
        );
        if (!followupPlansEqual(validated.plan.requirements, row.plan.requirements))
          throw Error('FOLLOWUP_REQUIREMENTS_CHANGED');
        if (followupPlansEqual(validated.plan, row.plan))
          return {
            goalId: goal,
            validated: true,
            changed: false,
            tasks: validated.plan.tasks.length,
          };
        const revision = row.revision + 1,
          planningId = randomUUID();
        await db.query(
          `INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state,fence,output,finished_at)
      VALUES($1,$2,$3,$4,'completed',$5,$6,now())`,
          [
            planningId,
            goal,
            row.revision,
            'operator-contract-validation:' + revision,
            randomUUID(),
            JSON.stringify(validated.plan),
          ],
        );
        await db.query(
          `INSERT INTO plan_revisions(id,objective_id,revision,planning_run_id,plan,status)
      VALUES($1,$2,$3,$4,$5,'waiting_predecessor')`,
          [randomUUID(), goal, revision, planningId, JSON.stringify(validated.plan)],
        );
        await db.query('UPDATE objectives SET revision=$2 WHERE id=$1', [goal, revision]);
        await db.query('UPDATE tasks SET plan_revision=$2 WHERE goal_id=$1', [goal, revision]);
        const result = {
          goalId: goal,
          validated: true,
          changed: true,
          revision,
          tasks: validated.plan.tasks.length,
          requestedCapacity: validated.request.requestedCapacity,
          planDigest: hash(JSON.stringify(validated.plan)),
          originalRequirementsPreserved: true,
          runs: 0,
        };
        await event(db, 'FOLLOWUP_PLAN_VALIDATED', goal, result);
        return result;
      }),
    ),
  );
} finally {
  await pool.end();
}
