import type {DB} from './db.js';

/** Small read model: no instructions, context, artifact bytes or exception text. */
export async function deliveryPanel(db:DB,goalId:string){
  const installed=(await db.query(`SELECT to_regclass('supervision_cases') cases,
    to_regclass('delivery_consumptions') consumptions`)).rows[0];
  const cases=installed.cases?(await db.query(`SELECT task_id,state,count(*)::int total,
    (array_agg(id ORDER BY created_at,id))[1] case_id,min(created_at) since
    FROM supervision_cases WHERE goal_id=$1 AND state IN('open','decided','delegated','waiting_evidence')
    GROUP BY task_id,state ORDER BY min(created_at),task_id,state`,[goalId])).rows:[];
  const consumptions=installed.consumptions?(await db.query(`SELECT consumer_task_id task_id,state,
    receipt->>'code' code,receipt->>'owner' owner,count(*)::int total,min(updated_at) since
    FROM delivery_consumptions WHERE goal_id=$1 AND state NOT IN('completed','superseded')
    GROUP BY consumer_task_id,state,receipt->>'code',receipt->>'owner'
    ORDER BY min(updated_at),consumer_task_id,state`,[goalId])).rows:[];
  const accepted=(await db.query(`SELECT t.id task_id,a.created_at accepted_at FROM tasks t
    JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count AND r.state='submitted'
    JOIN LATERAL(SELECT passed,created_at FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON a.passed
    WHERE t.goal_id=$1 AND t.state='accepted' AND t.stage IN('integrate','verify')
      AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id AND live.state IN('queued','running','cancelling','reconciling'))
    ORDER BY a.created_at DESC`,[goalId])).rows;
  return {observedAt:new Date().toISOString(),cases,consumptions,accepted};
}
