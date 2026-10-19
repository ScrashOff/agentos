import type {DB} from './db.js';

/** Scheduling hint only. A terminal current consumer earns an earlier read,
 * never a stop proof, retry, consumption receipt or acceptance. One of the four
 * slots always follows the existing oldest-checked order; unused priority
 * slots return to that order. Every effect keeps the reconciler's observation,
 * identity stamp, admission lock and fresh authoritative checks. */
export async function selectDeliveryConsumptionBatch(db:DB){
  return (await db.query(`WITH eligible AS MATERIALIZED(
    SELECT d.id,d.goal_id,d.consumer_task_id,d.state,d.checked_at,d.created_at
    FROM delivery_consumptions d JOIN goals g ON g.id=d.goal_id
    WHERE d.state NOT IN('completed','superseded') AND g.state='active'
      AND NOT EXISTS(SELECT 1 FROM settings a WHERE a.id='autonomy:'||g.project AND a.value->>'enabled'='false')
  ), priority_ids AS MATERIALIZED(
    SELECT e.id,e.checked_at,e.created_at,0 lane FROM eligible e
    JOIN tasks t ON t.id=e.consumer_task_id AND t.goal_id=e.goal_id
    JOIN LATERAL(SELECT id,state,attempt FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1) r ON true
    WHERE e.state='executing' AND t.state IN('accepted','submitted','failed','recovery_blocked','cancelled','superseded')
      AND r.state IN('submitted','failed','cancelled') AND r.attempt=t.attempt_count
      AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id AND live.state IN('queued','running','cancelling','reconciling'))
      AND NOT EXISTS(SELECT 1 FROM leases l JOIN runs leased ON leased.id=l.run_id WHERE leased.task_id=t.id)
    ORDER BY e.checked_at NULLS FIRST,e.created_at,e.id LIMIT 3
  ), fair_ids AS MATERIALIZED(
    SELECT e.id,e.checked_at,e.created_at,1 lane FROM eligible e
    WHERE NOT EXISTS(SELECT 1 FROM priority_ids p WHERE p.id=e.id)
    ORDER BY e.checked_at NULLS FIRST,e.created_at,e.id LIMIT(4-(SELECT count(*) FROM priority_ids))
  ), selected AS(SELECT * FROM priority_ids UNION ALL SELECT * FROM fair_ids)
  SELECT d.* FROM selected s JOIN delivery_consumptions d ON d.id=s.id
  ORDER BY s.lane,s.checked_at NULLS FIRST,s.created_at,s.id`)).rows;
}
