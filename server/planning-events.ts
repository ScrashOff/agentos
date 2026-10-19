import {pool,type DB} from './db.js';

/** Notification is a latency hint. Durable events + periodic reconciliation
 * remain authoritative if the connection or notification is lost. */
export async function installPlanningEvents(db:DB=pool){
  await db.query(`CREATE OR REPLACE FUNCTION agentos_notify_planning() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.kind IN('TASK_ACCEPTED','VALIDATION_PENDING','RUN_FAILED','RUN_CANCELLED',
        'HANDOFF_APPLIED','HANDOFF_REPLAN_REQUIRED','GOAL_ACTIVE','TASKS_DELEGATED',
        'PLANNING_STRATEGY_REQUESTED','SUPERVISION_APPLIED') THEN
        PERFORM pg_notify('agentos_planning_wake','');
      END IF;
      RETURN NEW;
    END $$;
    DROP TRIGGER IF EXISTS agentos_notify_planning ON events;
    CREATE TRIGGER agentos_notify_planning AFTER INSERT ON events
      FOR EACH ROW EXECUTE FUNCTION agentos_notify_planning();`);
}
