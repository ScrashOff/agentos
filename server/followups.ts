import { pool, tx, lock, event } from './db.js';

export async function installFollowups() {
  await pool.query(`CREATE TABLE IF NOT EXISTS goal_followups(
    goal_id uuid PRIMARY KEY REFERENCES goals(id), predecessor_id uuid NOT NULL REFERENCES goals(id),
    required_verifiers jsonb NOT NULL, state text NOT NULL DEFAULT 'waiting',
    created_at timestamptz NOT NULL DEFAULT now(), activated_at timestamptz,
    CHECK(goal_id<>predecessor_id));
    CREATE OR REPLACE FUNCTION agentos_followup_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.state='active' AND current_setting('agentos.followup_activate',true) IS DISTINCT FROM NEW.id::text
        AND EXISTS(SELECT 1 FROM goal_followups WHERE goal_id=NEW.id AND state='waiting') THEN
        RAISE EXCEPTION 'FOLLOWUP_PENDING';
      END IF; RETURN NEW;
    END $$;
    DROP TRIGGER IF EXISTS agentos_followup_hold ON goals;
    CREATE TRIGGER agentos_followup_hold BEFORE UPDATE OF state ON goals FOR EACH ROW EXECUTE FUNCTION agentos_followup_hold();`);
}

/** A persisted prerequisite, not a timer or an LLM's declaration of completion. */
export async function reconcileFollowups() {
  return tx(async (db) => {
    await lock(db);
    const waiting = (
      await db.query(`SELECT f.* FROM goal_followups f
      JOIN goals next ON next.id=f.goal_id JOIN goals previous ON previous.id=f.predecessor_id
      LEFT JOIN settings a ON a.id='autonomy:'||next.project
      WHERE f.state='waiting' AND next.state='waiting_predecessor' AND previous.state='completed'
      AND COALESCE(a.value->>'enabled','true')='true' FOR UPDATE OF f`)
    ).rows;
    const activated: string[] = [];
    for (const f of waiting) {
      if (
        !Array.isArray(f.required_verifiers) ||
        !f.required_verifiers.length ||
        new Set(f.required_verifiers).size !== f.required_verifiers.length
      )
        continue;
      // Re-check current tasks and each verifier's latest attempt. Historical passes do not count.
      if (
        (
          await db.query(
            "SELECT 1 FROM tasks WHERE goal_id=$1 AND state NOT IN('accepted','superseded') LIMIT 1",
            [f.predecessor_id],
          )
        ).rowCount
      )
        continue;
      if (
        (
          await db.query(
            `SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1
        AND r.state IN('queued','running','reconciling','cancelling') LIMIT 1`,
            [f.predecessor_id],
          )
        ).rowCount
      )
        continue;
      const verified = (
        await db.query(
          `SELECT t.id FROM tasks t
        JOIN LATERAL(SELECT id,attempt FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
        WHERE t.id=ANY($1::uuid[]) AND t.goal_id=$2 AND t.stage='verify' AND t.state='accepted' AND t.attempt_count=r.attempt
        AND EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=r.id AND a.passed)`,
          [f.required_verifiers, f.predecessor_id],
        )
      ).rows;
      if (verified.length !== f.required_verifiers.length) continue;
      await db.query("SELECT set_config('agentos.followup_activate',$1,true)", [f.goal_id]);
      await db.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal_id]);
      await db.query(
        "UPDATE objectives SET status='executing',reason=NULL,updated_at=now() WHERE id=$1",
        [f.goal_id],
      );
      await db.query(
        "UPDATE goal_followups SET state='activated',activated_at=now() WHERE goal_id=$1",
        [f.goal_id],
      );
      await event(db, 'FOLLOWUP_ACTIVATED', f.goal_id, {
        predecessorId: f.predecessor_id,
        verifiedTasks: verified.map((t) => t.id),
      });
      activated.push(f.goal_id);
    }
    return activated;
  });
}
