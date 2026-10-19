import type { DB } from './db.js';

// Database enforcement also covers native executors and an already-running scheduler.
// Opt in per project. A workstream is the logical base, not a worker's isolated copy.
export async function installWorkstreamPolicy(db: DB) {
  await db.query(`
ALTER TABLE goals ADD COLUMN IF NOT EXISTS merged_into uuid REFERENCES goals(id);
CREATE OR REPLACE FUNCTION agentos_workstream_resources() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE project_name text; base text;
BEGIN
 SELECT project INTO project_name FROM goals WHERE id=NEW.goal_id;
 IF EXISTS(SELECT 1 FROM settings WHERE id='exclusive-workstreams:'||project_name AND value->>'enabled'='true') THEN
  base := COALESCE(NEW.execution->>'baseWorkspace',NEW.execution->>'workspace');
  IF base IS NOT NULL THEN
   NEW.resources := COALESCE(NEW.resources,'[]'::jsonb);
   IF NOT NEW.resources ? ('workstream:'||base) THEN
    NEW.resources := NEW.resources || jsonb_build_array('workstream:'||base);
   END IF;
  END IF;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS agentos_workstream_resources ON tasks;
CREATE TRIGGER agentos_workstream_resources BEFORE INSERT OR UPDATE OF execution,resources,goal_id ON tasks
 FOR EACH ROW EXECUTE FUNCTION agentos_workstream_resources();
CREATE OR REPLACE FUNCTION agentos_workstream_admission() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE project_name text; base text;
BEGIN
 IF NEW.state NOT IN ('queued','running','cancelling','reconciling') THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND OLD.state IN ('queued','running','cancelling','reconciling') THEN RETURN NEW; END IF;
 SELECT g.project,COALESCE(t.execution->>'baseWorkspace',t.execution->>'workspace') INTO project_name,base
 FROM tasks t JOIN goals g ON g.id=t.goal_id WHERE t.id=NEW.task_id;
 IF base IS NOT NULL AND EXISTS(SELECT 1 FROM settings WHERE id='exclusive-workstreams:'||project_name AND value->>'enabled'='true') THEN
  PERFORM pg_advisory_xact_lock(4280001);
  IF EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
   WHERE r.id<>NEW.id AND r.state IN ('queued','running','cancelling','reconciling') AND g.project=project_name
    AND COALESCE(t.execution->>'baseWorkspace',t.execution->>'workspace')=base) THEN
   RAISE EXCEPTION 'WORKSTREAM_BUSY: %',base;
  END IF;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS agentos_workstream_admission ON runs;
CREATE TRIGGER agentos_workstream_admission BEFORE INSERT OR UPDATE OF state ON runs
 FOR EACH ROW EXECUTE FUNCTION agentos_workstream_admission();
`);
}
