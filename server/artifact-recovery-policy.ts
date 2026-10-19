import type { DB } from './db.js';

// The DB handoff also protects deployments whose scheduler is already running
// with an older retry loop. Collection failures belong to the collector first.
export async function installArtifactRecoveryPolicy(db: DB) {
  await db.query(`
CREATE OR REPLACE FUNCTION agentos_artifact_recovery() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_run runs%ROWTYPE; project_name text; automatic text;
BEGIN
 IF NEW.state <> 'failed' OR NOT EXISTS(SELECT 1 FROM settings WHERE id='artifact-recovery' AND value->>'enabled'='true') THEN RETURN NEW; END IF;
 SELECT g.project INTO project_name FROM goals g WHERE g.id=NEW.goal_id;
 SELECT value->>'enabled' INTO automatic FROM settings WHERE id='autonomy:'||project_name;
 IF automatic='false' OR (NEW.recovery->>'mode'='manual' AND automatic IS DISTINCT FROM 'true') THEN RETURN NEW; END IF;
 SELECT * INTO current_run FROM runs WHERE task_id=NEW.id ORDER BY attempt DESC LIMIT 1;
 IF current_run.state='failed' AND current_run.runtime IN('harness','opencode') AND current_run.error_code IN
 ('ARTIFACT_PATH','ARTIFACT_NAME','ARTIFACT_NAME_COLLISION','ARTIFACT_MISSING','RESULT_FORMAT','RESERVED_ARTIFACT_NAME') THEN
  NEW.state := 'recovering_artifacts';
  NEW.reason := 'Recuperando arquivos da entrega sem repetir a execução.';
  NEW.execution := jsonb_set(NEW.execution,'{continuation}',jsonb_build_object(
    'runId',current_run.id,'state','collecting','message',NEW.reason,'updatedAt',now()));
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS agentos_artifact_recovery ON tasks;
CREATE TRIGGER agentos_artifact_recovery BEFORE UPDATE OF state ON tasks FOR EACH ROW EXECUTE FUNCTION agentos_artifact_recovery();
`);
}
