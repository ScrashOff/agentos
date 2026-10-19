import { parseSchemaPreparationOptions, schemaPreparationPgOptions,
  assertSchemaPreparationTimeouts } from './schema-preparation-options.js';
// Set bounded PostgreSQL waits before importing db.ts / constructing its pool.
// No application service, worker or provider runtime is started by this command.
let pool: import('pg').Pool | undefined;
try {
  const options = parseSchemaPreparationOptions(process.argv.slice(2));
  process.env.PGOPTIONS = schemaPreparationPgOptions(options);
  ({ pool } = await import('../server/db.js'));
  const { prepareSchema, migrationSource } = await import('../server/schema-readiness.js');
  const { prepareBacklogSchema, backlogMigrationCompatibility } = await import('../server/backlog-schema-readiness.js');
  const { installTaskExecutionMetadata } = await import('../server/task-execution-metadata.js');
  const { installTaskPanelMetadata } = await import('../server/task-panel-metadata.js');
  const { installRunCriteriaMetadata } = await import('../server/run-criteria-metadata.js');
  const timeouts = (await pool.query(`SELECT current_setting('lock_timeout') lock_timeout,
    current_setting('statement_timeout') statement_timeout`)).rows[0];
  assertSchemaPreparationTimeouts(options, timeouts);
  const source = await migrationSource();
  if (options.sameSourceAs && (await migrationSource(options.sameSourceAs)).fingerprint !== source.fingerprint)
    throw Object.assign(Error('MIGRATION_COMPATIBILITY_UNPROVEN'), { code: 'MIGRATION_COMPATIBILITY_UNPROVEN' });
  if (options.sameSourceAs && (await backlogMigrationCompatibility(options.sameSourceAs)).compatibilityFingerprint !==
    (await backlogMigrationCompatibility()).compatibilityFingerprint)
    throw Object.assign(Error('BACKLOG_MIGRATION_COMPATIBILITY_UNPROVEN'), { code: 'BACKLOG_MIGRATION_COMPATIBILITY_UNPROVEN' });
  const receipt = await prepareSchema();
  const runCriteria = await installRunCriteriaMetadata();
  await installTaskExecutionMetadata();
  const backlog = await prepareBacklogSchema();
  const panel = await installTaskPanelMetadata();
  console.log(JSON.stringify({ ok: true, ...receipt, backlog, panel, runCriteria,
    sameSourceAsVerified: options.sameSourceAs !== undefined,
    lockWaitMs: options.lockWaitMs, maintenanceLockWait: options.maintenanceLockWait }));
} catch (error: any) {
  // Never print DB URLs, raw SQL or provider/environment data.
  const code = /^[A-Z0-9_]{4,80}$/.test(error?.code ?? '') ? error.code : 'MIGRATION_PREPARE_FAILED';
  console.error(JSON.stringify({ ok: false, code }));
  process.exitCode = 1;
} finally {
  await pool?.end();
}
