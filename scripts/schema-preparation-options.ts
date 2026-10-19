/** Pure CLI policy: no environment reads, database imports or migration effects. */
export type SchemaPreparationOptions = {
  sameSourceAs?: string;
  maintenanceLockWait: boolean;
  lockWaitMs: 5000 | 30000;
};
const failure = (code: string) => Object.assign(new Error(code), { code });

export function parseSchemaPreparationOptions(args: readonly string[]): SchemaPreparationOptions {
  let sameSourceAs: string | undefined;
  let maintenanceLockWait = false;
  for (let index = 0; index < args.length; index++) {
    const arg = args[index];
    if (arg === '--maintenance-lock-wait' && !maintenanceLockWait) {
      maintenanceLockWait = true;
    } else if (arg === '--same-source-as' && sameSourceAs === undefined) {
      const value = args[++index];
      if (!value?.trim() || value.startsWith('-')) throw failure('MIGRATION_ARGUMENTS');
      sameSourceAs = value;
    } else {
      throw failure('MIGRATION_ARGUMENTS');
    }
  }
  return { ...(sameSourceAs === undefined ? {} : { sameSourceAs }), maintenanceLockWait,
    lockWaitMs: maintenanceLockWait ? 30000 : 5000 };
}

export function schemaPreparationPgOptions(options: SchemaPreparationOptions) {
  if (typeof options.maintenanceLockWait !== 'boolean' ||
      options.lockWaitMs !== (options.maintenanceLockWait ? 30000 : 5000))
    throw failure('MIGRATION_ARGUMENTS');
  // Replaces inherited PGOPTIONS. Statement execution remains bounded at 3min;
  // the explicit maintenance option changes only acquisition of relation locks.
  return `-c lock_timeout=${options.lockWaitMs} -c statement_timeout=180000`;
}

export function assertSchemaPreparationTimeouts(options: SchemaPreparationOptions,
  effective: { lock_timeout?: string; statement_timeout?: string }) {
  schemaPreparationPgOptions(options);
  if (effective.lock_timeout !== (options.maintenanceLockWait ? '30s' : '5s') ||
      effective.statement_timeout !== '3min')
    throw failure('MIGRATION_TIMEOUT_CONFIGURATION');
}
