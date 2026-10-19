import type {DB} from './db.js';

/** A live upgrade first adds the generation columns/index, retaining the old
 * unique pair until every old controller has drained and adopted the release.
 * Old code must never see multiple historical cases for one source run. */
export async function supervisionGenerationReady(db:DB) {
  return !(await db.query(`SELECT 1 FROM pg_constraint c
    WHERE c.conrelid='supervision_cases'::regclass AND c.contype='u'
      AND (SELECT array_agg(a.attname::text ORDER BY a.attname)
        FROM unnest(c.conkey) k(attnum) JOIN pg_attribute a
          ON a.attrelid=c.conrelid AND a.attnum=k.attnum)
        = ARRAY['source_run_id','task_id']::text[] LIMIT 1`)).rowCount;
}
