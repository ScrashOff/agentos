import { createHash, randomUUID } from 'node:crypto';
import { readdir, lstat, readFile, realpath } from 'node:fs/promises';
import path from 'node:path';
import { pool } from './db.js';

/** Exactly the publication digest: paths and bytes, including docs/dist; never
 * follow symlinks or sign mutable dependency/test directories. Runtime
 * bytecode caches (__pycache__) are generated inside the release tree and
 * must never change the digest of an already published release. */
export async function workerSourceDigest(root: string) {
  const files: string[] = [];
  const visit = async (directory: string) => {
    for (const name of await readdir(directory)) {
      if (['node_modules', '.local-test', '.git', '__pycache__'].includes(name)) continue;
      const filename = path.join(directory, name),
        info = await lstat(filename);
      if (info.isSymbolicLink()) continue;
      if (info.isDirectory()) await visit(filename);
      else if (info.isFile()) files.push(filename);
    }
  };
  await visit(root);
  const digest = createHash('sha256');
  for (const filename of files.sort()) {
    digest.update(path.relative(root, filename));
    digest.update(await readFile(filename));
  }
  return digest.digest('hex');
}
export type WorkerReadiness = {
  service: string;
  pid: number;
  candidate: string;
  digest: string;
  startupId: string;
  readyAt: string;
  locks: Array<{ key: number; backendPid: number }>;
};
/** Call only after migrations, queue setup and exclusive owner acquisition.
 * Development checkouts don't claim readiness for a published release. */
export async function recordWorkerReadiness(
  service: string,
  lockKeys: number[] = [],
): Promise<WorkerReadiness | null> {
  const candidate = await realpath(process.cwd());
  if (!/^\/opt\/agentos\/releases\/managed-[0-9a-f-]{36}$/.test(candidate)) return null;
  const locks = (
    await pool.query(
      `SELECT objid::int key,pid AS "backendPid" FROM pg_locks
    WHERE locktype='advisory' AND classid=0 AND objsubid=1 AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
      AND objid=ANY($1::oid[]) AND granted`,
      [lockKeys],
    )
  ).rows;
  if (lockKeys.some((key) => !locks.some((row) => row.key === key)))
    throw Error('WORKER_READINESS_OWNER_MISSING');
  const value: WorkerReadiness = {
    service,
    pid: process.pid,
    candidate,
    digest: await workerSourceDigest(candidate),
    startupId: randomUUID(),
    readyAt: new Date().toISOString(),
    locks,
  };
  await pool.query(
    'INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    ['worker-readiness:' + service, JSON.stringify(value)],
  );
  return value;
}
