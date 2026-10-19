import { constants as fsConstants } from 'node:fs';
import { mkdir, open } from 'node:fs/promises';
import path from 'node:path';

const HOSTS_CONTENT = Buffer.from('127.0.0.1 localhost\n::1 localhost\n', 'ascii');

/**
 * Create a private, fresh home and a static localhost-only hosts file in it.
 * The returned bwrap arguments mount that file read-only at /etc/hosts.
 * Existing homes (including symlinks) are deliberately not reused.
 */
export async function prepareSandboxLocalhost(homePath) {
  if (typeof homePath !== 'string' || !path.isAbsolute(homePath)) {
    throw new TypeError('homePath must be an absolute path');
  }

  const home = path.resolve(homePath);
  await mkdir(home, { mode: 0o700 }); // exclusive: fails if anything already exists
  const hostsPath = path.join(home, 'hosts');
  const flags = fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_EXCL |
    (fsConstants.O_NOFOLLOW ?? 0);
  const file = await open(hostsPath, flags, 0o444);
  try {
    await file.writeFile(HOSTS_CONTENT);
    await file.chmod(0o444);
    if (!(await file.stat()).isFile()) throw new Error('hosts path is not a regular file');
  } finally {
    await file.close();
  }

  return Object.freeze({
    home,
    hostsPath,
    content: Buffer.from(HOSTS_CONTENT),
    mount: Object.freeze(['--ro-bind', hostsPath, '/etc/hosts']),
  });
}
