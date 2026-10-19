import { expect, it } from 'vitest';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdtemp, writeFile, readFile, unlink, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { verifyRepairSourceProof } from '../server/repair-proof.js';
import { criterionSchema, taskCriterionSchema } from '../server/contracts.js';
import { glmRepairCriteria } from '../server/glm-repair.js';

const sha = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const patch = Buffer.from('--- a/module.js\n+++ b/module.js\n@@ -1 +1 @@\n-before\n+after\n');
const hashes = [{ path: 'module.js', before: sha('before\n'), after: sha('after\n') }];
const report = (sourceHashes: unknown = hashes, extra: any = {}) => Buffer.from(JSON.stringify({ fixVerified: true, sourceHashes, ...extra }));

it('checks unified patch structure and returns hashes of actual bytes without claiming applicability or QA', () => {
  const bytes = report(), proof = verifyRepairSourceProof(patch, bytes);
  expect(proof).toEqual({ version: 1, patchSha256: sha(patch), reportSha256: sha(bytes),
    files: ['module.js'], applicabilityVerified: false, qaVerified: false });
  // Truth of pre/postimages belongs to composition against the candidate, not
  // this structural check: a syntactically valid hash is never called applied.
  expect(verifyRepairSourceProof(patch, report([{ ...hashes[0], after: 'a'.repeat(64) }])).applicabilityVerified).toBe(false);
});

it('accepts real git text diffs for addition, deletion, multiple hunks and no-final-newline markers', async () => {
  const root = await mkdtemp(path.join(tmpdir(), 'agentos-repair-proof-'));
  const git = (...args: string[]) => execFileSync('git', ['-c', 'core.autocrlf=false', '-c', 'core.hooksPath=/dev/null', ...args], {
    cwd: root, env: { ...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null' },
  });
  try {
    const oldLines = Array.from({ length: 40 }, (_, i) => 'line-' + i);
    const old = oldLines.join('\n') + '\n', changedLines = [...oldLines];
    changedLines.splice(2, 0, 'inserted'); changedLines[32] = 'changed';
    const changed = changedLines.join('\n') + '\n';
    const before = { 'module.js': old, 'removed.js': 'removed\n', 'no-newline.js': 'old last line' };
    git('init', '--quiet');
    for (const [name, bytes] of Object.entries(before)) await writeFile(path.join(root, name), bytes);
    git('add', '.');
    const after = { 'module.js': changed, 'added.js': 'new file\n', 'no-newline.js': 'new last line' };
    for (const [name, bytes] of Object.entries(after)) await writeFile(path.join(root, name), bytes);
    await unlink(path.join(root, 'removed.js'));
    git('add', '--intent-to-add', 'added.js');
    const bytes = git('diff', '--no-ext-diff', '--no-color', '--no-renames');
    expect(bytes.toString()).toContain('new file mode 100644');
    expect(bytes.toString()).toContain('deleted file mode 100644');
    expect(bytes.toString()).toContain('\\ No newline at end of file');
    const names = ['added.js', 'module.js', 'no-newline.js', 'removed.js'];
    const proof = verifyRepairSourceProof(bytes, report(names.map(name => ({ path: name,
      before: name in before ? sha(before[name as keyof typeof before]) : null,
      after: name in after ? sha(after[name as keyof typeof after]) : null,
    }))));
    expect(proof.files).toEqual(names);
    expect(await readFile(path.join(root, 'module.js'), 'utf8')).toBe(changed);
  } finally { await rm(root, { recursive: true, force: true }); }
});

it.each([undefined, null, {}, { 'module.js': hashes[0] }, [], [hashes[0], hashes[0]],
  [{ ...hashes[0], path: 'other.js' }], [{ ...hashes[0], before: null }],
  [{ ...hashes[0], after: null }], [{ ...hashes[0], before: 'old-hash' }],
  [{ ...hashes[0], after: hashes[0].before }], [{ ...hashes[0], before: 'A'.repeat(64) }]])(
  'rejects missing, malformed or mismatched source hash coverage %#', sourceHashes => {
    const bytes = Buffer.from(JSON.stringify({ fixVerified: true, sourceHashes }));
    expect(() => verifyRepairSourceProof(patch, bytes)).toThrow('REPAIR_SOURCE_PROOF_COVERAGE');
  },
);

it('does not accept self-declared proof flags or duplicate JSON keys', () => {
  expect(() => verifyRepairSourceProof(patch, Buffer.from('{"fixVerified":true,"sourceProofVerified":true}'))).toThrow('COVERAGE');
  expect(() => verifyRepairSourceProof(patch, Buffer.from('{"fixVerified":false,"fixVerified":true,"sourceHashes":[]}'))).toThrow('JSON_DUPLICATE_KEY');
  expect(() => verifyRepairSourceProof(patch, Buffer.from('{"fixVerified":true,"sourceHashes":[{"path":"a","p\\u0061th":"b"}]}'))).toThrow('JSON_DUPLICATE_KEY');
});

it.each(['../outside', 'assets/file.js', 'rules/rtp.js', '.git/config', '.env', '.ssh/key', 'src/../outside'])(
  'rejects unsafe patch path %s', name => {
    const bytes = Buffer.from(patch.toString().replaceAll('module.js', name));
    expect(() => verifyRepairSourceProof(bytes, report([{ ...hashes[0], path: name }]))).toThrow('PATCH_PATH');
  },
);

it.each([
  patch.toString() + patch.toString(),
  patch.toString().replace('+++ b/module.js', '+++ b/other.js'),
  patch.toString().replace('@@ -1 +1 @@', '@@ -1,2 +1 @@'),
  patch.toString().replace('@@ -1 +1 @@', '@@ -1 +2 @@'),
  patch.toString() + '+unaccounted\n',
  patch.toString().slice(0, -1),
  'diff --git a/module.js b/module.js\nGIT binary patch\nliteral 0\n',
  'diff --git a/module.js b/module.js\nold mode 100644\nnew mode 100755\n' + patch,
  'diff --git a/module.js b/module.js\nnew file mode 120000\n' + patch,
  'diff --git a/module.js b/module.js\nrename from module.js\nrename to other.js\n' + patch,
])('rejects ambiguous, truncated or unsupported diff syntax %#', value => {
  expect(() => verifyRepairSourceProof(Buffer.from(value), report())).toThrow('REPAIR_SOURCE_PROOF_');
});

it('rejects a duplicate source path even with the same list length as two distinct diff files', () => {
  const bytes = Buffer.concat([patch, Buffer.from(patch.toString().replaceAll('module.js', 'second.js'))]);
  expect(() => verifyRepairSourceProof(bytes, report([hashes[0], hashes[0]]))).toThrow('COVERAGE');
});

it('requires coherent null hashes for text additions and removals', () => {
  const added = Buffer.from('--- /dev/null\n+++ b/new.js\n@@ -0,0 +1 @@\n+new\n');
  const removed = Buffer.from('--- a/old.js\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\n');
  expect(verifyRepairSourceProof(added, report([{ path: 'new.js', before: null, after: sha('new\n') }])).files).toEqual(['new.js']);
  expect(verifyRepairSourceProof(removed, report([{ path: 'old.js', before: sha('old\n'), after: null }])).files).toEqual(['old.js']);
  expect(() => verifyRepairSourceProof(added, report([{ path: 'new.js', before: sha('old\n'), after: sha('new\n') }]))).toThrow('COVERAGE');
});

it('binds the new criterion version only to task validators that implement it', () => {
  const criterion = glmRepairCriteria.find(c => c.type === 'repair_source_proof')!;
  expect(taskCriterionSchema.safeParse(criterion).success).toBe(true);
  expect(taskCriterionSchema.safeParse({ ...criterion, version: 2 }).success).toBe(false);
  expect(criterionSchema.safeParse(criterion).success).toBe(false);
});
