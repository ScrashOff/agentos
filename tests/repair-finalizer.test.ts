import {it, expect} from 'vitest';
import {mkdtemp, mkdir, writeFile, readFile, rm, symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {randomUUID, createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {verifyRepairSourceProof} from '../server/repair-proof.js';
// @ts-expect-error The bridge runs native ESM, outside the TypeScript application.
import {prepareRepairFinalization, finalizeRepairArtifacts} from '../bridge/repair-finalizer.mjs';

const sha = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const artifact = (name: string, value: unknown) => {
  const bytes = Buffer.from(typeof value === 'string' ? value : JSON.stringify(value));
  return {name, mime: 'application/json', digest: sha(bytes), base64: bytes.toString('base64')};
};
async function fixture(test: (f: any) => Promise<void>) {
  const root = await mkdtemp(path.join(tmpdir(), 'agentos-repair-finalizer-'));
  try {
    await mkdir(path.join(root, 'data/repair-candidate'), {recursive: true});
    await writeFile(path.join(root, 'module.mjs'), 'export const value = 1;\n');
    await writeFile(path.join(root, 'untouched.mjs'), 'unchanged\n');
    await writeFile(path.join(root, 'data/repair-candidate/module.mjs'), 'export const value = 2;\n');
    const params = {goalId: randomUUID(), taskId: randomUUID(), runId: randomUUID(), runFence: randomUUID(), contextVersion: randomUUID(),
      managed: true, readOnlySource: true, workKind: 'repair'};
    const snapshot = await prepareRepairFinalization(params, async () => ({candidateDigest: sha('frozen source'),
      files: [{path: 'module.mjs', sha256: sha('export const value = 1;\n')}, {path: 'untouched.mjs', sha256: sha('unchanged\n')}]}));
    const events: any[] = [];
    const finalize = (artifacts: any[], extra = {}) => finalizeRepairArtifacts({params, workspace: root, snapshot, stopped: true, artifacts,
      emit: (kind: string, value: any) => events.push({kind, value}), ...extra});
    await test({root, params, snapshot, events, finalize});
  } finally {await rm(root, {recursive: true, force: true});}
}

it('generates an applicable strict patch from partial candidate, preserving worker claims and real test results', async () => fixture(async ({root, finalize, events}) => {
  const report = {fixVerified: true, changedFiles: ['module.mjs'], beforeExitCode: 1, afterExitCode: 0, limitations: ['Independent QA required']};
  const original = artifact('repair-report.json', report), claimed = artifact('implementation.patch', 'Invalid preamble\n');
  const artifacts = [original, claimed];
  expect((await finalize(artifacts)).generated).toBe(true);
  expect(artifacts.find(a => a.name === 'worker-repair-report.json')?.digest).toBe(original.digest);
  expect(artifacts.find(a => a.name === 'worker-implementation.patch')?.digest).toBe(claimed.digest);
  const patch = Buffer.from(artifacts.find(a => a.name === 'implementation.patch')!.base64, 'base64');
  const reportBytes = Buffer.from(artifacts.find(a => a.name === 'repair-report.json')!.base64, 'base64');
  const generated = JSON.parse(reportBytes.toString());
  expect(generated).toMatchObject(report);
  expect(verifyRepairSourceProof(patch, reportBytes)).toMatchObject({files: ['module.mjs'], applicabilityVerified: false, qaVerified: false});
  await writeFile(path.join(root, 'implementation.patch'), patch);
  const git = (...args: string[]) => execFileSync('/usr/bin/git', ['-c', 'core.hooksPath=/dev/null', ...args], {
    cwd: root, env: {PATH: '/usr/bin:/bin', GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null'}, stdio: 'pipe'});
  git('apply', '--check', 'implementation.patch');
  git('apply', 'implementation.patch');
  expect(await readFile(path.join(root, 'module.mjs'), 'utf8')).toBe('export const value = 2;\n');
  expect(await readFile(path.join(root, 'untouched.mjs'), 'utf8')).toBe('unchanged\n');
  expect(events[0].kind).toBe('REPAIR_ARTIFACTS_FINALIZED');
}));

it('preserves negative regression results and does not turn packaging into acceptance', async () => fixture(async ({finalize}) => {
  const artifacts = [artifact('repair-report.json', {fixVerified: false, changedFiles: ['module.mjs'], afterExitCode: 1})];
  expect((await finalize(artifacts)).generated).toBe(true);
  const bytes = Buffer.from(artifacts.find(a => a.name === 'repair-report.json')!.base64, 'base64');
  expect(JSON.parse(bytes.toString())).toMatchObject({fixVerified: false, afterExitCode: 1});
  expect(() => verifyRepairSourceProof(Buffer.from(artifacts.find(a => a.name === 'implementation.patch')!.base64, 'base64'), bytes)).toThrow('REPAIR_SOURCE_PROOF_REPORT');
}));

it.each(['stop', 'identity', 'base_changed', 'effective_source', 'missing_partial', 'duplicate_json', 'symlink', 'collision'])('preserves original evidence when finalization is unsafe: %s', async scenario => fixture(async ({root, finalize, params}) => {
  let report: any = {fixVerified: true, changedFiles: ['module.mjs']};
  const extra: any = {};
  if (scenario === 'stop') extra.stopped = false;
  if (scenario === 'identity') extra.params = {...params, runFence: randomUUID()};
  if (scenario === 'base_changed') await writeFile(path.join(root, 'module.mjs'), 'changed base\n');
  if (scenario === 'effective_source') report.sourceHashes = [{path: 'module.mjs', before: sha('another accepted source'), after: sha('corrected')}];
  if (scenario === 'missing_partial') report.changedFiles = ['untouched.mjs'];
  if (scenario === 'duplicate_json') report = '{"fixVerified":false,"fixVerified":true,"changedFiles":["module.mjs"]}';
  if (scenario === 'symlink') {await rm(path.join(root, 'data/repair-candidate/module.mjs')); await symlink(path.join(root, 'untouched.mjs'), path.join(root, 'data/repair-candidate/module.mjs'));}
  const artifacts = [artifact('repair-report.json', report), artifact('implementation.patch', 'worker patch')];
  if (scenario === 'collision') artifacts.push(artifact('worker-repair-report.json', '{}'));
  const original = structuredClone(artifacts);
  expect((await finalize(artifacts, extra)).generated).toBe(false);
  expect(artifacts).toEqual(original);
}));

it('handles explicit additions and removals without interpreting absent partial files as deletion', async () => fixture(async ({root, finalize}) => {
  await writeFile(path.join(root, 'data/repair-candidate/new.mjs'), 'new content without newline');
  const artifacts = [artifact('repair-report.json', {fixVerified: true, changedFiles: ['new.mjs', 'untouched.mjs'], removedFiles: ['untouched.mjs']})];
  expect((await finalize(artifacts)).generated).toBe(true);
  const patch = Buffer.from(artifacts.find(a => a.name === 'implementation.patch')!.base64, 'base64');
  const report = Buffer.from(artifacts.find(a => a.name === 'repair-report.json')!.base64, 'base64');
  expect(verifyRepairSourceProof(patch, report).files).toEqual(['new.mjs', 'untouched.mjs']);
  expect(JSON.parse(report.toString()).sourceHashes).toEqual([
    {path: 'new.mjs', before: null, after: sha('new content without newline')}, {path: 'untouched.mjs', before: sha('unchanged\n'), after: null}]);
}));

it('does not enable runtime finalization for an unbound or writable-source execution', async () => {
  let inspected = false;
  expect(await prepareRepairFinalization({workKind: 'repair', managed: true, readOnlySource: true}, async () => {inspected = true;})).toBeNull();
  expect(inspected).toBe(false);
});
