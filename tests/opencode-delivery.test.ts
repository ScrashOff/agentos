import { expect, it } from 'vitest';
import { hash } from '../server/memory.js';
import { verifyOpenCodeDelivery, verifyGenericNativeDelivery } from '../server/opencode-delivery.js';
import { randomUUID } from 'node:crypto';

const testFile = 'qa-protocol/wild-bandito.test.mjs';
const patch = `--- /dev/null\n+++ b/${testFile}\n@@ -0,0 +1,2 @@\n+import { test } from 'node:test';\n+test('duplicate round', () => { throw Error('regression found'); });\n`;
function artifact(name: string, content: string) {
  return {
    name,
    base64: Buffer.from(content).toString('base64'),
    digest: hash(content),
    mime: 'text/plain',
  };
}
function delivery(overrides: Record<string, string> = {}) {
  const contents = {
    'candidate.json': JSON.stringify({
      baseDigest: hash('base'),
      candidateDigest: hash('candidate'),
      files: [
        { path: 'server.mjs', sha256: hash('unchanged source') },
        { path: testFile, sha256: hash('test source') },
      ],
    }),
    'changes.patch': patch,
    'protocol-report.json': JSON.stringify({
      suiteExecuted: true,
      exitCode: 1,
      command: `node --test ${testFile}`,
    }),
    'protocol-report.md': 'Regression exposed; game is not accepted.',
    'protocol-tests.log': 'not ok 1 - duplicate round\n# tests 1\n# fail 1\n',
    ...overrides,
  };
  return { artifacts: Object.entries(contents).map(([name, content]) => artifact(name, content)) };
}

it('generic current-run delivery preserves failing evidence and accepts a legitimately empty source delta', () => {
  const runId = randomUUID();
  const result = { ...delivery({ 'changes.patch': '', 'verification.json': '{"passed":false}' }),
    manifest: { version: 1, runId, digest: hash('manifest') } };
  const files = verifyGenericNativeDelivery(result, runId, { kind: 'opencode', workKind: 'document' });
  expect(JSON.parse(files.find(f => f.name === 'verification.json')!.content.toString()).passed).toBe(false);
});

it('generic delivery cannot bypass malformed protocol assignments, planning validation or current-run identity', () => {
  const runId = randomUUID(), result = { ...delivery(), manifest: { version: 1, runId, digest: hash('manifest') } };
  for (const execution of [{ testFile: '' }, { testFile: null }, { testFile: undefined }, { workKind: 'planning' }])
    expect(() => verifyGenericNativeDelivery(result, runId, execution)).toThrow('GENERIC_DELIVERY_MANIFEST_REQUIRED');
  expect(() => verifyGenericNativeDelivery(result, randomUUID(), {})).toThrow('GENERIC_DELIVERY_MANIFEST_REQUIRED');
  expect(() => verifyGenericNativeDelivery(delivery(), runId, {})).toThrow('GENERIC_DELIVERY_MANIFEST_REQUIRED');
});

it('generic delivery does not widen source-writing paths or accept corrupted artifact bytes', () => {
  const runId = randomUUID(), manifest = { version: 1, runId, digest: hash('manifest') };
  expect(() => verifyGenericNativeDelivery({ ...delivery({ 'changes.patch': '--- a/server.mjs\n+++ b/server.mjs\n@@ -1 +1 @@\n-old\n+new\n' }), manifest }, runId, {})).toThrow('OPENCODE_DELIVERY_INVALID');
  const result = { ...delivery(), manifest };
  result.artifacts[0].base64 = Buffer.from('changed bytes').toString('base64');
  expect(() => verifyGenericNativeDelivery(result, runId, {})).toThrow('ARTIFACT_CORRUPT');
});

it('accepts delivery of a real regression suite even when its tests fail', () => {
  const result = verifyOpenCodeDelivery(delivery(), testFile);
  expect(result.find((item) => item.name === 'changes.patch')?.content.toString()).toBe(patch);
  expect(Buffer.isBuffer(result[0].content)).toBe(true);
});

it.each(['qa-protocol/../../server.mjs', 'server.mjs', 'qa-protocol/..\\server.mjs'])(
  'rejects patch paths outside the assigned scope: %s',
  (path) => {
    expect(() =>
      verifyOpenCodeDelivery(
        delivery({
          'changes.patch': patch + `--- a/${path}\n+++ b/${path}\n@@ -1 +1 @@\n-old\n+new\n`,
        }),
        testFile,
      ),
    ).toThrow('OPENCODE_DELIVERY_INVALID');
  },
);

it('rejects empty patches and patches that do not change the expected test', () => {
  for (const content of ['', patch.replaceAll(testFile, 'qa-protocol/another-game.test.mjs')])
    expect(() => verifyOpenCodeDelivery(delivery({ 'changes.patch': content }), testFile)).toThrow(
      'OPENCODE_DELIVERY_INVALID',
    );
});

it('requires the expected test to exist in the candidate manifest', () => {
  expect(() =>
    verifyOpenCodeDelivery(
      delivery({
        'candidate.json': JSON.stringify({
          baseDigest: hash('base'),
          candidateDigest: hash('candidate'),
          files: [],
        }),
      }),
      testFile,
    ),
  ).toThrow('OPENCODE_DELIVERY_INVALID');
});

it('rejects corrupted artifact hashes and duplicate artifact names', () => {
  const corrupt = delivery();
  corrupt.artifacts[0].digest = hash('wrong');
  expect(() => verifyOpenCodeDelivery(corrupt, testFile)).toThrow('ARTIFACT_CORRUPT');
  const duplicate = delivery();
  duplicate.artifacts.push(duplicate.artifacts[0]);
  expect(() => verifyOpenCodeDelivery(duplicate, testFile)).toThrow('OPENCODE_DELIVERY_INVALID');
});

it('requires an executed suite report, explicit test command, exit code and nonempty log', () => {
  const cases: Record<string, string>[] = [
    {
      'protocol-report.json': JSON.stringify({
        suiteExecuted: false,
        exitCode: 0,
        command: `node --test ${testFile}`,
      }),
    },
    {
      'protocol-report.json': JSON.stringify({
        suiteExecuted: true,
        exitCode: '0',
        command: `node --test ${testFile}`,
      }),
    },
    {
      'protocol-report.json': JSON.stringify({
        suiteExecuted: true,
        exitCode: 0,
        command: 'node --test',
      }),
    },
    { 'protocol-tests.log': '  ' },
  ];
  for (const overrides of cases)
    expect(() => verifyOpenCodeDelivery(delivery(overrides), testFile)).toThrow(
      'OPENCODE_DELIVERY_INVALID',
    );
});

it('does not mistake removed or added source text for patch file headers', () => {
  const content = `--- /dev/null\n+++ b/${testFile}\n@@ -0,0 +1,2 @@\n+--- a/server.mjs\n++++ b/server.mjs\n`;
  expect(() =>
    verifyOpenCodeDelivery(delivery({ 'changes.patch': content }), testFile),
  ).not.toThrow();
});

it('identifies a missing top-level report field without exposing arbitrary report data', () => {
  const report = {
    suiteExecuted: true,
    runs: [{ command: `node --test ${testFile}`, exitCode: 0 }],
    privateText: 'PRIVATE_SENTINEL',
  };
  let error: any;
  try {
    verifyOpenCodeDelivery(delivery({ 'protocol-report.json': JSON.stringify(report) }), testFile);
  } catch (caught) {
    error = caught;
  }
  expect(error.code).toBe('OPENCODE_DELIVERY_INVALID_REPORT_EXIT_CODE');
  expect(error.diagnostic).toMatchObject({ artifact: 'protocol-report.json', field: 'EXIT_CODE', expectedTestFile: testFile });
  expect(error.diagnostic.instruction).toContain(testFile);
  expect(JSON.stringify(error)).not.toContain('PRIVATE_SENTINEL');
});
