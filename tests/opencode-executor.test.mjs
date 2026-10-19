import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { parseResult, collectArtifacts, usageFromFrames, runOpenCode } from '../bridge/opencode-executor.mjs';

test('invalid requests are stopped preflight and broad write scopes are rejected', async () => {
  const signal = new AbortController().signal;
  await assert.rejects(runOpenCode({ runId: 'bad' }, signal), { code: 'INVALID_REQUEST', stopped: true });
  await assert.rejects(runOpenCode({ runId: '11111111-1111-1111-1111-111111111111',
    workspace: 'project/task', prompt: 'test', writePaths: ['../production/'] }, signal),
  { code: 'INVALID_WRITE_SCOPE', stopped: true });
});

test('only a structured final response can be collected', () => {
  const complete = '{"summary":"Implemented and verified","artifacts":[]}';
  assert.equal(parseResult(['working', complete]).summary, 'Implemented and verified');
  assert.throws(() => parseResult([complete, 'But then it failed']), { code: 'RESULT_FORMAT' });
  assert.throws(() => parseResult(['{"summary":"ok"}']), { code: 'RESULT_FORMAT' });
});

test('real artifacts are scoped, unique and hashed', async () => {
  const root = await mkdtemp(path.join(tmpdir(), 'opencode-worker-'));
  const outside = await mkdtemp(path.join(tmpdir(), 'opencode-outside-'));
  try {
    await writeFile(path.join(root, 'report.json'), '{"tested":true}');
    await writeFile(path.join(outside, 'private.txt'), 'outside');
    const [artifact] = await collectArtifacts({ artifacts: [{ name: 'report.json', path: 'report.json' }] }, root);
    assert.equal(artifact.digest, createHash('sha256').update('{"tested":true}').digest('hex'));
    assert.equal(Buffer.from(artifact.base64, 'base64').toString(), '{"tested":true}');
    await assert.rejects(collectArtifacts({ artifacts: [{ name: 'bad', path: path.join(outside, 'private.txt') }] }, root), { code: 'ARTIFACT_PATH' });
    await assert.rejects(collectArtifacts({ artifacts: [
      { name: 'report', path: 'report.json' }, { name: 'report', path: 'report.json' },
    ] }, root), { code: 'ARTIFACT_NAME_COLLISION' });
    if (process.platform !== 'win32') {
      await symlink(path.join(outside, 'private.txt'), path.join(root, 'escape'));
      await assert.rejects(collectArtifacts({ artifacts: [{ name: 'bad', path: 'escape' }] }, root), { code: 'ARTIFACT_PATH' });
    }
  } finally {
    await rm(root, { recursive: true, force: true });
    await rm(outside, { recursive: true, force: true });
  }
});

test('stream usage stays unknown until the provider reports it, and cumulative usage is not double counted', () => {
  const usage = { input: 0, output: 0, cacheRead: 0, totalTokens: 0, reported: false };
  usageFromFrames('{"choices":[{"delta":{"content":"hello"}}]}', usage);
  assert.equal(usage.reported, false);
  const frame = JSON.stringify({ usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15, prompt_tokens_details: { cached_tokens: 3 } } });
  usageFromFrames(frame, usage);
  usageFromFrames(frame, usage);
  usageFromFrames('[DONE]', usage);
  assert.deepEqual(usage, { input: 10, output: 5, cacheRead: 3, totalTokens: 15, reported: true });
});
