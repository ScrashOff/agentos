import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join,dirname } from 'node:path';
import { pathToFileURL,fileURLToPath } from 'node:url';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import { canonicalToolCallIdHash, projectHarnessToolFailure, rememberHarnessToolCall } from '../bridge/runtime-tool-failure.mjs';

// Run in the same read-only dependency namespace as real workers. Package
// manifests contain installed absolute links; the host application stays private.
const packageRoot = '/opt/deepseek-harness/app/node_modules/@deepseek-ai';
const load = (relative) => import(pathToFileURL(join(packageRoot, relative)).href);

async function actualReadContract() {
  const [{ Context }, { SystemPrompt }, { LocalFileSystem }, { ToolRuntime }, fsTool, llm] = await Promise.all([
    load('cordis/lib/index.js'),
    load('dsh-system-prompt/lib/index.js'),
    load('dsh-fs-local/lib/index.js'),
    load('dsh-tools/lib/index.js'),
    load('dsh-tool-fs/lib/index.js'),
    load('dsh-llm/lib/index.js'),
  ]);
  const directory = await mkdtemp(join(tmpdir(), 'agentos-dsh-read-contract-'));
  try {
    const ctx = new Context();
    new SystemPrompt(ctx, {});
    new LocalFileSystem(ctx, { cwd: directory, diffBasisMaxBytes: 1024 * 1024 });
    new ToolRuntime(ctx);
    fsTool.apply(ctx, {
      readLimit: 20,
      readMaxLineLength: 2000,
      readMaxBytes: 50000,
      readStreamMinSize: 10 * 1024 * 1024,
    });

    const callId = llm.ToolCallId('call_missing_read_contract');
    const args = { file_path: 'answer.json' };
    const result = await ctx.tools.execute({
      callId,
      name: 'read',
      arguments: args,
      signal: new AbortController().signal,
    });
    assert.equal(result.isError, true);
    assert.equal(result.error.info.name, 'FsError');
    assert.equal(result.error.info.code, 'FS_NOT_FOUND');

    // The same SDK factory used by dsh-agent-loop creates message.source.callId.
    const message = llm.createToolResultMessage({ callId, content: result.content, isError: result.isError });
    assert.equal(message.source.callId, callId);
    assert.equal(message.content[0].isError, true);
    const event = {
      type: 'tool/result',
      seq: 2,
      time: Date.now(),
      data: { turn: 1, step: 1, message, error: result.error.info },
    };
    const calls = new Map();
    rememberHarnessToolCall({ type: 'tool/call', data: { callId, name: 'read' } }, calls);
    const projected = projectHarnessToolFailure(event, {
      goalId: '11111111-1111-4111-8111-111111111111',
      taskId: '22222222-2222-4222-8222-222222222222',
      runId: '33333333-3333-4333-8333-333333333333',
      runFence: '44444444-4444-4444-8444-444444444444',
      contextVersion: '55555555-5555-4555-8555-555555555555',
    }, calls);
    assert.equal(projected.category, 'missing_resource');
    assert.equal(projected.toolName, 'read');
    assert.equal(projected.callId, canonicalToolCallIdHash('call_missing_read_contract'));
    assert.equal(JSON.stringify(projected).includes('call_missing_read_contract'), false);
    assert.equal(JSON.stringify(projected).includes(directory), false);
    assert.equal(JSON.stringify(projected).includes('FS_NOT_FOUND'), false);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

if(process.env.AGENTOS_DSH_CONTRACT_INNER==='1'){
  await actualReadContract();console.log('ACTUAL_DSH_READ_CONTRACT_PASSED');
}else test('installed DSH read of a missing temporary file produces a correlated failure event', {timeout:15000},async () => {
  const app=dirname(dirname(fileURLToPath(import.meta.url))),nodeRoot=dirname(dirname(process.execPath));
  const args=['--unshare-all','--die-with-parent','--clearenv','--new-session',
    '--ro-bind','/usr','/usr','--symlink','usr/bin','/bin','--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64',
    '--proc','/proc','--dev','/dev','--tmpfs','/tmp','--dir','/etc','--ro-bind','/etc/ld.so.cache','/etc/ld.so.cache',
    '--ro-bind',nodeRoot,nodeRoot,'--ro-bind','/opt/agentos-runtime','/opt/agentos-runtime',
    '--ro-bind','/opt/agentos-runtime/harness-node_modules','/opt/deepseek-harness/app/node_modules',
    '--ro-bind',app,'/contract-app','--chdir','/tmp','--setenv','HOME','/tmp',
    '--setenv','AGENTOS_DSH_CONTRACT_INNER','1','--',process.execPath,'/contract-app/tests/dsh-read-tool-contract.test.mjs'];
  const {stdout}=await promisify(execFile)('/usr/bin/bwrap',args,{timeout:10000,maxBuffer:65536});
  assert.match(stdout,/ACTUAL_DSH_READ_CONTRACT_PASSED/);
});
