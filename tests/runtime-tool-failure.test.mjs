import test from 'node:test';
import assert from 'node:assert/strict';
import { canonicalToolCallIdHash, projectHarnessToolFailure, rememberHarnessToolCall } from '../bridge/runtime-tool-failure.mjs';

const identity = {
  goalId: '11111111-1111-4111-8111-111111111111',
  taskId: '22222222-2222-4222-8222-222222222222',
  runId: '33333333-3333-4333-8333-333333333333',
  runFence: '44444444-4444-4444-8444-444444444444',
  contextVersion: '55555555-5555-4555-8555-555555555555',
};
const opaqueCallId = 'provider|response/segment?opaque=value:'.padEnd(91, 'x');

const result = ({ callId = 'call_abc123', isError = true, error = { name: 'FsError', code: 'FS_NOT_FOUND', reason: 'secret path detail' }, contentText = 'FS_NOT_FOUND: private /home/agent/data/secret.txt', blocks } = {}) => ({
  type: 'tool/result',
  seq: 12,
  time: 1700000000000,
  data: {
    turn: 2,
    step: 1,
    message: {
      id: 'tool-result-message',
      role: 'user',
      source: { kind: 'tool', callId },
      content: blocks ?? [{ type: 'tool-result', toolCallId: callId, isError,
        content: [{ type: 'text', text: contentText }] }],
    },
    ...(error ? { error } : {}),
    meta: { private: 'must never enter telemetry' },
  },
});

test('projects the actual DSH tool/result failure into bound, redacted telemetry', () => {
  const calls = new Map();
  rememberHarnessToolCall({ type: 'tool/call', data: {
    turn: 2, step: 1, callId: opaqueCallId, name: 'read_file',
    arguments: '{"path":"/home/agent/data/secret.txt"}',
  } }, calls);
  const projected = projectHarnessToolFailure(result({callId:opaqueCallId}), identity, calls);
  assert.deepEqual(Object.keys(projected).sort(), [
    'callId', 'category', 'contextVersion', 'goalId', 'runFence', 'runId',
    'semanticCauseDigest', 'taskId', 'toolName',
  ].sort());
  assert.deepEqual(projected, {
    goalId: identity.goalId,
    taskId: identity.taskId,
    runId: identity.runId,
    runFence: identity.runFence,
    contextVersion: identity.contextVersion,
    callId: canonicalToolCallIdHash(opaqueCallId),
    toolName: 'read_file',
    category: 'missing_resource',
    semanticCauseDigest: projected.semanticCauseDigest,
  });
  assert.match(projected.semanticCauseDigest, /^[a-f0-9]{64}$/);
  assert.equal(JSON.stringify(projected).includes('secret'), false);
  assert.equal(JSON.stringify(projected).includes('FS_NOT_FOUND'), false);
  assert.equal(JSON.stringify(projected).includes(opaqueCallId), false);
  assert.equal(calls.has(opaqueCallId), false);

  const secondRawId=opaqueCallId+'other';
  const secondCalls=new Map();
  rememberHarnessToolCall({type:'tool/call',data:{callId:secondRawId,name:'read_file'}},secondCalls);
  const second=projectHarnessToolFailure(result({callId:secondRawId}),identity,secondCalls);
  assert.notEqual(projected.callId,second.callId);
  assert.equal(projected.semanticCauseDigest,second.semanticCauseDigest,'same normalized cause deduplicates across provider call ids');
});

test('ignores successful results and failure-looking output with isError false', () => {
  assert.equal(projectHarnessToolFailure(result({ isError: false, error: null }), identity), null);
  assert.equal(projectHarnessToolFailure(result({ isError: false, error: null, contentText: 'ENOENT: not authoritative' }), identity), null);
  assert.equal(projectHarnessToolFailure(result(), null), null);
  assert.equal(projectHarnessToolFailure({ type: 'assistant/message', data: result().data }, identity), null);
});

test('requires the complete host-bound UUID identity before projecting a failure', () => {
  const calls=new Map();rememberHarnessToolCall({type:'tool/call',data:{callId:'call_uuid_guard',name:'read'}},calls);
  const event=result({callId:'call_uuid_guard'});
  for(const field of ['goalId','taskId','runId','runFence','contextVersion']){
    assert.equal(projectHarnessToolFailure(event,{...identity,[field]:'not-a-uuid'},calls),null,`${field} must be a UUID`);
  }
  assert.equal(projectHarnessToolFailure(event,{...identity,runFence:null},calls),null);
  assert.notEqual(projectHarnessToolFailure(event,identity,calls),null);
});

test('bash exitCode 1 is a normal tool result; read FS_NOT_FOUND is a thrown failure', () => {
  const calls = new Map();
  rememberHarnessToolCall({ type: 'tool/call', data: { callId: 'call_shell1', name: 'bash' } }, calls);
  const shellExit = projectHarnessToolFailure(result({ callId: 'call_shell1', isError: false, error: null,
    contentText: '{"kind":"foreground","exitCode":1}' }), identity, calls);
  assert.equal(shellExit, null);

  rememberHarnessToolCall({ type: 'tool/call', data: { callId: 'call_read1', name: 'read' } }, calls);
  const readFailure = projectHarnessToolFailure(result({ callId: 'call_read1', error: {
    name: 'FsError', code: 'FS_NOT_FOUND', reason: 'private path omitted',
  } }), identity, calls);
  assert.equal(readFailure.toolName, 'read');
  assert.equal(readFailure.category, 'missing_resource');
});

test('emits generic tool_error from isError true even when DSH omits optional error metadata', () => {
  const calls = new Map();
  rememberHarnessToolCall({ type: 'tool/call', data: { callId: 'call_abc123', name: 'bash' } }, calls);
  const payload = projectHarnessToolFailure(result({ error: null, contentText: 'ENOENT /secret/token=do-not-read' }), identity, calls);
  assert.equal(payload.category, 'tool_error');
  assert.equal(payload.toolName, 'bash');
  assert.equal(JSON.stringify(payload).includes('FS_NOT_FOUND'), false);
  assert.equal(JSON.stringify(payload).includes('secret'), false);
});

test('uses only bounded identifiers and a generic category for unknown codes', () => {
  const calls = new Map();
  rememberHarnessToolCall({ type: 'tool/call', data: { callId: 'call_abc123', name: 'tool\nwith-secret' } }, calls);
  const payload = projectHarnessToolFailure(result({ error: { name: 'CustomSecretError', code: 'CUSTOM_SECRET_VALUE' } }), identity, calls);
  assert.equal(payload.toolName, 'unknown');
  assert.equal(payload.category, 'tool_error');
  assert.equal(JSON.stringify(payload).includes('CUSTOM_SECRET'), false);
  assert.equal(JSON.stringify(payload).includes('CustomSecret'), false);
});

test('correlates parallel tool results by callId and ignores missing or ambiguous blocks', () => {
  const calls = new Map();
  rememberHarnessToolCall({ type: 'tool/call', data: { callId: 'call_first', name: 'read_file' } }, calls);
  rememberHarnessToolCall({ type: 'tool/call', data: { callId: 'call_second', name: 'bash' } }, calls);
  const firstPayload = projectHarnessToolFailure(result({ callId: 'call_first', error: null }), identity, calls);
  const secondPayload = projectHarnessToolFailure(result({ callId: 'call_second', error: null }), identity, calls);
  assert.equal(firstPayload.toolName, 'read_file');
  assert.equal(firstPayload.callId, canonicalToolCallIdHash('call_first'));
  assert.equal(secondPayload.toolName, 'bash');
  assert.equal(secondPayload.callId, canonicalToolCallIdHash('call_second'));
  assert.notEqual(firstPayload.semanticCauseDigest, secondPayload.semanticCauseDigest);

  assert.equal(projectHarnessToolFailure(result({ blocks: [] }), identity, calls), null);
  assert.equal(projectHarnessToolFailure(result({ blocks: [
    { type: 'tool-result', toolCallId: 'call_someone_else', isError: true, content: [] },
  ] }), identity, calls), null);
  assert.equal(projectHarnessToolFailure(result({ blocks: [
    { type: 'tool-result', toolCallId: 'call_abc123', isError: true, content: [] },
    { type: 'tool-result', toolCallId: 'call_abc123', isError: true, content: [] },
  ] }), identity, calls), null);
});

test('bounds the in-memory call correlation map', () => {
  const calls = new Map();
  for (let i = 0; i < 130; i++) {
    rememberHarnessToolCall({ type: 'tool/call', data: { callId: `call_${i}`, name: 'bash' } }, calls);
  }
  assert.equal(calls.size, 128);
  assert.equal(calls.has(canonicalToolCallIdHash('call_0')), false);
  assert.equal(calls.has(canonicalToolCallIdHash('call_129')), true);
});

test('canonical call identity hashes opaque UTF-8 ids and rejects oversized or malformed UTF-16', () => {
  const hash=canonicalToolCallIdHash(opaqueCallId);
  assert.match(hash,/^call_[a-f0-9]{64}$/);
  assert.equal(hash,canonicalToolCallIdHash(opaqueCallId));
  assert.notEqual(hash,canonicalToolCallIdHash(opaqueCallId+'x'));
  assert.match(canonicalToolCallIdHash('call with spaces / separators'),/^call_[a-f0-9]{64}$/);
  const composed='id:'+String.fromCodePoint(0xe9),decomposed='id:e'+String.fromCodePoint(0x301);
  assert.match(canonicalToolCallIdHash(composed),/^call_[a-f0-9]{64}$/);
  assert.notEqual(canonicalToolCallIdHash(composed),canonicalToolCallIdHash(decomposed));
  assert.equal(canonicalToolCallIdHash(''),null);
  assert.equal(canonicalToolCallIdHash('x'.repeat(513)),null);
  assert.equal(canonicalToolCallIdHash('\ud800'),null);
  assert.equal(canonicalToolCallIdHash('\udc00'),null);
  assert.match(canonicalToolCallIdHash('valid unicode'),/^call_[a-f0-9]{64}$/);
});
