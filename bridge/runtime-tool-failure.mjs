import { createHash } from 'node:crypto';

const safeName = (value) => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,80}$/.test(value)
  ? value : 'unknown';
const normalizedCodes = new Set(['FS_NOT_FOUND', 'FS_NOT_REGULAR_FILE']);
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function validIdentity(identity) {
  return identity !== null && typeof identity === 'object' &&
    ['goalId', 'taskId', 'runId', 'runFence', 'contextVersion'].every((field) => uuid.test(identity[field]));
}

// ToolCallId is branded but deliberately opaque: hash UTF-8 bytes as-is, with
// a strict size bound and no normalization so distinct provider ids stay distinct.
export function canonicalToolCallIdHash(value) {
  if (typeof value !== 'string' || value.length === 0 || Buffer.byteLength(value, 'utf8') > 512) return null;
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return null;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) return null;
  }
  return `call_${createHash('sha256').update(value, 'utf8').digest('hex')}`;
}

function failureDigest(category, toolName, cause) {
  return createHash('sha256')
    .update(`harness-tool-failure:v1\n${category}\n${toolName}\n${cause}`)
    .digest('hex');
}

// DSH tool/result data contains the full message, including arbitrary tool output.
// This projection intentionally reads only the structured error identity.
export function projectHarnessToolFailure(event, identity, toolNames = new Map()) {
  if (!validIdentity(identity) || event?.type !== 'tool/result') return null;
  const data = event.data;
  const err = data?.error;
  const rawCallId = data?.message?.source?.callId;
  const callIdHash = canonicalToolCallIdHash(rawCallId);
  if (!callIdHash) return null;

  // DSH ToolResultBlock is {type:'tool-result',toolCallId,content,isError?}.
  // Match exactly one block by id; never inspect its content/output text.
  const blocks = data?.message?.content;
  if (!Array.isArray(blocks)) return null;
  const matching = blocks.filter((block) => block?.type === 'tool-result' && block.toolCallId === rawCallId);
  if (matching.length !== 1 || matching[0].isError !== true) return null;

  const code = err && typeof err === 'object' && typeof err.code === 'string' ? err.code : '';
  const category = code === 'FS_NOT_FOUND' ? 'missing_resource' : 'tool_error';
  const toolName = safeName(toolNames.get(callIdHash));
  const normalizedCode = normalizedCodes.has(code) ? code : 'OTHER';
  const semanticCauseDigest = failureDigest(category, toolName, normalizedCode);
  return {
    goalId: identity.goalId,
    taskId: identity.taskId,
    runId: identity.runId,
    runFence: identity.runFence,
    contextVersion: identity.contextVersion,
    callId: callIdHash,
    toolName,
    category,
    semanticCauseDigest,
  };
}

export function rememberHarnessToolCall(event, toolNames, limit = 128) {
  if (event?.type !== 'tool/call') return;
  const { callId, name } = event.data ?? {};
  const callIdHash = canonicalToolCallIdHash(callId);
  if (!callIdHash) return;
  toolNames.delete(callIdHash);
  toolNames.set(callIdHash, safeName(name));
  while (toolNames.size > limit) toolNames.delete(toolNames.keys().next().value);
}

export function forgetHarnessToolCall(event, toolNames) {
  if (event?.type !== 'tool/result') return;
  const callIdHash = canonicalToolCallIdHash(event.data?.message?.source?.callId);
  if (callIdHash) toolNames.delete(callIdHash);
}
