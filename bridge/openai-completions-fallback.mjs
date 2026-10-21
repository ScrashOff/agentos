// OpenAI-compatible transport used when the pinned harness SDK is absent.
// Message shapes mirror bridge/runtime-gateway.mjs, which converts the same
// wire format in the opposite direction; the two must stay symmetric.
// Only typed availability evidence leaves this module: an upstream body never
// becomes an error message, an event or an artifact.
import { classifyProviderError } from './opencode-provider-error.mjs';

export const RETRY_ATTEMPTS = 3;
export const RETRY_BUDGET_MS = 30000;
const MAX_DELAY_MS = 15000;

const textOf = (content) =>
  typeof content === 'string'
    ? content
    : (content ?? []).filter((b) => b?.type === 'text').map((b) => b.text).join('\n');

/** Internal generation messages to OpenAI chat-completions messages. */
export function toCompletionsMessages(systemPrompt, messages) {
  const out = [];
  if (systemPrompt) out.push({ role: 'system', content: systemPrompt });
  for (const m of messages ?? []) {
    if (m.role === 'toolResult') {
      out.push({ role: 'tool', tool_call_id: m.toolCallId, content: textOf(m.content) });
      continue;
    }
    if (m.role === 'assistant') {
      const calls = (Array.isArray(m.content) ? m.content : []).filter((b) => b?.type === 'toolCall');
      const text = textOf(m.content);
      out.push({
        role: 'assistant',
        content: text || null,
        ...(calls.length
          ? {
              tool_calls: calls.map((c) => ({
                id: c.id,
                type: 'function',
                function: { name: c.name, arguments: JSON.stringify(c.arguments ?? {}) },
              })),
            }
          : {}),
      });
      continue;
    }
    out.push({ role: m.role, content: textOf(m.content) });
  }
  return out;
}

/** OpenAI chat-completions response to an internal assistant message. */
export function fromCompletionsResponse(data) {
  const choice = data?.choices?.[0];
  return buildMessage({
    text: choice?.message?.content ?? '',
    calls: (choice?.message?.tool_calls ?? []).map((c, index) => ({
      index, id: c.id, name: c.function?.name, arguments: c.function?.arguments,
    })),
    finish: choice?.finish_reason,
    usage: data?.usage,
  });
}

function buildMessage({ text, calls, finish, usage }) {
  const content = [];
  if (text) content.push({ type: 'text', text: String(text) });
  for (const call of calls) {
    let args = {};
    try { args = JSON.parse(call.arguments || '{}'); } catch {}
    content.push({ type: 'toolCall', id: call.id, name: call.name, arguments: args });
  }
  return {
    role: 'assistant',
    content,
    stopReason: content.some((b) => b.type === 'toolCall')
      ? 'toolUse'
      : finish === 'length' ? 'length' : 'stop',
    usage: usage
      ? { input: usage.prompt_tokens ?? 0, output: usage.completion_tokens ?? 0,
          totalTokens: usage.total_tokens ?? 0 }
      : null,
  };
}

/** Consume one chat-completions SSE body into the same internal message.
 * This provider streams at a few tokens per second, so a long generation that
 * is not streamed is one silent request held open for many minutes. */
export async function readCompletionsStream(body, signal) {
  const decoder = new TextDecoder();
  const calls = new Map();
  let buffer = '', text = '', finish, usage;
  for await (const part of body) {
    if (signal?.aborted) throw cancelled();
    buffer += decoder.decode(part, { stream: true });
    let cut;
    while ((cut = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, cut).trim();
      buffer = buffer.slice(cut + 1);
      if (!line.startsWith('data:')) continue;
      const payload = line.slice(5).trim();
      if (payload === '[DONE]') continue;
      let event;
      try { event = JSON.parse(payload); } catch { continue; }
      if (event.usage) usage = event.usage;
      const choice = event.choices?.[0];
      if (!choice) continue;
      if (choice.finish_reason) finish = choice.finish_reason;
      const delta = choice.delta ?? {};
      if (typeof delta.content === 'string') text += delta.content;
      for (const call of delta.tool_calls ?? []) {
        // Deltas carry the id and name once and then append arguments.
        const index = call.index ?? calls.size;
        const current = calls.get(index) ?? { index, id: undefined, name: undefined, arguments: '' };
        if (call.id) current.id = call.id;
        if (call.function?.name) current.name = call.function.name;
        if (call.function?.arguments) current.arguments += call.function.arguments;
        calls.set(index, current);
      }
    }
  }
  return buildMessage({ text, calls: [...calls.values()].sort((a, b) => a.index - b.index), finish, usage });
}

function typedFailure(failure) {
  return Object.assign(Error(failure.code), {
    code: failure.code,
    status: failure.status,
    retryAfterSeconds: failure.retryAfterSeconds ?? null,
  });
}
const cancelled = () => Object.assign(Error('CANCELLED'), { code: 'CANCELLED' });

function wait(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(cancelled());
    const onAbort = () => { clearTimeout(timer); reject(cancelled()); };
    const timer = setTimeout(() => { signal?.removeEventListener('abort', onAbort); resolve(); }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/** Honour Retry-After when present, otherwise exponential backoff with jitter.
 * A wait that would outlive the budget is refused: the permit this request
 * holds belongs to the gate, and a long provider pause is the scheduler's to
 * absorb, not this call's. */
export function backoffDelay(attempt, retryAfterSeconds, random = Math.random) {
  const header = Number.isSafeInteger(retryAfterSeconds) && retryAfterSeconds >= 0
    ? retryAfterSeconds * 1000 : null;
  // A stated wait is obeyed exactly, never shortened; one longer than the
  // budget is refused above instead of being retried too early.
  if (header !== null) return header;
  return Math.min(500 * 2 ** attempt + random() * 250, MAX_DELAY_MS);
}

export function openAICompletionsFallback({ fetch: fetchImpl = fetch, random = Math.random } = {}) {
  return {
    streamSimple(model, { systemPrompt, messages, tools }, options = {}) {
      const resultPromise = (async () => {
        // The base URL is operator input and this request carries the credential.
        const endpoint = new URL(model.baseUrl);
        if (endpoint.protocol !== 'https:' &&
            !['127.0.0.1', 'localhost', '::1'].includes(endpoint.hostname))
          throw Object.assign(Error('INSECURE_PROVIDER_URL'), { code: 'INSECURE_PROVIDER_URL' });
        const url = model.baseUrl.replace(/\/+$/, '') +
          (model.baseUrl.endsWith('/chat/completions') ? '' : '/chat/completions');
        const body = {
          model: model.id,
          messages: toCompletionsMessages(systemPrompt, messages),
          stream: true,
          max_tokens: options.maxTokens ?? model.maxTokens ?? 4096,
          ...(tools?.length ? { tools: tools.map((t) => ({ type: 'function', function: t })) } : {}),
        };
        const request = {
          method: 'POST',
          headers: { 'content-type': 'application/json', authorization: `Bearer ${options.apiKey}` },
          body: JSON.stringify(body),
          signal: options.signal,
        };
        const deadline = Date.now() + RETRY_BUDGET_MS;
        for (let attempt = 0; ; attempt++) {
          if (options.signal?.aborted) throw cancelled();
          const last = attempt >= RETRY_ATTEMPTS - 1;
          let response;
          try {
            response = await fetchImpl(url, request);
          } catch (error) {
            if (last || options.signal?.aborted) throw error;
            const delay = backoffDelay(attempt, null, random);
            if (Date.now() + delay > deadline) throw error;
            await wait(delay, options.signal);
            continue;
          }
          if (response.ok) {
            return /event-stream/i.test(response.headers.get('content-type') ?? '')
              ? await readCompletionsStream(response.body, options.signal)
              : fromCompletionsResponse(await response.json());
          }
          const failure = await classifyProviderError(response);
          // Quota and authentication are states of the account, not transient
          // congestion: retrying them wastes a permit and hides the real cause.
          const retryable = failure.code === 'RATE_LIMIT' || failure.status >= 500;
          if (!retryable || last) throw typedFailure(failure);
          const delay = backoffDelay(attempt, failure.retryAfterSeconds, random);
          if (Date.now() + delay > deadline) throw typedFailure(failure);
          await wait(delay, options.signal);
        }
      })();
      return { result: () => resultPromise };
    },
  };
}
