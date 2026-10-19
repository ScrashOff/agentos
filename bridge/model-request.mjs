import { setTimeout as delay } from 'node:timers/promises';

// A private admission refusal is not a failed provider transport. The gated
// caller handles this typed result after its permit unwinds.
export class DeferredModelRequest extends Error {
  constructor(result) { super('MODEL_REQUEST_DEFERRED'); this.result = result; }
}

// Retry only before returning a response to the runtime. A partially streamed
// tool call is never replayed by this gateway.
export async function requestModel(url, options, emit, dependencies = {}) {
  const fetcher = dependencies.fetch ?? fetch;
  const wait = dependencies.wait ?? delay;
  for (let attempt = 1; attempt <= 3; attempt++) {
    // Optional synchronous guard: outside transport retries, immediately before
    // fetch. A local refusal must not be caught/replayed as a network failure.
    dependencies.beforeRequest?.();
    try {
      const response = await fetcher(url, {
        ...options,
        signal: AbortSignal.any([options.signal, AbortSignal.timeout(300000)]),
      });
      if (![408, 500, 502, 503, 504].includes(response.status) || attempt === 3) return response;
      await response.body?.cancel();
      emit('MODEL_RETRY', { attempt, reason: 'HTTP_' + response.status });
    } catch (error) {
      if (options.signal.aborted || attempt === 3) throw error;
      emit('MODEL_RETRY', {
        attempt,
        reason: error.name === 'TimeoutError' ? 'MODEL_REQUEST_TIMEOUT' : 'MODEL_TRANSPORT_ERROR',
      });
    }
    await wait(1000 * attempt, undefined, { signal: options.signal });
  }
}
