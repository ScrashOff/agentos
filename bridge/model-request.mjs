import { setTimeout as delay } from 'node:timers/promises';

// A private admission refusal is not a failed provider transport. The gated
// caller handles this typed result after its permit unwinds.
export class DeferredModelRequest extends Error {
  constructor(result) { super('MODEL_REQUEST_DEFERRED'); this.result = result; }
}

// Retry only before returning a response to the runtime. A partially streamed
// tool call is never replayed by this gateway.
//
// The watchdog is idle-based: every streamed chunk re-arms it, so a long but
// progressing generation is never cut mid-stream (the previous fixed 300s
// total fetch timeout aborted such bodies and retried them from scratch,
// burning the outer gated budget). A total backstop bounds the pathological
// case; callers with their own gated deadline abort earlier through
// options.signal, which always wins and is never retried.
export async function requestModel(url, options, emit, dependencies = {}) {
  const fetcher = dependencies.fetch ?? fetch;
  const wait = dependencies.wait ?? delay;
  const idleTimeoutMs = dependencies.idleTimeoutMs ?? 300_000;
  const totalTimeoutMs = dependencies.totalTimeoutMs ?? 720_000;
  for (let attempt = 1; attempt <= 3; attempt++) {
    // Optional synchronous guard: outside transport retries, immediately before
    // fetch. A local refusal must not be caught/replayed as a network failure.
    dependencies.beforeRequest?.();
    const controller = new AbortController();
    let idleTimer, totalTimer;
    const clearTimers = () => { clearTimeout(idleTimer); clearTimeout(totalTimer); };
    const armIdle = () => {
      clearTimeout(idleTimer);
      idleTimer = setTimeout(
        () => controller.abort(new DOMException('Provider stream idle', 'TimeoutError')),
        idleTimeoutMs,
      );
      idleTimer.unref?.();
    };
    const onCallerAbort = () => controller.abort(options.signal?.reason);
    options.signal?.addEventListener?.('abort', onCallerAbort, { once: true });
    if (options.signal?.aborted) onCallerAbort();
    totalTimer = setTimeout(
      () => controller.abort(new DOMException('Provider request timed out', 'TimeoutError')),
      totalTimeoutMs,
    );
    totalTimer.unref?.();
    armIdle();
    const detach = () => options.signal?.removeEventListener?.('abort', onCallerAbort);
    try {
      const response = await fetcher(url, { ...options, signal: controller.signal });
      if (![408, 500, 502, 503, 504].includes(response.status) || attempt === 3) {
        if (!response.body) { clearTimers(); detach(); return response; }
        const reader = response.body.getReader();
        controller.signal.addEventListener('abort', () => reader.cancel(controller.signal.reason).catch(() => {}), { once: true });
        const body = new ReadableStream({
          async pull(ctrl) {
            try {
              const { done, value } = await reader.read();
              if (done) {
                clearTimers(); detach();
                // Reader cancellation resolves reads as done; an aborted
                // watchdog/caller must surface as an error, never clean EOF.
                if (controller.signal.aborted) ctrl.error(controller.signal.reason);
                else ctrl.close();
                return;
              }
              armIdle();
              ctrl.enqueue(value);
            } catch (error) { clearTimers(); detach(); ctrl.error(error); }
          },
          cancel(reason) { clearTimers(); detach(); return reader.cancel(reason); },
        });
        return new Response(body, response);
      }
      clearTimers(); detach();
      await response.body?.cancel();
      emit('MODEL_RETRY', { attempt, reason: 'HTTP_' + response.status });
    } catch (error) {
      clearTimers(); detach();
      if (options.signal?.aborted || attempt === 3) throw error;
      emit('MODEL_RETRY', {
        attempt,
        reason: error.name === 'TimeoutError' ? 'MODEL_REQUEST_TIMEOUT' : 'MODEL_TRANSPORT_ERROR',
      });
    }
    await wait(1000 * attempt, undefined, { signal: options.signal });
  }
}
