import { createServer } from 'node:http';

const fail = (code) => Object.assign(new Error(code), { code });
const excludedHeaders = new Set([
  'authorization',
  'proxy-authorization',
  'cookie',
  'x-api-key',
  'host',
  'connection',
  'content-length',
  'transfer-encoding',
  'upgrade',
  'keep-alive',
  'te',
  'trailer',
]);

// These are the only provider operations exposed to the isolated native CLI.
export function museRoute(method, requestPath, baseUrl) {
  if (
    !['/muse-code/models', '/v1/responses'].includes(requestPath) ||
    (requestPath === '/muse-code/models' ? method !== 'GET' : method !== 'POST')
  )
    throw fail('INVALID_ROUTE');
  const base = new URL(baseUrl);
  if (base.protocol !== 'https:' || base.username || base.password || base.search || base.hash)
    throw fail('ACCOUNT_CONFIGURATION');
  return requestPath === '/muse-code/models'
    ? base.origin + requestPath
    : base.origin + base.pathname.replace(/\/$/, '') + requestPath.slice(3);
}

export function museHeaders(incoming, key) {
  const headers = {};
  for (const [name, value] of Object.entries(incoming))
    if (!excludedHeaders.has(name.toLowerCase()) && typeof value === 'string')
      headers[name] = value;
  headers.authorization = `Bearer ${key}`;
  return headers;
}

// The final native event, not an arbitrary JSON-looking draft, certifies a turn.
export class MuseEvents {
  constructor(sessionId) {
    this.sessionId = sessionId;
    this.started = false;
    this.terminal = null;
    this.toolCalls = 0;
    this.commandId = null;
  }
  accept(item) {
    if (
      item?.schema_version !== 1 ||
      item?.stream?.kind !== 'session' ||
      item.stream.id !== this.sessionId
    )
      return null;
    const type = item.payload_type;
    const payload = item.payload ?? {};
    if (
      type === 'runtime.command.accepted' &&
      payload.command_kind === 'turn.submit' &&
      !this.commandId
    )
      this.commandId = payload.command_id;
    if (this.commandId && payload.command_id !== this.commandId) return null;
    if (type === 'run.lifecycle.started') {
      this.commandId ??= payload.command_id;
      this.started = true;
      this.terminal = null;
    }
    if (type?.startsWith('run.terminal.') && type === `run.terminal.${payload.terminal}`)
      this.terminal = payload;
    if (
      type?.startsWith('task.lifecycle.') &&
      /tool|shell|command|filesystem|browser/.test(
        payload.event?.operation ?? payload.event?.task_kind ?? '',
      )
    ) {
      this.toolCalls++;
      return {
        type,
        toolEvents: this.toolCalls,
        operation: String(payload.event.operation ?? payload.event.task_kind).slice(0, 120),
      };
    }
    return null;
  }
  finalText() {
    if (
      !this.started ||
      this.terminal?.terminal !== 'completed' ||
      typeof this.terminal.text !== 'string'
    )
      throw fail('INCOMPLETE_TURN');
    return this.terminal.text;
  }
}

export function createMuseGateway({
  credential,
  model,
  accountId,
  signal,
  emit = () => {},
  onFailure = () => {},
  fetchImpl = fetch,
}) {
  if (!credential?.key || !credential?.baseUrl) throw fail('ACCOUNT_CONFIGURATION');
  museRoute('GET', '/muse-code/models', credential.baseUrl);
  const stats = {
    requests: 0,
    responses: 0,
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: null,
      reported: false,
    },
  };
  const server = createServer(async (req, res) => {
    const disconnected = new AbortController();
    res.once('close', () => disconnected.abort());
    const requestSignal = AbortSignal.any([
      signal,
      disconnected.signal,
      AbortSignal.timeout(300000),
    ]);
    let route,
      isResponse = false;
    try {
      route = museRoute(req.method, req.url, credential.baseUrl);
      isResponse = req.url === '/v1/responses';
      let body;
      if (isResponse) {
        const chunks = [];
        let size = 0;
        for await (const chunk of req) {
          size += chunk.length;
          if (size > 4 * 1024 * 1024) throw fail('CONTEXT_TOO_LARGE');
          chunks.push(chunk);
        }
        body = Buffer.concat(chunks);
        let payload;
        try {
          payload = JSON.parse(body.toString('utf8'));
        } catch {
          throw fail('INVALID_REQUEST');
        }
        if (payload.model !== model) throw fail('INVALID_ROUTE');
        requestSignal.throwIfAborted();
        stats.requests++;
        emit('MODEL_REQUEST', { requests: stats.requests, contextBytes: size, accountId, model });
      }
      const response = await fetchImpl(route, {
        method: req.method,
        headers: museHeaders(req.headers, credential.key),
        ...(body ? { body } : {}),
        redirect: 'error',
        signal: requestSignal,
      });
      if (!response.ok) {
        await response.body?.cancel();
        throw fail(
          [401, 403].includes(response.status)
            ? 'AUTH_REQUIRED'
            : [402, 429].includes(response.status)
              ? 'QUOTA_EXCEEDED'
              : 'MODEL_REQUEST_FAILED',
        );
      }
      res.writeHead(200, {
        'content-type': response.headers.get('content-type') ?? 'application/json',
        'cache-control': 'no-store',
      });
      const decoder = new TextDecoder();
      let pending = '',
        streamFailure;
      let responseUsage,
        completionObserved = false;
      const frame = (line) => {
        if (!isResponse || !line.startsWith('data:')) return;
        try {
          const value = JSON.parse(line.slice(5).trim());
          if (value.type === 'response.completed') {
            completionObserved = true;
            responseUsage = value.response?.usage;
          }
          if (['response.failed', 'error'].includes(value.type))
            streamFailure = 'MODEL_REQUEST_FAILED';
          if (value.type === 'response.incomplete') streamFailure = 'MODEL_CONTEXT_LIMIT';
        } catch {
          /* Non-JSON heartbeat is not a completed response. */
        }
      };
      for await (const chunk of response.body) {
        pending += decoder.decode(chunk, { stream: true });
        let boundary;
        while ((boundary = pending.indexOf('\n')) >= 0) {
          frame(pending.slice(0, boundary));
          pending = pending.slice(boundary + 1);
        }
        if (pending.length > 4 * 1024 * 1024) throw fail('MODEL_RESPONSE_TOO_LARGE');
        if (!res.destroyed) res.write(chunk);
      }
      pending += decoder.decode();
      if (pending) frame(pending);
      if (streamFailure) throw fail(streamFailure);
      if (isResponse) {
        if (!completionObserved) throw fail('MODEL_INCOMPLETE_RESPONSE');
        stats.responses++;
        if (responseUsage) {
          const number = (v) => (Number.isFinite(v) && v >= 0 ? v : 0);
          stats.usage.input += number(responseUsage.input_tokens);
          stats.usage.output += number(responseUsage.output_tokens);
          stats.usage.cacheRead += number(responseUsage.input_tokens_details?.cached_tokens);
          stats.usage.totalTokens += number(responseUsage.total_tokens);
          stats.usage.reported = true;
        }
        emit('MODEL_RESPONSE', {
          requests: stats.requests,
          stopReason: 'completed',
          tokens: responseUsage?.total_tokens ?? null,
        });
      }
      res.end();
    } catch (e) {
      const allowed = [
        'INVALID_ROUTE',
        'INVALID_REQUEST',
        'CONTEXT_TOO_LARGE',
        'AUTH_REQUIRED',
        'QUOTA_EXCEEDED',
        'MODEL_REQUEST_FAILED',
        'MODEL_CONTEXT_LIMIT',
        'MODEL_INCOMPLETE_RESPONSE',
        'MODEL_RESPONSE_TOO_LARGE',
      ];
      const code = signal.aborted
        ? 'CANCELLED'
        : e.name === 'TimeoutError'
          ? 'MODEL_TIMEOUT'
          : allowed.includes(e.code)
            ? e.code
            : 'MODEL_REQUEST_FAILED';
      // Never forward provider bodies, headers, thrown messages or credentials.
      if (!res.headersSent)
        res.writeHead(code === 'INVALID_ROUTE' ? 404 : 502, { 'content-type': 'application/json' });
      if (!res.destroyed) res.end(JSON.stringify({ error: { code, message: code } }));
      if (code !== 'INVALID_ROUTE' && code !== 'INVALID_REQUEST') {
        emit('MODEL_ERROR', { code });
        onFailure(fail(code));
      }
    }
  });
  return { server, stats };
}
