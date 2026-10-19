// Protocol fixture only: never calls a model or runs project commands.
import { createInterface } from 'node:readline';
const mode = process.argv[2] ?? 'success';
const emit = (value) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', ...value }) + '\n');
createInterface({ input: process.stdin }).on('line', (line) => {
  const m = JSON.parse(line);
  if (m.method === 'initialize')
    emit({
      id: m.id,
      result: { serverInfo: { name: 'deepseek-harness-sdk-runtime', version: 'test-fixture' } },
    });
  if (m.method === 'session/prompt') {
    const sessionId = m.params.sessionId;
    if (mode === 'hang') {
      emit({ id: m.id, result: { messageId: 'receipt-1' } });
      return;
    }
    // Deliberately emit events before the request response to exercise ordering.
    emit({ method: 'session.status', params: { sessionId, status: 'running' } });
    emit({
      method: 'session.event',
      params: {
        sessionId,
        event: { type: 'agent/inbox/spliced', data: { inserted: [{ id: 'receipt-1' }] } },
      },
    });
    const output =
      mode === 'invalid'
        ? 'invalid'
        : JSON.stringify({
            summary: 'Protocol fixture delivered',
            artifacts: [{ name: 'result.json', content: '{"validated":true}' }],
          });
    emit({
      method: 'session.event',
      params: {
        sessionId,
        event: {
          type: 'assistant/message',
          data: { message: { content: [{ type: 'text', text: output }] } },
        },
      },
    });
    emit({ method: 'session.status', params: { sessionId, status: 'idle' } });
    emit({ id: m.id, result: { messageId: 'receipt-1' } });
  }
});
