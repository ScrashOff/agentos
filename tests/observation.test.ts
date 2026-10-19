import { describe, it, expect } from 'vitest';
import { parseTrace, redact, observeRun, changedFiles } from '../bridge/observe-run.mjs';

describe('read-only execution observations', () => {
  it('includes new test files without exposing credential paths or inventing line counts', () => {
    const files = changedFiles(
      '3\t1\tserver.mjs\n',
      'qa-protocol/lucky-neko.test.mjs\0.env\0auth.json\0',
    );
    expect(files).toEqual([
      { path: 'server.mjs', added: '3', removed: '1', status: 'modified' },
      { path: 'qa-protocol/lucky-neko.test.mjs', added: null, removed: null, status: 'new' },
    ]);
  });
  it('pairs actions and results without collecting reasoning or ordinary conversation', () => {
    const trace = [
      {
        type: 'assistant/message',
        data: { message: { content: [{ type: 'text', text: 'PRIVATE_REASONING' }] } },
      },
      {
        type: 'tool/call',
        seq: 1,
        time: 1000,
        data: {
          callId: 'a',
          name: 'bash',
          arguments: JSON.stringify({ command: 'node test.mjs' }),
        },
      },
      {
        type: 'tool/result',
        seq: 2,
        time: 3000,
        data: {
          message: {
            content: [
              {
                type: 'tool-result',
                toolCallId: 'a',
                isError: false,
                content: [
                  {
                    type: 'text',
                    text: 'real assertion failed\n[Command finished with exit code 1]',
                  },
                ],
              },
            ],
          },
        },
      },
      {
        type: 'tool/call',
        seq: 3,
        time: 4000,
        data: {
          callId: 'b',
          name: 'bash',
          arguments: JSON.stringify({ command: 'node browser.mjs' }),
        },
      },
    ]
      .map((x) => JSON.stringify(x))
      .join('\n');
    const result = parseTrace(trace + '\n{"partial":');
    expect(result.actions).toHaveLength(2);
    expect(result.actions[0]).toMatchObject({
      state: 'error',
      exitCode: 1,
      startedAt: '1970-01-01T00:00:01.000Z',
      finishedAt: '1970-01-01T00:00:03.000Z',
    });
    expect(result.actions[1]).toMatchObject({ state: 'running', output: '' });
    expect(JSON.stringify(result)).not.toContain('PRIVATE_REASONING');
  });
  it('omits credential commands and their outputs and redacts common secrets before truncation', () => {
    const raw =
      'sk-fakefixture123456 password=never-expose Bearer secretfixture https://user:pass@private.example';
    const safe = redact(raw);
    expect(safe).not.toMatch(/fakefixture|never-expose|secretfixture|user:pass/);
    const result = parseTrace(
      [
        {
          type: 'tool/call',
          data: { callId: 'a', name: 'bash', arguments: '{"command":"cat .env"}' },
        },
        {
          type: 'tool/result',
          data: {
            message: {
              content: [
                {
                  type: 'tool-result',
                  toolCallId: 'a',
                  content: [{ type: 'text', text: 'a secret without any marker' }],
                },
              ],
            },
          },
        },
      ]
        .map((x) => JSON.stringify(x))
        .join('\n'),
    );
    expect(JSON.stringify(result)).not.toContain('a secret without');
    expect(result.actions[0].command).toContain('omitido');
  });
  it('rejects traversal and non-run paths before reading the filesystem', async () => {
    await expect(observeRun('../../production')).rejects.toThrow('INVALID_RUN');
    await expect(observeRun('------------------------------------')).rejects.toThrow('INVALID_RUN');
  });
});
