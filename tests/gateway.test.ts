import { expect, it } from 'vitest';
// @ts-expect-error Remote ESM has no generated declarations.
import { toGeneration, completion } from '../bridge/runtime-gateway.mjs';
import { taskSchema } from '../server/contracts.js';

it('preserves tool calls and results across provider turns while fixing the account route', () => {
  const value = toGeneration(
    {
      model: 'untrusted-model',
      messages: [
        { role: 'system', content: 'Scoped worker' },
        { role: 'user', content: 'Run test' },
        {
          role: 'assistant',
          tool_calls: [
            { id: 't1', function: { name: 'bash', arguments: '{"command":"node test.mjs"}' } },
          ],
        },
        { role: 'tool', tool_call_id: 't1', content: 'exit 0' },
      ],
      tools: [
        {
          type: 'function',
          function: { name: 'bash', description: 'shell', parameters: { type: 'object' } },
        },
      ],
    },
    { accountId: 'chosen', model: 'chosen-model' },
  );
  expect(value.model).toBe('chosen-model');
  expect(value.messages[1].content[0]).toMatchObject({
    type: 'toolCall',
    id: 't1',
    arguments: { command: 'node test.mjs' },
  });
  expect(value.messages[2]).toMatchObject({ role: 'toolResult', toolCallId: 't1' });
  const chunks = completion(
    {
      content: [{ type: 'toolCall', id: 't2', name: 'bash', arguments: { command: 'pwd' } }],
      usage: { input: 10, cacheRead: 5, output: 3, totalTokens: 18 },
    },
    'chosen-model',
  );
  expect(chunks[0].choices[0].delta.tool_calls[0].function.arguments).toBe('{"command":"pwd"}');
  expect(chunks[1].choices[0].finish_reason).toBe('tool_calls');
  expect(chunks[1].usage.total_tokens).toBe(18);
});
it('does not accept arbitrary host paths as execution workspaces', () => {
  const t = {
    key: 'test',
    title: 'test',
    agentId: 'qa.backend',
    criteria: [{ id: 'x', label: 'x', type: 'review' }],
  };
  for (const workspace of ['/srv/agents', '../production', 'pilot/../../prod', 'pilot/good/more'])
    expect(taskSchema.safeParse({ ...t, execution: { kind: 'harness', workspace } }).success).toBe(
      false,
    );
  expect(
    taskSchema.parse({ ...t, execution: { kind: 'harness', workspace: 'pilot/game' } }).execution
      ?.workspace,
  ).toBe('pilot/game');
});
