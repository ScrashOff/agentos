import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { agyPrintRequest, parseAgyPrintResult } from '../bridge/agy-print.mjs';

test('streams a complete large Unicode context without putting it in argv', async () => {
  const prompt = 'Memória, evidência e contexto.\n'.repeat(14000);
  const request = agyPrintRequest(prompt, 'gemini-3.8-flash-high');
  assert.ok(Buffer.byteLength(prompt) > 131072);
  assert.ok(request.args.every((arg) => Buffer.byteLength(arg) < 100));
  assert.deepEqual(request.args.slice(0, 4), ['--print', '', '--input-format', 'stream-json']);
  assert.equal(request.input.split('\n').length, 2);
  assert.equal(JSON.parse(request.input).message.content, prompt);
  // Exercise an actual pipe across process boundaries; no provider or account.
  const result = await new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [
        '--input-type=module',
        '-e',
        `let text='';process.stdin.setEncoding('utf8');process.stdin.on('data',chunk=>text+=chunk);process.stdin.on('end',()=>{const x=JSON.parse(text);process.stdout.write(JSON.stringify({event:'result',result:{status:'SUCCESS',response:x.message.content,usage:{total_tokens:1}}})+'\\n')});`,
      ],
      { stdio: ['pipe', 'pipe', 'pipe'] },
    );
    let output = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => (output += chunk));
    child.on('error', reject);
    child.stdin.on('error', reject);
    child.on('close', (code) =>
      code === 0 ? resolve(parseAgyPrintResult(output)) : reject(Error('child failed')),
    );
    child.stdin.end(request.input);
  });
  assert.equal(result.response, prompt);
});

test('reads the documented final result frame and retains actual usage', () => {
  const result = {
    status: 'SUCCESS',
    response: 'OK\n',
    usage: { input_tokens: 12001, output_tokens: 95, total_tokens: 12096 },
  };
  const frames = [
    { event: 'init', conversation_id: 'test' },
    { event: 'step_update', step_update: {} },
    { event: 'result', result },
  ];
  assert.deepEqual(
    parseAgyPrintResult(frames.map((x) => JSON.stringify(x)).join('\r\n') + '\r\n'),
    result,
  );
});

test('partial output, multiple turns and errors cannot become a successful delivery', () => {
  for (const frames of [
    [{ event: 'step_update', step_update: { text: 'partial' } }],
    [{ event: 'result', result: { status: 'ERROR', response: 'partial' } }],
    [{ event: 'result', result: { status: 'SUCCESS', response: '' } }],
    [
      { event: 'result', result: { status: 'SUCCESS', response: 'first' } },
      { event: 'result', result: { status: 'SUCCESS', response: 'second' } },
    ],
  ])
    assert.throws(
      () => parseAgyPrintResult(frames.map((x) => JSON.stringify(x)).join('\n')),
      /AGY_REQUEST_FAILED/,
    );
  assert.throws(() => parseAgyPrintResult('{broken'));
});
