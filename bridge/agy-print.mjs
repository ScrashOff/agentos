// AGY's plain --print argument hits Linux MAX_ARG_STRLEN for large context
// packets. Its supported streaming input keeps the entire prompt on stdin.
export function agyPrintRequest(prompt, model) {
  return {
    args: [
      '--print',
      '',
      '--input-format',
      'stream-json',
      '--model',
      model,
      '--mode',
      'plan',
      '--output-format',
      'stream-json',
      '--print-timeout',
      '180s',
      '--disable-slash-commands',
    ],
    input: JSON.stringify({ event: 'user', message: { role: 'user', content: prompt } }) + '\n',
  };
}

export function parseAgyPrintResult(output) {
  const results = output
    .split(/\r?\n/)
    .filter((line) => line.trim())
    .map((line) => JSON.parse(line))
    .filter((frame) => frame.event === 'result')
    .map((frame) => frame.result);
  if (
    results.length !== 1 ||
    results[0]?.status !== 'SUCCESS' ||
    typeof results[0].response !== 'string' ||
    !results[0].response
  )
    throw Error('AGY_REQUEST_FAILED');
  return results[0];
}
