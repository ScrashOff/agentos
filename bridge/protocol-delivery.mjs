import { collectArtifacts } from './artifact-collector.mjs';

// Recover a delivery manifest from real files, never synthesize test evidence.
export async function protocolDelivery(workspace, testFile) {
  if (typeof testFile !== 'string' || !/^qa-protocol\/[a-zA-Z0-9_./-]+$/.test(testFile) ||
      testFile.split('/').some(p => p === '..' || p === '.')) throw Error('INVALID_TEST_FILE');
  const artifacts = [
    { name: 'protocol-report.json', path: 'evidence/protocol-report.json', mime: 'application/json' },
    { name: 'protocol-tests.log', path: 'evidence/protocol-tests.log', mime: 'text/plain' },
  ];
  const files = await collectArtifacts({ artifacts }, workspace);
  const report = JSON.parse(Buffer.from(files[0].base64, 'base64').toString());
  if (report.suiteExecuted !== true || !Number.isInteger(report.exitCode) ||
      typeof report.command !== 'string' || !report.command.includes(testFile) ||
      !Buffer.from(files[1].base64, 'base64').toString().trim()) throw Error('DELIVERY_NOT_RECOVERABLE');
  const companion = { name: 'protocol-report.md', path: 'evidence/protocol-report.md', mime: 'text/markdown' };
  try {
    await collectArtifacts({ artifacts: [companion] }, workspace);
    artifacts.push(companion);
  } catch (e) {
    // Missing files stay missing so the original acceptance criteria still fail.
    if (e.code !== 'ARTIFACT_MISSING') throw e;
  }
  // These are existing files, never invented reports or rewritten verdicts.
  for (const item of [
    { name: testFile.split('/').at(-1), path: testFile, mime: 'text/javascript' },
    { name: 'implementation.patch', path: 'evidence/implementation.patch', mime: 'text/plain' },
    { name: 'repair-report.json', path: 'evidence/repair-report.json', mime: 'application/json' },
    { name: 'repair-tests.log', path: 'evidence/repair-tests.log', mime: 'text/plain' },
  ]) {
    try { await collectArtifacts({ artifacts: [item] }, workspace); artifacts.push(item); }
    catch (error) { if (error.code !== 'ARTIFACT_MISSING') throw error; }
  }
  return { summary: `Suíte recuperada dos arquivos preservados; código de saída ${report.exitCode}. Aceite do jogo continua dependente das validações.`, artifacts };
}
