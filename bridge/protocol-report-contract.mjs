// Only fixed diagnostics leave this validator; report text never becomes an error.
const fields = {
  JSON: 'protocol-report.json precisa ser um objeto JSON válido.',
  SUITE: 'protocol-report.json: suiteExecuted deve ser true após a execução real.',
  EXIT_CODE: 'protocol-report.json: exitCode precisa ser um inteiro no nível principal, inclusive quando o teste falha.',
  COMMAND: 'protocol-report.json: command precisa conter o comando literal executado, no nível principal.',
  TEST_FILE: 'protocol-report.json: command precisa incluir o arquivo de teste atribuído.',
};
const prefix = 'OPENCODE_DELIVERY_INVALID_REPORT_';
function assignedTestFile(value) {
  if (typeof value !== 'string' || value.length > 240 ||
      !/^qa-protocol\/[a-zA-Z0-9_.\/-]+\.mjs$/.test(value) ||
      value.split('/').some((part) => !part || part === '.' || part === '..')) return undefined;
  return value;
}
export function protocolReportRepairInstruction(code, expectedTestFile) {
  const field = String(code).startsWith(prefix) ? String(code).slice(prefix.length) : '';
  const instruction = Object.hasOwn(fields, field) ? fields[field] : 'Confira protocol-report.json: suiteExecuted:true, command literal e exitCode inteiro no nível principal. Campos aninhados em runs não substituem esses campos. Preserve também todos os resultados antes/depois; nunca invente execução ou sucesso.';
  const testFile = assignedTestFile(expectedTestFile);
  return instruction + (testFile ? ` Arquivo de teste atribuído: ${testFile}. O campo command precisa registrar a execução real desse arquivo exato. Se apenas outro arquivo foi executado, execute a suíte atribuída; nunca altere o comando registrado para simular uma execução.` : '');
}
export function protocolReportDiagnostic(code, expectedTestFile) {
  const field = String(code).startsWith(prefix) ? String(code).slice(prefix.length) : '';
  if (!Object.hasOwn(fields, field)) return undefined;
  const testFile = assignedTestFile(expectedTestFile);
  return { artifact: 'protocol-report.json', field,
    ...(testFile ? { expectedTestFile: testFile } : {}),
    instruction: protocolReportRepairInstruction(code, expectedTestFile) };
}
export function verifyProtocolReport(report, expectedTestFile) {
  const reject = (field) => { throw Object.assign(Error(prefix + field), {
    code: prefix + field, diagnostic: protocolReportDiagnostic(prefix + field, expectedTestFile),
  }); };
  if (!report || typeof report !== 'object' || Array.isArray(report)) reject('JSON');
  if (report.suiteExecuted !== true) reject('SUITE');
  if (!Number.isInteger(report.exitCode)) reject('EXIT_CODE');
  if (typeof report.command !== 'string' || !report.command.trim()) reject('COMMAND');
  if (!report.command.includes(expectedTestFile)) reject('TEST_FILE');
}
