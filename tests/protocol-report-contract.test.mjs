import test from 'node:test';
import assert from 'node:assert/strict';
import { verifyProtocolReport, protocolReportRepairInstruction, protocolReportDiagnostic } from '../bridge/protocol-report-contract.mjs';
const file='qa-protocol/actual.test.mjs';
test('preserves negative test results and reports the exact missing contract field', () => {
  assert.doesNotThrow(()=>verifyProtocolReport({suiteExecuted:true,command:'node --test '+file,exitCode:1},file));
  const cases=[
    [null,'JSON'],
    [{suiteExecuted:false},'SUITE'],
    [{suiteExecuted:true,runs:[{command:'node --test '+file,exitCode:0}]},'EXIT_CODE'],
    [{suiteExecuted:true,exitCode:0},'COMMAND'],
    [{suiteExecuted:true,exitCode:0,command:'node --test qa-protocol/other.test.mjs'},'TEST_FILE'],
  ];
  for(const [report,field] of cases) assert.throws(()=>verifyProtocolReport(report,file),error=>error.code==='OPENCODE_DELIVERY_INVALID_REPORT_'+field);
});

test('rejection and recovery expose the assigned suite without reflecting the rejected command', () => {
  const report = { suiteExecuted: true, exitCode: 1, command: 'node other.test.mjs PRIVATE_COMMAND_SENTINEL' };
  assert.throws(() => verifyProtocolReport(report, file), (error) => {
    assert.equal(error.code, 'OPENCODE_DELIVERY_INVALID_REPORT_TEST_FILE');
    assert.equal(error.diagnostic.expectedTestFile, file);
    assert.match(error.diagnostic.instruction, /nunca altere o comando registrado para simular/);
    assert.doesNotMatch(JSON.stringify(error.diagnostic), /PRIVATE_COMMAND_SENTINEL|other\.test/);
    return true;
  });
  assert.match(protocolReportRepairInstruction('OPENCODE_DELIVERY_INVALID_REPORT_TEST_FILE', file), /qa-protocol\/actual\.test\.mjs/);
  assert.match(protocolReportRepairInstruction('', file), /exitCode inteiro/);
  assert.doesNotThrow(() => verifyProtocolReport({ suiteExecuted: true, exitCode: 1, command: 'node --test '+file }, file));
});

test('diagnostics reject traversal, private paths, shell text and unknown error fields', () => {
  for (const value of ['qa-protocol/../PRIVATE_SENTINEL.mjs', 'qa-protocol//PRIVATE_SENTINEL.mjs',
    '/var/lib/PRIVATE_SENTINEL.mjs', 'qa-protocol/PRIVATE_SENTINEL\n.mjs',
    'qa-protocol/$(PRIVATE_SENTINEL).mjs', 'https://PRIVATE_SENTINEL.mjs']) {
    const diagnostic = protocolReportDiagnostic('OPENCODE_DELIVERY_INVALID_REPORT_TEST_FILE', value);
    assert.equal(diagnostic.expectedTestFile, undefined);
    assert.doesNotMatch(JSON.stringify(diagnostic), /PRIVATE_SENTINEL/);
  }
  assert.equal(protocolReportDiagnostic('OPENCODE_DELIVERY_INVALID_REPORT_PRIVATE_SENTINEL', file), undefined);
  assert.match(protocolReportRepairInstruction('OPENCODE_DELIVERY_INVALID_REPORT___proto__'), /Confira protocol-report/);
});
test('recovery instruction is fixed, actionable, and never reflects arbitrary error contents', () => {
  assert.match(protocolReportRepairInstruction('OPENCODE_DELIVERY_INVALID_REPORT_EXIT_CODE'),/exitCode.*inteiro.*principal/);
  assert.match(protocolReportRepairInstruction('OPENCODE_DELIVERY_INVALID'),/Campos aninhados em runs/);
  assert.doesNotMatch(protocolReportRepairInstruction('OPENCODE_DELIVERY_INVALID_REPORT_SECRET_SENTINEL'),/SECRET_SENTINEL/);
});
