export function protocolReportRepairInstruction(code: string, expectedTestFile?: string): string;
export function protocolReportDiagnostic(code: string, expectedTestFile?: string): {
  artifact: string; field: string; expectedTestFile?: string; instruction: string;
} | undefined;
export function verifyProtocolReport(report: any, expectedTestFile: string): void;
