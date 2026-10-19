export type UnitExit = { source: 'systemd-journal'; unit: string; result: 'oom-kill' };
export function unitExitFromJournal(unit: string, text: string): UnitExit | null;
export function inspectUnitExit(unit: string, execute?: (...args: any[]) => Promise<{stdout: string}>): Promise<UnitExit | null>;
