export interface ObservedAction {
  id: string;
  sequence: number;
  tool: string;
  command: string;
  startedAt: string | null;
  finishedAt: string | null;
  output: string;
  exitCode: number | null;
  state: 'running' | 'error' | 'returned';
}
export function redact(text: unknown, limit?: number): string;
export function parseTrace(text: string): { actions: ObservedAction[]; toolCalls: number };
export function observeRun(runId: string): Promise<Record<string, unknown>>;
export function changedFiles(diff: string, untracked: string): Array<{path:string;added:string|null;removed:string|null;status:string}>;
