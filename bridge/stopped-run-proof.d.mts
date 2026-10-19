export function stoppedRunProof(
  runId: string,
  notStartedWorkspace: string | undefined,
  io: {
    readState: (id: string) => Promise<any>;
    inspectUnit: (unit: string) => Promise<any>;
    inspectWorkspace: (workspace: string) => Promise<any>;
  },
): Promise<any>;
