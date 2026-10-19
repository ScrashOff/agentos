export function pendingProviderBackoff(
  state: unknown,
  identity: { runId: string; workspace: string; model: string },
  now?: number,
): { notBefore: string; seconds: number } | null;
