import { workspaceOperation } from './workspaces.mjs';

/** This function runs before gateway creation or any worker spawn. A durable
 * receipt describes only this phase; it never substitutes for stop inspection. */
export async function prepareOpenCodeWorkspace(params, { signal, emit, persist, operation = workspaceOperation }) {
  if (!params.sourceWorkspace) return;
  try {
    await operation({ action: 'prepare', workspace: params.workspace, sourceWorkspace: params.sourceWorkspace });
  } catch (error) {
    if (error.code === 'DISK_CAPACITY' && !signal.aborted) {
      const receipt = { version: 1, runId: params.runId, workspace: params.workspace,
        runtime: 'opencode', confirmed: true, remoteStartAttempted: true,
        workerStartAttempted: false, cancelRequested: false,
        phase: 'remote_workspace_prepare', errorCode: 'DISK_CAPACITY' };
      await persist(receipt);
      error.notStarted = receipt;
      await emit('OPENCODE_NOT_STARTED', receipt);
    }
    throw error;
  }
}
