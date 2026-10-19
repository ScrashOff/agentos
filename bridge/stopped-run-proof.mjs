/** A controller preflight can have no record; a remote preparation preflight
 * must have its exact durable receipt. Both still inspect the unit twice. */
export async function stoppedRunProof(runId, notStartedWorkspace, io) {
  let state;
  try {
    state = await io.readState(runId);
    if (notStartedWorkspace) {
      const receipt = state.notStarted;
      if (!(state.runId === runId && state.workspace === notStartedWorkspace &&
        state.runtime === 'opencode' && state.state === 'failed' && state.stopped === true &&
        state.code === 'DISK_CAPACITY' && state.requests === 0 && state.toolCalls === 0 &&
        receipt?.version === 1 && receipt.runId === runId && receipt.workspace === notStartedWorkspace &&
        receipt.runtime === 'opencode' && receipt.confirmed === true &&
        receipt.phase === 'remote_workspace_prepare' && receipt.errorCode === 'DISK_CAPACITY' &&
        receipt.remoteStartAttempted === true && receipt.workerStartAttempted === false && receipt.cancelRequested === false))
        throw Error('NOT_STARTED_RECEIPT_CONFLICT');
      state = { ...state, state: 'not_started' };
    }
  } catch (e) {
    if (e.code !== 'ENOENT' || !notStartedWorkspace) throw e;
    state = { runId, workspace: notStartedWorkspace, state: 'not_started' };
  }
  if (state.runId !== runId) throw Error('RUN_IDENTITY_MISMATCH');
  const unit = `agentos-run-${runId}`;
  const before = await io.inspectUnit(unit);
  if (!before.stopped) throw Error('STOP_UNCONFIRMED');
  const candidate = await io.inspectWorkspace(state.workspace);
  const after = await io.inspectUnit(unit);
  if (!after.stopped) throw Error('STOP_UNCONFIRMED');
  return {
    runId,
    workspace: state.workspace,
    state: state.state,
    candidate,
    process: after,
    checkedAt: after.checkedAt,
  };
}
