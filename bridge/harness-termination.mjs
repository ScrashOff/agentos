// Typed turn/end from the pinned runtime, never model/tool output.
export function harnessTurnFailureCode(reason) {
  if (!reason?.kind) return 'MISSING_TURN_END';
  if (reason.kind === 'completed') return null;
  return reason.kind === 'max-tokens' ? 'MODEL_OUTPUT_LIMIT' : 'INCOMPLETE_TURN';
}

const unitPattern = /^agentos-run-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\.service$/;

// Called with the typed result of inspectUnitExit, never model/tool output.
// A cancellation, integrity failure or provider failure keeps its own cause.
export function harnessFailureCode(code, termination, expectedUnit) {
  return code === 'RUNTIME_EXITED' && termination?.source === 'systemd-journal' &&
    termination.result === 'oom-kill' && unitPattern.test(termination.unit) &&
    termination.unit === expectedUnit + '.service' ? 'HARNESS_SANDBOX_OOM' : code;
}
