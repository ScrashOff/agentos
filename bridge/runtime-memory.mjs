// This envelope is supplied by the authenticated controller, never the model.
// The controller owns the PostgreSQL reservation; the bridge cannot invent one.
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
export function openCodeMemory(params) {
  const value = params.memoryReservation;
  if (value?.version !== 1 || value.state !== 'reserved' || !uuid.test(value.id) ||
      value.runId !== params.runId || value.workspace !== params.workspace ||
      !uuid.test(value.runFence) || !uuid.test(value.launchToken) ||
      ![1800,3072].includes(value.memoryMiB) || !Number.isFinite(Date.parse(value.reservedAt)) ||
      (value.memoryMiB === 3072 && (!uuid.test(value.oomSource?.runId) ||
        !value.oomSource?.terminationEventId || !value.oomSource?.stopEventId)))
    throw Object.assign(Error('RUNTIME_MEMORY_RESERVATION_INVALID'),{code:'RUNTIME_MEMORY_RESERVATION_INVALID'});
  return { memoryMiB: value.memoryMiB, memoryMax: 'MemoryMax='+value.memoryMiB+'M',
    slotMemory: String(value.memoryMiB), minAvailableKiB: (value.memoryMiB+512)*1024,
    reservationId: value.id };
}

// Legacy Harness callers keep their existing limits. An elevated limit is
// accepted only through the authenticated controller's exact launch envelope.
// The reservation is charged at 1800 even when the legacy code profile uses 1400.
export function harnessMemory(params) {
  const legacyMiB = params.profile === 'code' ? 1400 : 1800;
  if (params.memoryReservation === undefined)
    return { memoryMiB: legacyMiB, memoryMax: 'MemoryMax='+legacyMiB+'M',
      slotMemory: String(legacyMiB), minAvailableKiB: 1800000 };
  const value = params.memoryReservation;
  const validated = openCodeMemory(params);
  if (value.runtime !== 'harness' || !uuid.test(params.runFence) || !uuid.test(params.launchToken) ||
      value.runFence !== params.runFence || value.launchToken !== params.launchToken ||
      (value.memoryMiB === 3072 && value.oomSource.runId === params.runId))
    throw Object.assign(Error('RUNTIME_MEMORY_RESERVATION_INVALID'), {code:'RUNTIME_MEMORY_RESERVATION_INVALID'});
  if (validated.memoryMiB === 3072) return validated;
  return { ...validated, memoryMiB: legacyMiB, memoryMax: 'MemoryMax='+legacyMiB+'M',
    slotMemory: String(legacyMiB), minAvailableKiB: 1800000 };
}
