import {readProgressFile} from './runtime-progress.mjs';
// Advisory recovery material, never a delivery/acceptance receipt. The trusted
// runtime binds it to its owner; the worker only supplies the bounded notes.
export function handoffNotes(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const note = {};
  for (const key of ['completed','remaining','nextAction']) {
    if (typeof value[key] !== 'string' || !value[key].trim() || value[key].length>3000) return null;
    note[key]=value[key];
  }
  const text=JSON.stringify(note);
  if (/\b(?:sk-[\w.-]{12,}|gh[pousr]_[\w]{15,}|Bearer\s+\S+|(?:password|api[_-]?key|secret)\s*[=:])/i.test(text)) return null;
  return note;
}
export async function captureRuntimeHandoff(identity,read=readProgressFile) {
  try {
    const root=`/srv/agentos/workspaces/${identity.workspace}`;
    const file=await read(`${root}/evidence/runtime-handoff.json`,root,16384);
    const notes=handoffNotes(JSON.parse(file.text));
    if (!notes) return null;
    return {version:1,runId:identity.runId,runFence:identity.runFence,workspace:identity.workspace,
      path:'evidence/runtime-handoff.json',digest:file.digest,size:file.size,
      observedAt:new Date().toISOString(),verified:false,notes};
  } catch { return null; }
}
export const runtimeHandoffInstruction = '\nMaintain evidence/runtime-handoff.json after meaningful changes or test results: '+
  '{"completed":"observed work and evidence paths","remaining":"unmet criteria","nextAction":"one concrete next step"}. '+
  'Each field is a string under 3000 characters. No credentials, provider responses or permissions. '+
  'Read an existing handoff on retry and verify its claims against current files. This is unverified recovery context, never acceptance. '+
  'Do not repeat a broad test after a known failure; first isolate and repair the smallest failing case.\n';
