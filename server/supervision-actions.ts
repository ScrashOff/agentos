import { z } from 'zod';
export const actionSchema = z.object({
  caseId: z.uuid(),
  kind: z.enum(['revise', 'delegate', 'wait_for_evidence']),
  diagnosis: z.string().min(20).max(3000),
  instruction: z.string().min(40).max(8000),
  route: z.object({
    accountId: z.string().min(1).max(180).regex(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/),
    model: z.string().min(1).max(180).regex(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/),
  }).strict().optional(),
  validationActionId: z.uuid().optional(),
  lane: z.enum(['glm', 'muse']).optional(),
  sourceTaskId: z.uuid().optional(),
  initialAdmission: z.object({phaseEventId:z.string().regex(/^[1-9][0-9]{0,19}$/),contractDigest:z.string().regex(/^[a-f0-9]{64}$/)}).strict().optional(),
  deliveryResolution: z.object({obligationId:z.uuid(),generation:z.string().regex(/^[a-f0-9]{64}$/),expectedSourceRunId:z.uuid()}).strict().optional(),
  title: z.string().min(8).max(160).optional(),
  workKind: z.enum(['diagnostic', 'repair']).optional(),
  waitingFor: z.array(z.string().min(5).max(300)).min(1).max(10).optional(),
  evidenceArtifactIds: z.array(z.uuid()).min(1).max(8).optional(),
}).refine(a=>!a.deliveryResolution||(a.kind==='revise'&&!!a.sourceTaskId),{message:'Delivery resolution requires an explicit accepted repair revision.'})
 .refine(a=>!a.initialAdmission||(a.kind==='revise'&&!a.sourceTaskId&&!a.deliveryResolution&&!a.lane&&!a.route),{message:'Initial admission requires a separate source-free read-only verification revision.'})
 .refine(a=>!a.validationActionId||(a.kind==='revise'&&!a.sourceTaskId&&!a.deliveryResolution&&!a.initialAdmission&&!a.lane&&!a.workKind),{message:'CI evidence requires a regular revise action without source, lane, initial admission or work-kind override.'})
 .refine(a=>!a.route||(a.kind==='revise'&&!a.sourceTaskId&&!a.deliveryResolution&&!a.initialAdmission&&!a.lane&&!a.workKind),{message:'Route changes require a regular revise action without source resolution, lane override, work-kind override, or initial admission.'});
export function acceptedSupervisionActions(raw: any[]) {
  const accepted: any[] = [];
  for (const item of raw.slice(0, 30)) {
    const parsed = actionSchema.safeParse(item);
    if (!parsed.success) continue;
    const a = parsed.data;
    if (a.kind === 'delegate' && (!a.lane || !a.sourceTaskId || !a.title)) continue;
    if (a.kind === 'wait_for_evidence' && (!a.waitingFor || !a.evidenceArtifactIds)) continue;
    if (!accepted.some((prior) => JSON.stringify(prior) === JSON.stringify(a))) accepted.push(a);
  }
  return accepted;
}
