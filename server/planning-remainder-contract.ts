import {createHash} from 'node:crypto';
import {z} from 'zod';

export const planningRemainderReferenceSchema=z.object({requestId:z.uuid(),runId:z.uuid(),artifactId:z.uuid(),
  artifactDigest:z.string().regex(/^[a-f0-9]{64}$/),index:z.number().int().min(0).max(11),
  proposalDigest:z.string().regex(/^[a-f0-9]{64}$/)}).strict();
export type PlanningRemainderReference=z.infer<typeof planningRemainderReferenceSchema>;
export const backlogProposalSchema=z.object({coverageKey:z.string().regex(/^[a-z0-9_-]{3,80}$/),
  title:z.string().min(8).max(180),description:z.string().min(40).max(12000),baseWorkspace:z.string().optional(),
  deliverToTaskId:z.uuid(),workKind:z.enum(['diagnostic','repair']).default('diagnostic'),sourceTaskId:z.uuid().optional(),
  lane:z.enum(['glm','muse']).default('glm'),delivery:z.enum(['immediate','deferred']).default('immediate'),
  remainingPlanItem:planningRemainderReferenceSchema.optional()});
export type BacklogProposal=z.infer<typeof backlogProposalSchema>;
/** Canonical explicit fields: JSON key order and omitted defaults are not new work. */
export function planningProposalDigest(raw:unknown){
  const p=backlogProposalSchema.parse(raw);
  return createHash('sha256').update(JSON.stringify([p.coverageKey,p.title,p.description,p.baseWorkspace??null,
    p.deliverToTaskId,p.workKind,p.sourceTaskId??null,p.lane,p.delivery])).digest('hex');
}
export const planningRemainderInstruction=`
PLANNING_REMAINDER_V1: operation.planningDemands[].remainingItems are refused items of an accepted partial plan, not requests to repeat that entire plan. Existing createdTaskIds keep their owners. Retrieve remainingItemRequest through management.contextRequests to read just that original proposal with its hash; the full scope-plan.json also remains available. Submit only the corrected item in backlog.tasks with remainingPlanItem copied exactly (requestId,runId,artifactId,artifactDigest,index,proposalDigest). Keep its coverageKey, baseWorkspace, workKind and deliverToTaskId. Choose any changed lane/source/instruction explicitly and justify it in backlog.reason; current acceptance, quota, permissions, deduplication and stop guards still apply. Creation of a new plan or unrelated task does not resolve the item. The controller links it atomically only when the authorized replacement task is actually created. Never duplicate or retry the other items, and never treat this link as delivery acceptance.
`;
