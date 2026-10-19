import { createHash } from 'node:crypto';
import { managementReportExcerpt, redactManagementText } from './management-evidence.js';
import { focusManagerTasks } from './manager-focus.js';

export const managerTaskIntentCharacterLimit = 12000;
export const managerTaskIntentDefaultCharacterLimit = 1000;

/** A reference to exact persisted bytes, never a claim that an excerpt contains
 * every requirement. Attempt and digest are checked again by the task reader. */
export function managerTaskInstructionReference(task:any) {
  const description=String(task.description??'');
  const digest=createHash('sha256').update(description).digest('hex');
  const validIdentity=typeof task.id==='string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(task.id) &&
    Number.isSafeInteger(task.attempt_count) && task.attempt_count>=0;
  const uuid=(value:any)=>typeof value==='string'&&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
  const live=task.guidance_available===true&&task.has_live_run===true&&task.last_run_state==='running'&&
    Number.isSafeInteger(task.attempt_count)&&task.attempt_count>0&&task.last_run_attempt===task.attempt_count&&
    uuid(task.goal_id)&&uuid(task.last_run_id)&&uuid(task.run_fence)&&uuid(task.context_version);
  const base={digest,totalCharacters:description.length,revisionId:task.instruction_revision_id??null};
  if(live)return {...base,liveInstructionReference:{source:'run.instruction_snapshot.task.description',
    request:{kind:'live_task_instruction' as const,goalId:task.goal_id,taskId:task.id,
      expectedAttempt:task.attempt_count,runId:task.last_run_id,runFence:task.run_fence,
      contextVersion:task.context_version,offset:0}}};
  return {...base,...(validIdentity?{request:{kind:'task' as const,taskId:task.id,
    expectedAttempt:task.attempt_count,offset:0,snapshotDigest:digest}}:{})};
}

/** Current decision contracts do not depend on the rotating discovery window.
 * Reuse the existing graph selection; a changed graph/state is a new decision.
 * Only instruction excerpts are budgeted. Acceptance criteria remain exact. */
export function projectManagerTaskContracts(tasks:any[], consumers:{taskId:string}[], focus:any) {
  const closed=new Set(['accepted','cancelled','superseded']);
  const discovery=new Set(consumers.map(consumer=>consumer.taskId));
  const graph=focus?.taskId?focusManagerTasks(tasks.map(task=>({
    id:task.id,state:task.state,baseWorkspace:task.execution?.baseWorkspace,
    dependencies:task.dependencies??[],
  })),focus).filter(task=>!closed.has(task.state) && !task.selection?.detailOmitted):[];
  const required=new Set(graph.map(task=>task.id));
  const focalBudget=Math.min(managerTaskIntentDefaultCharacterLimit,
    Math.floor(managerTaskIntentCharacterLimit/Math.max(1,required.size)));
  const discoveryBudget=Math.floor(managerTaskIntentCharacterLimit/Math.max(1,discovery.size));
  return new Map(tasks.filter(task=>!closed.has(task.state) && (required.has(task.id)||discovery.has(task.id)))
    .map(task=>[task.id,{criteria:task.criteria??[],
      intent:projectManagerTaskIntent(task,required.has(task.id)?focalBudget:discoveryBudget)}]));
}

/** Planning data from the exact persisted instruction, never a replacement permission policy. */
export function projectManagerTaskIntent(task: any, maxCharacters = managerTaskIntentDefaultCharacterLimit) {
  const description = String(task.description ?? '');
  const omitted = new Set<string>();
  const sections = description.split(
    /\r?\n(?:\r?\n)?(?:REQUIREMENTS|CONSTRAINTS|WRITE SCOPE|STAGE):/,
  );
  const originalInstruction = sections[0];
  const instruction = redactManagementText(originalInstruction).slice(0, 260);
  if (instruction.length < originalInstruction.length) omitted.add('/instruction');
  const requirementLine = description.match(/(?:^|\n)REQUIREMENTS:\s*([^\n]+)/)?.[1];
  let requirements: any[] = [];
  if (requirementLine) {
    try {
      const parsed = JSON.parse(requirementLine);
      if (!Array.isArray(parsed)) throw Error('NOT_ARRAY');
      requirements = parsed
        .filter(
          (entry: any) =>
            typeof entry?.id === 'string' &&
            typeof entry?.description === 'string' &&
            (!task.covers?.length || task.covers.includes(entry.id)),
        )
        .map((entry: any) => ({ id: entry.id, description: entry.description }));
      if (requirements.length !== parsed.length) omitted.add('/requirements');
    } catch {
      omitted.add('/requirements');
    }
  }
  const currentStart = description.lastIndexOf('\nSUPERVISOR:');
  const currentInstruction =
    currentStart >= 0
      ? redactManagementText(description.slice(currentStart + 1)).slice(0, 180)
      : undefined;
  if (currentInstruction && currentInstruction.length < description.length - currentStart - 1)
    omitted.add('/currentInstruction');
  // Other sections can contain CURRENT, unstructured requirements. Keep them
  // available through the exact instruction reference; do not classify them as
  // history or let this excerpt supersede their authority.
  if (sections.length > 1) omitted.add('/otherInstructionSections');
  const source:any=managerTaskInstructionReference(task);
  const metadata = {
    descriptionHash: source.digest,
    originalCharacters: source.totalCharacters,
    revisionId: source.revisionId,
    dataOnly: true,
    representation: 'instruction_excerpt',
    ...(source.request?{recovery:source.request}:{}),
    ...(source.liveInstructionReference?{liveInstructionReference:source.liveInstructionReference}:{}),
  };
  let budget = Math.max(2, maxCharacters - JSON.stringify(metadata).length - 180);
  let excerpt = managementReportExcerpt(
    { requirements, instruction, ...(currentInstruction ? { currentInstruction } : {}) },
    budget,
  );
  let result: any;
  do {
    const paths = [...new Set([...omitted, ...excerpt.projection.omittedPaths])].slice(0, 4);
    result = {
      ...metadata,
      text: excerpt.text,
      truncated: omitted.size > 0 || excerpt.truncated,
      omittedPaths: paths,
    };
    if (JSON.stringify(result).length <= maxCharacters || budget <= 2) break;
    budget = Math.max(2, budget - (JSON.stringify(result).length - maxCharacters) - 10);
    excerpt = managementReportExcerpt(
      { requirements, instruction, ...(currentInstruction ? { currentInstruction } : {}) },
      budget,
    );
  } while (true);
  return result;
}
