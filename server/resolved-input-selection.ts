import type {DB} from './db.js';
import {DomainError} from './contracts.js';
import {deliveryDigest} from './delivery-supervision.js';
import {readDeliverySourceResolution} from './delivery-source-resolution.js';

const repairNames=new Set(['implementation.patch','repair-report.json']);
const fail=(subcode:string):never=>{throw Object.assign(new DomainError('INPUT_MANIFEST_CHANGED','Seleção de insumos históricos exige nova validação.'),
 {inputFailure:{phase:'resolved_input_selection',subcode}});};

/** Propose historical classification only for a completely resolved producer
 * family. The unchanged effective-source validator must subsequently prove
 * every selected substitution; this reader never authorizes one by itself. */
export async function resolvedInputSelection(db:DB,run:any,task:any,dependencies:any[]){
 const unchanged={dependencies,selection:null as any};
 if(task.stage!=='integrate'||task.execution?.readOnlySource||!task.execution?.sourceWorkspace)return unchanged;
 if(!(await db.query("SELECT to_regclass('delivery_consumptions') present")).rows[0].present)return unchanged;
 const resolved=(await db.query(`SELECT d.* FROM delivery_consumptions d WHERE d.consumer_task_id=$1
  AND EXISTS(SELECT 1 FROM events e WHERE e.kind='DELIVERY_SOURCE_RESOLVED' AND e.entity_id=d.id::text)
  ORDER BY d.id LIMIT 257`,[task.id])).rows;
 if(!resolved.length||resolved.length>256)return unchanged;
 const families=[...new Set(resolved.map(d=>d.producer_task_id))];
 const related=(await db.query(`SELECT id,producer_task_id,producer_run_id FROM delivery_consumptions
  WHERE consumer_task_id=$1 AND producer_task_id=ANY($2::uuid[]) ORDER BY id LIMIT 257`,[task.id,families])).rows;
 if(related.length>256)return unchanged;
 const resolutions=new Map<string,any>();
 for(const d of resolved){const value=await readDeliverySourceResolution(db,d);if(value)resolutions.set(d.id,value);}
 const activeTasks=new Set([...resolutions.values()].map(value=>value.replacement.producerTaskId));
 const retired=families.filter(id=>!activeTasks.has(id)&&related.some(d=>d.producer_task_id===id)&&
  related.filter(d=>d.producer_task_id===id).every(d=>resolutions.has(d.id)));
 if(!retired.length)return unchanged;
 const excluded=dependencies.filter(a=>retired.includes(a.task_id)&&repairNames.has(a.name));
 if(!excluded.length)return unchanged;
 const actual=(await db.query(`SELECT a.id,a.name,a.digest,r.task_id,t.goal_id FROM artifacts a
  JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id WHERE a.id=ANY($1::uuid[])`,[excluded.map(a=>a.id)])).rows;
 if(excluded.some(a=>!actual.some(b=>b.id===a.id&&b.name===a.name&&b.digest===a.digest&&b.task_id===a.task_id&&b.goal_id===task.goal_id)))
  fail('HISTORICAL_DEPENDENCY_UNVERIFIED');
 const covered=related.filter(d=>retired.includes(d.producer_task_id)).map(d=>({obligationId:d.id,
  originalTaskId:d.producer_task_id,originalRunId:d.producer_run_id,
  resolutionEventId:resolutions.get(d.id).eventId,effectiveTaskId:resolutions.get(d.id).replacement.producerTaskId}));
 const body={version:1,authority:'selection_only',runId:run.id,runFence:run.fence,taskId:task.id,goalId:task.goal_id,
  originalDependencyDigest:deliveryDigest(dependencies),covered,
  historicalLatestArtifacts:excluded.map(a=>({id:a.id,name:a.name,digest:a.digest,task_id:a.task_id})),
  historyPreserved:true,acceptance:false,independentQARequired:true};
 return {dependencies:dependencies.filter(a=>!excluded.some(b=>b.id===a.id)),selection:{...body,digest:deliveryDigest(body)}};
}

/** Historical classification is committed only alongside the existing full
 * frozen-source proof. Any missing/changed binding prevents context creation. */
export function assertResolvedInputSelection(selection:any,effective:any){
 if(!selection)return;
 const sources=effective?.effectiveSources;
 if(!sources||sources.runId!==selection.runId||sources.runFence!==selection.runFence||
  selection.covered.some((c:any)=>!sources.bindings?.some((b:any)=>b.obligationId===c.obligationId&&
   b.original.taskId===c.originalTaskId&&b.original.runId===c.originalRunId&&b.resolutionEventId===c.resolutionEventId&&
   b.effectiveSource.taskId===c.effectiveTaskId&&c.effectiveTaskId!==c.originalTaskId)))fail('HISTORICAL_SELECTION_UNPROVEN');
}
