import {createHash} from 'node:crypto';
import type {DB} from './db.js';

const hex=(value:any):value is string=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const uuid=(value:any):value is string=>typeof value==='string'&&/^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value);
export const deliveryResolutionGenerationGuide='deliveryResolution.generation must equal sourceConflictGeneration (receipt.sourceConflict.generation), never consumptionGeneration (receipt.generation). deliveryResolutionBinding contains only obligationId and that source-conflict generation. Set expectedSourceRunId from the selected currently accepted frozen repair, not the original producer run. Re-read before acting; this context is not authorization.';
/** Distinct identities: the consumption lifecycle hash is not the source-conflict
 * binding. Never invent a successor source run from the original producer. */
export function deliveryGenerationContext(row:any) {
 const consumptionGeneration=hex(row?.receipt?.generation)?row.receipt.generation:null;
 const sourceConflictGeneration=hex(row?.receipt?.sourceConflict?.generation)?row.receipt.sourceConflict.generation:null;
 return {consumptionGeneration,sourceConflictGeneration,
  deliveryResolutionBinding:uuid(row?.id)&&sourceConflictGeneration?
   {obligationId:row.id,generation:sourceConflictGeneration}:null};
}
const sha=(value:string)=>createHash('sha256').update(value).digest('hex');
const fields=`c.id,c.goal_id,c.handoff_id,c.producer_task_id,c.producer_run_id,c.consumer_task_id,c.scope,
 c.consumer_contract_digest,c.producer_manifest_digest,c.state,c.receipt,c.candidate_run_id,c.candidate_digest,
 c.qa_task_id,c.qa_run_id,c.created_at,c.updated_at`;
export type DeliveryCatalogQuery={scope?:string;excludeScope?:string;consumerTaskId?:string;obligationId?:string;
 activeOnly?:boolean;offset?:number;limit?:number;expectedDigest?:string};

/** Operational timestamps are not part of the paged semantic record. Keeping
 * them out prevents unchanged reconciliation from mixing text pages. */
export function deliveryConsumptionRecord(row:any) {
  const {updated_at:_updated,total:_total,truncated:_truncated,...record}=row;
  return {...record,...deliveryGenerationContext(row)};
}
export function deliveryConsumptionDirectoryRow(row:any) {
  const {receipt,...record}=deliveryConsumptionRecord(row);
  return {...record,receipt:{code:receipt?.code,owner:receipt?.owner,caseId:receipt?.caseId,
    generation:receipt?.generation,nextAction:typeof receipt?.nextAction==='string'?receipt.nextAction.slice(0,500):undefined},
    receiptProjection:{fullCharacters:JSON.stringify(receipt??{}).length,fullSourcesPreserved:true,
      recovery:{kind:'delivery_consumption',obligationId:row.id,offset:0}}};
}

/** One SQL snapshot: only the selected page returns receipts. The complete
 * filtered identity digest prevents joining pages from different generations.
 * Observation/checked_at timestamps never manufacture a new generation. */
export async function readDeliveryConsumptionPage(db:DB,goalId:string,query:DeliveryCatalogQuery={}) {
  const offset=query.offset??0,limit=query.limit??10;
  if(!Number.isSafeInteger(offset)||offset<0||offset>10000000||!Number.isSafeInteger(limit)||limit<1||limit>24)
    throw Error('DELIVERY_CATALOG_PAGE');
  if(!(await db.query("SELECT to_regclass('public.delivery_consumptions') present")).rows[0].present)
    return {status:'OK',rows:[] as any[],totalRows:0,offset,nextOffset:null as number|null,truncated:false,
      snapshotDigest:sha(''),scope:query.scope??null};
  const result=(await db.query(`WITH selected AS MATERIALIZED (
    SELECT c.id,c.created_at,(c.state IN('completed','superseded')) closed,encode(sha256(convert_to(jsonb_build_array(c.id,c.goal_id,c.handoff_id,c.producer_task_id,c.created_at,c.scope,c.state,
      c.producer_run_id,c.producer_manifest_digest,c.consumer_task_id,c.consumer_contract_digest,
      c.candidate_run_id,c.candidate_digest,c.qa_task_id,c.qa_run_id,c.receipt)::text,'UTF8')),'hex') digest
    FROM delivery_consumptions c WHERE c.goal_id=$1
      AND ($2::text IS NULL OR c.scope=$2) AND ($3::text IS NULL OR c.scope IS DISTINCT FROM $3)
      AND ($4::uuid IS NULL OR c.consumer_task_id=$4) AND ($5::uuid IS NULL OR c.id=$5)
      AND (NOT $6::boolean OR c.state NOT IN('completed','superseded'))
  ), header AS (SELECT count(*)::int total,
    encode(sha256(convert_to(COALESCE(string_agg(id::text||':'||digest,',' ORDER BY closed,created_at,id),''),'UTF8')),'hex') digest
    FROM selected), page AS (SELECT id FROM selected ORDER BY closed,created_at,id LIMIT $7 OFFSET $8)
    SELECT h.total catalog_total,h.digest catalog_digest,${fields} FROM header h
    LEFT JOIN page p ON true LEFT JOIN delivery_consumptions c ON c.id=p.id
    ORDER BY (c.state IN('completed','superseded')),c.created_at,c.id`,
    [goalId,query.scope??null,query.excludeScope??null,query.consumerTaskId??null,query.obligationId??null,
      query.activeOnly??false,limit,offset])).rows;
  const totalRows=result[0]?.catalog_total??0,snapshotDigest=result[0]?.catalog_digest??sha('');
  if(query.expectedDigest && query.expectedDigest!==snapshotDigest)
    return {status:'STALE_CATALOG',rows:[] as any[],totalRows,offset,nextOffset:0 as number|null,truncated:true,snapshotDigest,scope:query.scope??null};
  const rows=result.filter(row=>row.id).map(({catalog_total:_total,catalog_digest:_digest,...row})=>
    ({...row,obligationKey:'delivery-consumption:'+row.id,...deliveryGenerationContext(row)}));
  const nextOffset=offset+rows.length<totalRows?offset+rows.length:null;
  return {status:'OK',rows,totalRows,offset,nextOffset,truncated:nextOffset!==null,snapshotDigest,scope:query.scope??null};
}

/** Compatibility reader: explicit metadata, never interpreted as the complete
 * objective. Management uses the rotating focal window below instead. */
export async function readDeliveryConsumptions(db:DB,goalId:string) {
  const page=await readDeliveryConsumptionPage(db,goalId,{limit:24});
  return page.rows.map(row=>({...row,total:page.totalRows,truncated:page.truncated}));
}

/** Two independently rotating lanes prevent another scope's old obligations
 * from hiding the focal consumer. Cursors advance only when wake commits. */
export async function readDeliveryConsumptionWindow(db:DB,goalId:string,focusScope?:string,cursor:any={}) {
  const safe=(value:any)=>Number.isSafeInteger(value)&&value>=0?value:0;
  const read=async(query:DeliveryCatalogQuery)=>{
    let page=await readDeliveryConsumptionPage(db,goalId,query);
    if(query.offset && !page.rows.length && page.totalRows)page=await readDeliveryConsumptionPage(db,goalId,{...query,offset:0});
    return page;
  };
  const focal=focusScope?await read({scope:focusScope,activeOnly:true,limit:16,
    offset:cursor.scope===focusScope?safe(cursor.focusOffset):0}):null;
  const other=await read({excludeScope:focusScope,activeOnly:true,limit:24-(focal?.rows.length??0),offset:safe(cursor.otherOffset)});
  const rows=[...(focal?.rows??[]),...other.rows],totalRows=(focal?.totalRows??0)+other.totalRows;
  const pageInfo=(page:any)=>page?{totalRows:page.totalRows,offset:page.offset,nextOffset:page.nextOffset,
    snapshotDigest:page.snapshotDigest,includedRows:page.rows.length}:null;
  return {rows,selection:{version:1,scope:focusScope??null,totalRows,includedRows:rows.length,
    omittedRows:totalRows-rows.length,truncated:totalRows>rows.length,fullSourcesPreserved:true,
    focal:pageInfo(focal),other:pageInfo(other),nextCursor:{scope:focusScope??null,
      focusOffset:focal?.nextOffset??0,otherOffset:other.nextOffset??0},
    recovery:{tool:'management.contextRequests',kind:'delivery_consumptions',
      byIdKind:'delivery_consumption',scopeOptional:true,pageSize:10,
      guide:'Use offset e snapshotDigest da página; STALE_CATALOG exige recomeçar em zero. A janela prioriza o foco e rotaciona as demais obrigações. Omissão não significa ausência nem aceite.'}}};
}
