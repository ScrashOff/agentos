import {createHash} from 'node:crypto';

export const managerContextSectionNames = ['workOpportunities','deferredWorkOpportunities','progress','frontier',
  'sourceCatalog','deferredDeliveries','deliveryConsumptions'] as const;
export type ManagerContextSectionName = typeof managerContextSectionNames[number];
export const managerContextSectionByteLimit = 1048576;
export type ManagerContextSectionBlob = {section:ManagerContextSectionName;digest:string;text:string;characters:number};
export type ManagerContextSectionReference = {
  kind:'MANAGER_CONTEXT_SECTION_V1';section:ManagerContextSectionName;digest:string;characters:number;goalId:string;
  complete:false;fullSourcePreserved:true;
  recovery:{tool:'management.contextRequests';request:{kind:'context_section';section:ManagerContextSectionName;digest:string;offset:0}};
  guide:string;summary?:unknown;
};
const uuid=/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const hash=/^[a-f0-9]{64}$/;
const object=(value:any):value is Record<string,any>=>!!value&&typeof value==='object'&&!Array.isArray(value);
const keys=(value:any,allowed:string[])=>object(value)&&Object.keys(value).every(key=>allowed.includes(key));
const sha=(text:string)=>createHash('sha256').update(text).digest('hex');
export const managerContextSectionDigest=(section:ManagerContextSectionName,text:string)=>sha(section+'\n'+text);
const guide='This is an immutable reference to a complete operational observation, not an empty or fully read section. Read recovery.request and follow nextOffset before relying on omitted details. The digest and summary grant no action, acceptance, or materialContextGap authority.';

export function isManagerContextSectionReference(value:any,section?:ManagerContextSectionName,goalId?:string):value is ManagerContextSectionReference {
  return keys(value,['kind','section','digest','characters','goalId','complete','fullSourcePreserved','recovery','guide','summary'])&&
    value.kind==='MANAGER_CONTEXT_SECTION_V1'&&(managerContextSectionNames as readonly string[]).includes(value.section)&&
    (!section||value.section===section)&&uuid.test(value.goalId)&&(!goalId||value.goalId===goalId)&&
    typeof value.digest==='string'&&hash.test(value.digest)&&Number.isSafeInteger(value.characters)&&value.characters>0&&
    value.characters<=managerContextSectionByteLimit&&value.complete===false&&value.fullSourcePreserved===true&&
    value.guide===guide&&keys(value.recovery,['tool','request'])&&value.recovery.tool==='management.contextRequests'&&
    keys(value.recovery.request,['kind','section','digest','offset'])&&value.recovery.request.kind==='context_section'&&
    value.recovery.request.section===value.section&&value.recovery.request.digest===value.digest&&value.recovery.request.offset===0;
}

// A standalone marker-shaped value is source data. Only the matching directory
// emitted with a frozen projection identifies references on a later projection.
export function managerContextSectionGoal(operation:any):string|undefined {
  const directory=operation?.contextSectionDirectory;
  if(!keys(directory,['version','goalId','sections'])||directory.version!==1||!uuid.test(directory.goalId)||
    !Array.isArray(directory.sections)||directory.sections.length<1||directory.sections.length>7)return;
  const seen=new Set<string>();
  for(const row of directory.sections){
    if(!keys(row,['section','digest','characters'])||seen.has(row.section)||
      !isManagerContextSectionReference(operation[row.section],row.section,directory.goalId)||
      row.digest!==operation[row.section].digest||row.characters!==operation[row.section].characters)return;
    seen.add(row.section);
  }
  return directory.goalId;
}

// Reconstitute only the verified, existing cross-section sharing protocol.
// Unknown representations stay inline rather than becoming orphaned references.
function exactSection(value:any,operation:any,section:ManagerContextSectionName):any {
  // Source JSON is literal. Interpret sharing only at the three paths emitted
  // by projectManagerContext, never inside arbitrary receipts or user content.
  const copy=JSON.parse(JSON.stringify(value));
  if(section!=='deferredWorkOpportunities'||!object(copy))return copy;
  for(const key of ['acceptedSources','existingCoverage','liveActors']){
    const entry=copy[key];if(!object(entry)||(!Object.hasOwn(entry,'$ref')&&!Object.hasOwn(entry,'$rowsOf')))continue;
    const path='operation.workOpportunities.'+key,allowed=entry.$ref?['$ref','sha256','items']:['$rowsOf','sha256','rows'];
    if(!keys(entry,allowed)||(entry.$ref??entry.$rowsOf)!==path||!hash.test(entry.sha256))throw Error('MANAGER_SECTION_REFERENCE');
    let rows=operation?.workOpportunities?.[key];
    if(object(rows)&&rows.lossless===true&&Array.isArray(rows.schemas)&&Array.isArray(rows.rows)){
      if(!keys(rows,['schemas','rows','lossless']))throw Error('MANAGER_SECTION_PACKED');
      const schemas=rows.schemas;
      rows=rows.rows.map((row:any)=>{
        if(!Array.isArray(row)||!Number.isSafeInteger(row[0]))throw Error('MANAGER_SECTION_PACKED');
        const fields=schemas[row[0]];
        if(!Array.isArray(fields)||fields.length!==row.length-1||new Set(fields).size!==fields.length||
          fields.some((field:any)=>typeof field!=='string'||['__proto__','constructor','prototype'].includes(field)))throw Error('MANAGER_SECTION_PACKED');
        return Object.fromEntries(fields.map((field:string,i:number)=>[field,row[i+1]]));
      });
    }
    if(!Array.isArray(rows)||sha(JSON.stringify(rows))!==entry.sha256)throw Error('MANAGER_SECTION_REFERENCE');
    if(entry.$ref){if(entry.items!==rows.length)throw Error('MANAGER_SECTION_REFERENCE');copy[key]=JSON.parse(JSON.stringify(rows));continue;}
    if(!Array.isArray(entry.rows))throw Error('MANAGER_SECTION_REFERENCE');
    copy[key]=entry.rows.map((row:any)=>{
      if(typeof row!=='number')return row;
      if(!Number.isSafeInteger(row)||row<0||row>=rows.length)throw Error('MANAGER_SECTION_REFERENCE');
      return JSON.parse(JSON.stringify(rows[row]));
    });
  }
  return copy;
}

function summary(section:ManagerContextSectionName,value:any,focus:any) {
  if(!object(value))return;
  const selected:any={};
  const fields=section==='progress'?['delivery','stagnation','nextTransition']:
    section==='frontier'?['counts','next']:section==='workOpportunities'||section==='deferredWorkOpportunities'?['selection']:[];
  for(const key of fields)if(value[key]!==undefined)selected[key]=value[key];
  if(section==='frontier'&&Array.isArray(value.scopeSummary))
    selected.scopeSummary=value.scopeSummary.filter((row:any)=>row?.scope===focus?.scope);
  if(!Object.keys(selected).length||JSON.stringify(selected).length>6000)return;
  return {complete:false,observationOnly:true,...selected};
}

export function prepareManagerContextSections(operation:any,goalId:string,focus?:any) {
  if(typeof goalId!=='string'||!uuid.test(goalId))throw Error('MANAGER_SECTION_GOAL');
  const references:Partial<Record<ManagerContextSectionName,ManagerContextSectionReference>>={};
  const blobs:ManagerContextSectionBlob[]=[];
  const exactValues:Record<string,any>={...operation};
  const existingGoal=managerContextSectionGoal(operation);
  if(existingGoal&&existingGoal!==goalId)throw Error('MANAGER_SECTION_GOAL');
  if(operation&&Object.hasOwn(operation,'contextSectionDirectory')&&!existingGoal)
    return {references,blobs,directory:{version:1 as const,goalId,sections:[] as {section:ManagerContextSectionName;digest:string;characters:number}[]}};
  for(const section of managerContextSectionNames){
    const original=operation?.[section];if(original===undefined)continue;
    if(existingGoal===goalId&&isManagerContextSectionReference(original,section,goalId)){
      references[section]=original;continue;
    }
    // Scalar or malformed view roots are not interpreted as operational views.
    if(!object(original)&&!Array.isArray(original))continue;
    let value:any,text:string;
    try {value=exactSection(original,operation,section);text=JSON.stringify(value);}catch{continue;}
    exactValues[section]=value;
    if(!text||Buffer.byteLength(text,'utf8')>managerContextSectionByteLimit)continue;
    const digest=managerContextSectionDigest(section,text),characters=text.length;
    const brief=summary(section,value,focus);
    references[section]={kind:'MANAGER_CONTEXT_SECTION_V1',section,digest,characters,goalId,
      complete:false,fullSourcePreserved:true,
      recovery:{tool:'management.contextRequests',request:{kind:'context_section',section,digest,offset:0}},
      guide,...(brief?{summary:brief}:{})};
    blobs.push({section,digest,text,characters});
  }
  // References left literal anywhere in the delivered/source JSON must not
  // lose their original target when that target is moved behind a reader.
  const targets=new Set<string>();let visited=0;
  const scan=(value:any,depth=0)=>{
    if(++visited>100000||depth>80)throw Error('MANAGER_SECTION_COMPLEXITY');
    if(!value||typeof value!=='object')return;
    for(const [key,item] of Object.entries(value)){
      if(['$ref','$rowsOf','$rowOf'].includes(key)&&typeof item==='string'){
        const match=/^operation\.([A-Za-z][A-Za-z0-9]*)(?:\.|$)/.exec(item);
        if(match)targets.add(match[1]);
      }
      scan(item,depth+1);
    }
  };
  try {scan(exactValues);}catch{for(const section of managerContextSectionNames)targets.add(section);}
  for(const section of managerContextSectionNames)if(targets.has(section)&&blobs.some(blob=>blob.section===section)){
    delete references[section];
    blobs.splice(blobs.findIndex(blob=>blob.section===section),1);
  }
  const directory={version:1 as const,goalId,sections:managerContextSectionNames.filter(section=>references[section])
    .map(section=>({section,digest:references[section]!.digest,characters:references[section]!.characters}))};
  return {references,blobs,directory};
}
