import {createHash} from 'node:crypto';

const format='MANAGER_SHARED_CONTEXT_V1';
const sha=(value:string)=>createHash('sha256').update(value).digest('hex');
const own=(value:any,key:string)=>Object.prototype.hasOwnProperty.call(value,key);
const marker=(value:any)=>value && !Array.isArray(value) && typeof value==='object' &&
  Object.keys(value).length===1 && (own(value,'$v') || own(value,'$r'));
const nodeMarker=(value:any)=>value && !Array.isArray(value) && typeof value==='object' &&
  Object.keys(value).length===1 && own(value,'$n');
const nodeGuide=' Quando sharedContext.nodeReferences=true, {$n:N} referencia sharedContext.nodes[N], um no ja codificado: expanda-o recursivamente antes de decidir. Dentro de nodes[i], N deve ser menor que i; na raiz pode referir qualquer no. As demais regras $r/$v/^N continuam. Marcadores originais estao escapados em values, sempre literais. Sem a flag, $n permanece literal.';
// Reserve the entire prefix, including malformed-looking references. Original
// strings with this prefix go into the literal table and are never re-decoded.
const shortMarker=(value:any):value is string=>typeof value==='string' && value.startsWith('^');
const shortGuide=' SOMENTE quando sharedContext.shortValueReferences=true, nos mesmos campos explicitamente compartilhados, "^N" equivale a {$v:N}; N é decimal canônico sem sinal nem zeros iniciais, exceto 0. Copie values[N] literalmente, sem expandir seu conteúdo. Strings originais iniciadas por ^ estão escapadas na tabela. Nos campos protegidos, ^N continua literal; sem a flag, toda string ^N é literal. Referências $v/$r legadas continuam válidas.';
const guide='MANAGER_SHARED_CONTEXT_V1 é representação sem perdas, não conteúdo omitido. O array cases e cada objeto de caso permanecem comuns; SOMENTE cases[i].evidence usa $v/$r quando sharedContext.caseEvidenceShared=true; cases[i].materialConsumptionReview também usa $v/$r SOMENTE quando sharedContext.caseMaterialConsumptionShared=true. Todos os demais campos do caso, evidenceTexts, inlineMemoryHashes, caseWindow e operation.focus já são literais: não expanda marcadores neles. Nos campos de caso explicitamente compartilhados e nos demais campos compartilhados, antes de decidir, expanda {$v:N} copiando literalmente sharedContext.values[N] (não interprete marcadores dentro da cópia); expanda {$r:[S,...valores]} usando sharedContext.schemas[S] como nomes dos campos, na mesma ordem, e expanda recursivamente os valores. Objetos e arrays comuns mantêm sua estrutura. Somente depois resolva os diretórios schemas/rows e $text/$ref já documentados. sourceSha256 e sourceCharacters conferem JSON.stringify do retrato completo reconstruído sem sharedContext. Todos os IDs, instruções, critérios, pendências, donos, fontes e recibos continuam integrais; referência não autoriza ação nem aceite.';
// These fields are consumed directly by existing deterministic readers. Keep
// their exact ordinary JSON representation, independently of model expansion.
const protectedRoot=new Set(['cases','evidenceTexts','inlineMemoryHashes','caseWindow']);
const protectedPath=(path:string[])=>path.length===1 && protectedRoot.has(path[0]) ||
  path.length===2 && path[0]==='operation' && path[1]==='focus';
// These two explicit subtrees can share already delivered identities. Other
// envelope fields stay literal; legacy V1 has only evidence sharing enabled.
const mapCaseEvidence=(cases:any,visit:(value:any,key:string)=>any,material=false)=>Array.isArray(cases)?cases.map(entry=>
  entry && typeof entry==='object' && !Array.isArray(entry)?Object.fromEntries(Object.entries(entry).map(([key,value])=>
    [key,key==='evidence'||(material&&key==='materialConsumptionReview')?visit(value,key):value])):entry):cases;
const maxCharacters=8_000_000;

/** An inline lossless representation, used only after normal projection cannot
 * fit. Literal marker-shaped source data is escaped via the literal table.
 * Values remain literal; encoded node entries only reference earlier nodes. */
export function compactSharedManagerContext(snapshot:any) {
  const baseline=compactRepresentation(snapshot,false);
  if(own(snapshot,'sharedContext') || JSON.stringify(snapshot).length>maxCharacters)return baseline;
  let interned:any;
  try {interned=compactRepresentation(snapshot,true);}
  catch(error){
    // Optional nodes can add traversal depth to an otherwise valid baseline.
    if(error instanceof Error && error.message==='MANAGER_SHARED_CONTEXT_EXPANSION')return baseline;
    throw error;
  }
  // Compare the complete escaped output: interning can change the amortized
  // cost of schemas/strings, so raw subtree length is not evidence of savings.
  return JSON.stringify(JSON.stringify(interned)).length<JSON.stringify(JSON.stringify(baseline)).length?interned:baseline;
}

function compactRepresentation(snapshot:any,internSubtrees:boolean) {
  if(own(snapshot,'sharedContext')){expandSharedManagerContext(snapshot);return snapshot;}
  const original=JSON.stringify(snapshot);
  if(original.length>maxCharacters)return snapshot;
  const source=JSON.parse(original),strings=new Map<string,number>(),schemasCount=new Map<string,number>();
  const subtrees=new Map<string,number>();let subtreeBudget=maxCharacters;
  const subtreePath=(path:string[])=>path.length>1 && path.join('.')!=='operation.tasks' && !protectedPath(path);
  const shareMaterial=Array.isArray(source.cases)&&source.cases.some((entry:any)=>entry&&typeof entry==='object'&&own(entry,'materialConsumptionReview'));
  const count=(value:any,path:string[]=[])=>{
    if(path.length===1 && path[0]==='cases'){mapCaseEvidence(value,(item,key)=>{count(item,['cases','*',key]);return item;},shareMaterial);return;}
    if(protectedPath(path))return;
    if(internSubtrees && subtreePath(path) && value && typeof value==='object' && !marker(value) && !nodeMarker(value)){
      const text=JSON.stringify(value);
      if(text.length>=80){
        subtreeBudget-=text.length;
        // Bound auxiliary exact-JSON keys even for deeply nested input.
        if(subtreeBudget<0){internSubtrees=false;subtrees.clear();}
        else subtrees.set(text,(subtrees.get(text)??0)+1);
      }
    }
    if(typeof value==='string'){
      if(value.length>=20)strings.set(value,(strings.get(value)??0)+1);
    }else if(Array.isArray(value))value.forEach((item,index)=>count(item,[...path,String(index)]));
    else if(value && typeof value==='object'){
      if(marker(value) || internSubtrees && nodeMarker(value))return;
      const keys=JSON.stringify(Object.keys(value));schemasCount.set(keys,(schemasCount.get(keys)??0)+1);
      for(const [key,item]of Object.entries(value))count(item,[...path,key]);
    }
  };
  count(source);
  const values:any[]=[],schemas:string[][]=[],valueIds=new Map<string,number>(),schemaIds=new Map<string,number>();
  const nodeTable:any[]=[],nodeIds=new Map<string,number>();
  const literal=(value:any)=>{
    const key=JSON.stringify(value);let id=valueIds.get(key);
    if(id===undefined){id=values.length;valueIds.set(key,id);values.push(value);}
    return '^'+id;
  };
  const encode=(value:any,path:string[]=[]):any=>{
    if(path.length===1 && path[0]==='cases')return mapCaseEvidence(value,(item,key)=>encode(item,['cases','*',key]),shareMaterial);
    if(protectedPath(path))return value;
    if(typeof value==='string')return shortMarker(value) || (strings.get(value)??0)>1 &&
      (JSON.stringify(value).length-14)*((strings.get(value)??0)-1)>20?literal(value):value;
    if(!value || typeof value!=='object')return value;
    if(marker(value) || internSubtrees && nodeMarker(value))return literal(value);
    const rawKey=internSubtrees && subtreePath(path)?JSON.stringify(value):null;
    const repeated=rawKey!==null && (subtrees.get(rawKey)??0)>1;
    if(repeated){const id=nodeIds.get(rawKey);if(id!==undefined)return {$n:id};}
    let encoded:any;
    if(Array.isArray(value))encoded=value.map((item,index)=>encode(item,[...path,String(index)]));
    else {
      const keys=Object.keys(value),key=JSON.stringify(keys),items=keys.map(name=>encode(value[name],[...path,name]));
      // Keep the envelope, cases, operation and focus readable by the existing
      // consumers; nested records may share their repeated field names.
      const ordinary=Object.fromEntries(keys.map((name,index)=>[name,items[index]]));
      encoded=ordinary;
      if(subtreePath(path) && (schemasCount.get(key)??0)>1 && key.length>26){
        let id=schemaIds.get(key);
        const candidate={$r:[id??schemas.length,...items]};
        const amortized=id===undefined?key.length/(schemasCount.get(key)??1):0;
        if(JSON.stringify(candidate).length+amortized<JSON.stringify(ordinary).length){
          if(id===undefined){id=schemas.length;schemaIds.set(key,id);schemas.push(keys);}
          encoded={$r:[id,...items]};
        }
      }
    }
    // Children are encoded first, so every node reference points backwards.
    // Repeated parents reuse this encoded tree without allocating orphan values.
    if(repeated && JSON.stringify(encoded).length>=80 && nodeTable.length<100000){
      const id=nodeTable.length;nodeTable.push(encoded);nodeIds.set(rawKey!,id);return {$n:id};
    }
    return encoded;
  };
  const packed=encode(source);
  const result={...packed,sharedContext:{format,guide:guide+shortGuide+(nodeTable.length?nodeGuide:''),...(nodeTable.length?{nodeReferences:true,nodes:nodeTable}:{}),caseEvidenceShared:true,...(shareMaterial?{caseMaterialConsumptionShared:true}:{}),shortValueReferences:true,sourceSha256:sha(original),sourceCharacters:original.length,schemas,values}};
  // Escaping matters: description itself is subsequently embedded in JSON.
  if(JSON.stringify(JSON.stringify(result)).length>=JSON.stringify(original).length)return snapshot;
  if(JSON.stringify(expandSharedManagerContext(result))!==original)throw Error('MANAGER_SHARED_CONTEXT_ROUNDTRIP');
  return result;
}

/** Decode only this explicitly versioned envelope, checking its complete hash.
 * Invalid references are errors, never silent omissions or partial authority. */
export function expandSharedManagerContext(snapshot:any) {
  const encoding=snapshot?.sharedContext;
  if(encoding?.format!==format)return snapshot;
  if((own(encoding,'caseEvidenceShared') && encoding.caseEvidenceShared!==true)||
    (own(encoding,'caseMaterialConsumptionShared') && (encoding.caseMaterialConsumptionShared!==true||encoding.caseEvidenceShared!==true))||
    (own(encoding,'shortValueReferences') && encoding.shortValueReferences!==true)||
    (own(encoding,'nodeReferences') && encoding.nodeReferences!==true)||
    (encoding.nodeReferences===true && (!Array.isArray(encoding.nodes)||encoding.nodes.length>100000))||
    (own(encoding,'nodes') && encoding.nodeReferences!==true)||
    !Array.isArray(encoding.schemas)||!Array.isArray(encoding.values)||
    encoding.schemas.length>100000||encoding.values.length>100000||
    !Number.isInteger(encoding.sourceCharacters)||encoding.sourceCharacters<0||encoding.sourceCharacters>maxCharacters||
    !/^[a-f0-9]{64}$/.test(encoding.sourceSha256??'')||JSON.stringify(encoding).length>maxCharacters)
    throw Error('MANAGER_SHARED_CONTEXT_INVALID');
  let nodes=0,characters=0;
  const chargeSize=(size:number)=>{characters+=size;if(characters>maxCharacters)throw Error('MANAGER_SHARED_CONTEXT_EXPANSION');};
  const charge=(value:any)=>{chargeSize(JSON.stringify(value).length);return value;};
  const chargeKeys=(keys:string[])=>chargeSize(2+Math.max(0,keys.length-1)+keys.reduce((sum,key)=>sum+JSON.stringify(key).length+1,0));
  const literal=(id:any)=>{
    if(!Number.isSafeInteger(id)||id<0||id>=encoding.values.length)throw Error('MANAGER_SHARED_CONTEXT_REFERENCE');
    // Deliberately literal: source data may itself look like either reference.
    charge(encoding.values[id]);return structuredClone(encoding.values[id]);
  };
  const nodeTable:any[]=encoding.nodeReferences===true?encoding.nodes:[];
  const nodeId=(id:any,limit:number)=>{
    if(!Number.isSafeInteger(id)||id<0||id>=limit)throw Error('MANAGER_SHARED_CONTEXT_REFERENCE');
    return id as number;
  };
  // Check even unused table entries. No forward/self references or cycles can
  // hide outside the reconstructed envelope. Literal values are not traversed.
  let checked=0;
  const checkNode=(value:any,limit:number,depth=0):void=>{
    if(++checked>500000 || depth>128)throw Error('MANAGER_SHARED_CONTEXT_EXPANSION');
    if(nodeMarker(value)){nodeId(value.$n,limit);return;}
    if(encoding.shortValueReferences===true && shortMarker(value)){
      const id=Number(value.slice(1));
      if(!/^\^(0|[1-9][0-9]*)$/.test(value)||value!=='^'+id||!Number.isSafeInteger(id)||id>=encoding.values.length)
        throw Error('MANAGER_SHARED_CONTEXT_REFERENCE');
      return;
    }
    if(marker(value)){
      if(own(value,'$v')){if(!Number.isSafeInteger(value.$v)||value.$v<0||value.$v>=encoding.values.length)throw Error('MANAGER_SHARED_CONTEXT_REFERENCE');return;}
      const row=value.$r,id=Array.isArray(row)?row[0]:undefined,keys=encoding.schemas[id];
      if(!Number.isInteger(id)||id<0||!Array.isArray(keys)||row.length!==keys.length+1||
        !keys.every((key:any)=>typeof key==='string')||new Set(keys).size!==keys.length)throw Error('MANAGER_SHARED_CONTEXT_REFERENCE');
      for(const item of row.slice(1))checkNode(item,limit,depth+1);
      return;
    }
    if(Array.isArray(value))for(const item of value)checkNode(item,limit,depth+1);
    else if(value && typeof value==='object')for(const item of Object.values(value))checkNode(item,limit,depth+1);
  };
  nodeTable.forEach((value,index)=>checkNode(value,index));
  const decode=(value:any,depth=0,limit=nodeTable.length):any=>{
    if(++nodes>500000 || depth>128)throw Error('MANAGER_SHARED_CONTEXT_EXPANSION');
    if(encoding.nodeReferences===true && nodeMarker(value)){
      const id=nodeId(value.$n,limit);return decode(nodeTable[id],depth+1,id);
    }
    if(encoding.shortValueReferences===true && shortMarker(value)){
      const id=Number(value.slice(1));
      if(!/^\^(0|[1-9][0-9]*)$/.test(value)||value!=='^'+id)throw Error('MANAGER_SHARED_CONTEXT_REFERENCE');
      return literal(id);
    }
    if(marker(value)){
      if(own(value,'$v'))return literal(value.$v);
      const row=value.$r,id=Array.isArray(row)?row[0]:undefined,keys=encoding.schemas[id];
      if(!Number.isInteger(id)||id<0||!Array.isArray(keys)||row.length!==keys.length+1||
        !keys.every((key:any)=>typeof key==='string')||new Set(keys).size!==keys.length)
        throw Error('MANAGER_SHARED_CONTEXT_REFERENCE');
      chargeKeys(keys);
      return Object.fromEntries(keys.map((key:string,index:number)=>[key,decode(row[index+1],depth+1,limit)]));
    }
    if(Array.isArray(value)){chargeSize(2+Math.max(0,value.length-1));return value.map(item=>decode(item,depth+1,limit));}
    if(value && typeof value==='object'){
      chargeKeys(Object.keys(value));return Object.fromEntries(Object.entries(value).map(([key,item])=>[key,decode(item,depth+1,limit)]));
    }
    if(value!==undefined)charge(value);return value;
  };
  const result=Object.fromEntries(Object.entries(snapshot).filter(([key])=>key!=='sharedContext').map(([key,value])=>
    [key,key==='cases' && encoding.caseEvidenceShared===true?mapCaseEvidence(value,item=>decode(item),encoding.caseMaterialConsumptionShared===true):protectedRoot.has(key)?value:key==='operation' && value && typeof value==='object' && !Array.isArray(value)?Object.fromEntries(Object.entries(value as any).map(([name,item])=>
      [name,name==='focus'?item:decode(item)])):decode(value)]));
  const text=JSON.stringify(result);
  if(text.length>maxCharacters)throw Error('MANAGER_SHARED_CONTEXT_EXPANSION');
  if(text.length!==encoding.sourceCharacters||sha(text)!==encoding.sourceSha256)throw Error('MANAGER_SHARED_CONTEXT_DIGEST');
  return result;
}
