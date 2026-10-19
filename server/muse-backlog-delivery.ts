import { verifyProtocolReport } from '../bridge/protocol-report-contract.mjs';

/** Structured reserve identity, independent of titles, original DAG stages and live occupancy. */
export function isMuseBacklogExecution(execution: any) {
  return execution?.kind === 'harness' &&
    (execution.workKind === 'diagnostic' || execution.workKind === 'repair') &&
    (execution.protocolDelivery === 'backlog-v1' ||
      (execution.backlogContext!==null && typeof execution.backlogContext==='object' && !Array.isArray(execution.backlogContext)) ||
      (typeof execution.testFile === 'string' && execution.testFile.length > 0));
}
export const museBacklogReserveSql = `execution->>'kind'='harness'
 AND execution->>'workKind' IN('diagnostic','repair')
 AND (execution->>'protocolDelivery'='backlog-v1' OR jsonb_typeof(execution->'backlogContext')='object'
  OR length(COALESCE(execution->>'testFile',''))>0)`;

function uniqueJsonKeys(text:string) {
  const value=JSON.parse(text);
  const stack:{object:boolean;keys:Set<string>;expectKey:boolean}[]=[];
  for(const token of text.match(/"(?:\\.|[^"\\])*"|[{}\[\],:]|[^\s{}\[\],:]+/gs)??[]){
    if(token==='{'||token==='[')stack.push({object:token==='{',keys:new Set(),expectKey:token==='{'});
    else if(token==='}'||token===']')stack.pop();
    else {
      const current=stack.at(-1);
      if(current?.object&&token===',')current.expectKey=true;
      else if(current?.object&&current.expectKey&&token.startsWith('"')){
        const key=JSON.parse(token);
        if(current.keys.has(key))throw Error('DUPLICATE_JSON_KEY');
        current.keys.add(key);current.expectKey=false;
      }
    }
  }
  return value;
}

/** Apply only to the new Muse backlog contract, never to existing integration/QA reports. */
export function verifyMuseBacklogDelivery(task: any, artifacts: {name:string;content:string|Buffer}[]) {
  const execution=task.execution;
  // A provider change retains this delivery contract and its runtime verifier.
  if(!['muse','glm'].includes(execution?.workforceLane)||execution.protocolDelivery!=='backlog-v1'||!isMuseBacklogExecution(execution))return;
  try {
    if(typeof execution.testFile!=='string'||!/^data\/qa-protocol\/[a-z0-9_-]+\.test\.mjs$/.test(execution.testFile))throw Error('JSON');
    const reports=artifacts.filter(a=>a.name==='protocol-report.json');
    if(reports.length!==1)throw Error('JSON');
    const report=uniqueJsonKeys(reports[0].content.toString());
    verifyProtocolReport(report,execution.testFile);
    const reject=(field:string)=>{throw Object.assign(Error(field),{museField:field});};
    if(!Array.isArray(report.cases)||report.cases.length===0)reject('CASES');
    if(!Array.isArray(report.limitations))reject('LIMITATIONS');
    for(const [name,field] of [['protocol-tests.log','LOG'],['test-source.mjs','TEST_SOURCE']]){
      const files=artifacts.filter(a=>a.name===name);
      if(files.length!==1||!files[0].content.toString().trim())reject(field);
    }
    return {verified:true};
  }catch(error:any){
    const field=['CASES','LIMITATIONS','LOG','TEST_SOURCE'].includes(error?.museField)?error.museField:
      /^OPENCODE_DELIVERY_INVALID_REPORT_(JSON|SUITE|EXIT_CODE|COMMAND|TEST_FILE)$/.exec(error?.code??'')?.[1]??'JSON';
    return {verified:false,code:'MUSE_DELIVERY_INVALID_REPORT_'+field,
      instruction:'Entrega Muse precisa de protocol-report.json com suiteExecuted:true, command literal, exitCode inteiro, cases não vazio e limitations como array. Publique log e fonte do teste não vazios; quando o comando não produzir stdout/stderr, registre explicitamente esse fato com comando e código real.'};
  }
}
