import {afterEach,beforeEach,expect,it} from 'vitest';
import {createHash} from 'node:crypto';
import {mkdtemp,mkdir,readFile,writeFile,rm,unlink,symlink,rename} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {inspectQualificationEvidence} from '../scripts/inspect-qualification-evidence.js';

const hash=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
const prefix='evidence/executor-qualification';
const json=(file:string,value:unknown)=>writeFile(file,JSON.stringify(value,null,2));
async function fixture(){
 const directory=await mkdtemp(path.join(tmpdir(),'agentos-evidence-'));
 const workspace=path.join(directory,'workspace');
 await mkdir(path.join(workspace,prefix,'recibos'),{recursive:true});
 await mkdir(path.join(workspace,'server'));
 const fixed='// fixed external contract\n';
 await writeFile(path.join(workspace,'server/autonomy-score.ts'),fixed);
 await writeFile(path.join(workspace,'server/module.ts'),'export const value=1;\n');
 const candidate={candidateDigest:'a'.repeat(64),files:[
  {path:'server/autonomy-score.ts',sha256:hash(fixed)},
  {path:'server/module.ts',sha256:hash('export const value=1;\n')}]};
 const manifestPath=path.join(directory,'candidate.json');
 await json(manifestPath,candidate);
 const hashes=[];
 for(let i=0;i<60;i++){
  const scenario=`C${String(Math.floor(i/3)+1).padStart(2,'0')}`,id=`${scenario}-V${i%3+1}`;
  const payload={trialId:id,cenarioId:scenario,varianteId:id,startedAt:'2026-01-01T00:00:00.000Z',
   deadlineAt:'2026-01-01T00:00:20.000Z',finishedAt:'2026-01-01T00:00:01.000Z',passed:i<25,
   humanIntervention:false,checkedBy:'deterministic',passosExecutados:[{passo:'fixture',ok:i<25}],
   ...(i>=25?{motivoFalha:i<45?'PASSO_FALHOU':'PENDENCIA_REGISTRADA'}:{})};
  const receipt={...payload,evidenceSha256:hash(JSON.stringify(payload))};
  const arquivo=`${prefix}/recibos/${id}.json`;await json(path.join(workspace,arquivo),receipt);
  hashes.push({trialId:id,evidenceSha256:receipt.evidenceSha256,arquivo});
 }
 const report:any={schema:'agentos/autonomy-executor-integrado-v1',runId:'deterministic-fixture',finishedAt:'2026-01-01T00:00:01.000Z',hashesPorEnsaio:hashes,
  contagem:{totalEnsaios:60,denominador:60,aprovados:25,reprovados:20,pendentes:15},
  verificacaoExterna:hashes.map(r=>({trialId:r.trialId,ok:true})),
  identidade:{contratoFixo:{sha256Antes:hash(fixed),sha256Depois:hash(fixed)}},
  negativosContrato:[{caso:'unknown-id',rejeitado:true}],negativosOk:true,
  scoreQualificationSmoke:{qualified:false},esperas24h:{workerAlocado:false}};
 return {directory,workspace,candidate,manifestPath,expectedManifestSha256:hash(await readFile(manifestPath)),expectedFixedContractSha256:hash(fixed),report};
}
let f:Awaited<ReturnType<typeof fixture>>;
async function refresh(){
 const reportPath=path.join(f.workspace,prefix,'report.json');await json(reportPath,f.report);
 const payload={schema:'agentos/autonomy-aggregate-receipt-v1',runId:f.report.runId,finishedAt:f.report.finishedAt,
  relatorioPath:prefix+'/report.json',relatorioSha256:hash(await readFile(reportPath)),
  totalEnsaios:f.report.contagem.totalEnsaios,denominador:f.report.contagem.denominador,hashesPorEnsaio:f.report.hashesPorEnsaio};
 await json(path.join(f.workspace,prefix,'aggregate-receipt.json'),{...payload,aggregateSha256:hash(JSON.stringify(payload))});
}
async function alterReceipt(id:string,edit:(value:any)=>void,resign=false){
 const file=path.join(f.workspace,prefix,'recibos',id+'.json'),r=JSON.parse(await readFile(file,'utf8'));
 edit(r);
 if(resign){const {evidenceSha256,...payload}=r;r.evidenceSha256=hash(JSON.stringify(payload));f.report.hashesPorEnsaio.find((v:any)=>v.trialId===id).evidenceSha256=r.evidenceSha256;}
 await json(file,r);if(resign)await refresh();
}
beforeEach(async()=>{f=await fixture();await refresh();});
afterEach(async()=>{if(f)await rm(f.directory,{recursive:true,force:true});});
const inspect=()=>inspectQualificationEvidence(f);

it('inspects all sixty receipts without certifying runtime autonomy or 24 hours',async()=>{
 const result=await inspect();expect(result.structuralEvidencePassed).toBe(true);
 expect(result.counts).toEqual({aprovados:25,reprovados:20,pendentes:15});
 expect(result.autonomyQualified).toBe(false);expect(result.observation24hProved).toBe(false);
 expect(result.limits.some(l=>l.includes('does not execute scenario'))).toBe(true);
});
it('rejects a missing trial without shrinking the denominator',async()=>{
 await unlink(path.join(f.workspace,prefix,'recibos/C20-V3.json'));
 const result=await inspect();expect(result.failures).toContain('sixtyRegularReceiptFiles');expect(result.failures).toContain('fixedDenominator');
});
it('rejects duplicate IDs even when aggregate and report hashes were recalculated',async()=>{
 f.report.hashesPorEnsaio[59]={...f.report.hashesPorEnsaio[0]};await refresh();
 const result=await inspect();expect(result.failures).toContain('sixtyUniqueReportTrials');expect(result.failures).toContain('sixtyUniqueAggregateTrials');
});
it('rejects a forged receipt whose original evidence hash was retained',async()=>{
 await alterReceipt('C01-V1',r=>r.passed=false);
 expect((await inspect()).failures).toContain('allReceiptHashesAndIdentities');
});
it('rejects a receipt reference reused for another ID',async()=>{
 f.report.hashesPorEnsaio[1].arquivo=f.report.hashesPorEnsaio[0].arquivo;await refresh();
 expect((await inspect()).failures).toContain('allReceiptHashesAndIdentities');
});
it('rejects deadline violations even with a valid replacement receipt hash',async()=>{
 await alterReceipt('C01-V1',r=>r.finishedAt='2026-01-01T00:00:21.000Z',true);
 expect((await inspect()).checks.find(c=>c.name==='allReceiptHashesAndIdentities')?.detail).toEqual([{trialId:'C01-V1',code:'RECEIPT_TIME'}]);
});
it('rejects omission of failures and pending cases from the denominator',async()=>{
 f.report.contagem.denominador=25;await refresh();expect((await inspect()).failures).toContain('fixedDenominator');
});
it('recomputes counts from disk instead of trusting the report',async()=>{
 f.report.contagem.aprovados=60;f.report.contagem.reprovados=0;f.report.contagem.pendentes=0;await refresh();
 expect((await inspect()).failures).toContain('countsMatchReceipts');
});
it('rejects source drift from the supplied canonical manifest',async()=>{
 await writeFile(path.join(f.workspace,'server/module.ts'),'export const value=2;\n');
 expect((await inspect()).failures).toContain('manifestListedSourcesMatch');
});
it('requires the independently supplied manifest digest',async()=>{
 await writeFile(f.manifestPath,(await readFile(f.manifestPath,'utf8'))+'\n');
 await expect(inspect()).rejects.toThrow('CANONICAL_MANIFEST_DIGEST');
});
it('rejects a manifest inside the worker workspace even with a matching digest',async()=>{
 const inside=path.join(f.workspace,'candidate.json');await json(inside,f.candidate);
 await expect(inspectQualificationEvidence({...f,manifestPath:inside})).rejects.toThrow('MANIFEST_MUST_BE_EXTERNAL');
});
it('rejects a rewritten fixed contract even if a new source manifest was supplied',async()=>{
 await writeFile(path.join(f.workspace,'server/autonomy-score.ts'),'// weakened\n');
 f.candidate.files[0].sha256=hash('// weakened\n');await json(f.manifestPath,f.candidate);
 f.expectedManifestSha256=hash(await readFile(f.manifestPath));
 expect((await inspect()).failures).toContain('fixedContractMatchesTrustedDigest');
});
it('rejects a receipt symlink without following its external contents',async()=>{
 const file=path.join(f.workspace,prefix,'recibos/C01-V1.json');await unlink(file);
 await json(path.join(f.directory,'outside.json'),{secret:'must not be read'});await symlink(path.join(f.directory,'outside.json'),file);
 const result=await inspect();expect(result.failures).toContain('sixtyRegularReceiptFiles');expect(result.failures).toContain('allReceiptHashesAndIdentities');
});
it('rejects a symlinked receipt directory before enumerating external files',async()=>{
 const receipts=path.join(f.workspace,prefix,'recibos'),outside=path.join(f.directory,'outside-receipts');
 await rename(receipts,outside);await symlink(outside,receipts,'dir');
 await expect(inspect()).rejects.toThrow('UNSAFE_EVIDENCE_DIRECTORY');
});
it('rejects an empty execution trace even after all reported hashes were recalculated',async()=>{
 await alterReceipt('C01-V1',r=>r.passosExecutados=[],true);
 expect((await inspect()).failures).toContain('allReceiptHashesAndIdentities');
});
it('does not accept empty negative declarations or a claimed autonomy qualification',async()=>{
 f.report.negativosContrato=[];f.report.scoreQualificationSmoke.qualified=true;await refresh();
 const result=await inspect();expect(result.failures).toContain('negativeAssertionsDeclared');expect(result.failures).toContain('doesNotClaimAutonomyQualification');
});
it('returns a failing CLI exit for invalid evidence without touching source',async()=>{
 await alterReceipt('C01-V1',r=>r.trialId='C99-V1');
 const script=fileURLToPath(new URL('../scripts/inspect-qualification-evidence.ts',import.meta.url));
 const result=await promisify(execFile)(process.execPath,[script,'--workspace',f.workspace,'--candidate',f.manifestPath,
  '--candidate-sha256',f.expectedManifestSha256,'--fixed-contract-sha256',f.expectedFixedContractSha256]).then(()=>null,(e:any)=>e);
 expect(result?.code).toBe(1);expect(JSON.parse(result.stdout).structuralEvidencePassed).toBe(false);
 expect(await readFile(path.join(f.workspace,'server/module.ts'),'utf8')).toBe('export const value=1;\n');
});
