/** Offline integrity inspection. No model, application, database or acceptance imports. */
import {createHash} from 'node:crypto';
import {readFile,lstat,realpath,readdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

type Input={workspace:string;manifestPath:string;expectedManifestSha256:string;expectedFixedContractSha256:string};
type Check={name:string;passed:boolean;detail?:unknown};
const hash=(value:Buffer|string)=>createHash('sha256').update(value).digest('hex');
const digest=/^[a-f0-9]{64}$/;
const expectedIds=Array.from({length:20},(_,i)=>Array.from({length:3},(_,j)=>`C${String(i+1).padStart(2,'0')}-V${j+1}`)).flat();
const sameIds=(ids:unknown)=>Array.isArray(ids)&&JSON.stringify([...ids].sort())===JSON.stringify([...expectedIds].sort());
const fail=(code:string):never=>{throw Object.assign(new Error(code),{code});};
const evidencePrefix='evidence/executor-qualification';

export async function inspectQualificationEvidence(input:Input){
 if(!digest.test(input.expectedManifestSha256)||!digest.test(input.expectedFixedContractSha256))fail('TRUSTED_DIGEST_REQUIRED');
 const workspace=path.resolve(input.workspace);
 if((await realpath(workspace))!==workspace||!(await lstat(workspace)).isDirectory())fail('WORKSPACE_NOT_CANONICAL');
 const manifestLocation=await realpath(input.manifestPath);
 if(manifestLocation===workspace||manifestLocation.startsWith(workspace+path.sep))fail('MANIFEST_MUST_BE_EXTERNAL');
 async function bytes(relative:string){
  if(typeof relative!=='string'||path.isAbsolute(relative)||relative.includes('\\')||relative.includes('\0')||relative.split('/').some(p=>['','.','..'].includes(p)))fail('INVALID_EVIDENCE_PATH');
  const file=path.join(workspace,relative),info=await lstat(file);
  if(!info.isFile()||info.isSymbolicLink()||info.size>8*1024*1024||(await realpath(file))!==file)fail('UNSAFE_EVIDENCE_FILE');
  return readFile(file);
 }
 const info=await lstat(input.manifestPath);
 if(!info.isFile()||info.isSymbolicLink()||info.size>8*1024*1024)fail('INVALID_CANONICAL_MANIFEST');
 const manifestBytes=await readFile(input.manifestPath);
 if(hash(manifestBytes)!==input.expectedManifestSha256)fail('CANONICAL_MANIFEST_DIGEST');
 const candidate=JSON.parse(manifestBytes.toString());
 if(!candidate||!Array.isArray(candidate.files)||!candidate.files.length||candidate.files.length>20000||!digest.test(candidate.candidateDigest))fail('CANONICAL_MANIFEST_SHAPE');
 const checks:Check[]=[],check=(name:string,passed:boolean,detail?:unknown)=>checks.push({name,passed,...(detail===undefined?{}:{detail})});
 const seen=new Set<string>(),drift:string[]=[];
 for(const entry of candidate.files){
  if(!entry||typeof entry.path!=='string'||!digest.test(entry.sha256)||seen.has(entry.path))fail('CANONICAL_MANIFEST_INVENTORY');
  seen.add(entry.path);
  try{if(hash(await bytes(entry.path))!==entry.sha256)drift.push(entry.path);}catch{drift.push(entry.path);}
 }
 check('manifestListedSourcesMatch',drift.length===0,{listedFiles:candidate.files.length,drift});
 check('fixedContractIsListed',seen.has('server/autonomy-score.ts'));
 check('fixedContractMatchesTrustedDigest',hash(await bytes('server/autonomy-score.ts'))===input.expectedFixedContractSha256);
 const reportBytes=await bytes(evidencePrefix+'/report.json'),report=JSON.parse(reportBytes.toString());
 const aggregate=JSON.parse((await bytes(evidencePrefix+'/aggregate-receipt.json')).toString());
 const {aggregateSha256,...aggregatePayload}=aggregate;
 check('schemas',report.schema==='agentos/autonomy-executor-integrado-v1'&&aggregate.schema==='agentos/autonomy-aggregate-receipt-v1');
 check('aggregateDigest',hash(JSON.stringify(aggregatePayload))===aggregateSha256);
 check('reportDigest',aggregate.relatorioSha256===hash(reportBytes));
 check('reportBinding',typeof report.runId==='string'&&report.runId.length>0&&aggregate.runId===report.runId&&aggregate.finishedAt===report.finishedAt&&aggregate.relatorioPath===evidencePrefix+'/report.json');
 const reportTrials=Array.isArray(report.hashesPorEnsaio)?report.hashesPorEnsaio:[];
 const aggregateTrials=Array.isArray(aggregate.hashesPorEnsaio)?aggregate.hashesPorEnsaio:[];
 check('sixtyUniqueReportTrials',sameIds(reportTrials.map((r:any)=>r?.trialId)));
 check('sixtyUniqueAggregateTrials',sameIds(aggregateTrials.map((r:any)=>r?.trialId)));
 const receiptDirectory=path.join(workspace,evidencePrefix,'recibos'),directoryInfo=await lstat(receiptDirectory);
 if(!directoryInfo.isDirectory()||directoryInfo.isSymbolicLink()||(await realpath(receiptDirectory))!==receiptDirectory)fail('UNSAFE_EVIDENCE_DIRECTORY');
 const receiptFiles=await readdir(receiptDirectory,{withFileTypes:true});
 check('sixtyRegularReceiptFiles',receiptFiles.every(f=>f.isFile()&&!f.isSymbolicLink()&&f.name.endsWith('.json'))&&sameIds(receiptFiles.map(f=>f.name.replace(/\.json$/,''))));
 const counts={aprovados:0,reprovados:0,pendentes:0},receiptFailures:{trialId:string;code:string}[]=[];
 for(const trialId of expectedIds){
  try{
   const receipt=JSON.parse((await bytes(`${evidencePrefix}/recibos/${trialId}.json`)).toString());
   const {evidenceSha256,...payload}=receipt;
   const records=[reportTrials.find((r:any)=>r?.trialId===trialId),aggregateTrials.find((r:any)=>r?.trialId===trialId)];
   if(!digest.test(evidenceSha256)||hash(JSON.stringify(payload))!==evidenceSha256||records.some(r=>r?.evidenceSha256!==evidenceSha256))fail('RECEIPT_DIGEST');
   if(records.some(r=>r?.arquivo!==`${evidencePrefix}/recibos/${trialId}.json`))fail('RECEIPT_REFERENCE');
   if(receipt.trialId!==trialId||receipt.cenarioId!==trialId.split('-')[0]||receipt.varianteId!==trialId)fail('RECEIPT_IDENTITY');
   const start=Date.parse(receipt.startedAt),end=Date.parse(receipt.finishedAt),deadline=Date.parse(receipt.deadlineAt);
   if(![start,end,deadline].every(Number.isFinite)||end<start||deadline<start||end>deadline)fail('RECEIPT_TIME');
   if(typeof receipt.passed!=='boolean'||typeof receipt.humanIntervention!=='boolean'||receipt.checkedBy!=='deterministic'||!Array.isArray(receipt.passosExecutados)||!receipt.passosExecutados.length)fail('RECEIPT_SHAPE');
   if(receipt.passed&&!receipt.motivoFalha)counts.aprovados++;
   else if(receipt.motivoFalha==='PASSO_FALHOU')counts.reprovados++;
   else counts.pendentes++;
  }catch(error:any){receiptFailures.push({trialId,code:/^[A-Z_]+$/.test(error?.code??'')?error.code:'RECEIPT_UNREADABLE'});}
 }
 check('allReceiptHashesAndIdentities',receiptFailures.length===0,receiptFailures);
 const count=report.contagem??{};
 check('fixedDenominator',count.totalEnsaios===60&&count.denominador===60&&aggregate.totalEnsaios===60&&aggregate.denominador===60&&Object.values(counts).reduce((a,b)=>a+b,0)===60);
 check('countsMatchReceipts',Object.keys(counts).every(key=>counts[key as keyof typeof counts]===count[key]),counts);
 const external=Array.isArray(report.verificacaoExterna)?report.verificacaoExterna:[];
 check('externalFlagsCoverAllSixty',sameIds(external.map((v:any)=>v?.trialId))&&external.every((v:any)=>v?.ok===true));
 check('fixedContractReportBinding',report.identidade?.contratoFixo?.sha256Antes===input.expectedFixedContractSha256&&report.identidade?.contratoFixo?.sha256Depois===input.expectedFixedContractSha256);
 check('negativeAssertionsDeclared',Array.isArray(report.negativosContrato)&&report.negativosContrato.length>0&&report.negativosContrato.every((n:any)=>n?.rejeitado===true)&&report.negativosOk===true);
 check('doesNotClaimAutonomyQualification',report.scoreQualificationSmoke?.qualified===false);
 check('waitDeclaresNoWorker',report.esperas24h?.workerAlocado===false);
 return {schemaVersion:1,observedAt:new Date().toISOString(),workspace,candidateDigest:candidate.candidateDigest,
  canonicalManifestSha256:input.expectedManifestSha256,structuralEvidencePassed:checks.every(c=>c.passed),counts,checks,
  failures:checks.filter(c=>!c.passed).map(c=>c.name),autonomyQualified:false,observation24hProved:false,
  authority:'offline_integrity_inspection_only',
  limits:['Inspects deterministic receipts; does not execute scenario actions or verify live runtime behavior.',
   'Checks every manifest-listed source, not absence of every unlisted workspace file.',
   'Trusted manifest and fixed-contract digests must come from outside worker-controlled output.',
   'Negative declarations and worker-allocation declarations are not independent execution proofs.']};
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{
  const args=process.argv.slice(2),allowed=new Set(['--workspace','--candidate','--candidate-sha256','--fixed-contract-sha256','--output']);
  const options:Record<string,string>={};
  if(args.length%2)fail('CLI_ARGUMENTS');
  for(let i=0;i<args.length;i+=2){if(!allowed.has(args[i])||options[args[i]]!==undefined||!args[i+1])fail('CLI_ARGUMENTS');options[args[i]]=args[i+1];}
  if(['--workspace','--candidate','--candidate-sha256','--fixed-contract-sha256'].some(k=>!options[k]))fail('CLI_ARGUMENTS');
  const result=await inspectQualificationEvidence({workspace:options['--workspace'],manifestPath:options['--candidate'],expectedManifestSha256:options['--candidate-sha256'],expectedFixedContractSha256:options['--fixed-contract-sha256']});
  if(options['--output']){
   const output=path.resolve(options['--output']),parent=await realpath(path.dirname(output));
   if(parent===result.workspace||parent.startsWith(result.workspace+path.sep))fail('OUTPUT_MUST_BE_EXTERNAL');
   const file=path.join(parent,path.basename(output));
   await writeFile(file,JSON.stringify(result,null,2)+'\n',{flag:'wx',mode:0o600});
  }
  console.log(JSON.stringify(result,null,2));if(!result.structuralEvidencePassed)process.exitCode=1;
 }catch(error:any){console.log(JSON.stringify({structuralEvidencePassed:false,autonomyQualified:false,code:/^[A-Z_]+$/.test(error?.code??'')?error.code:'INSPECTION_FAILED'}));process.exitCode=1;}
}
