import {it,expect,vi} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {deliveryRepairProofFailure,verifyDeliveryRepairArtifacts,observeDeliveryRepairArtifacts,deliveryRepairReceiptProof} from '../server/delivery-proof-feedback.js';
const hash=(bytes:Buffer|string)=>createHash('sha256').update(bytes).digest('hex');
function fixture(report:any={fixVerified:true,sourceHashes:[{path:'src/app.js',before:hash('original\n'),after:hash('fixed\n')}]},patch='--- a/src/app.js\n+++ b/src/app.js\n@@ -1 +1 @@\n-original\n+fixed\n'){
  const runId=randomUUID(),bytes=[Buffer.from(patch),Buffer.from(JSON.stringify(report))];
  const refs=bytes.map((value,i)=>({id:randomUUID(),run_id:runId,name:i?'repair-report.json':'implementation.patch',digest:hash(value),size:value.length}));
  const read=vi.fn(async(id:string)=>({...refs.find(ref=>ref.id===id)!,bytes:bytes[refs.findIndex(ref=>ref.id===id)]}));
  return {runId,bytes,refs,read};
}
it('preserves only exact verifier-owned codes, never arbitrary messages or bare suffixes',()=>{
  expect(deliveryRepairProofFailure({code:'REPAIR_SOURCE_PROOF_COVERAGE',message:'PRIVATE'})).toBe('DELIVERY_RESOLUTION_PROOF_COVERAGE');
  for(const error of [{code:'SIZE'}, {code:'OTHER_REPAIR_SOURCE_PROOF_SIZE'}, {message:'REPAIR_SOURCE_PROOF_COVERAGE'}, {code:'REPAIR_SOURCE_PROOF_PRIVATE_SECRET'}])
    expect(deliveryRepairProofFailure(error)).toBe('DELIVERY_RESOLUTION_ARTIFACTS_INVALID');
});
it.each(['missing','duplicate','wrong_run','oversize_patch','oversize_report'])('refuses %s before reading any artifact',async(mode)=>{
  const f=fixture();if(mode==='missing')f.refs.pop();if(mode==='duplicate')f.refs.push({...f.refs[0],id:randomUUID()});
  if(mode==='wrong_run')f.refs[0].run_id=randomUUID();
  if(mode==='oversize_patch')f.refs[0].size=8*1024*1024+1;
  if(mode==='oversize_report')f.refs[1].size=8*1024*1024+1;
  await expect(verifyDeliveryRepairArtifacts(f.refs,f.read,f.runId)).rejects.toThrow(/^DELIVERY_RESOLUTION_/);
  expect(f.read).not.toHaveBeenCalled();
});
it.each(['digest','size','identity'])('refuses an artifact %s mismatch with a typed code',async(mode)=>{
  const f=fixture();if(mode==='digest')f.refs[0].digest='b'.repeat(64);if(mode==='size')f.refs[0].size++;
  const read=mode==='identity'?vi.fn(async(id:string)=>({...await f.read(id),id:randomUUID()})):f.read;
  await expect(verifyDeliveryRepairArtifacts(f.refs,read,f.runId)).rejects.toThrow('DELIVERY_RESOLUTION_ARTIFACT_CHANGED');
});
it('distinguishes legacy hash coverage from malformed diff and never labels either eligible',async()=>{
  const legacy=fixture({fixVerified:true,sourceHashes:{'src/app.js@old':hash('original\n'),'src/app.js@new':hash('fixed\n')}});
  expect(await observeDeliveryRepairArtifacts(legacy.refs,legacy.read,legacy.runId)).toMatchObject({verified:false,code:'DELIVERY_RESOLUTION_PROOF_COVERAGE',liveAuthorization:false});
  const badPatch=fixture(undefined,'arbitrary PRIVATE bytes\n');
  const result=await observeDeliveryRepairArtifacts(badPatch.refs,badPatch.read,badPatch.runId);
  expect(result).toMatchObject({verified:false,code:'DELIVERY_RESOLUTION_PROOF_PATCH_FORMAT'});expect(JSON.stringify(result)).not.toContain('PRIVATE');
});
it('returns current artifact pins for verified proof without claiming application or QA and rereads on every call',async()=>{
  const f=fixture();const result=await observeDeliveryRepairArtifacts(f.refs,f.read,f.runId);
  expect(result).toMatchObject({verified:true,fileCount:1,sourceRunId:f.runId,liveAuthorization:false,
    patchDigest:f.refs[0].digest,reportDigest:f.refs[1].digest});
  expect(result).not.toHaveProperty('acceptance');
  await observeDeliveryRepairArtifacts(f.refs,f.read,f.runId);expect(f.read).toHaveBeenCalledTimes(4);
});
it('sanitizes unavailable storage errors without exposing paths or exception text',async()=>{
  const f=fixture();const result=await observeDeliveryRepairArtifacts(f.refs,async()=>{throw Error('PRIVATE /secret/path');},f.runId);
  expect(result).toMatchObject({verified:false,code:'DELIVERY_RESOLUTION_ARTIFACTS_INVALID'});expect(JSON.stringify(result)).not.toContain('PRIVATE');
});

it.each(['ARTIFACT_CORRUPT','NOT_FOUND','UNKNOWN'])('normalizes authoritative reader failure %s to a recoverable code',async(code)=>{
 const f=fixture(),reader=async()=>{throw Object.assign(Error('PRIVATE storage path'),{code});};
 await expect(verifyDeliveryRepairArtifacts(f.refs,reader,f.runId)).rejects.toThrow(code==='ARTIFACT_CORRUPT'?'DELIVERY_RESOLUTION_ARTIFACT_CHANGED':'DELIVERY_RESOLUTION_ARTIFACTS_INVALID');
});

it.each(['valid','missing','wrong_run','wrong_task','changed_digest','failed','duplicate','missing_attestation'])('observes validation receipt %s using only exact metadata, never bytes',mode=>{
 const f=fixture(),source={id:randomUUID(),run_id:f.runId};
 const check={type:'repair_source_proof',passed:true,evidence:{version:1,patchArtifactId:f.refs[0].id,reportArtifactId:f.refs[1].id,
   patchSha256:f.refs[0].digest,reportSha256:f.refs[1].digest,applicabilityVerified:false,qaVerified:false}};
 const acceptance={id:randomUUID(),task_id:source.id,run_id:f.runId,passed:true,checks:[check]};
 if(mode==='wrong_run')acceptance.run_id=randomUUID();if(mode==='wrong_task')acceptance.task_id=randomUUID();
 if(mode==='changed_digest')f.refs[0].digest='a'.repeat(64);if(mode==='failed')check.passed=false;
 if(mode==='duplicate')acceptance.checks.push(check);if(mode==='missing_attestation')acceptance.checks=[];
 const observed=deliveryRepairReceiptProof(source,f.refs,mode==='missing'?null:acceptance);
 expect(observed).toMatchObject({verified:mode==='valid',liveAuthorization:false,basis:'accepted_validation_receipt'});
 if(mode!=='valid')expect(observed).toMatchObject({status:'unknown',code:'DELIVERY_RESOLUTION_PROOF_NOT_ATTESTED'});
 expect(f.read).not.toHaveBeenCalled();
});
