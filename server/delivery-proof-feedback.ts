import {createHash} from 'node:crypto';
import {verifyRepairSourceProof} from './repair-proof.js';

const suffixes=['SIZE','ENCODING','JSON','JSON_DUPLICATE_KEY','PATCH_FORMAT','PATCH_PATH','REPORT','COVERAGE'] as const;
export const deliveryRepairProofCodes=suffixes.map(suffix=>'DELIVERY_RESOLUTION_PROOF_'+suffix);
/** Preserve only verifier-owned codes. Never copy artifact text or arbitrary
 * exception messages into a supervisor prompt. */
export function deliveryRepairProofFailure(error:unknown){
  const code=(error as any)?.code;
  const suffix=typeof code==='string'&&code.startsWith('REPAIR_SOURCE_PROOF_')?code.slice('REPAIR_SOURCE_PROOF_'.length):'';
  return (suffixes as readonly string[]).includes(suffix)?'DELIVERY_RESOLUTION_PROOF_'+suffix:'DELIVERY_RESOLUTION_ARTIFACTS_INVALID';
}
export const deliveryRepairProofNextAction='Obtain an explicitly proposed focal workKind:"repair" from the exact retained source and writable consumer, reusing equivalent work. Publish an actual implementation.patch and repair-report.json with fixVerified:true and sourceHashes:[{path,before,after}], one exact entry per changed file and SHA-256 pre/post images (null only for a real add/delete). Preserve the accepted historical artifacts; do not convert ambiguous hashes by inference. A diagnostic or accepted checklist alone cannot replace the source. Select the new accepted frozen repair explicitly; composition and independent QA remain required.';
export const deliveryRepairProofRemedies:Record<string,{cause:string;nextAction:string}>=Object.fromEntries(suffixes.map(suffix=>[
  'DELIVERY_RESOLUTION_PROOF_'+suffix,{cause:{
    SIZE:'The selected repair proof was empty or exceeded the existing artifact size limit.',
    ENCODING:'The selected repair proof was not strict UTF-8 text.',
    JSON:'The selected repair report was not valid bounded JSON.',
    JSON_DUPLICATE_KEY:'The selected repair report contained ambiguous duplicate JSON keys.',
    PATCH_FORMAT:'The selected implementation patch did not satisfy the strict unified-diff contract.',
    PATCH_PATH:'The selected patch contained an unsafe or inconsistent path.',
    REPORT:'The selected repair report did not contain fixVerified:true.',
    COVERAGE:'The selected repair report did not bind exactly every changed patch file to distinct before/after SHA-256 values in the canonical sourceHashes array. Legacy acceptance or free-form hash objects do not supply that proof.',
  }[suffix],nextAction:deliveryRepairProofNextAction},
]));
type Ref={id:string;run_id:string;name:string;digest:string;size:number};
type Reader=(id:string)=>Promise<{bytes:Buffer;id?:string;run_id?:string;name?:string;digest?:string;size?:number}>;
const fail=(code:string):never=>{throw Error(code);};
/** Reads only the two selected artifacts. Used by the existing authoritative
 * resolution check and bounded support discovery; it performs no writes. */
export async function verifyDeliveryRepairArtifacts(refs:Ref[],read:Reader,expectedRunId:string){
  const patch=refs.filter(ref=>ref.name==='implementation.patch'),report=refs.filter(ref=>ref.name==='repair-report.json');
  if(patch.length!==1||report.length!==1)fail('DELIVERY_RESOLUTION_ARTIFACTS_INVALID');
  const selected=[patch[0],report[0]];
  if(!expectedRunId||selected.some(ref=>ref.run_id!==expectedRunId))fail('DELIVERY_RESOLUTION_ARTIFACTS_INVALID');
  if(selected.some(ref=>!Number.isSafeInteger(ref.size)||ref.size<=0||ref.size>8*1024*1024))fail('DELIVERY_RESOLUTION_PROOF_SIZE');
  const bytes:Buffer[]=[];
  for(const ref of selected){
    let value:Awaited<ReturnType<Reader>>;
    try{value=await read(ref.id);}
    catch(error:any){return fail(error?.code==='ARTIFACT_CORRUPT'?'DELIVERY_RESOLUTION_ARTIFACT_CHANGED':'DELIVERY_RESOLUTION_ARTIFACTS_INVALID');}
    if(['id','run_id','name','digest','size'].some(key=>(value as any)[key]!==undefined&&(value as any)[key]!==ref[key as keyof Ref])||
      value.bytes.length!==ref.size||createHash('sha256').update(value.bytes).digest('hex')!==ref.digest)
      fail('DELIVERY_RESOLUTION_ARTIFACT_CHANGED');
    bytes.push(value.bytes);
  }
  try{return verifyRepairSourceProof(bytes[0],bytes[1]);}
  catch(error){return fail(deliveryRepairProofFailure(error));}
}
/** Explanatory observation only. Admission always verifies the current bytes
 * again. There is no cache, source substitution or acceptance in this reader. */
export async function observeDeliveryRepairArtifacts(refs:Ref[],read:Reader,expectedRunId:string){
  try{const proof=await verifyDeliveryRepairArtifacts(refs,read,expectedRunId);
    return {version:1,verified:true,code:'DELIVERY_RESOLUTION_PROOF_VERIFIED',patchDigest:proof.patchSha256,
      reportDigest:proof.reportSha256,fileCount:proof.files.length,sourceRunId:expectedRunId,
      artifacts:refs.filter(ref=>['implementation.patch','repair-report.json'].includes(ref.name)).map(ref=>({id:ref.id,name:ref.name,digest:ref.digest,size:ref.size})),liveAuthorization:false};
  }catch(error:any){const code=deliveryRepairProofCodes.includes(error?.message)||
    ['DELIVERY_RESOLUTION_ARTIFACTS_INVALID','DELIVERY_RESOLUTION_ARTIFACT_CHANGED'].includes(error?.message)?error.message:'DELIVERY_RESOLUTION_ARTIFACTS_INVALID';
    return {version:1,verified:false,code,liveAuthorization:false,nextAction:deliveryRepairProofNextAction};}
}

/** Metadata-only advisory projection of the current validation receipt. Legacy
 * acceptance without this exact structural check is unknown, not invalid.
 * This function is safe under admission locks: it never reads artifact bytes. */
export function deliveryRepairReceiptProof(source:{id:string;run_id:string},refs:Ref[],acceptance:any){
  const unavailable={version:1,verified:false,code:'DELIVERY_RESOLUTION_PROOF_NOT_ATTESTED',
    status:'unknown',basis:'accepted_validation_receipt',sourceRunId:source.run_id,liveAuthorization:false,
    nextAction:'No current canonical repair_source_proof receipt is bound to this artifact pair. Historical acceptance alone is insufficient to advertise replacement eligibility. '+deliveryRepairProofNextAction};
  if(!acceptance||acceptance.task_id!==source.id||acceptance.run_id!==source.run_id||acceptance.passed!==true)return unavailable;
  const patch=refs.filter(ref=>ref.name==='implementation.patch'),report=refs.filter(ref=>ref.name==='repair-report.json');
  if(patch.length!==1||report.length!==1||[...patch,...report].some(ref=>ref.run_id!==source.run_id||!Number.isSafeInteger(ref.size)||ref.size<=0||ref.size>8*1024*1024))return unavailable;
  const checks=Array.isArray(acceptance.checks)?acceptance.checks.filter((check:any)=>check.type==='repair_source_proof'):[];
  if(checks.length!==1)return unavailable;
  const check=checks[0],e=check.evidence;
  if(check.passed!==true||e?.version!==1||e.patchArtifactId!==patch[0].id||e.reportArtifactId!==report[0].id||
    e.patchSha256!==patch[0].digest||e.reportSha256!==report[0].digest||e.applicabilityVerified!==false||e.qaVerified!==false)return unavailable;
  return {version:1,verified:true,status:'attested',code:'DELIVERY_RESOLUTION_PROOF_VERIFIED',basis:'accepted_validation_receipt',
    sourceRunId:source.run_id,acceptanceId:acceptance.id,liveAuthorization:false,
    artifacts:[patch[0],report[0]].map(ref=>({id:ref.id,name:ref.name,digest:ref.digest,size:ref.size})),
    nextAction:'The recorded structural check is bound to these artifact identities. Current authoritative source resolution still rereads bytes and revalidates acceptance/fence; application, composition and QA remain unproved.'};
}
