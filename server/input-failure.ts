/** Only structural diagnostics; never preserve raw exception/provider text. */
export function inputFailureMetadata(error:any){
 if(!['INPUT_MANIFEST_CHANGED','SUPERVISION_INPUTS_TOO_LARGE','INPUT_BUNDLE_INVALID','INPUT_BUNDLE_DIGEST',
  'INPUT_BUNDLE_CAPACITY','INPUT_BUNDLE_OWNER_CHANGED','INPUT_BUNDLE_STORE_UNAVAILABLE'].includes(error?.code))return null;
 const data:any={code:error.code};
 if(/^[A-Z0-9_]{1,80}$/.test(error?.inputFailure?.subcode??''))data.subcode=error.inputFailure.subcode;
 if(/^[a-z_]{1,60}$/.test(error?.inputFailure?.phase??''))data.phase=error.inputFailure.phase;
 for(const key of ['inputCount','totalBytes','limitBytes'])if(Number.isSafeInteger(error?.inputFailure?.[key])&&error.inputFailure[key]>=0)
  data[key]=error.inputFailure[key];
 return data;
}
