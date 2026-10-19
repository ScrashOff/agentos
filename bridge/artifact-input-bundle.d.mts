export const referencedInputLimitBytes:number;
export const referencedInputFileLimitBytes:number;
export const referencedInputCountLimit:number;
export const inputBundleRoot:string;
export function inputBundleDigest(bytes:string|Buffer):string;
export function inputBundleValueDigest(value:unknown):string;
export function hydrateArtifactInputBundle(params:any,signal:AbortSignal,roots?:{bundleRoot?:string;artifactRoot?:string}):Promise<any>;
