export function prepareOpenCodeWorkspace(params: {runId:string;workspace:string;sourceWorkspace?:string}, io: {
  signal:AbortSignal;
  emit:(kind:string,receipt:any)=>unknown;
  persist:(receipt:any)=>Promise<unknown>;
  operation?:(params:any)=>Promise<unknown>;
}):Promise<void>;
