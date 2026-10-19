export interface CompositionReference {id:string;name:string;digest:string}
export interface CompositionInput extends CompositionReference {base64:string;mime?:string}
export interface BoundComposition {version:1;patches:Array<{producerTaskId:string;patch:CompositionInput;report:CompositionInput}>}
export function bindCompositionInputs(plan:unknown,inputs?:CompositionInput[]):BoundComposition;
