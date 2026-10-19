export interface TaskDeliveryArtifact { name: string; path: string; mime: string }
export interface TaskDeliveryContract { version: 1; artifacts: TaskDeliveryArtifact[] }
export function taskDeliveryContract(criteria?: unknown): TaskDeliveryContract;
export function validateTaskDeliveryContract(value: unknown): TaskDeliveryContract;
export function museTaskDeliveryInstruction(value?: TaskDeliveryContract): string;
