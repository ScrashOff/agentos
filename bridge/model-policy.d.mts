export type ModelPolicy = { version: 1; mode: 'glm-only'; accountId: string; bridgeId: string; model: string; requestLimit: 6; sessionLimit: 12 };
export function readModelPolicy(): ModelPolicy | null;
export function validateModelPolicy(value: unknown): ModelPolicy;
export function modelRouteAllowed(accountId: string, model: string, policy?: ModelPolicy | null): boolean;
export function assertModelRoute(accountId: string, model: string, policy?: ModelPolicy | null): void;
