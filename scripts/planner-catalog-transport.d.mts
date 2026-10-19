export const plannerCatalogInputVersion: 'PLANNER_CATALOG_PARTS_V1';
export type PlannerCatalogTransport = { format: typeof plannerCatalogInputVersion; indexDigest: string };
export function createPlannerCatalogInputs(requestId: string, bytes: Buffer): {
  inputs: Array<{ id: string; name: string; digest: string; base64: string }>;
  catalogDigest: string; catalogBytes: number; transport?: PlannerCatalogTransport;
};
export function readPlannerCatalog(binding: { path: string; digest: string; transport?: PlannerCatalogTransport }): Promise<Buffer>;
