import { it, expect } from 'vitest';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createScopedPlannerCatalogReaderInput, projectScopedPlannerContext } from '../server/scoped-planner-context.js';

const readerURL = new URL('../scripts/planner-catalog-query.mjs', import.meta.url);
const { queryCatalog } = await import(readerURL.href);
const sha = (value: Buffer | string) => createHash('sha256').update(value).digest('hex');
const valueSha = (value: unknown) => sha(JSON.stringify(value));
const execute = promisify(execFile);
function queryFixture(catalog: unknown) {
  const bytes = Buffer.from(JSON.stringify(catalog)), digest = sha(bytes);
  return { bytes, digest, query: (request: any) => queryCatalog(bytes, digest, request) };
}

it('reads the actual projected previous no-work reference through the mounted hash-bound catalog reader', async () => {
  const directory = await mkdtemp(path.join(tmpdir(), 'agentos-planner-no-work-'));
  const requestId = randomUUID(), previousNoWork = {requestId: randomUUID(), reconciliation: {
    authority: 'observation_only', consumerReadyClaimed: false, criterionAccepted: false,
    entries: [{taskId: randomUUID(), criterionId: 'final', acceptedTaskIds: [randomUUID()],
      pendingTaskIds: [randomUUID()], code: 'BLOCKER_PREREQUISITES_PENDING', evidenceContentRead: false}]}};
  const snapshot = {tasks: [], artifacts: [], coverage: [], repairConsumption: [], deferredDeliveries: [],
    opportunities: {consumers: []}, previousNoWork};
  // assignPlanningRequest mounts this exact snapshot with these three fields.
  const fixture = queryFixture({...snapshot, provenance: [], omitted: [], inputManifest: []});
  const reader = await createScopedPlannerCatalogReaderInput(requestId, fixture.digest, directory);
  const scriptPath = path.join(directory, reader.name), catalogPath = path.join(directory, requestId, 'planning-catalog.json');
  try {
    await mkdir(path.dirname(catalogPath));
    await writeFile(scriptPath, Buffer.from(reader.base64, 'base64'));
    await writeFile(catalogPath, fixture.bytes);
    for (const onDemand of [false, true]) {
      const projection = projectScopedPlannerContext(snapshot, {path: catalogPath, digest: fixture.digest,
        ...(onDemand ? {readerPath: scriptPath, readerDigest: reader.digest} : {})}, []);
      const ref = projection.previousNoWork.record.$catalog;
      const request = {operation: 'read', jsonPath: ref.jsonPath, valueSha256: ref.valueSha256, offset: 0};
      const direct = fixture.query(request);
      const mounted = JSON.parse((await execute(process.execPath, [scriptPath, catalogPath, fixture.digest, JSON.stringify(request)])).stdout);
      expect(mounted).toEqual(direct);
      expect(mounted.reference).toEqual(ref);
      expect(mounted.selection).toMatchObject({offset: 0, complete: true, nextOffset: null});
      expect(Object.fromEntries(mounted.entries.map((entry: any) => [entry.key, entry.value]))).toEqual(previousNoWork);
      const reconciliation = mounted.entries.find((entry: any) => entry.key === 'reconciliation').value;
      expect(reconciliation).toMatchObject({authority: 'observation_only', consumerReadyClaimed: false, criterionAccepted: false});
      await expect(execute(process.execPath, [scriptPath, catalogPath, fixture.digest,
        JSON.stringify({...request, valueSha256: 'b'.repeat(64)})])).rejects.toMatchObject({
          code: 1, stdout: '{"error":"CATALOG_VALUE_MISMATCH"}\n'});
    }
    expect(() => queryFixture({tasks: []}).query({operation: 'read', jsonPath: ['previousNoWork']}))
      .toThrow('CATALOG_QUERY_PATH');
    expect(snapshot.previousNoWork).toEqual(previousNoWork);
  } finally {await rm(directory, {recursive: true, force: true});}
});

it.each(['read', 'directory'])('walks coverage using the nested monotonic cursor and the exact %s entry shape', operation => {
  const coverage = Array.from({length: 29}, (_, index) => ({id: randomUUID(), coverage_key: 'covered-' + index}));
  const {query} = queryFixture({coverage}), recovered: any[] = [], offsets: number[] = [];
  let offset = 0;
  for (;;) {
    const page = query({operation, jsonPath: ['coverage'], offset, limit: 4, maxCharacters: 3000});
    expect(page).not.toHaveProperty('nextOffset');
    expect(page.selection.offset).toBe(offset);
    expect(page.instruction).toContain('response.selection.nextOffset');
    offsets.push(offset);
    for (const entry of page.entries) {
      if (operation === 'read') {
        expect(entry).toHaveProperty('value');expect(entry).not.toHaveProperty('identity');
        recovered.push(entry.value);
      } else {
        expect(entry).toHaveProperty('identity');expect(entry).not.toHaveProperty('value');
        recovered.push(entry.identity);
      }
    }
    const next = page.selection.nextOffset;
    if (next === null) {expect(page.selection.complete).toBe(true);break;}
    expect(Number.isSafeInteger(next)).toBe(true);expect(next).toBeGreaterThan(offset);
    offset = next;
  }
  expect(offsets[0]).toBe(0);expect(new Set(offsets).size).toBe(offsets.length);
  expect(recovered).toEqual(coverage);
});

async function mountedArtifactFixture(artifactBytes: Buffer, name = 'report.json', manifestOverrides: any = {}) {
  const directory = await mkdtemp(path.join(tmpdir(), 'agentos-planner-artifact-')), inputRoot = path.join(directory, 'inputs');
  const requestId = randomUUID(), artifactId = randomUUID();
  const catalog = { tasks: [], provenance: [{ artifactId, name, digest: sha(artifactBytes), role: 'accepted_repair' }],
    omitted: [], inputManifest: [{ id: artifactId, name, digest: sha(artifactBytes), size: artifactBytes.length, ...manifestOverrides }] };
  const fixture = queryFixture(catalog), reader = await createScopedPlannerCatalogReaderInput(requestId, fixture.digest, inputRoot);
  const scriptPath = path.join(directory, reader.name), catalogPath = path.join(inputRoot, requestId, 'planning-catalog.json');
  const artifactDirectory = path.join(inputRoot, artifactId), artifactPath = path.join(artifactDirectory, name);
  await mkdir(path.dirname(catalogPath), { recursive: true }); await mkdir(artifactDirectory);
  await writeFile(scriptPath, Buffer.from(reader.base64, 'base64')); await writeFile(catalogPath, fixture.bytes);
  await writeFile(artifactPath, artifactBytes);
  return { directory, inputRoot, artifactId, artifactDirectory, artifactPath, catalog, digest: fixture.digest,
    query: async (query: any) => JSON.parse((await execute(process.execPath, [scriptPath, catalogPath, fixture.digest,
      JSON.stringify({ operation: 'artifact', artifactId, ...query })])).stdout),
    close: () => rm(directory, { recursive: true, force: true }) };
}

it('queries the actually mounted reader in one execution and verifies raw catalog bytes before printing any content', async () => {
  const directory = await mkdtemp(path.join(tmpdir(), 'agentos-planner-query-'));
  const id = randomUUID(), catalog = { tasks: [{ id, criteria: [{ id: 'exact', label: 'Full original criterion' }] }] };
  const requestId = randomUUID(), fixture = queryFixture(catalog);
  const reader = await createScopedPlannerCatalogReaderInput(requestId, fixture.digest, directory);
  const scriptPath = path.join(directory, reader.name), catalogPath = path.join(directory, requestId, 'planning-catalog.json');
  try {
    await mkdir(path.dirname(catalogPath));
    await writeFile(scriptPath, Buffer.from(reader.base64, 'base64'));
    await writeFile(catalogPath, fixture.bytes);
    const invocation = JSON.stringify({ operation: 'find', jsonPath: ['tasks'], where: { id } });
    const result = JSON.parse((await execute(process.execPath, [scriptPath, catalogPath, fixture.digest, invocation])).stdout);
    expect(result.entries[0]).toMatchObject({ identity: { id }, reference: { jsonPath: ['tasks', 0],
      valueSha256: valueSha(catalog.tasks[0]) } });
    const foreignPath = path.join(directory, 'planning-catalog.json');
    await writeFile(foreignPath, fixture.bytes);
    await expect(execute(process.execPath, [scriptPath, foreignPath, fixture.digest, invocation])).rejects.toMatchObject({
      code: 1, stdout: '{"error":"CATALOG_QUERY_PATH"}\n' });
    await writeFile(catalogPath, JSON.stringify({ tasks: [{ id, secret: 'must-not-print' }] }));
    await expect(execute(process.execPath, [scriptPath, catalogPath, fixture.digest, invocation])).rejects.toMatchObject({
      code: 1, stdout: '{"error":"CATALOG_DIGEST_MISMATCH"}\n' });
    expect(reader.id).not.toBe(requestId);
    expect(await createScopedPlannerCatalogReaderInput(requestId, fixture.digest, directory)).toEqual(reader);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

it('paginates all 147 task identities exactly once including live and closed tasks, with explicit incomplete pages', () => {
  const tasks = Array.from({ length: 147 }, (_, index) => ({ id: randomUUID(),
    state: index % 2 ? 'running' : 'accepted', has_live_run: index % 2 === 1, last_run_id: randomUUID(),
    criteria: [{ id: 'original', label: 'Large authoritative criteria '.repeat(100) }] }));
  const { query } = queryFixture({ tasks });
  const recovered: any[] = []; let offset: number | null = 0;
  while (offset !== null) {
    const result: any = query({ operation: 'directory', jsonPath: ['tasks'], offset, limit: 12, maxCharacters: 3000 });
    expect(JSON.stringify(result).length).toBeLessThanOrEqual(3000);
    expect(result.selection.total).toBe(147);
    expect(result.selection.returned).toBeGreaterThan(0);
    if (result.selection.nextOffset !== null) expect(result.selection.complete).toBe(false);
    recovered.push(...result.entries); offset = result.selection.nextOffset;
  }
  expect(recovered.map(row => row.identity.id)).toEqual(tasks.map(task => task.id));
  expect(new Set(recovered.map(row => row.key)).size).toBe(147);
  for (const [index, entry] of recovered.entries()) {
    expect(entry.identity.last_run_id).toBe(tasks[index].last_run_id);
    expect(entry.reference).toMatchObject({ jsonPath: ['tasks', index], valueSha256: valueSha(tasks[index]) });
  }
});

it('recovers a huge original instruction and escaped Unicode criterion without silent truncation', () => {
  const label = 'Exact evidence 😀\n"quoted" \\ bytes\t'.repeat(5000);
  const task = { id: randomUUID(), intent: label, criteria: [{ id: 'final', label }] };
  const { query } = queryFixture({ tasks: [task] });
  const criteria = query({ operation: 'read', jsonPath: ['tasks', 0, 'criteria'], maxCharacters: 2000 });
  expect(criteria.entries[0]).toMatchObject({ representation: 'query_child', reference: {
    jsonPath: ['tasks', 0, 'criteria', 0], valueSha256: valueSha(task.criteria[0]) } });
  expect(criteria.selection.complete).toBe(true); // complete reference page, not full child bytes
  const criterion = query({ operation: 'read', ...criteria.entries[0].reference, maxCharacters: 2000 });
  const labelReference = criterion.entries.find((entry: any) => entry.key === 'label').reference;
  let recovered = '', offset: number | null = 0;
  while (offset !== null) {
    const page: any = query({ operation: 'read', ...labelReference, offset, maxCharacters: 2000 });
    expect(JSON.stringify(page).length).toBeLessThanOrEqual(2000);
    recovered += page.text; offset = page.selection.nextOffset;
  }
  expect(recovered).toBe(label);
  expect(valueSha(recovered)).toBe(labelReference.valueSha256);
});

it('finds exact sources, artifact hashes, coverage, deferred deliveries and consumption in the same immutable snapshot', () => {
  const taskId = randomUUID(), artifactId = randomUUID(), consumerId = randomUUID();
  const collections: Record<string, any[]> = {
    tasks: [{ id: taskId, criteria: [{ id: 'final', required: true }] }],
    artifacts: [{ id: artifactId, task_id: taskId, digest: 'a'.repeat(64) }],
    coverage: [{ id: randomUUID(), source_task_id: taskId, coverage_key: 'already-covered' }],
    provenance: [{ artifactId, taskId, digest: 'a'.repeat(64), role: 'accepted_repair', proof: { stopped: true } }],
    deferredDeliveries: [{ producer_task_id: taskId, consumer_task_id: consumerId, consumer_proof: { exact: true } }],
    repairConsumption: [{ sourceTaskId: taskId, artifactId, consumers: [{ taskId: consumerId, attempt: 9 }] }],
  };
  const { query, digest } = queryFixture(collections);
  for (const [collection, where] of Object.entries({ tasks: { id: taskId }, artifacts: { id: artifactId },
    coverage: { source_task_id: taskId }, provenance: { artifactId },
    deferredDeliveries: { consumer_task_id: consumerId }, repairConsumption: { sourceTaskId: taskId } })) {
    const result = query({ operation: 'find', jsonPath: [collection], where });
    expect(result.catalogDigest).toBe(digest);
    expect(result.selection).toMatchObject({ complete: true, total: 1 });
    expect(result.entries[0].reference).toMatchObject({ jsonPath: [collection, 0], valueSha256: valueSha(collections[collection][0]) });
  }
  expect(query({ operation: 'find', jsonPath: ['tasks'], where: { id: taskId.slice(0, 8) } }).selection)
    .toMatchObject({ total: 0, returned: 0, complete: true });
});

it('rejects changed value hashes, out-of-range cursors, unsafe paths and ambiguous non-scalar filters', () => {
  const { query } = queryFixture({ tasks: [{ id: 'exact', criteria: [] }] });
  expect(() => query({ operation: 'read', jsonPath: ['tasks', 0], valueSha256: 'b'.repeat(64) })).toThrow('CATALOG_VALUE_MISMATCH');
  for (const jsonPath of [['tasks', '__proto__'], ['tasks', 0, 'constructor'], ['unknown'], ['tasks', '0'], ['tasks', 9]])
    expect(() => query({ operation: 'read', jsonPath })).toThrow('CATALOG_QUERY_PATH');
  for (const offset of [-1, 1.5, 2]) expect(() => query({ operation: 'directory', jsonPath: ['tasks'], offset })).toThrow('CATALOG_QUERY_RANGE');
  expect(() => query({ operation: 'find', jsonPath: ['tasks'], where: { criteria: [] } })).toThrow('CATALOG_QUERY_FILTER');
  expect(() => query({ operation: 'read', jsonPath: ['tasks'], maxCharacters: 999999 })).toThrow('CATALOG_QUERY_RANGE');
});

it('reads mounted artifact content in the same execution with exact digest and bounded lossless pages', async () => {
  const summary = 'Original evidence 😀\n"quoted" \\ unchanged '.repeat(600);
  const bytes = Buffer.from(JSON.stringify({ criteria: [{ id: 'final', evidence: summary }] }));
  const fixture = await mountedArtifactFixture(bytes);
  try {
    const first = await fixture.query({ jsonPath: [], maxCharacters: 2000 });
    expect(first).toMatchObject({ catalogDigest: fixture.digest,
      artifact: { id: fixture.artifactId, digest: sha(bytes), size: bytes.length, format: 'json' } });
    expect(first.entries[0]).toMatchObject({ key: 'criteria', representation: 'query_child',
      reference: { jsonPath: ['criteria'] } });
    let recovered = '', offset: number | null = 0;
    while (offset !== null) {
      const page: any = await fixture.query({ jsonPath: ['criteria', 0, 'evidence'], offset, maxCharacters: 2000,
        valueSha256: valueSha(summary) });
      expect(JSON.stringify(page).length).toBeLessThanOrEqual(2000);
      recovered += page.text; offset = page.selection.nextOffset;
    }
    expect(recovered).toBe(summary);
    await expect(fixture.query({ jsonPath: ['criteria', 0, 'evidence'], valueSha256: 'b'.repeat(64) }))
      .rejects.toMatchObject({ stdout: '{"error":"CATALOG_VALUE_MISMATCH"}\n' });
    await expect(fixture.query({ artifactId: randomUUID() }))
      .rejects.toMatchObject({ stdout: '{"error":"CATALOG_ARTIFACT_NOT_MOUNTED"}\n' });
  } finally { await fixture.close(); }
});

it('fails closed for changed, missing or symlinked mounted artifact bytes without printing their content', async () => {
  const bytes = Buffer.from('Original evidence.');
  const fixture = await mountedArtifactFixture(bytes, 'evidence.log');
  try {
    expect((await fixture.query({})).text).toBe(bytes.toString());
    await writeFile(fixture.artifactPath, 'Tampered evidence!');
    await expect(fixture.query({})).rejects.toMatchObject({ stdout: '{"error":"CATALOG_ARTIFACT_DIGEST"}\n' });
    await rm(fixture.artifactPath);
    await expect(fixture.query({})).rejects.toMatchObject({ stdout: '{"error":"CATALOG_ARTIFACT_NOT_MOUNTED"}\n' });
    const foreign = path.join(fixture.directory, 'foreign');
    await mkdir(foreign); await writeFile(path.join(foreign, 'evidence.log'), bytes);
    await symlink(path.join(foreign, 'evidence.log'), fixture.artifactPath);
    await expect(fixture.query({})).rejects.toMatchObject({ stdout: '{"error":"CATALOG_ARTIFACT_PATH"}\n' });
    await rm(fixture.artifactPath); await rm(fixture.artifactDirectory, { recursive: true });
    await symlink(foreign, fixture.artifactDirectory);
    await expect(fixture.query({})).rejects.toMatchObject({ stdout: '{"error":"CATALOG_ARTIFACT_PATH"}\n' });
  } finally { await fixture.close(); }
});

it('rejects path traversal from a forged manifest even when the catalog itself has a valid digest', async () => {
  const fixture = await mountedArtifactFixture(Buffer.from('Original evidence.'), 'evidence.log', { name: '../foreign.log' });
  try {
    await expect(fixture.query({})).rejects.toMatchObject({ stdout: '{"error":"CATALOG_ARTIFACT_MANIFEST"}\n' });
  } finally { await fixture.close(); }
});
