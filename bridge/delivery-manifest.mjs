// A delivery envelope is packaging, never proof that the task passed.
// The controller supplies a fresh identity in an attempt-private home. Workers
// may reuse real evidence intentionally, but cannot reuse an old envelope.
import { constants } from 'node:fs';
import { open, lstat, realpath, readFile, writeFile, rename } from 'node:fs/promises';
import { createHash, randomUUID } from 'node:crypto';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const artifactName = /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/;
const reservedNames = new Set(['candidate.json', 'changes.patch', 'runtime-protocol-check.json', 'runtime-plan-check.json']);
const sha = value => createHash('sha256').update(value).digest('hex');
const fail = (code, reason) => Object.assign(Error(code), { code, reason });
const relative = value => typeof value === 'string' && value.length <= 1024 &&
  !/[\\\x00-\x1f\x7f]/.test(value) && value.split('/').every(p => p && p !== '.' && p !== '..');
export function extractPromptJsonFields(prompt = '') {
  if (typeof prompt !== 'string') return [];
  const idx = prompt.lastIndexOf('\nCriteria: ');
  if (idx < 0) return [];
  const line = prompt.slice(idx + '\nCriteria: '.length).split('\n')[0]?.trim();
  if (!line) return [];
  try {
    const criteria = JSON.parse(line);
    if (!Array.isArray(criteria)) return [];
    const fields = [];
    for (const c of criteria) {
      if (c?.type !== 'json_field' || !artifactName.test(c.name ?? '') || reservedNames.has(c.name) ||
          typeof c.field !== 'string' || !c.field.trim() || c.field.length > 100) continue;
      const boolean = typeof c.equals === 'boolean';
      if (!fields.some(f => f.name === c.name && f.field === c.field))
        fields.push({ name: c.name, field: c.field, ...(boolean ? { boolean: true } : {}) });
    }
    return fields.slice(0, 30);
  } catch { return []; }
}
export function deliveryContract(runId, requiredArtifacts = [], requiredJsonFields = []) {
  if (!uuid.test(runId)) throw fail('RUN_IDENTITY_MISMATCH', 'invalid_run');
  const jsonFields = Array.isArray(requiredJsonFields) ? requiredJsonFields.filter(Boolean) : [];
  const value = {
    version: 1,
    runId,
    nonce: randomUUID(),
    requiredArtifacts: [...new Set(requiredArtifacts)],
    ...(jsonFields.length ? { requiredJsonFields: jsonFields } : {}),
  };
  checkContract(value);
  return value;
}
function checkContract(value) {
  if (value?.version !== 1 || !uuid.test(value.runId ?? '') || !uuid.test(value.nonce ?? ''))
    throw fail('RESULT_FORMAT', 'manifest_identity');
  if (!Array.isArray(value.requiredArtifacts ?? []) || (value.requiredArtifacts ?? []).length > 30 ||
      (value.requiredArtifacts ?? []).some(name => !artifactName.test(name) || reservedNames.has(name)))
    throw fail('RESULT_FORMAT', 'manifest_required_artifacts');
  if (value.requiredJsonFields !== undefined && (!Array.isArray(value.requiredJsonFields) || value.requiredJsonFields.length > 30 ||
      value.requiredJsonFields.some(f => !f || !artifactName.test(f.name ?? '') || reservedNames.has(f.name) ||
        typeof f.field !== 'string' || !f.field.trim() || f.field.length > 100 ||
        (f.boolean !== undefined && typeof f.boolean !== 'boolean'))))
    throw fail('RESULT_FORMAT', 'manifest_required_json_fields');
}

/** No symlink component, special file, unbounded read or pathname-only check. */
export async function readDeliveryFile(root, relativePath, maximum) {
  if (!relative(relativePath)) throw fail('ARTIFACT_PATH', 'unsafe_path');
  const canonicalRoot = await realpath(root);
  if (path.resolve(root) !== canonicalRoot) throw fail('ARTIFACT_PATH', 'root_symlink');
  const file = path.join(canonicalRoot, ...relativePath.split('/'));
  let current = canonicalRoot;
  for (const component of relativePath.split('/')) {
    current = path.join(current, component);
    const info = await lstat(current);
    if (info.isSymbolicLink()) throw fail('ARTIFACT_PATH', 'symlink_component');
  }
  const handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = await handle.stat();
    if (!before.isFile()) throw fail('ARTIFACT_PATH', 'not_regular_file');
    if (before.size > maximum) throw fail('ARTIFACT_TOO_LARGE', 'size_limit');
    // Linux is the execution platform. Resolve the opened descriptor as well,
    // so a replaced parent cannot redirect the read outside this exact path.
    if (process.platform === 'linux' && await realpath(`/proc/self/fd/${handle.fd}`) !== file)
      throw fail('ARTIFACT_PATH', 'opened_path_changed');
    const bytes = Buffer.alloc(before.size + 1);
    let bytesRead = 0;
    while (bytesRead < bytes.length) {
      const read = await handle.read(bytes, bytesRead, bytes.length - bytesRead, bytesRead);
      if (!read.bytesRead) break;
      bytesRead += read.bytesRead;
    }
    const after = await handle.stat(), currentInfo = await lstat(file);
    if (bytesRead !== before.size || after.size !== before.size || after.mtimeMs !== before.mtimeMs ||
        after.ctimeMs !== before.ctimeMs || currentInfo.isSymbolicLink() ||
        currentInfo.dev !== before.dev || currentInfo.ino !== before.ino || await realpath(file) !== file)
      throw fail('ARTIFACT_CORRUPT', 'file_changed');
    return bytes.subarray(0, bytesRead);
  } finally { await handle.close(); }
}

function envelope(value, contract, sealed) {
  checkContract(contract);
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
      typeof value.summary !== 'string' || !value.summary.trim() || value.summary.length > 32000 ||
      !Array.isArray(value.artifacts) || value.artifacts.length > 30)
    throw fail('RESULT_FORMAT', 'manifest_contract');
  if (sealed && (value.version !== 1 || value.runId !== contract.runId || value.nonce !== contract.nonce))
    throw fail('RESULT_FORMAT', 'manifest_identity');
  const seen = new Set();
  for (const item of value.artifacts) {
    if (!item || !artifactName.test(item.name ?? '') ||
        seen.has(item.name) || reservedNames.has(item.name) ||
        !relative(item.path) || item.path === 'evidence/delivery-draft.json' ||
        path.posix.basename(String(item.path)) === 'delivery-draft.json' ||
        (item.mime != null && (typeof item.mime !== 'string' || item.mime.length > 160)))
      throw fail('ARTIFACT_PATH', item?.path === 'evidence/delivery-draft.json' || path.posix.basename(String(item?.path)) === 'delivery-draft.json'
        ? 'delivery_draft_is_not_a_task_artifact' : 'manifest_artifact');
    if (sealed && (!/^[a-f0-9]{64}$/.test(item.digest ?? '') || !Number.isSafeInteger(item.size) || item.size < 0))
      throw fail('RESULT_FORMAT', 'manifest_artifact_digest');
    seen.add(item.name);
  }
  if ((contract.requiredArtifacts ?? []).some(name => !seen.has(name))) throw fail('ARTIFACT_MISSING', 'required_artifact_absent');
}

async function artifactBytes(root, name) {
  try { return await readDeliveryFile(root, name, 8 * 1024 * 1024); }
  catch (error) {
    if (error.code === 'ENOENT') throw fail('ARTIFACT_MISSING', 'file_not_found');
    if (error.code === 'ELOOP' || error.code === 'ENOTDIR') throw fail('ARTIFACT_PATH', 'path_changed');
    throw error;
  }
}

export async function sealDeliveryManifest(root, draft, contract) {
  envelope(draft, contract, false);
  let total = 0;
  const artifacts = [];
  const requiredFields = contract.requiredJsonFields ?? [];
  for (const item of draft.artifacts) {
    const bytes = await artifactBytes(root, item.path);
    total += bytes.length;
    if (total > 24 * 1024 * 1024) throw fail('ARTIFACT_TOO_LARGE', 'total_size_limit');
    const fieldReqs = requiredFields.filter(f => f.name === item.name);
    if (fieldReqs.length || item.name === 'delivery.json' || item.name === 'verification.json') {
      let parsed;
      try { parsed = JSON.parse(bytes.toString('utf8')); }
      catch { throw fail('RESULT_FORMAT', `artifact_invalid_json:${item.name}`); }
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed))
        throw fail('RESULT_FORMAT', `artifact_json_object_required:${item.name}`);
      if (typeof parsed.summary === 'string' && Array.isArray(parsed.artifacts) && !Object.hasOwn(parsed, 'passed') && !Object.hasOwn(parsed, 'validated'))
        throw fail('RESULT_FORMAT', `artifact_contains_manifest_envelope_instead_of_criterion_report:${item.name}`);
      for (const req of fieldReqs) {
        if (!Object.hasOwn(parsed, req.field))
          throw fail('RESULT_FORMAT', `artifact_json_field_missing:${item.name}:${req.field}`);
        if (req.boolean && typeof parsed[req.field] !== 'boolean')
          throw fail('RESULT_FORMAT', `artifact_json_field_not_boolean:${item.name}:${req.field}`);
      }
    }
    artifacts.push({ name: item.name, path: item.path, mime: item.mime ?? 'application/octet-stream',
      digest: sha(bytes), size: bytes.length });
  }
  return { ...contract, summary: draft.summary, artifacts };
}

export async function collectDeliveryManifest(root, home, contract) {
  checkContract(contract);
  let text;
  try { text = await readDeliveryFile(home, 'delivery.json', 128 * 1024); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  let value;
  try { value = JSON.parse(text.toString('utf8')); }
  catch { throw fail('RESULT_FORMAT', 'manifest_json'); }
  envelope(value, contract, true);
  let total = 0;
  const artifacts = [];
  for (const item of value.artifacts) {
    const bytes = await artifactBytes(root, item.path);
    total += bytes.length;
    if (total > 24 * 1024 * 1024) throw fail('ARTIFACT_TOO_LARGE', 'total_size_limit');
    if (bytes.length !== item.size || sha(bytes) !== item.digest) throw fail('ARTIFACT_CORRUPT', 'manifest_bytes_changed');
    artifacts.push({ name: item.name, mime: item.mime, digest: item.digest, base64: bytes.toString('base64') });
  }
  return { value: { summary: value.summary, artifacts: value.artifacts }, artifacts,
    manifestDigest: sha(text), runId: contract.runId };
}

export async function prepareDeliveryManifest(home, contract) {
  checkContract(contract);
  await writeFile(`${home}/delivery-contract.json`, JSON.stringify(contract), { flag: 'wx', mode: 0o600 });
  await writeFile(`${home}/delivery-validator.mjs`, await readFile(new URL('./delivery-manifest.mjs', import.meta.url)), { flag: 'wx', mode: 0o600 });
}
export function deliveryManifestInstructionFor(contract) {
  const required = contract?.requiredArtifacts?.length ? contract.requiredArtifacts : ['report.json'];
  const exampleArtifacts = required.map(name => ({
    name,
    path: `evidence/${name}`,
    mime: name.endsWith('.json') ? 'application/json' : 'text/plain',
  }));
  const jsonFields = contract?.requiredJsonFields?.length
    ? ` Required JSON criterion fields to include inside those evidence files (with true or false according to real execution): ${JSON.stringify(contract.requiredJsonFields)}.`
    : '';
  return `
DELIVERY_MANIFEST_V1: Antes de terminar, leia requiredArtifacts em /home/agent/delivery-contract.json.${jsonFields}
1. Grave primeiro cada artefato de critério exigido em /workspace/evidence/<name> (por exemplo ${required.map(n => `evidence/${n}`).join(', ')}) contendo o relatório técnico real da tarefa (como {"passed":true,...} ou {"passed":false,...}). NUNCA aponte um artefato de critério para evidence/delivery-draft.json.
2. Em seguida, salve o rascunho da lista de entrega em /workspace/evidence/delivery-draft.json com {"summary":"resultado real e limitações","artifacts":${JSON.stringify(exampleArtifacts)}}. Preserve falhas nos relatórios; este envelope não aprova a tarefa.
3. Execute:
node /home/agent/delivery-validator.mjs /workspace /home/agent/delivery-contract.json /workspace/evidence/delivery-draft.json /home/agent/delivery.json
O validador verifica caminhos, tamanhos, campos JSON obrigatórios e hashes, vincula a entrega à tentativa atual e grava /home/agent/delivery.json. Cada arquivo deve ter no máximo 8 MiB; a entrega inteira, 24 MiB e 30 arquivos. Corrija erros apontados pelo validador sem refazer testes válidos. Quando o validador imprimir DELIVERY_MANIFEST_VALIDATED, /home/agent/delivery.json já está selado como autoridade de coleta: encerre imediatamente respondendo apenas com o JSON curto {"summary":"...","artifacts":${JSON.stringify(exampleArtifacts)}}. Se editar qualquer artefato após validar, execute o validador novamente. Não copie delivery.json de outra tentativa nem declare candidate.json/changes.patch; o runtime gera esses dois arquivos.
`;
}
export const deliveryManifestInstruction = deliveryManifestInstructionFor();

if (process.argv[1] && import.meta.url === pathToFileURL(await realpath(process.argv[1])).href) {
  try {
    const [root, contractFile, draftFile, output] = process.argv.slice(2);
    if (!root || !contractFile || !draftFile || !output || path.basename(output) !== 'delivery.json') throw fail('RESULT_FORMAT', 'validator_arguments');
    const contract = JSON.parse((await readDeliveryFile(path.dirname(contractFile), path.basename(contractFile), 4096)).toString());
    const draft = JSON.parse((await readDeliveryFile(root, path.relative(root, draftFile).split(path.sep).join('/'), 128 * 1024)).toString());
    const value = await sealDeliveryManifest(root, draft, contract);
    // Exclusive temporary output and atomic rename; do not truncate a symlink.
    if (await realpath(path.dirname(output)) !== path.dirname(output)) throw fail('ARTIFACT_PATH', 'output_directory');
    const temporary = `${output}.${randomUUID()}.tmp`;
    await writeFile(temporary, JSON.stringify(value), { flag: 'wx', mode: 0o600 });
    await rename(temporary, output);
    console.log(JSON.stringify({ code: 'DELIVERY_MANIFEST_VALIDATED', runId: contract.runId, artifacts: value.artifacts.length }));
  } catch (error) { console.log(JSON.stringify({ error: error.code ?? 'RESULT_FORMAT', reason: error.reason ?? 'validator_failed' })); process.exitCode = 1; }
}
