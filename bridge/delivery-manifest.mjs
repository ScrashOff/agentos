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
export function deliveryContract(runId, requiredArtifacts = []) {
  if (!uuid.test(runId)) throw fail('RUN_IDENTITY_MISMATCH', 'invalid_run');
  const value = { version: 1, runId, nonce: randomUUID(), requiredArtifacts: [...new Set(requiredArtifacts)] };
  checkContract(value);
  return value;
}
function checkContract(value) {
  if (value?.version !== 1 || !uuid.test(value.runId ?? '') || !uuid.test(value.nonce ?? ''))
    throw fail('RESULT_FORMAT', 'manifest_identity');
  if (!Array.isArray(value.requiredArtifacts ?? []) || (value.requiredArtifacts ?? []).length > 30 ||
      (value.requiredArtifacts ?? []).some(name => !artifactName.test(name) || reservedNames.has(name)))
    throw fail('RESULT_FORMAT', 'manifest_required_artifacts');
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
        !relative(item.path) || (item.mime != null && (typeof item.mime !== 'string' || item.mime.length > 160)))
      throw fail('ARTIFACT_PATH', 'manifest_artifact');
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
  for (const item of draft.artifacts) {
    const bytes = await artifactBytes(root, item.path);
    total += bytes.length;
    if (total > 24 * 1024 * 1024) throw fail('ARTIFACT_TOO_LARGE', 'total_size_limit');
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
export const deliveryManifestInstruction = `
DELIVERY_MANIFEST_V1: Antes de terminar, leia requiredArtifacts em /home/agent/delivery-contract.json e salve evidence/delivery-draft.json com {"summary":"resultado real e limitações","artifacts":[{"name":"report.json","path":"evidence/report.json","mime":"application/json"}]}, usando os artefatos reais exigidos pela sua tarefa, não apenas o nome do exemplo. Preserve falhas nos relatórios; este envelope não aprova a tarefa. Execute:
node /home/agent/delivery-validator.mjs /workspace /home/agent/delivery-contract.json /workspace/evidence/delivery-draft.json /home/agent/delivery.json
O validador verifica caminhos, tamanhos e hashes, vincula a entrega à tentativa atual e grava delivery.json. Cada arquivo deve ter no máximo 8 MiB; a entrega inteira, 24 MiB e 30 arquivos. Preserve logs brutos no workspace; se necessário, publique evidência relevante em arquivos menores com referência explícita ao original, sem esconder falhas ou dispensar artefatos obrigatórios. Corrija erros de embalagem sem refazer testes válidos. Depois responda com o JSON summary/artifacts. Se editar qualquer artefato após validar, execute o validador novamente. Não copie delivery.json de outra tentativa nem declare candidate.json/changes.patch; o runtime gera esses dois arquivos.
`;

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
