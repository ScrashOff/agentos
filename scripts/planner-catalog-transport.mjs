import { createHash as pcCreateHash } from 'node:crypto';
import { constants as pcConstants } from 'node:fs';
import { open as pcOpen, realpath as pcRealpath } from 'node:fs/promises';
import pcPath from 'node:path';

export const plannerCatalogInputVersion = 'PLANNER_CATALOG_PARTS_V1';
const pcIndividual = 8 * 1024 * 1024, pcTotal = 24 * 1024 * 1024, pcChunk = 4 * 1024 * 1024;
const pcUuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/, pcDigest = /^[a-f0-9]{64}$/;
const pcHash = bytes => pcCreateHash('sha256').update(bytes).digest('hex');
const pcFail = code => { throw Object.assign(Error(code), { code }); };
function pcPartId(requestId, index, digest) {
  const text = pcHash('planner-catalog-part:' + requestId + ':' + index + ':' + digest).slice(0, 32);
  return `${text.slice(0,8)}-${text.slice(8,12)}-5${text.slice(13,16)}-${(8 | (parseInt(text[16],16) & 3)).toString(16)}${text.slice(17,20)}-${text.slice(20)}`;
}

/** Preserve the exact legacy catalog bytes below the input limit. Oversized
 * catalogs use byte parts, so even one enormous JSON string remains lossless. */
export function createPlannerCatalogInputs(requestId, bytes) {
  if (!pcUuid.test(requestId) || !Buffer.isBuffer(bytes)) pcFail('CATALOG_INPUT_BINDING');
  if (bytes.length > pcTotal) pcFail('SUPERVISION_INPUTS_TOO_LARGE');
  const catalogDigest = pcHash(bytes), catalogBytes = bytes.length;
  const input = (id, name, value) => ({ id, name, digest: pcHash(value), base64: value.toString('base64') });
  if (bytes.length <= pcIndividual) return { inputs: [input(requestId, 'planning-catalog.json', bytes)], catalogDigest, catalogBytes };
  const inputs = [], parts = [];
  for (let offset = 0, index = 0; offset < bytes.length; offset += pcChunk, index++) {
    const value = bytes.subarray(offset, Math.min(offset + pcChunk, bytes.length)), digest = pcHash(value);
    const id = pcPartId(requestId, index, digest), name = `planning-catalog-part-${index}.bin`;
    parts.push({ index, id, name, digest, size: value.length }); inputs.push(input(id, name, value));
  }
  const indexBytes = Buffer.from(JSON.stringify({ format: plannerCatalogInputVersion, version: 1,
    requestId, catalogDigest, catalogBytes, parts }));
  if (indexBytes.length > pcIndividual || indexBytes.length + bytes.length > pcTotal) pcFail('SUPERVISION_INPUTS_TOO_LARGE');
  const indexInput = input(requestId, 'planning-catalog.json', indexBytes);
  return { inputs: [indexInput, ...inputs], catalogDigest, catalogBytes,
    transport: { format: plannerCatalogInputVersion, indexDigest: indexInput.digest } };
}

async function pcRead(filename, maximum, expectedSize) {
  if (!pcPath.isAbsolute(filename) || pcPath.normalize(filename) !== filename) pcFail('CATALOG_INPUT_PATH');
  let handle;
  try {
    if (await pcRealpath(filename) !== filename) pcFail('CATALOG_INPUT_PATH');
    handle = await pcOpen(filename, pcConstants.O_RDONLY | pcConstants.O_NOFOLLOW | pcConstants.O_NONBLOCK);
    const before = await handle.stat();
    if (!before.isFile() || before.size > maximum || (expectedSize !== undefined && before.size !== expectedSize)) pcFail('CATALOG_INPUT_SIZE');
    if (process.platform === 'linux' && await pcRealpath('/proc/self/fd/' + handle.fd) !== filename) pcFail('CATALOG_INPUT_PATH');
    // Bounded descriptor reads also reject a file growing after the stat.
    const buffer = Buffer.alloc(before.size + 1); let offset = 0;
    while (offset < buffer.length) {
      const { bytesRead } = await handle.read(buffer, offset, buffer.length - offset, offset);
      if (!bytesRead) break; offset += bytesRead;
    }
    const after = await handle.stat();
    if (offset !== before.size || before.size !== after.size || before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs)
      pcFail('CATALOG_INPUT_CHANGED');
    return buffer.subarray(0, offset);
  } catch (error) {
    if (error?.code === 'ENOENT') pcFail('CATALOG_INPUT_MISSING');
    if (error?.code === 'ELOOP') pcFail('CATALOG_INPUT_PATH');
    throw error;
  } finally { await handle?.close(); }
}

/** The reader and preflight use this same transport verifier. The logical
 * digest always identifies the original JSON bytes, never merely the index. */
export async function readPlannerCatalog(binding) {
  if (!binding || !pcDigest.test(binding.digest ?? '') || typeof binding.path !== 'string') pcFail('CATALOG_INPUT_BINDING');
  const bytes = await pcRead(binding.path, pcIndividual);
  if (binding.transport === undefined) {
    if (pcHash(bytes) !== binding.digest) pcFail('CATALOG_DIGEST_MISMATCH');
    return bytes;
  }
  const transport = binding.transport;
  if (transport?.format !== plannerCatalogInputVersion || !pcDigest.test(transport.indexDigest ?? '') ||
    Object.keys(transport).sort().join(',') !== 'format,indexDigest') pcFail('CATALOG_INDEX_BINDING');
  if (pcHash(bytes) !== transport.indexDigest) pcFail('CATALOG_INDEX_DIGEST');
  let index; try { index = JSON.parse(bytes.toString('utf8')); } catch { pcFail('CATALOG_INDEX_FORMAT'); }
  const requestId = pcPath.basename(pcPath.dirname(binding.path));
  if (!index || Object.keys(index).sort().join(',') !== 'catalogBytes,catalogDigest,format,parts,requestId,version' ||
    index.format !== plannerCatalogInputVersion || index.version !== 1 || !pcUuid.test(requestId) || index.requestId !== requestId ||
    pcPath.basename(binding.path) !== 'planning-catalog.json' || index.catalogDigest !== binding.digest ||
    !Number.isSafeInteger(index.catalogBytes) || index.catalogBytes <= pcIndividual || index.catalogBytes > pcTotal ||
    !Array.isArray(index.parts) || index.parts.length !== Math.ceil(index.catalogBytes / pcChunk)) pcFail('CATALOG_INDEX_FORMAT');
  const seen = new Set(); let total = bytes.length;
  for (let i = 0; i < index.parts.length; i++) {
    const part = index.parts[i], expectedSize = Math.min(pcChunk, index.catalogBytes - i * pcChunk);
    if (!part || Object.keys(part).sort().join(',') !== 'digest,id,index,name,size' || part.index !== i ||
      !pcDigest.test(part.digest ?? '') || part.id !== pcPartId(requestId, i, part.digest) ||
      part.name !== `planning-catalog-part-${i}.bin` || part.size !== expectedSize || part.size > pcIndividual ||
      part.id === requestId || seen.has(part.id)) pcFail('CATALOG_PART_IDENTITY');
    seen.add(part.id); total += part.size;
  }
  // Validate the complete manifest before reading or allocating any part.
  if (total > pcTotal) pcFail('CATALOG_INPUT_TOTAL');
  const root = pcPath.dirname(pcPath.dirname(binding.path)), parts = [];
  for (const part of index.parts) {
    const value = await pcRead(pcPath.join(root, part.id, part.name), pcIndividual, part.size);
    if (pcHash(value) !== part.digest) pcFail('CATALOG_PART_DIGEST'); parts.push(value);
  }
  const reconstructed = Buffer.concat(parts, index.catalogBytes);
  if (reconstructed.length !== index.catalogBytes || pcHash(reconstructed) !== binding.digest) pcFail('CATALOG_DIGEST_MISMATCH');
  return reconstructed;
}
