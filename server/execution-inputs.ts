import { createHash } from 'node:crypto';
import { DomainError } from './contracts.js';
import {referencedInputLimitBytes} from '../bridge/artifact-input-bundle.mjs';

const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const name = /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/;
export function mergeSupervisorInputs(existing: any[] = [], delivered: any[] = []) {
  return mergeInputs(existing,delivered,24*1024*1024);
}
function mergeInputs(existing:any[],delivered:any[],totalLimit:number){
  if (!Array.isArray(existing) || !Array.isArray(delivered))
    throw new DomainError('SUPERVISION_INPUTS_INVALID', 'Entradas devem ser uma lista.');
  const inputs: any[] = [];
  let total = 0;
  for (const input of [...existing, ...delivered]) {
    if (
      !input ||
      typeof input.id !== 'string' ||
      typeof input.name !== 'string' ||
      typeof input.digest !== 'string' ||
      !uuid.test(input.id) ||
      !name.test(input.name) ||
      typeof input.base64 !== 'string' ||
      !/^[0-9a-f]{64}$/.test(input.digest)
    )
      throw new DomainError(
        'SUPERVISION_INPUTS_INVALID',
        'Identidade ou caminho da entrada inválido.',
      );
    const bytes = Buffer.from(input.base64, 'base64');
    if (
      bytes.toString('base64') !== input.base64 ||
      createHash('sha256').update(bytes).digest('hex') !== input.digest
    )
      throw new DomainError(
        'SUPERVISION_INPUT_DIGEST',
        'SUPERVISION_INPUT_DIGEST: integridade da entrada não confere.',
      );
    if (bytes.length > 8 * 1024 * 1024)
      throw Object.assign(new DomainError('SUPERVISION_INPUTS_TOO_LARGE', 'Entrada excede 8 MiB.'),
        {inputFailure:{phase:'runtime_inputs',subcode:'FILE_BYTES',totalBytes:bytes.length,limitBytes:8*1024*1024}});
    const previous = inputs.find((a) => a.id === input.id);
    if (previous) {
      if (previous.name !== input.name || previous.digest !== input.digest)
        throw new DomainError(
          'SUPERVISION_INPUT_COLLISION',
          'SUPERVISION_INPUT_COLLISION: identidade da entrada foi reutilizada.',
        );
      continue;
    }
    total += bytes.length;
    if (total > totalLimit)
      throw Object.assign(new DomainError('SUPERVISION_INPUTS_TOO_LARGE', 'Entradas excedem a capacidade deste transporte.'),
        {inputFailure:{phase:'runtime_inputs',subcode:'TOTAL_BYTES',totalBytes:total,limitBytes:totalLimit,inputCount:inputs.length+1}});
    inputs.push(input);
  }
  return inputs;
}
export const executionInputManifest = (inputs: any[]) =>
  mergeSupervisorInputs(inputs).map((input) => ({
    id: input.id,
    name: input.name,
    digest: input.digest,
    size: Buffer.byteLength(input.base64, 'base64'),
    source: 'execution_input',
  }));
const normalizedManifest = (entries: any[]) =>
  entries.map((i) => [i.id, i.name, i.digest, i.size, i.source]);
export const inputMime = (input: any) =>
  input.mime ??
  (/\.json$/i.test(input.name)
    ? 'application/json'
    : /\.(?:txt|log|patch|diff|md|mjs|cjs|js|ts|html|css|svg)$/i.test(input.name)
      ? 'text/plain'
      : 'application/octet-stream');

/** Operator inputs are byte evidence, never an invented accepted artifact/run. */
export async function runtimeInputs(
  task: any,
  manifest: any,
  readArtifact: (id: string) => Promise<any>,
  options: {transport?:'referenced-v1'} = {},
) {
  const inputs = mergeSupervisorInputs(task.execution?.inputs ?? []);
  const declared = executionInputManifest(inputs);
  if (
    manifest.executionInputs &&
    JSON.stringify(normalizedManifest(declared)) !==
      JSON.stringify(normalizedManifest(manifest.executionInputs))
  )
    throw Object.assign(new DomainError('INPUT_MANIFEST_CHANGED', 'Entradas diferem do manifesto congelado.'),
      {inputFailure:{phase:'runtime_inputs',subcode:'EXECUTION_INPUT_SET'}});
  const artifacts: any[] = [];
  for (const ref of [
    ...(manifest.dependencyArtifacts ?? []),
    ...(manifest.recoveryArtifacts ?? []),
    ...(manifest.instructionArtifacts ?? []),
  ]) {
    const a = await readArtifact(ref.id);
    if ((ref.digest && ref.digest !== a.digest) || (ref.name && ref.name !== a.name))
      throw Object.assign(new DomainError('INPUT_MANIFEST_CHANGED', 'Artefato difere do manifesto.'),
        {inputFailure:{phase:'runtime_inputs',subcode:'ARTIFACT_METADATA'}});
    artifacts.push({
      id: a.id,
      name: a.name,
      digest: a.digest,
      base64: a.bytes.toString('base64'),
      mime: a.mime,
    });
  }
  return options.transport==='referenced-v1'
    ? mergeInputs(inputs,artifacts,referencedInputLimitBytes)
    : mergeSupervisorInputs(inputs, artifacts);
}
