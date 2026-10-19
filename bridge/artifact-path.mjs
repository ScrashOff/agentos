// /workspace is a virtual sandbox path, never an arbitrary host absolute path.
export function artifactPath(value) {
  if (typeof value !== 'string' || /[\\\u0000-\u001f]/.test(value)) throw Error('ARTIFACT_PATH');
  const relative = value.startsWith('/workspace/') ? value.slice('/workspace/'.length) : value;
  if (!relative || relative.startsWith('/') || /^[A-Za-z]:/.test(relative) || relative.split('/').some(p=>!p || p==='.' || p==='..')) throw Error('ARTIFACT_PATH');
  return relative;
}
