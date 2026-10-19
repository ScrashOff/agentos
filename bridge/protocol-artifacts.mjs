// Complete only the standard manifest; collection still checks actual files,
// root boundaries, collisions, sizes and hashes. Missing files remain errors.
export function withProtocolArtifacts(value, testFile) {
  if (!testFile) return value;
  const artifacts = [...value.artifacts];
  for (const item of [
    {name:'protocol-report.json',path:'evidence/protocol-report.json',mime:'application/json'},
    {name:'protocol-tests.log',path:'evidence/protocol-tests.log',mime:'text/plain'},
  ]) if (!artifacts.some(artifact=>artifact.name===item.name)) artifacts.push(item);
  return {...value,artifacts};
}
