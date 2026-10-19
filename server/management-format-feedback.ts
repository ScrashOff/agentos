// Only schema-owned paths/codes are persisted. Never reflect received values,
// model prose, regex inputs or the validator's free-form message.
const codes = new Set(['invalid_type', 'too_big', 'too_small', 'invalid_format', 'not_multiple_of',
  'unrecognized_keys', 'invalid_union', 'invalid_key', 'invalid_element', 'invalid_value', 'custom']);
export function managementFormatFeedback(issues: ReadonlyArray<{code: string; path: ReadonlyArray<PropertyKey>}>) {
  const selected = issues.slice(0, 12).map(issue => ({
    code: codes.has(issue.code) ? issue.code : 'SCHEMA_INVALID',
    path: issue.path.slice(0, 12).map(part => typeof part === 'number' && Number.isSafeInteger(part) && part >= 0
      ? part : typeof part === 'string' && /^[a-zA-Z_][a-zA-Z0-9_]{0,79}$/.test(part) ? part : '[field]'),
    ...(issue.path.length === 2 && issue.path[0] === 'nextTransition' && issue.path[1] === 'strategyKey' && issue.code === 'invalid_format'
      ? {expectedPattern: '^[a-z][a-z_-]{9,99}$'} : {}),
    ...(issue.path.length === 2 && issue.path[0] === 'materialContextGap' && issue.path[1] === 'identityDigest' && issue.code === 'invalid_format'
      ? {expectedPattern: '^[a-f0-9]{64}$', expectedSource: 'operation.requiredDecision.contextGapContract.template.identityDigest'} : {}),
  }));
  return {issues: selected, issueCount: issues.length, issuesTruncated: issues.length > selected.length};
}
