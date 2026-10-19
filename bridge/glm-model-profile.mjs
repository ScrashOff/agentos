// The connected GLM endpoint implements Z.ai's thinking contract. Keep this
// profile scoped to the verified account/model; never infer it from a prompt.
export function glmModelProfile(accountId, modelId) {
  if (accountId !== 'agentos:api:00000000-0000-4000-8000-000000000001' || modelId !== 'glm-5.3-flash') return {};
  return {
    reasoning: true,
    maxTokens: 32768,
    compat: {
      thinkingFormat: 'zai', supportsReasoningEffort: true,
      maxTokensField: 'max_tokens', supportsStore: false, supportsDeveloperRole: false,
    },
    thinkingLevelMap: { off: null, minimal: 'low', low: 'low', medium: 'high', high: 'high', xhigh: 'max' },
  };
}
