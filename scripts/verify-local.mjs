import 'dotenv/config';
const origin = process.env.AGENTOS_ORIGIN ?? 'http://127.0.0.1:4280';
const response = await fetch(origin + '/api/state', {
  headers: { Authorization: 'Bearer ' + process.env.AGENTOS_API_TOKEN },
  signal: AbortSignal.timeout(5000),
});
if (!response.ok) throw Error('AgentOS HTTP ' + response.status);
const state = await response.json();
if (
  state.engine?.error ||
  !state.engine?.lastTick ||
  Date.now() - new Date(state.engine.lastTick).getTime() > 15000
)
  throw Error('Scheduler is not healthy.');
console.log(
  JSON.stringify(
    {
      url: origin,
      scheduler: 'healthy',
      definitions: state.agents.length,
      demonstrationsCompleted: state.goals.filter((g) => g.is_demo && g.state === 'completed')
        .length,
      activeRuns: state.runs.filter((r) => r.state === 'running').length,
      artifacts: state.artifacts.length,
      contexts: state.contexts.length,
      configuredIntegrations: state.integrations,
      recall: state.recall,
    },
    null,
    2,
  ),
);
