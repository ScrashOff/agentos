# Private execution bridge

The bridge connects an AgentOS controller to accounts and execution runtimes on a separate server over SSH stdio. It does not provide a public HTTP management endpoint.

Provision the bridge and runtime in an isolated development environment. Configure `AGENTOS_BRIDGE_HOST` with your own SSH destination. The example network addresses in this repository are reserved documentation addresses.

Runtime adapters expect additional dependencies such as the DeepSeek Harness SDK, provider SDKs, Bubblewrap, systemd and optional Playwright browsers. Paths under `/opt/agentos-*` and `/opt/deepseek-harness` describe the adapter layout; these dependencies are not bundled. Inspect the selected adapter before enabling it.

Account credentials belong in private files outside the repository. New account storage uses `/var/lib/agentos-bridge`; its directory and credential files require restrictive permissions. Existing provider stores must remain read-only unless their owner explicitly authorizes a separate login or renewal flow.

Worker workspaces live below `/srv/agentos/workspaces`. Each execution has a fixed account, isolated workspace and recorded attempt identity. Uncertain termination retains its lease until reconciliation. Credentials must not be mounted into worker environments or returned in prompts, artifacts or logs.

The GLM profile includes a shared request gate and an optional exclusive-model policy. Example account identifiers are not usable credentials. Configure the profile and its policy consistently with your own account metadata; preserve fail-closed validation and the global concurrency limit.

Installations require environment-specific provisioning and validation. This repository does not include a preconfigured server, account, tailnet identity or production dataset.
