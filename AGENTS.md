# AgentOS — public development checkout

This repository is a sanitized development snapshot of an isolated agent control plane.

- Do not connect development scripts, tests or workers to an existing production queue, database, credential store or Recall namespace.
- Run project services, builds and tests on an explicitly provisioned development server. Local source editing and Git operations are allowed.
- Keep secrets in external configuration. Never commit environment files, provider credentials, OAuth grants, cookies, database dumps or execution artifacts.
- Network addresses and installation IDs in examples are placeholders. Configure your own isolated environment before operating the bridge.
- A returned model response or exit code is not task acceptance. Preserve immutable artifacts, hashes, criteria and attempt identity.
- Do not restart a live or uncertain execution solely because observation timed out. Confirm termination and reconcile effects first.
- Preserve GLM request concurrency enforcement, model-policy checks and publication guards when changing the runtime.
- Verify meaningful state transitions and database concurrency with tests. Browser checks should use isolated test data.

See README.md and bridge/README.md for the public setup overview.
