import { pool, tx } from './db.js';
const definitions: Record<string, Array<[string, string, string[]]>> = {
  engineering: [
    ['frontend.react', 'React', ['react', 'typescript', 'frontend']],
    ['frontend.vue', 'Vue', ['vue', 'frontend']],
    ['frontend.angular', 'Angular', ['angular', 'frontend']],
    ['frontend.svelte', 'Svelte', ['svelte', 'frontend']],
    ['frontend.next', 'Next.js', ['nextjs', 'react']],
    ['frontend.accessibility', 'Acessibilidade', ['wcag', 'html', 'accessibility']],
    ['frontend.css', 'Design de interfaces', ['css', 'design', 'responsive']],
    ['backend.node', 'Node.js', ['nodejs', 'typescript', 'api']],
    ['backend.python', 'Python', ['python', 'api']],
    ['backend.go', 'Go', ['golang', 'api']],
    ['backend.java', 'Java', ['java', 'api']],
    ['backend.dotnet', '.NET', ['dotnet', 'csharp']],
    ['database.postgres', 'PostgreSQL', ['postgres', 'sql', 'performance']],
    ['database.redis', 'Redis', ['redis', 'cache']],
    ['database.migrations', 'Migrações', ['sql', 'migrations']],
    ['architecture', 'Arquitetura', ['architecture', 'system-design']],
    ['integration', 'Integração de código', ['git', 'integration']],
    ['api.design', 'Contratos de API', ['openapi', 'rest']],
    ['performance', 'Performance', ['performance', 'profiling']],
    ['mobile', 'Aplicações móveis', ['mobile', 'react-native']],
  ],
  research: [
    ['web', 'Pesquisa web', ['web', 'research']],
    ['github', 'Pesquisa de código', ['github', 'code']],
    ['documentation', 'Documentação técnica', ['docs', 'research']],
    ['requirements', 'Requisitos', ['requirements', 'product']],
    ['data', 'Análise de dados', ['data', 'statistics']],
    ['benchmarks', 'Benchmarks', ['benchmark', 'evaluation']],
    ['alternatives', 'Análise de alternativas', ['comparison', 'architecture']],
    ['memory', 'Curadoria de memória', ['memory', 'evidence']],
  ],
  ops: [
    ['docker', 'Containers', ['docker', 'containers']],
    ['kubernetes', 'Kubernetes', ['kubernetes', 'infra']],
    ['cloudflare', 'Cloudflare', ['cloudflare', 'dns']],
    ['network', 'Redes e Tailscale', ['tailscale', 'network']],
    ['linux', 'Linux', ['linux', 'systemd']],
    ['observability', 'Observabilidade', ['metrics', 'tracing']],
    ['backup', 'Backup e restauração', ['backup', 'recovery']],
    ['release', 'Publicação', ['release', 'deployment']],
    ['storage', 'Armazenamento', ['storage', 'filesystems']],
    ['capacity', 'Capacidade', ['capacity', 'performance']],
  ],
  security: [
    ['code-review', 'Revisão de segurança', ['security', 'code-review']],
    ['infrastructure', 'Segurança de infraestrutura', ['security', 'infra']],
    ['authentication', 'Autenticação', ['oauth', 'authentication']],
    ['authorization', 'Autorização', ['authorization', 'permissions']],
    ['dependencies', 'Dependências', ['dependencies', 'supply-chain']],
    ['privacy', 'Privacidade', ['privacy', 'data']],
  ],
  qa: [
    ['backend', 'Testes de backend', ['api', 'testing']],
    ['frontend', 'Testes de interface', ['browser', 'testing']],
    ['integration', 'Testes de integração', ['integration', 'testing']],
    ['regression', 'Regressão', ['regression', 'testing']],
    ['performance', 'Teste de carga', ['load', 'performance']],
    ['accessibility', 'Validação acessível', ['accessibility', 'testing']],
    ['acceptance', 'Critérios de aceite', ['acceptance', 'evidence']],
    ['artifacts', 'Integridade de artefatos', ['artifact', 'sha256']],
  ],
};
export async function seedRegistry() {
  await tx(async (db) => {
    for (const [department, rows] of Object.entries(definitions)) {
      await db.query(
        `INSERT INTO agents(id,name,department,capabilities,runtime,description) VALUES($1,$2,$3,$4,'harness',$5) ON CONFLICT DO NOTHING`,
        [
          department + '.supervisor',
          'Supervisor · ' + department,
          department,
          JSON.stringify([department, 'planning', 'review']),
          'Organiza decisões semânticas da equipe e propõe tarefas ao orquestrador.',
        ],
      );
      for (const [key, name, caps] of rows)
        await db.query(
          `INSERT INTO agents(id,name,department,reports_to,capabilities,runtime,description) VALUES($1,$2,$3,$4,$5,'harness',$6) ON CONFLICT DO NOTHING`,
          [
            department + '.' + key,
            name,
            department,
            department + '.supervisor',
            JSON.stringify(caps),
            'Especialização declarada. Requer runtime configurado e validação no projeto.',
          ],
        );
    }
    await db.query(
      `INSERT INTO agents(id,name,department,capabilities,runtime,description,status) VALUES('system.validation','Validação do orquestrador','qa','["artifact","dag","fixture"]','builtin','Executa exclusivamente o exemplo técnico local; não usa um LLM.','validated') ON CONFLICT DO NOTHING`,
    );
    await db.query(
      `INSERT INTO accounts(id,label,provider,model,enabled,max_concurrency,quota_status,allow_unknown) VALUES('local-validation','Validação local','local','deterministic',true,4,'not_applicable',false) ON CONFLICT DO NOTHING`,
    );
  });
}
export async function searchAgents(query = '', capabilities: string[] = []) {
  const rows = (await pool.query('SELECT * FROM agents ORDER BY department,name')).rows;
  const terms = [
    ...query.toLowerCase().split(/\W+/).filter(Boolean),
    ...capabilities.map((s) => s.toLowerCase()),
  ];
  return rows
    .map((a) => ({
      ...a,
      score: terms.length
        ? terms.reduce(
            (n, t) =>
              n +
              (a.capabilities.includes(t)
                ? 3
                : (a.id + ' ' + a.name + ' ' + a.description).toLowerCase().includes(t)
                  ? 1
                  : 0),
            0,
          )
        : 1,
    }))
    .filter((a) => a.score > 0)
    .sort((a, b) => b.score - a.score);
}
