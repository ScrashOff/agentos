# AgentOS

Control plane experimental para organizar e executar trabalho de agentes sobre um runtime separado, como o DeepSeek Harness.

O projeto mantém objetivos, tarefas, dependências, tentativas, filas, artefatos e critérios de aceite no PostgreSQL. Uma interface React permite acompanhar execuções, contas, memória e ciclos recorrentes. O modelo decide sobre o trabalho; o controlador administra estado, concorrência e encaminhamentos.

## Estado do projeto

Esta é uma publicação inicial do código em desenvolvimento. Existem limitações conhecidas no planejamento inicial, na recuperação e na convergência de tarefas para entregas verificadas. A autonomia contínua de ponta a ponta ainda não foi demonstrada. Número de sessões, heartbeat e resposta do modelo não comprovam conclusão de uma tarefa.

A publicação contém uma cópia sanitizada do código, com histórico novo. Credenciais, bancos, sessões, memórias privadas, relatórios operacionais, dados dos projetos e scripts específicos de operações não fazem parte deste repositório. Hosts e identificadores de instalação foram substituídos por exemplos.

## Componentes

| Diretório | Responsabilidade |
| --- | --- |
| `server/` | API, estado persistido, scheduler, planejamento, gestão e validação |
| `bridge/` | Adaptadores de contas, ferramentas e runtimes isolados |
| `web/` | Interface React |
| `scripts/` | Serviços, provisionamento e ferramentas de operação |
| `tests/` | Testes de contratos, banco, runtime e interface |
| `shared/` | Contratos compartilhados |

Stack: Node.js, TypeScript, Fastify, PostgreSQL, pg-boss, React e Vite.

## Configuração inicial em um servidor de desenvolvimento

Requisitos: Node.js 24, npm e Docker Compose para um PostgreSQL isolado. O runtime remoto requer provisionamento adicional; instalar a interface não instala automaticamente o DeepSeek Harness nem conecta contas.

```sh
git clone https://github.com/lucasaugustodev/agentos.git
cd agentos
npm ci
npm run setup
npm run db:up
npm run build
npm start
```

`setup` cria `.env` a partir de `.env.example` e gera senha de banco e token de API aleatórios, sem exibi-los. Não substitui um `.env` existente. A configuração gerada é privada e ignorada pelo Git.

A aplicação escuta em `127.0.0.1:4280`, e o PostgreSQL em `127.0.0.1:15482`. Acesso remoto deve usar ingresso privado e autenticado, por exemplo em uma tailnet. Não publique o banco nem as interfaces administrativas diretamente na internet.

O arquivo `.env.example` contém somente placeholders. Configure suas próprias contas e credenciais fora do código. Os valores `192.0.2.10`, `*.example.*` e UUIDs iniciados por zeros são exemplos; não representam serviços ou contas disponíveis.

## Execução e modelos

A ponte conecta o controlador aos adaptadores remotos por SSH. Consulte [bridge/README.md](bridge/README.md) antes de provisioná-la. Runtimes podem precisar de Bubblewrap, systemd, navegadores e dependências instaladas separadamente.

O código inclui adaptadores para diferentes famílias de modelos e um perfil exclusivo de GLM. A política da instalação é carregada externamente; nenhuma conta acompanha o repositório. O perfil GLM desta versão ainda contém identificadores de exemplo em pontos do adaptador que precisam ser alinhados com a configuração da sua instalação.

O gate GLM distingue sessões de chamadas ao provedor: sessões podem aguardar ferramentas ou dependências, enquanto um limite global controla as chamadas simultâneas. Preserve os limites reais da sua conta. Limites de contexto e resposta também continuam aplicáveis.

## Testes

Execute testes em um servidor de desenvolvimento isolado, nunca contra uma fila ou banco de produção.

```sh
npm test
npm run build
npm run test:browser
```

Os testes de banco usam `agentos_test`, separado de `agentos`, e exigem provisionamento e autorização de reset próprios. Testes de navegador podem exigir instalação do Chromium. Consulte os arquivos de configuração e os testes antes de conectar um ambiente.

Os testes presentes no repositório não constituem certificação de autonomia nem garantia de compatibilidade com qualquer provedor ou infraestrutura.

## Segurança de publicação

- Não versionar `.env`, credenciais, grants OAuth, cookies, sessões, chaves SSH, bancos ou artefatos de execução.
- O workflow `Secret scan` executa Gitleaks no histórico Git em pushes e pull requests.
- Exemplos e fixtures usam dados sintéticos; cada instalação gera seus próprios segredos.
- Novos adaptadores devem manter credenciais fora de prompts, eventos e artefatos.

## Referências

O desenho foi estudado a partir de [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness), [ShackleAI Orchestrator](https://github.com/shackleai/orchestrator), [OSymandias](https://github.com/andreisilva1/OSymandias) e [Laya](https://github.com/NandhaKishorM/laya). Esses projetos são referências e integrações separadas; suas contas, runtimes e serviços não são distribuídos aqui.
