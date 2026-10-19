// GLM keeps the existing read-only source mount. Repairs are delivered as artifacts
// from a writable scratch candidate and must be integrated and independently verified.
export const glmRepairInstruction = `
GLM_REPAIR_V1
VERSIONED_REPAIR_V2: sourceHashes deve ser uma lista [{path:"src/file.js",before:"SHA256 completo dos bytes originais",after:"SHA256 completo dos bytes corrigidos"}], cobrindo exatamente todos os arquivos de implementation.patch. Use null em before para arquivo novo e null em after para remoção. Os hashes são calculados nos arquivos reais, não textos simbólicos. Este contrato permite ao runtime compor patches compatíveis sem nova sessão de integração para aplicá-los; conflitos continuam explícitos e o QA permanece independente.
Implemente a correção focal numa cópia em data/repair-candidate. Copie os arquivos locais necessários, excluindo .git, evidence/, data/ e node_modules; use assets/ e dependências originais somente leitura, sem copiá-los nem alterá-los. O fonte original em /workspace continua somente leitura. Nenhum acesso externo ou a produção.
Primeiro reproduza o defeito no original; depois aplique a correção na cópia e execute a mesma regressão. O teste qa-protocol/ deve aceitar CANDIDATE_ROOT e funcionar também contra a futura candidata integrada; não fixe caminhos absolutos da cópia nem falsifique APIs/cliente para passar.
Entregue evidence/implementation.patch, diff unificado aplicável sobre o fonte original, apenas com a correção necessária (sem assets, .git, node_modules, data, credenciais, regras ou arquivos fora do escopo). O changes.patch canônico continua sendo gerado pelo runtime e contém apenas qa-protocol/; NÃO coloque a implementação nele.
Entregue evidence/repair-report.json com fixVerified:true somente após reproduzir o defeito e passar a regressão na cópia, beforeCommand, beforeExitCode, afterCommand, afterExitCode, changedFiles, sourceHashes e limitações. Preserve stdout/stderr de ambos em evidence/repair-tests.log. Uma hipótese sem correção comprovada deve declarar fixVerified:false. Este resultado fornece um patch para integração e não aceita o jogo.
`;

export const glmRepairCriteria = [
  {
    id: 'repair_verified',
    label: 'Regressão antes/depois da correção',
    type: 'json_field',
    name: 'repair-report.json',
    field: 'fixVerified',
    equals: true,
  },
  {
    id: 'repair_patch',
    label: 'Patch da correção para integração',
    type: 'artifact',
    name: 'implementation.patch',
  },
  {
    id: 'repair_log',
    label: 'Saída real antes/depois',
    type: 'artifact',
    name: 'repair-tests.log',
  },
  {
    id: 'repair_source_proof_v1',
    label: 'Prova estrutural versionada do patch e dos hashes de origem',
    type: 'repair_source_proof',
    version: 1,
    name: 'repair-report.json',
    patch: 'implementation.patch',
  },
] as const;

export const glmRepairHandoff = `
Dependência GLM de implementação: leia implementation.patch, repair-report.json, repair-tests.log e o teste em changes.patch. Confira os hashes da origem, revise o diff e aplique somente correções pertinentes à sua candidata; resolva conflitos preservando correções já existentes. Execute a regressão contra SUA candidata integrada. Não copie data/repair-candidate, não aceite a declaração do worker como prova final e não remova critérios. O QA final permanece independente.
`;
