import { pool } from './db.js';
import { config } from './config.js';
import { hash, relevance } from './memory.js';
import { DomainError } from './contracts.js';
import { rankContext } from './context-rank.js';
import { importRecall } from './recall.js';
import type { ObjectiveInput } from './objective-contracts.js';

export async function planningContext(request: ObjectiveInput, compact = false) {
  const recall = await importRecall(request.project, request.message);
  const memories = (
    await pool.query(
      `SELECT m.* FROM memories m WHERE project=$1
    AND (expires_at IS NULL OR expires_at>now()) AND NOT EXISTS(SELECT 1 FROM memories n WHERE n.supersedes=m.id)
    ORDER BY CASE WHEN kind='decision' AND verification='confirmed' THEN 0 ELSE 1 END,created_at DESC`,
      [request.project],
    )
  ).rows;
  const agents = (
    await pool.query(
      "SELECT id,name,capabilities,department,reports_to,runtime FROM agents WHERE runtime='harness' ORDER BY id",
    )
  ).rows;
  const required = memories.filter((m) => m.kind === 'decision' && m.verification === 'confirmed');
  const blocks = [
    ...memories
      .filter((m) => !required.includes(m))
      .map((m) => ({
        id: 'memory:' + m.id,
        text: m.title + '\n' + m.content.slice(0, 2500),
        value: m,
        type: 'memory',
      })),
    ...agents.map((a) => ({
      id: 'agent:' + a.id,
      text: a.name + ' ' + a.capabilities.join(' '),
      value: a,
      type: 'agent',
    })),
  ]
    .map((b) => ({ ...b, score: relevance(request.message, b.text) }))
    .sort((a, b) => b.score - a.score);
  // Shortlist first: do not send the entire registry or project history to the selector.
  const shortlist = [
    ...blocks.filter((b) => b.type === 'agent').slice(0, 16),
    ...blocks.filter((b) => b.type === 'memory').slice(0, 12),
  ];
  let selector = 'lexical',
    fallbackReason: string | null =
      config.layaUrl || config.bridgeHost ? null : 'Laya não configurado';
  if ((config.layaUrl || config.bridgeHost) && shortlist.length)
    try {
      const scores = await rankContext(request.message, shortlist, request.constraints);
      shortlist.forEach((b) => (b.score = scores[b.id]));
      selector = 'laya';
    } catch {
      fallbackReason = 'Laya indisponível/incompleto; seleção lexical aplicada';
    }
  let selectedMemories = [
    ...required,
    ...shortlist
      .filter((b) => b.type === 'memory' && b.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, 8)
      .map((b) => b.value),
  ];
  if (compact) {
    // Keep required decisions complete; omit optional duplicates whole.
    const seen = new Set(required.map(m => hash(m.content)));
    let remaining = 4000;
    selectedMemories = [...required, ...selectedMemories.filter(m => {
      if (required.includes(m)) return false;
      const digest = hash(m.content), size = m.content.length + m.title.length;
      if (seen.has(digest) || size > remaining) return false;
      seen.add(digest); remaining -= size; return true;
    })];
  }
  const selectedAgents = [
    ...new Map(
      [
        ...shortlist
          .filter((b) => b.type === 'agent')
          .sort((a, b) => b.score - a.score)
          .slice(0, 10)
          .map((b) => b.value),
        ...agents.filter((a) =>
          [
            'engineering.integration',
            'engineering.backend.node',
            'qa.integration',
            'qa.acceptance',
          ].includes(a.id),
        ),
      ].map((a) => [a.id, a]),
    ).values(),
  ];
  const content = JSON.stringify(
    selectedMemories.map((m) => ({
      id: m.id,
      title: m.title,
      content: m.content,
      verification: m.verification,
      source: m.source,
      version: m.version,
    })),
  );
  if (content.length > 60000)
    throw new DomainError(
      'CONTEXT_TOO_LARGE',
      'Decisões obrigatórias excedem a janela do Planner; separar o objetivo.',
    );
  return {
    strategy: compact ? 'incremental-v1' : 'complete',
    content,
    agents: selectedAgents,
    manifest: {
      recall,
      selector,
      fallbackReason,
      selected: selectedMemories.map((m) => ({
        id: m.id,
        version: m.version,
        hash: hash(m.content),
        source: m.source,
      })),
      agents: selectedAgents.map((a) => a.id),
      generatedAt: new Date().toISOString(),
    },
  };
}
