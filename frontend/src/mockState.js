function minutesAgo(minutes) {
  return new Date(Date.now() - minutes * 60_000).toISOString()
}

export const initialConfig = {
  mode: 'live',
  backendUrl: 'http://168.231.97.169:3015',
  bradialBaseUrl: 'https://chat.bradial.com.br',
  bradialAccountId: '6008',
  autoPoll: false,
}

export const initialHealth = {
  status: 'idle',
  detail: 'aguardando conexao com backend',
  lastCheckedAt: null,
  latencyMs: null,
}

export const initialLeads = [
  {
    id: 'lead-001',
    name: 'Marina Costa',
    phone: '+5511998456123',
    clickupTaskId: 'CU-8421',
    chatContactId: 'CW-1001',
    chatConversationId: 'CNV-42091',
    clickupStage: 'Qualificado',
    chatStatus: 'pending',
    owner: 'Caique',
    syncEnabled: true,
    health: 'healthy',
    tags: ['lead_qualificado', 'vip'],
    summary: 'Lead com conversa ativa e owner definido. Bom candidato para validar sync de labels e status.',
    lastAction: 'sync concluido sem divergencia',
    lastSyncAt: minutesAgo(12),
  },
  {
    id: 'lead-002',
    name: 'Paulo Nery',
    phone: '+5511981182233',
    clickupTaskId: 'CU-8428',
    chatContactId: 'CW-1039',
    chatConversationId: 'CNV-42108',
    clickupStage: 'Negociacao',
    chatStatus: 'open',
    owner: 'Julia',
    syncEnabled: true,
    health: 'warning',
    tags: ['lead_negociacao'],
    summary: 'Lead com alteracao recente no ClickUp e fila de retry em andamento para refletir no Bradial.',
    lastAction: 'retry pendente no worker',
    lastSyncAt: minutesAgo(37),
  },
  {
    id: 'lead-003',
    name: 'Davi Serra',
    phone: '+5511977765544',
    clickupTaskId: 'CU-8471',
    chatContactId: 'CW-1102',
    chatConversationId: 'CNV-42331',
    clickupStage: 'Novo',
    chatStatus: 'open',
    owner: 'Nina',
    syncEnabled: true,
    health: 'risk',
    tags: ['lead_novo', 'urgente'],
    summary: 'Mesmo telefone apareceu em duas tasks antigas. Caso tipico para fila de resolucao manual.',
    lastAction: 'ambiguidade detectada',
    lastSyncAt: minutesAgo(84),
  },
  {
    id: 'lead-004',
    name: 'Lais Mendes',
    phone: '+5511944412300',
    clickupTaskId: 'CU-8494',
    chatContactId: 'CW-1120',
    chatConversationId: 'CNV-42388',
    clickupStage: 'Ganho',
    chatStatus: 'resolved',
    owner: 'Caique',
    syncEnabled: false,
    health: 'healthy',
    tags: ['cliente_ganho'],
    summary: 'Lead pausado para sync automatico apos fechamento. Mantido no painel para consulta e auditoria.',
    lastAction: 'sync pausado manualmente',
    lastSyncAt: minutesAgo(240),
  },
]

export const initialExceptions = [
  {
    id: 'exc-001',
    status: 'open',
    leadId: 'lead-003',
    kind: 'ambiguous_phone',
    source: 'clickup-webhook',
    detectedAt: minutesAgo(19),
    phone: '+5511977765544',
    summary: 'Duas tasks com o mesmo telefone. Vinculo automatico foi bloqueado.',
  },
  {
    id: 'exc-002',
    status: 'open',
    leadId: 'lead-002',
    kind: 'retry_exhausted',
    source: 'bradial-api',
    detectedAt: minutesAgo(42),
    phone: '+5511981182233',
    summary: 'Tentativa de atualizar label no Bradial falhou apos retries. Reprocessamento recomendado.',
  },
]

export const initialLogs = [
  {
    id: 'log-001',
    level: 'success',
    title: 'Sync aplicado no Bradial',
    message: 'lead_qualificado confirmado para Marina Costa.',
    leadId: 'lead-001',
    createdAt: minutesAgo(12),
  },
  {
    id: 'log-002',
    level: 'warning',
    title: 'Retry agendado',
    message: 'Falha temporaria ao aplicar label de negociacao em Paulo Nery.',
    leadId: 'lead-002',
    createdAt: minutesAgo(37),
  },
  {
    id: 'log-003',
    level: 'error',
    title: 'Ambiguidade detectada',
    message: 'Telefone duplicado para Davi Serra. Acao enviada para revisao manual.',
    leadId: 'lead-003',
    createdAt: minutesAgo(19),
  },
]

export const mockEventCatalog = [
  {
    kind: 'sync-ok',
    level: 'success',
    source: 'clickup',
    title: 'ClickUp refletido no Bradial',
    message: 'Mudanca de etapa aplicada com sucesso na conversa ativa.',
  },
  {
    kind: 'warning',
    level: 'warning',
    source: 'worker',
    title: 'Worker aguardando retry',
    message: 'API externa respondeu com atraso. Novo retry agendado.',
  },
  {
    kind: 'exception',
    level: 'error',
    source: 'lead-matcher',
    title: 'Lead entrou em excecao',
    message: 'Mais de uma conversa ativa encontrada para o mesmo contato.',
    exceptionKind: 'ambiguous_conversation',
  },
  {
    kind: 'sync-ok',
    level: 'info',
    source: 'bradial',
    title: 'Status operacional sincronizado',
    message: 'ClickUp recebeu atualizacao de status da conversa.',
  },
]
