import { normalizePhone } from '../utils/normalizers.js'

function formatSyncStatus(labels) {
  const normalized = (labels || []).map((item) => String(item).toLowerCase())

  if (normalized.includes('lead_perdido')) return 'resolved'
  if (normalized.includes('cliente_ganho')) return 'resolved'
  if (normalized.includes('lead_qualificado')) return 'pending'
  if (normalized.includes('lead_negociacao')) return 'pending'
  return 'open'
}

function formatHealth(labels) {
  const normalized = (labels || []).map((item) => String(item).toLowerCase())
  if (normalized.includes('sync_error')) return 'risk'
  if (normalized.includes('vip')) return 'healthy'
  if (!normalized.length) return 'warning'
  return 'healthy'
}

function toLead(contact, snapshotAt) {
  const labels = Array.isArray(contact.labels) ? contact.labels : []
  const phone = normalizePhone(contact.phoneNumber || contact.phone_number || '')

  return {
    id: `lead-${contact.id}`,
    conversationId: null,
    chatConversationId: null,
    chatContactId: String(contact.id),
    name: contact.name || phone || `Contato ${contact.id}`,
    phone: phone || 'sem telefone',
    clickupTaskId: null,
    clickupStage: null,
    chatStatus: formatSyncStatus(labels),
    owner: 'Nao atribuido',
    syncEnabled: true,
    health: phone ? formatHealth(labels) : 'risk',
    tags: labels.length ? labels : ['sem_label_controlada'],
    summary: `Contato real importado do Bradial Partner API. ID ${contact.id}.`,
    lastAction: 'contato sincronizado do Bradial',
    lastSyncAt: snapshotAt,
    raw: {
      bradialContactId: String(contact.id),
    },
  }
}

export function createBradialAdapter(config, pushLog) {
  const baseUrl = String(config.baseUrl || '').replace(/\/$/, '')
  const apiToken = String(config.apiToken || '').trim()
  const maxPages = Math.max(1, Number(config.maxPages || 2))

  if (!baseUrl || !apiToken) {
    throw new Error('Defina BRADIAL_BASE_URL e BRADIAL_API_TOKEN no .env')
  }

  async function request(path, params = {}) {
    const url = new URL(`${baseUrl}${path}`)
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        url.searchParams.set(key, String(value))
      }
    })

    const response = await fetch(url, {
      headers: {
        'x-api-key': apiToken,
        'Content-Type': 'application/json',
      },
    })

    if (!response.ok) {
      const body = await response.text()
      throw new Error(`Bradial HTTP ${response.status}: ${body.slice(0, 300)}`)
    }

    return response.json()
  }

  async function listPaged(path) {
    const rows = []

    for (let page = 1; page <= maxPages; page += 1) {
      const payload = await request(path, { page })
      const data = Array.isArray(payload?.data) ? payload.data : []
      if (!data.length) break
      rows.push(...data)
      if (data.length < 20) break
    }

    return rows
  }

  async function fetchSnapshot(trigger = 'manual') {
    const snapshotAt = new Date().toISOString()
    const [contacts, agents, inboxes] = await Promise.all([
      listPaged('/v2/public-api/v1/contacts'),
      listPaged('/v2/public-api/v1/agents'),
      listPaged('/v2/public-api/v1/inboxes'),
    ])

    const leads = contacts.map((contact) => toLead(contact, snapshotAt))
    pushLog(
      'success',
      'Bradial carregado',
      `${leads.length} contatos, ${agents.length} agentes e ${inboxes.length} inboxes importados`,
      { trigger },
    )

    return {
      enabled: true,
      source: 'bradial-partner-api',
      snapshotAt,
      contacts,
      leads,
      agents,
      inboxes,
    }
  }

  return {
    fetchSnapshot,
  }
}
