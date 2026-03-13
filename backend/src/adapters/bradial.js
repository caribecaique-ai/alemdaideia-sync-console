import { normalizePhone, normalizeText } from '../utils/normalizers.js'

function normalizeLabels(labels) {
  const unique = new Map()

  for (const item of Array.isArray(labels) ? labels : []) {
    const label = String(item || '').trim()
    if (!label) continue
    const key = normalizeText(label)
    if (!unique.has(key)) unique.set(key, label)
  }

  return [...unique.values()]
}

function mergeLabels(currentLabels, nextLabels = []) {
  return normalizeLabels([...(currentLabels || []), ...nextLabels])
}

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
  const labels = normalizeLabels(contact.labels)
  const phone = normalizePhone(contact.phoneNumber || contact.phone_number || '')
  const email = contact.email ? String(contact.email).trim() : null

  return {
    id: `lead-${contact.id}`,
    conversationId: null,
    chatConversationId: null,
    chatContactId: String(contact.id),
    name: contact.name || phone || `Contato ${contact.id}`,
    phone: phone || 'sem telefone',
    email,
    clickupTaskId: null,
    clickupStage: null,
    chatStatus: formatSyncStatus(labels),
    owner: 'Nao atribuido',
    syncEnabled: true,
    health: phone ? formatHealth(labels) : 'risk',
    bradialLabels: labels,
    tags: labels.length ? labels : ['sem_label_controlada'],
    summary: `Contato real importado do Bradial Partner API. ID ${contact.id}.`,
    lastAction: 'contato sincronizado do Bradial',
    lastSyncAt: snapshotAt,
    raw: {
      bradialContactId: String(contact.id),
      bradialLabels: labels,
      bradialEmail: email,
    },
  }
}

export function createBradialAdapter(config, pushLog) {
  const baseUrl = String(config.baseUrl || '').replace(/\/$/, '')
  const apiToken = String(config.apiToken || '').trim()
  const maxPages = Math.max(1, Number(config.maxPages || 2))
  const opportunityLabel = String(config.opportunityLabel || 'OPORTUNIDADE').trim() || 'OPORTUNIDADE'

  if (!baseUrl || !apiToken) {
    throw new Error('Defina BRADIAL_BASE_URL e BRADIAL_API_TOKEN no .env')
  }

  async function request(path, { method = 'GET', params = {}, body } = {}) {
    const url = new URL(`${baseUrl}${path}`)
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        url.searchParams.set(key, String(value))
      }
    })

    const response = await fetch(url, {
      method,
      headers: {
        'x-api-key': apiToken,
        'Content-Type': 'application/json',
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    })

    if (!response.ok) {
      const body = await response.text()
      throw new Error(`Bradial HTTP ${response.status}: ${body.slice(0, 300)}`)
    }

    if (response.status === 204) return null

    const text = await response.text()
    if (!text) return null

    try {
      return JSON.parse(text)
    } catch {
      return text
    }
  }

  async function listPaged(path) {
    const rows = []

    for (let page = 1; page <= maxPages; page += 1) {
      const payload = await request(path, { params: { page } })
      const data = Array.isArray(payload?.data) ? payload.data : []
      if (!data.length) break
      rows.push(...data)
      if (data.length < 20) break
    }

    return rows
  }

  function buildContactPayload(task, existingLead = null) {
    const labels = mergeLabels(
      existingLead?.bradialLabels || existingLead?.raw?.bradialLabels || [],
      [opportunityLabel],
    )

    const payload = {
      name: String(task?.name || existingLead?.name || '').trim() || undefined,
      email:
        String(task?.email || existingLead?.email || existingLead?.raw?.bradialEmail || '').trim() ||
        undefined,
      phoneNumber: normalizePhone(task?.phone || existingLead?.phone),
      labels,
    }

    return Object.fromEntries(
      Object.entries(payload).filter(([, value]) => {
        if (Array.isArray(value)) return value.length > 0
        return value !== undefined && value !== null && value !== ''
      }),
    )
  }

  function labelsEqual(left, right) {
    const leftKey = normalizeLabels(left).map((item) => normalizeText(item)).sort().join('|')
    const rightKey = normalizeLabels(right).map((item) => normalizeText(item)).sort().join('|')
    return leftKey === rightKey
  }

  function isSamePayload(existingLead, payload) {
    return (
      normalizeText(existingLead?.name) === normalizeText(payload.name) &&
      normalizeText(existingLead?.email || existingLead?.raw?.bradialEmail) === normalizeText(payload.email) &&
      normalizePhone(existingLead?.phone) === normalizePhone(payload.phoneNumber) &&
      labelsEqual(existingLead?.bradialLabels || existingLead?.raw?.bradialLabels, payload.labels)
    )
  }

  async function fetchContact(contactId) {
    return request(`/v2/public-api/v1/contacts/${contactId}`)
  }

  async function hydrateContactFromPhone(phone, preferredContactId = null) {
    const normalizedPhone = normalizePhone(phone)
    if (!normalizedPhone) return null

    const contacts = await listPaged('/v2/public-api/v1/contacts')
    const matches = contacts.filter(
      (contact) => normalizePhone(contact.phoneNumber || contact.phone_number) === normalizedPhone,
    )

    if (preferredContactId) {
      return matches.find((contact) => String(contact.id) === String(preferredContactId)) || matches[0] || null
    }

    return matches[0] || null
  }

  async function upsertOpportunityContact(task, existingLead = null, options = {}) {
    const dryRun = Boolean(options.dryRun)
    const payload = buildContactPayload(task, existingLead)

    if (!payload.phoneNumber) {
      throw new Error('A task nao possui telefone canonico para criar o contato na Bradial.')
    }

    if (existingLead && isSamePayload(existingLead, payload)) {
      return {
        operation: 'noop',
        dryRun,
        payload,
        contact: existingLead,
      }
    }

    if (dryRun) {
      return {
        operation: existingLead?.chatContactId ? 'update' : 'create',
        dryRun: true,
        payload,
        contact: existingLead,
      }
    }

    if (existingLead?.chatContactId) {
      await request(`/v2/public-api/v1/contacts/${existingLead.chatContactId}`, {
        method: 'PATCH',
        body: payload,
      })

      const updatedContact =
        (await fetchContact(existingLead.chatContactId).catch(() => null)) ||
        (await hydrateContactFromPhone(payload.phoneNumber, existingLead.chatContactId))

      return {
        operation: 'update',
        dryRun: false,
        payload,
        contact: updatedContact,
      }
    }

    const created = await request('/v2/public-api/v1/contacts', {
      method: 'POST',
      body: payload,
    })
    const createdContact =
      (created && typeof created === 'object' && created.id ? created : null) ||
      (await hydrateContactFromPhone(payload.phoneNumber))

    return {
      operation: 'create',
      dryRun: false,
      payload,
      contact: createdContact,
    }
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
    upsertOpportunityContact,
    opportunityLabel,
  }
}
