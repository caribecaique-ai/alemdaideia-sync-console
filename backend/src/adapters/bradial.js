import {
  buildPhoneVariants,
  digitsOnly,
  normalizePhone,
  normalizeText,
  phonesMatchLoose,
} from '../utils/normalizers.js'
import {
  labelsIncludeEquivalent,
  normalizeLabelKey,
  pickControlledLabels,
  resolveBradialStageLabels,
  stripControlledStageLabels,
} from '../services/clickupStageLabels.js'
import { matchChatAgent } from '../services/agentMatching.js'
import { isClosedOpportunityTask } from '../services/clickupLeadContext.js'

function normalizeLabels(labels) {
  const unique = new Map()

  for (const item of Array.isArray(labels) ? labels : []) {
    const label = String(item || '').trim()
    if (!label) continue
    const key = normalizeLabelKey(label)
    if (!unique.has(key)) unique.set(key, label)
  }

  return [...unique.values()]
}

function normalizeContactRecordId(value) {
  const raw = String(value || '').trim()
  if (!raw) return null

  const match = raw.match(/^lead-(\d+)$/)
  if (match) return match[1]

  return raw
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function toConversationLabelValue(label) {
  return normalizeText(label).replace(/\s+/g, '-')
}

function mergeLabels(currentLabels, nextLabels = []) {
  return normalizeLabels([...(currentLabels || []), ...nextLabels])
}

function normalizeConversationPriority(value) {
  const normalized = normalizeText(value)
  if (!normalized) return null

  if (normalized === 'urgent' || normalized === 'urgente') return 'urgent'
  if (normalized === 'high' || normalized === 'alta' || normalized === 'alto') return 'high'
  if (normalized === 'medium' || normalized === 'media' || normalized === 'medio' || normalized === 'normal') return 'medium'
  if (normalized === 'low' || normalized === 'baixa' || normalized === 'baixo') return 'low'

  return null
}

function resolveStoredTaskActor(task = {}) {
  const structuredCandidates = [
    task?.ownerActor,
    ...(Array.isArray(task?.assigneeActors) ? task.assigneeActors : []),
    ...(Array.isArray(task?.assignees) ? task.assignees : []),
    task?.owner,
  ].filter(Boolean)

  for (const candidate of structuredCandidates) {
    if (candidate && typeof candidate === 'object') {
      const email = String(candidate.email || '').trim() || null
      const username = String(candidate.username || '').trim() || null
      const name =
        String(candidate.name || candidate.full_name || username || (email ? email.split('@')[0] : '')).trim() ||
        null

      if (email || username || name) {
        return {
          id: candidate.id == null ? null : String(candidate.id),
          username: username || name || null,
          email,
          name: name || username || null,
        }
      }
      continue
    }

    const primary = String(candidate || '').trim()
    if (!primary) continue

    const email = primary.includes('@') ? primary : null
    const name = email ? email.split('@')[0] : primary

    return {
      id: null,
      username: name || null,
      email,
      name: name || null,
    }
  }

  return null
}

function shallowEqualObject(left = {}, right = {}) {
  const leftKeys = Object.keys(left || {}).sort()
  const rightKeys = Object.keys(right || {}).sort()
  if (leftKeys.length !== rightKeys.length) return false

  return leftKeys.every((key, index) => {
    if (key !== rightKeys[index]) return false
    return String(left[key] ?? '') === String(right[key] ?? '')
  })
}

function isPhoneAlreadyInUseError(error) {
  const message = String(error?.message || '')
  return Number(error?.statusCode || 0) === 422 && /phone number.+em uso|phone number.+in use/i.test(message)
}

function isBlankPartnerLabelError(error) {
  const message = String(error?.message || '')
  return Number(error?.statusCode || 0) === 422 && /title.+branco|title.+blank|title.+invalido|title.+invalid/i.test(message)
}

function extractChatwootList(data) {
  if (Array.isArray(data)) return data
  if (Array.isArray(data?.payload)) return data.payload
  if (Array.isArray(data?.data?.payload)) return data.data.payload
  return []
}

function extractChatwootObject(data) {
  if (data && typeof data === 'object' && !Array.isArray(data)) {
    if (data.payload && typeof data.payload === 'object' && !Array.isArray(data.payload)) {
      return data.payload
    }
    return data
  }

  return {}
}

function conversationStatusRank(status) {
  const normalized = normalizeText(status)
  if (normalized === 'open') return 0
  if (normalized === 'pending') return 1
  if (normalized === 'snoozed') return 2
  if (normalized === 'resolved') return 3
  return 4
}

function conversationActivityAt(conversation) {
  return Number(
    conversation?.last_activity_at ||
      conversation?.updated_at ||
      conversation?.created_at ||
      conversation?.timestamp ||
      0,
  )
}

function pickBestConversation(conversations, preferredInboxId = null) {
  const normalizedPreferredInboxId = String(preferredInboxId || '').trim()

  return [...(conversations || [])].sort((left, right) => {
    const leftPreferred =
      normalizedPreferredInboxId && String(left?.inbox_id || '') === normalizedPreferredInboxId ? 0 : 1
    const rightPreferred =
      normalizedPreferredInboxId && String(right?.inbox_id || '') === normalizedPreferredInboxId ? 0 : 1
    if (leftPreferred !== rightPreferred) return leftPreferred - rightPreferred

    const statusDelta = conversationStatusRank(left?.status) - conversationStatusRank(right?.status)
    if (statusDelta !== 0) return statusDelta

    return conversationActivityAt(right) - conversationActivityAt(left)
  })[0]
}

function dedupeConversations(conversations = []) {
  const unique = new Map()
  for (const conversation of conversations || []) {
    const id = String(conversation?.id || '').trim()
    if (!id) continue
    if (!unique.has(id)) unique.set(id, conversation)
  }
  return [...unique.values()]
}

function conversationMatchesIdentity(conversation, { contactId, phone, name } = {}) {
  if (!conversation) return false

  const sender = conversation.meta?.sender || {}
  const senderId = String(sender.id || '')
  const targetContactId = String(contactId || '')

  if (senderId && targetContactId && senderId === targetContactId) return true
  if (phonesMatchLoose(sender.phone_number || sender.phoneNumber, phone)) return true

  const normalizedName = normalizeText(name)
  if (normalizedName && normalizeText(sender.name) === normalizedName) return true

  return false
}

function formatSyncStatus(labels) {
  if (labelsIncludeEquivalent(labels, 'perdido')) return 'resolved'
  if (labelsIncludeEquivalent(labels, 'negocio-fechado')) return 'resolved'
  if (labelsIncludeEquivalent(labels, 'negocio fechado')) return 'resolved'
  if (labelsIncludeEquivalent(labels, 'em-negociacao')) return 'pending'
  if (labelsIncludeEquivalent(labels, 'em negociacao')) return 'pending'
  if (labelsIncludeEquivalent(labels, 'qualificacao')) return 'pending'
  if (labelsIncludeEquivalent(labels, 'em qualificacao')) return 'pending'
  if (labelsIncludeEquivalent(labels, 'reuniao-agendada')) return 'pending'
  if (labelsIncludeEquivalent(labels, 'reuniao agendada')) return 'pending'
  if (labelsIncludeEquivalent(labels, 'confirmado')) return 'pending'

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
  const chatBaseUrl = String(config.chatBaseUrl || '').replace(/\/$/, '')
  const chatApiToken = String(config.chatApiToken || '').trim()
  const chatAccountId = String(config.chatAccountId || config.accountId || '').trim()
  const chatInboxId = String(config.chatInboxId || '').trim()
  const maxPages = Math.max(1, Number(config.maxPages || 2))
  const opportunityLabel = String(config.opportunityLabel || 'OPORTUNIDADE').trim() || 'OPORTUNIDADE'
  const configuredStageLabelMap = config.stageLabelMap || null
  const chatEnabled = Boolean(chatBaseUrl && chatApiToken && chatAccountId)
  const syncConversationLabels = config.syncConversationLabels !== false
  const syncContactLabels = config.syncContactLabels === true
  const requestMaxAttempts = Math.max(1, Number(config.requestMaxAttempts || 3))
  const requestRetryBaseMs = Math.max(50, Number(config.requestRetryBaseMs || 150))
  const conversationSearchPages = Math.max(3, Number(config.conversationSearchPages || 8))
  const contactSearchPages = Math.max(maxPages, Number(config.contactSearchPages || 20))
  const labelVerifyAttempts = Math.max(1, Number(config.labelVerifyAttempts || 2))
  const labelVerifyDelayMs = Math.max(25, Number(config.labelVerifyDelayMs || 75))
  const syncConversationPriority = config.syncConversationPriority !== false
  const syncConversationAssignment = config.syncConversationAssignment !== false
  const syncClosedByAssignment = config.syncClosedByAssignment !== false
  const syncClosedByAttributes = config.syncClosedByAttributes !== false
  const autoCreateConversations = config.autoCreateConversations === true
  const agentAliasMap = config.agentAliasMap || null
  const closedStageLabels = config.closedStageLabels || ''
  let chatLabelCatalogCache = null

  if (!baseUrl || !apiToken) {
    throw new Error('Defina BRADIAL_BASE_URL e BRADIAL_API_TOKEN no .env')
  }

  async function fetchWithRetry(url, options, kind) {
    let lastError = null

    for (let attempt = 1; attempt <= requestMaxAttempts; attempt += 1) {
      try {
        const response = await fetch(url, options)
        if (response.ok) return response

        const responseBody = await response.text()
        const error = new Error(`${kind} HTTP ${response.status}: ${responseBody.slice(0, 300)}`)
        error.statusCode = response.status
        throw error
      } catch (error) {
        lastError = error
        const statusCode = Number(error?.statusCode || 0)
        const retryable =
          statusCode === 429 ||
          statusCode >= 500 ||
          error?.cause?.code === 'ECONNRESET' ||
          error?.cause?.code === 'ETIMEDOUT' ||
          /network|fetch|timeout/i.test(String(error?.message || ''))

        if (!retryable || attempt >= requestMaxAttempts) break
        await wait(requestRetryBaseMs * attempt)
      }
    }

    throw lastError || new Error(`Falha ao chamar ${kind}`)
  }

  async function request(path, { method = 'GET', params = {}, body } = {}) {
    const url = new URL(`${baseUrl}${path}`)
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        url.searchParams.set(key, String(value))
      }
    })

    const response = await fetchWithRetry(
      url,
      {
        method,
        headers: {
          'x-api-key': apiToken,
          'Content-Type': 'application/json',
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      },
      'Bradial',
    )

    if (response.status === 204) return null

    const text = await response.text()
    if (!text) return null

    try {
      return JSON.parse(text)
    } catch {
      return text
    }
  }

  async function requestChat(path, { method = 'GET', params = {}, body } = {}) {
    if (!chatEnabled) {
      throw new Error('Defina BRADIAL_CHAT_BASE_URL, BRADIAL_CHAT_ACCOUNT_ID e BRADIAL_CHAT_API_TOKEN.')
    }

    const url = new URL(`${chatBaseUrl}${path}`)
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        url.searchParams.append(key, String(value))
      }
    })

    const response = await fetchWithRetry(
      url,
      {
        method,
        headers: {
          api_access_token: chatApiToken,
          'Content-Type': 'application/json',
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      },
      'Bradial Chat',
    )

    if (response.status === 204) return null

    const text = await response.text()
    if (!text) return null

    try {
      return JSON.parse(text)
    } catch {
      return text
    }
  }

  async function listPaged(path, pageLimit = maxPages) {
    const rows = []

    for (let page = 1; page <= pageLimit; page += 1) {
      const payload = await request(path, { params: { page } })
      const data = Array.isArray(payload?.data) ? payload.data : []
      if (!data.length) break
      rows.push(...data)
      if (data.length < 20) break
    }

    return rows
  }

  function buildContactPayload(task, existingLead = null, options = {}) {
    const includeLabels = options.includeLabels !== false
    const targetStageLabels = Array.isArray(options.targetStageLabels)
      ? normalizeLabels(options.targetStageLabels)
      : resolveBradialStageLabels(task, configuredStageLabelMap)
    const controlledStageLabels = Array.isArray(options.controlledStageLabels)
      ? options.controlledStageLabels
      : []
    const preservedLabels = stripControlledStageLabels(
      existingLead?.bradialLabels || existingLead?.raw?.bradialLabels || [],
      controlledStageLabels,
    )
    const labels = mergeLabels(
      preservedLabels,
      (targetStageLabels.length ? targetStageLabels : [opportunityLabel]).filter(Boolean),
    )

    const payload = {
      name: String(task?.name || existingLead?.name || '').trim() || undefined,
      email:
        String(task?.email || existingLead?.email || existingLead?.raw?.bradialEmail || '').trim() ||
        undefined,
      phoneNumber: normalizePhone(task?.phone || existingLead?.phone),
    }

    if (includeLabels) {
      payload.labels = labels
    }

    return Object.fromEntries(
      Object.entries(payload).filter(([, value]) => {
        if (Array.isArray(value)) return true
        return value !== undefined && value !== null && value !== ''
      }),
    )
  }

  function labelsEqual(left, right) {
    const leftKey = normalizeLabels(left).map((item) => normalizeLabelKey(item)).sort().join('|')
    const rightKey = normalizeLabels(right).map((item) => normalizeLabelKey(item)).sort().join('|')
    return leftKey === rightKey
  }

  async function listChatLabelsCatalog() {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/labels`)
    const labels = extractChatwootList(data)
    return labels
      .map((item) => String(item?.title || item || '').trim())
      .filter(Boolean)
  }

  async function getChatLabelCatalog() {
    if (!chatEnabled) return []

    if (chatLabelCatalogCache?.labels?.length) {
      return chatLabelCatalogCache.labels
    }

    const labels = await listChatLabelsCatalog()
    chatLabelCatalogCache = {
      labels,
      keyed: new Map(labels.map((label) => [normalizeLabelKey(label), label])),
    }

    return labels
  }

  async function resolveAccountLabelTitle(label) {
    const fallback = String(label || '').trim()
    const normalizedKey = normalizeLabelKey(fallback)
    if (!normalizedKey || !chatEnabled) return fallback

    if (!chatLabelCatalogCache?.keyed) {
      await getChatLabelCatalog()
    }

    return chatLabelCatalogCache?.keyed?.get(normalizedKey) || fallback
  }

  function isSamePayload(existingLead, payload) {
    const compareLabels = Array.isArray(payload?.labels)
    return (
      normalizeText(existingLead?.name) === normalizeText(payload.name) &&
      normalizeText(existingLead?.email || existingLead?.raw?.bradialEmail) === normalizeText(payload.email) &&
      normalizePhone(existingLead?.phone) === normalizePhone(payload.phoneNumber) &&
      (!compareLabels ||
        labelsEqual(existingLead?.bradialLabels || existingLead?.raw?.bradialLabels, payload.labels))
    )
  }

  async function fetchContact(contactId) {
    return request(`/v2/public-api/v1/contacts/${contactId}`)
  }

  function resolvePartnerContactId(contact, fallbackContact = null) {
    return normalizeContactRecordId(
      contact?.raw?.bradialContactId ||
        contact?.id ||
        contact?.chatContactId ||
        fallbackContact?.raw?.bradialContactId ||
        fallbackContact?.id ||
        fallbackContact?.chatContactId ||
        null,
    )
  }

  function buildLinkedLeadStub(
    existingLead,
    payload,
    {
      preferredContactId = null,
      preferredChatContactId = null,
      preferredConversationId = null,
    } = {},
  ) {
    const partnerContactId =
      normalizeContactRecordId(preferredContactId || resolvePartnerContactId(existingLead, null)) || null
    const conversationId =
      String(
        preferredConversationId || existingLead?.conversationId || existingLead?.chatConversationId || '',
      ).trim() || null
    const chatContactId =
      String(preferredChatContactId || existingLead?.chatContactId || partnerContactId || '').trim() || null
    const labels = normalizeLabels(existingLead?.bradialLabels || existingLead?.raw?.bradialLabels || [])

    return {
      ...(existingLead || {}),
      id:
        existingLead?.id ||
        (partnerContactId ? `lead-${partnerContactId}` : conversationId ? `lead-conversation-${conversationId}` : null),
      conversationId: conversationId || existingLead?.conversationId || null,
      chatConversationId: conversationId || existingLead?.chatConversationId || null,
      chatContactId: chatContactId || existingLead?.chatContactId || null,
      name: payload?.name || existingLead?.name || null,
      phone: payload?.phoneNumber || existingLead?.phone || null,
      email:
        payload?.email || existingLead?.email || existingLead?.raw?.bradialEmail || null,
      bradialLabels: labels,
      raw: {
        ...(existingLead?.raw || {}),
        bradialContactId: partnerContactId || existingLead?.raw?.bradialContactId || null,
        bradialLabels: labels,
        bradialEmail:
          payload?.email || existingLead?.raw?.bradialEmail || existingLead?.email || null,
      },
    }
  }

  async function clearPartnerContactLabels(contactId) {
    const currentContact = await fetchContact(contactId)
    if (!normalizeLabels(currentContact?.labels).length) {
      return currentContact
    }

    try {
      await request(`/v2/public-api/v1/contacts/${contactId}`, {
        method: 'PATCH',
        body: { labels: [''] },
      })
    } catch (error) {
      if (!isBlankPartnerLabelError(error)) {
        throw error
      }
    }

    const verifiedContact = await fetchContact(contactId)
    if (normalizeLabels(verifiedContact?.labels).length) {
      pushLog(
        'warning',
        'Limpeza de etiqueta do contato nao convergiu',
        `Contato Bradial ${contactId} ainda manteve etiquetas de contato apos a limpeza.`,
        {
          contactId,
          labels: normalizeLabels(verifiedContact?.labels),
        },
      )
    }

    return verifiedContact
  }

  async function fetchContactSafely(contactId) {
    try {
      return await fetchContact(contactId)
    } catch (error) {
      if (String(error?.message || '').includes('404')) {
        return null
      }

      throw error
    }
  }

  async function hydrateContactFromId(contactId, expectedPhone = null) {
    const normalizedId = String(contactId || '').trim()
    if (!normalizedId) return null

    const contact = await fetchContactSafely(normalizedId)
    if (!contact?.id) return null

    if (expectedPhone && !phonesMatchLoose(contact.phoneNumber || contact.phone_number, expectedPhone)) {
      return null
    }

    return contact
  }

  async function fetchChatContact(contactId) {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/contacts/${contactId}`)
    return extractChatwootObject(data)
  }

  async function searchChatContacts(phone) {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/contacts/search`, {
      params: { q: phone },
    })

    return extractChatwootList(data)
  }

  async function searchChatContactsByVariants(phone, name = null) {
    const normalizedPhone = normalizePhone(phone)
    const queries = new Set()

    if (normalizedPhone) {
      queries.add(normalizedPhone)
      for (const digits of buildPhoneVariants(normalizedPhone)) {
        queries.add(`+${digits}`)
        queries.add(digits)
      }
    }

    const normalizedName = String(name || '').trim()
    if (normalizedName) {
      queries.add(normalizedName)
    }

    const found = new Map()

    for (const query of queries) {
      const candidates = await searchChatContacts(query)
      for (const candidate of candidates) {
        if (!candidate?.id) continue
        found.set(String(candidate.id), candidate)
      }
    }

    return [...found.values()]
  }

  async function resolveChatContact(contactId, phone, preferredChatContactId = null) {
    if (!chatEnabled) return null

    if (preferredChatContactId) {
      try {
        const preferred = await fetchChatContact(preferredChatContactId)
        if (preferred?.id) return preferred
      } catch (error) {
        if (!String(error.message || '').includes('404')) {
          throw error
        }
      }
    }

    if (contactId) {
      try {
        const direct = await fetchChatContact(contactId)
        if (direct?.id) return direct
      } catch (error) {
        if (!String(error.message || '').includes('404')) {
          throw error
        }
      }
    }

    const normalizedPhone = normalizePhone(phone)
    if (!normalizedPhone) return null

    const candidates = await searchChatContactsByVariants(normalizedPhone)
    return (
      candidates.find((item) => phonesMatchLoose(item.phone_number || item.phoneNumber, normalizedPhone)) ||
      null
    )
  }

  async function listContactConversations(contactId) {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/contacts/${contactId}/conversations`)
    return extractChatwootList(data)
  }

  async function listContactableInboxes(contactId) {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/contacts/${contactId}/contactable_inboxes`)
    return extractChatwootList(data)
  }

  async function updateChatContact(contactId, payload) {
    await requestChat(`/api/v1/accounts/${chatAccountId}/contacts/${contactId}`, {
      method: 'PUT',
      body: payload,
    })
    return fetchChatContact(contactId)
  }

  async function listConversationLabels(conversationId) {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/conversations/${conversationId}/labels`)
    return normalizeLabels(extractChatwootList(data))
  }

  async function listChatContactLabels(contactId) {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/contacts/${contactId}/labels`)
    return normalizeLabels(extractChatwootList(data))
  }

  async function listFilteredConversations({ page = 1, inboxId = chatInboxId, status = 'all' } = {}) {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/conversations/filter`, {
      method: 'POST',
      params: {
        page,
        inbox_id: inboxId || undefined,
        status: status || undefined,
      },
      body: { payload: [] },
    })

    return extractChatwootList(data)
  }

  async function listAssignableChatAgents() {
    if (!chatEnabled) return []

    if (chatInboxId) {
      const data = await requestChat(`/api/v1/accounts/${chatAccountId}/inbox_members/${chatInboxId}`)
      return extractChatwootList(data)
    }

    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/agents`)
    return extractChatwootList(data)
  }

  async function fetchConversation(conversationId) {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/conversations/${conversationId}`)
    return extractChatwootObject(data)
  }

  async function listChatAccountLabels() {
    if (!chatEnabled) return []

    try {
      const data = await requestChat(`/api/v2/accounts/${chatAccountId}/reports/inbox_label_matrix`)
      const labels = Array.isArray(data?.labels) ? data.labels : []
      const titles = labels
        .map((item) => String(item?.title || item?.name || item || '').trim())
        .filter(Boolean)

      if (titles.length) {
        return normalizeLabels(titles)
      }
    } catch {
      // Fallback below.
    }

    const snapshot = await fetchSnapshot('chat-label-catalog-fallback')
    const observed = new Set()

    for (const lead of snapshot.leads || []) {
      for (const label of lead.labels || []) {
        const normalized = String(label || '').trim()
        if (normalized) observed.add(normalized)
      }
    }

    return normalizeLabels([...observed])
  }

  async function updateConversationLabels(conversationId, labels) {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/conversations/${conversationId}/labels`, {
      method: 'POST',
      body: { labels },
    })
    return normalizeLabels(extractChatwootList(data))
  }

  async function updateChatContactLabels(contactId, labels) {
    const data = await requestChat(`/api/v1/accounts/${chatAccountId}/contacts/${contactId}/labels`, {
      method: 'POST',
      body: { labels },
    })
    return normalizeLabels(extractChatwootList(data))
  }

  async function updateConversationPriority(conversationId, priority) {
    return requestChat(`/api/v1/accounts/${chatAccountId}/conversations/${conversationId}/toggle_priority`, {
      method: 'POST',
      body: { priority: priority || 'none' },
    })
  }

  async function updateConversationCustomAttributes(conversationId, customAttributes) {
    return requestChat(
      `/api/v1/accounts/${chatAccountId}/conversations/${conversationId}/custom_attributes`,
      {
        method: 'POST',
        body: { custom_attributes: customAttributes },
      },
    )
  }

  async function assignConversation(conversationId, assigneeId) {
    return requestChat(`/api/v1/accounts/${chatAccountId}/conversations/${conversationId}/assignments`, {
      method: 'POST',
      body: {
        assignee_id: assigneeId == null ? null : Number(assigneeId),
      },
    })
  }

  function extractContactInboxBindings(source) {
    const bindings = Array.isArray(source?.contact_inboxes)
      ? source.contact_inboxes
      : Array.isArray(source?.contactInboxes)
        ? source.contactInboxes
        : []

    return bindings
      .map((binding) => {
        const inboxId =
          String(binding?.inbox?.id || binding?.inbox_id || binding?.inboxId || '').trim() || null
        const sourceId = String(binding?.source_id || binding?.sourceId || '').trim() || null
        return {
          inboxId,
          sourceId,
          raw: binding,
        }
      })
      .filter((binding) => binding.inboxId && binding.sourceId)
  }

  function pickConversationBinding(bindings = [], preferredInboxId = chatInboxId) {
    const normalizedPreferredInboxId = String(preferredInboxId || '').trim()
    const normalizedBindings = (bindings || []).filter((binding) => binding?.inboxId && binding?.sourceId)
    if (!normalizedBindings.length) return null

    return (
      normalizedBindings.find((binding) => binding.inboxId === normalizedPreferredInboxId) ||
      normalizedBindings[0]
    )
  }

  async function resolveConversationCreationBinding(contactId, chatContact, preferredInboxId = chatInboxId) {
    const directBinding = pickConversationBinding(
      extractContactInboxBindings(chatContact),
      preferredInboxId,
    )
    if (directBinding) return directBinding

    const normalizedContactId = String(contactId || '').trim()
    if (!normalizedContactId) return null

    const contactableBindings = pickConversationBinding(
      extractContactInboxBindings({
        contact_inboxes: await listContactableInboxes(normalizedContactId).catch(() => []),
      }),
      preferredInboxId,
    )

    return contactableBindings || null
  }

  async function createConversation({
    contactId,
    sourceId,
    inboxId = chatInboxId,
    assigneeId = null,
    customAttributes = null,
    additionalAttributes = null,
    status = 'open',
  }) {
    const normalizedContactId = Number(contactId)
    const normalizedInboxId = Number(inboxId)
    const normalizedSourceId = String(sourceId || '').trim()

    if (!Number.isFinite(normalizedContactId) || normalizedContactId <= 0) {
      throw new Error('Nao foi possivel criar a conversa: contact_id invalido.')
    }
    if (!Number.isFinite(normalizedInboxId) || normalizedInboxId <= 0) {
      throw new Error('Nao foi possivel criar a conversa: inbox_id invalido.')
    }
    if (!normalizedSourceId) {
      throw new Error('Nao foi possivel criar a conversa: source_id ausente.')
    }

    const body = {
      source_id: normalizedSourceId,
      inbox_id: normalizedInboxId,
      contact_id: normalizedContactId,
      status: status || 'open',
    }

    if (assigneeId != null && assigneeId !== '') {
      body.assignee_id = Number(assigneeId)
    }
    if (customAttributes && typeof customAttributes === 'object' && Object.keys(customAttributes).length) {
      body.custom_attributes = customAttributes
    }
    if (
      additionalAttributes &&
      typeof additionalAttributes === 'object' &&
      Object.keys(additionalAttributes).length
    ) {
      body.additional_attributes = additionalAttributes
    }

    return requestChat(`/api/v1/accounts/${chatAccountId}/conversations`, {
      method: 'POST',
      body,
    })
  }

  async function mergeChatContacts(baseContactId, mergeeContactId) {
    return requestChat(`/api/v1/accounts/${chatAccountId}/actions/contact_merge`, {
      method: 'POST',
      body: {
        base_contact_id: Number(baseContactId),
        mergee_contact_id: Number(mergeeContactId),
      },
    })
  }

  async function ensureSingleChatContact({ baseContactId, phone, name, email }) {
    if (!chatEnabled || !baseContactId) {
      return {
        contactId: baseContactId ? String(baseContactId) : null,
        mergedIds: [],
      }
    }

    const normalizedPhone = normalizePhone(phone)
    const canonicalPayload = {
      name: String(name || '').trim() || undefined,
      email: String(email || '').trim() || undefined,
      phone_number: normalizedPhone || undefined,
    }

    await updateChatContact(baseContactId, canonicalPayload).catch(() => null)

    const candidates = await searchChatContactsByVariants(normalizedPhone, name)
    const duplicates = candidates.filter((candidate) => {
      const candidateId = String(candidate.id || '').trim()
      if (!candidateId || candidateId === String(baseContactId)) return false

      const sameName =
        normalizeText(candidate.name) &&
        normalizeText(name) &&
        normalizeText(candidate.name) === normalizeText(name)
      const samePhone = phonesMatchLoose(candidate.phone_number || candidate.phoneNumber, normalizedPhone)

      return samePhone || sameName
    })

    const mergedIds = []

    for (const duplicate of duplicates) {
      await mergeChatContacts(baseContactId, duplicate.id)
      mergedIds.push(String(duplicate.id))
    }

    const finalContact = await updateChatContact(baseContactId, canonicalPayload).catch(
      async () => fetchChatContact(baseContactId),
    )

    return {
      contactId: String(finalContact?.id || baseContactId),
      mergedIds,
      contact: finalContact,
    }
  }

  async function findConversationByIdentity(identity) {
    const conversations = []

    for (let page = 1; page <= conversationSearchPages; page += 1) {
      try {
        const batch = await listFilteredConversations({ page })
        if (!batch.length) break
        conversations.push(...batch)
        if (batch.length < 25) break
      } catch (error) {
        if (page === 1) throw error
        break
      }
    }

    const matched = conversations.filter((conversation) => conversationMatchesIdentity(conversation, identity))
    return pickBestConversation(matched, chatInboxId) || null
  }

  async function verifyLabels({ fetchLabels, expectedLabels, kind }) {
    let lastLabels = []

    for (let attempt = 1; attempt <= labelVerifyAttempts; attempt += 1) {
      lastLabels = normalizeLabels(await fetchLabels())
      if (labelsEqual(lastLabels, expectedLabels)) {
        return lastLabels
      }

      if (attempt < labelVerifyAttempts) {
        await wait(labelVerifyDelayMs * attempt)
      }
    }

    throw new Error(
      `${kind} nao convergiu para as etiquetas esperadas. Atual: ${lastLabels.join(', ') || '[]'}`,
    )
  }

  async function applyAndVerifyLabelSet({
    shouldUpdate,
    update,
    verify,
    fallback,
  }) {
    if (!shouldUpdate) {
      return fallback
    }

    await update()
    return verify()
  }

  async function syncConversationStageLabels({
    contactId,
    phone,
    name,
    targetStageLabel,
    targetStageLabels = [],
    controlledStageLabels,
    dryRun,
    preferredChatContactId = null,
    preferredConversationId = null,
  }) {
    if (!chatEnabled) {
      return {
        enabled: false,
        skipped: true,
        reason: 'chat_api_not_configured',
      }
    }

    const preferredConversation = preferredConversationId
      ? await fetchConversation(preferredConversationId)
          .catch(async () => ({
            id: preferredConversationId,
            labels: await listConversationLabels(preferredConversationId).catch(() => []),
            meta: {
              sender: {
                id: preferredChatContactId || contactId || null,
              },
            },
          }))
      : null
    const shouldResolveChatContact = !preferredConversation || syncContactLabels
    const chatContact = shouldResolveChatContact
      ? await resolveChatContact(contactId, phone, preferredChatContactId)
      : null
    const fallbackConversation = chatContact?.id
      ? null
      : preferredConversation
        ? null
        : await findConversationByIdentity({
          contactId: chatContact?.id || preferredChatContactId || contactId,
          phone,
          name,
        })

    if (!chatContact?.id && !fallbackConversation?.id && !preferredConversationId) {
      return {
        enabled: true,
        skipped: true,
        reason: 'chat_contact_not_found',
      }
    }

    const conversations = preferredConversation || !chatContact?.id
      ? []
      : await listContactConversations(chatContact.id)
    const discoveredConversation =
      preferredConversation
        ? null
        : fallbackConversation ||
          pickBestConversation(conversations, chatInboxId) ||
          (await findConversationByIdentity({
            contactId: chatContact?.id || preferredChatContactId || contactId,
            phone,
            name,
          }))
    let knownConversations = dedupeConversations(
      [preferredConversation, discoveredConversation, ...(conversations || [])].filter(Boolean),
    )
    let conversation = preferredConversation || discoveredConversation || knownConversations[0] || null
    const effectiveChatContactId =
      String(
        chatContact?.id ||
          preferredConversation?.meta?.sender?.id ||
          conversation?.meta?.sender?.id ||
          preferredChatContactId ||
          contactId ||
          '',
      ).trim() || null
    const resolvedControlledChatStageLabels = await Promise.all(
      (controlledStageLabels || []).map((label) => resolveAccountLabelTitle(label)),
    )
    const controlledChatStageLabels = resolvedControlledChatStageLabels.map((label) =>
      toConversationLabelValue(label),
    )
    const effectiveTargetStageLabels = normalizeLabels(
      (targetStageLabels || []).length ? targetStageLabels : [targetStageLabel || opportunityLabel],
    )
    const targetAccountLabels = await Promise.all(
      effectiveTargetStageLabels.map((label) => resolveAccountLabelTitle(label)),
    )
    const targetConversationLabels = normalizeLabels(
      targetAccountLabels.map((label) => toConversationLabelValue(label)),
    )
    const currentChatContactLabels = effectiveChatContactId
      ? await listChatContactLabels(effectiveChatContactId).catch(() => [])
      : []
    const strippedChatContactLabels = stripControlledStageLabels(
      currentChatContactLabels,
      controlledChatStageLabels,
    )
    const nextChatContactLabels = syncContactLabels
      ? mergeLabels(strippedChatContactLabels, targetConversationLabels.filter(Boolean))
      : strippedChatContactLabels

    let contactLabelOperation = syncContactLabels ? 'noop' : 'disabled'
    let syncedChatContactLabels = currentChatContactLabels
    let createdConversationId = null

    const contactLabelsChanged =
      Boolean(effectiveChatContactId) && !labelsEqual(currentChatContactLabels, nextChatContactLabels)

    if (contactLabelsChanged) {
      contactLabelOperation = syncContactLabels ? 'update' : 'clear'
      syncedChatContactLabels = dryRun
        ? nextChatContactLabels
        : currentChatContactLabels
    }

    const applyChatContactLabelState = async () => {
      if (!contactLabelsChanged || dryRun || !effectiveChatContactId) {
        return syncedChatContactLabels
      }

      const expectedLabels = nextChatContactLabels

      return applyAndVerifyLabelSet({
        shouldUpdate: true,
        update: () => updateChatContactLabels(effectiveChatContactId, expectedLabels),
        verify: () =>
          verifyLabels({
            fetchLabels: () => listChatContactLabels(effectiveChatContactId),
            expectedLabels,
            kind: `Etiquetas do contato do chat ${effectiveChatContactId}`,
          }),
        fallback: syncedChatContactLabels,
      })
    }

    if (!syncConversationLabels) {
      syncedChatContactLabels = await applyChatContactLabelState()
      return {
        enabled: true,
        skipped: contactLabelOperation === 'noop' || contactLabelOperation === 'disabled',
        operation:
          contactLabelOperation === 'update' || contactLabelOperation === 'clear' ? 'update' : 'noop',
        conversationId: conversation?.id ? String(conversation.id) : null,
        chatContactId: effectiveChatContactId,
        labels: conversation?.labels ? normalizeLabels(conversation.labels) : [],
        contactLabels: syncedChatContactLabels,
        contactLabelOperation,
        conversationLabelOperation: 'disabled',
      }
    }

    if (!conversation?.id && autoCreateConversations && effectiveChatContactId && chatInboxId) {
      const creationBinding = await resolveConversationCreationBinding(
        effectiveChatContactId,
        chatContact,
        chatInboxId,
      )

      if (creationBinding?.sourceId) {
        if (dryRun) {
          return {
            enabled: true,
            skipped: false,
            operation: 'create',
            dryRun: true,
            reason: 'conversation_will_be_created',
            conversationId: null,
            knownConversationId: preferredConversationId ? String(preferredConversationId) : null,
            chatContactId: effectiveChatContactId,
            labels: targetConversationLabels.filter(Boolean),
            contactLabels: nextChatContactLabels,
            contactLabelOperation,
            conversationLabelOperation: 'update',
          }
        }

        const createdConversation = await createConversation({
          contactId: effectiveChatContactId,
          sourceId: creationBinding.sourceId,
          inboxId: creationBinding.inboxId,
          status: 'open',
        })
        const hydratedConversation =
          (createdConversation?.id
            ? await fetchConversation(createdConversation.id).catch(() => createdConversation)
            : null) || createdConversation

        if (hydratedConversation?.id) {
          createdConversationId = String(hydratedConversation.id)
          conversation = hydratedConversation
          knownConversations = dedupeConversations([...knownConversations, hydratedConversation])
          pushLog(
            'info',
            'Conversa iniciada automaticamente no Bradial',
            `${name || phone || `Contato ${effectiveChatContactId}`} recebeu uma conversa vazia para sincronizar etapa e responsavel.`,
            {
              chatContactId: effectiveChatContactId,
              conversationId: createdConversationId,
              inboxId: creationBinding.inboxId,
              stageLabels: effectiveTargetStageLabels,
            },
          )
        }
      }
    }

    if (!conversation?.id) {
      syncedChatContactLabels = await applyChatContactLabelState()
      const contactOnlyHandled =
        syncContactLabels &&
        Boolean(effectiveChatContactId) &&
        labelsEqual(syncedChatContactLabels, nextChatContactLabels)
      return {
        enabled: true,
        skipped: !contactOnlyHandled,
        reason: contactOnlyHandled ? 'contact_labels_synced_without_conversation' : 'conversation_required_for_stage_label',
        operation:
          contactLabelOperation === 'update' || contactLabelOperation === 'clear' ? 'update' : 'noop',
        conversationId: null,
        knownConversationId: preferredConversationId ? String(preferredConversationId) : null,
        chatContactId: effectiveChatContactId,
        labels: [],
        contactLabels: syncedChatContactLabels,
        contactLabelOperation,
        conversationLabelOperation: 'skipped',
      }
    }

    const conversationPlans = await Promise.all(
      knownConversations.map(async (item) => {
        const currentLabels = Array.isArray(item.labels) && item.labels.length
          ? normalizeLabels(item.labels)
          : await listConversationLabels(item.id)
        const nextLabels = mergeLabels(
          stripControlledStageLabels(currentLabels, controlledChatStageLabels),
          targetConversationLabels.filter(Boolean),
        )

        return {
          id: String(item.id),
          currentLabels,
          nextLabels,
          labelsChanged: !labelsEqual(currentLabels, nextLabels),
        }
      }),
    )
    const primaryPlan =
      conversationPlans.find((plan) => String(plan.id) === String(conversation.id)) ||
      conversationPlans[0]
    const conversationLabelsChanged = conversationPlans.some((plan) => plan.labelsChanged)

    if (!conversationLabelsChanged && (contactLabelOperation === 'noop' || contactLabelOperation === 'disabled')) {
      return {
        enabled: true,
        skipped: false,
        operation: createdConversationId ? 'create' : 'noop',
        conversationId: String(primaryPlan.id),
        knownConversationId: preferredConversationId ? String(preferredConversationId) : null,
        chatContactId: effectiveChatContactId,
        labels: primaryPlan.nextLabels,
        contactLabels: syncedChatContactLabels,
        contactLabelOperation,
        conversationLabelOperation: 'noop',
      }
    }

    if (dryRun) {
      return {
        enabled: true,
        skipped: false,
        operation: 'update',
        dryRun: true,
        conversationId: String(primaryPlan.id),
        knownConversationId: preferredConversationId ? String(preferredConversationId) : null,
        chatContactId: effectiveChatContactId,
        labels: primaryPlan.nextLabels,
        contactLabels: syncedChatContactLabels,
        contactLabelOperation,
        conversationLabelOperation: conversationLabelsChanged ? 'update' : 'noop',
      }
    }

    const [verifiedChatContactLabels, verifiedConversationLabels] = await Promise.all([
      applyChatContactLabelState(),
      applyAndVerifyLabelSet({
        shouldUpdate: conversationLabelsChanged,
        update: async () => {
          for (const plan of conversationPlans) {
            if (!plan.labelsChanged) continue
            await updateConversationLabels(plan.id, plan.nextLabels)
          }
        },
        verify: async () => {
          let verifiedPrimaryLabels = primaryPlan.currentLabels
          for (const plan of conversationPlans) {
            const verifiedLabels = await verifyLabels({
              fetchLabels: () => listConversationLabels(plan.id),
              expectedLabels: plan.nextLabels,
              kind: `Etiquetas da conversa ${plan.id}`,
            })
            if (String(plan.id) === String(primaryPlan.id)) {
              verifiedPrimaryLabels = verifiedLabels
            }
          }
          return verifiedPrimaryLabels
        },
        fallback: primaryPlan.currentLabels,
      }),
    ])

    return {
      enabled: true,
      skipped: false,
      operation:
        createdConversationId
          ? 'create'
          : contactLabelOperation === 'update' ||
              contactLabelOperation === 'clear' ||
              conversationLabelsChanged
            ? 'update'
            : 'noop',
      conversationId: String(primaryPlan.id),
      knownConversationId: preferredConversationId ? String(preferredConversationId) : null,
      chatContactId: effectiveChatContactId,
      labels: verifiedConversationLabels,
      contactLabels: verifiedChatContactLabels,
      contactLabelOperation,
      conversationLabelOperation: conversationLabelsChanged ? 'update' : 'noop',
    }
  }

  async function syncConversationMetadata({
    conversationId,
    task,
    clickupContext = null,
    chatAgents = [],
    dryRun = false,
  }) {
    if (!chatEnabled) {
      return {
        enabled: false,
        skipped: true,
        reason: 'chat_api_not_configured',
      }
    }

    const normalizedConversationId = String(conversationId || '').trim()
    if (!normalizedConversationId) {
      return {
        enabled: true,
        skipped: true,
        reason: 'conversation_not_found',
      }
    }

    const currentConversation = await fetchConversation(normalizedConversationId)
    const currentPriority = normalizeConversationPriority(currentConversation?.priority)
    const currentAssigneeId =
      String(currentConversation?.meta?.assignee?.id || currentConversation?.assignee_id || '').trim() || null
    const currentCustomAttributes =
      currentConversation?.custom_attributes && typeof currentConversation.custom_attributes === 'object'
        ? currentConversation.custom_attributes
        : {}

    const targetPriority = syncConversationPriority
      ? normalizeConversationPriority(task?.priority)
      : null
    const closedTask = isClosedOpportunityTask(task, {
      stageLabelMap: configuredStageLabelMap,
      closedStageLabels,
    })
    const responsibleActor = resolveStoredTaskActor(task)
    const closerActor = closedTask
      ? (clickupContext?.isStatusEvent ? clickupContext.actor : null) || resolveStoredTaskActor(task)
      : null
    const assignmentActor = responsibleActor || (syncClosedByAssignment ? closerActor : null)
    const assignableAgents =
      Array.isArray(chatAgents) && chatAgents.length ? chatAgents : await listAssignableChatAgents().catch(() => [])
    const agentMatch = assignmentActor
      ? matchChatAgent(assignableAgents, assignmentActor, agentAliasMap)
      : null
    const targetAssigneeId =
      syncConversationAssignment && assignmentActor && agentMatch?.matched && agentMatch?.agent?.id != null
        ? String(agentMatch.agent.id)
        : null

    const nextCustomAttributes = { ...currentCustomAttributes }

    if (task?.urgency) {
      nextCustomAttributes.clickup_urgency = task.urgency
    } else {
      delete nextCustomAttributes.clickup_urgency
    }
    delete nextCustomAttributes.clickup_urgency_source

    if (syncClosedByAttributes && closerActor) {
      nextCustomAttributes.clickup_closed_by_name = closerActor.name || closerActor.username || ''
      nextCustomAttributes.clickup_closed_by_email = closerActor.email || ''
      nextCustomAttributes.clickup_closed_by_id = closerActor.id || ''
      nextCustomAttributes.clickup_closed_via_status = String(task?.status || '').trim()
    } else if (!closedTask) {
      delete nextCustomAttributes.clickup_closed_by_name
      delete nextCustomAttributes.clickup_closed_by_email
      delete nextCustomAttributes.clickup_closed_by_id
      delete nextCustomAttributes.clickup_closed_via_status
    }

    const priorityChanged = syncConversationPriority && currentPriority !== targetPriority
    const customAttributesChanged = !shallowEqualObject(currentCustomAttributes, nextCustomAttributes)
    const assignmentUnmatched =
      syncConversationAssignment && Boolean(assignmentActor) && !agentMatch?.matched
    const assignmentChanged =
      syncConversationAssignment &&
      !assignmentUnmatched &&
      (Boolean(assignmentActor) || Boolean(currentAssigneeId)) &&
      currentAssigneeId !== targetAssigneeId
    const assignmentOperation = assignmentUnmatched
      ? 'unmatched'
      : assignmentChanged
        ? targetAssigneeId
          ? 'update'
          : 'clear'
        : 'noop'

    if (!priorityChanged && !customAttributesChanged && !assignmentChanged) {
      return {
        enabled: true,
        skipped: false,
        operation: 'noop',
        conversationId: normalizedConversationId,
        priority: targetPriority,
        assigneeId: currentAssigneeId,
        matchedAgent: agentMatch?.matched ? agentMatch.agent : null,
        matchReason: agentMatch?.reason || null,
        assignmentOperation,
      }
    }

    if (dryRun) {
      return {
        enabled: true,
        skipped: false,
        operation: 'update',
        dryRun: true,
        conversationId: normalizedConversationId,
        priority: targetPriority,
        assigneeId: assignmentChanged ? targetAssigneeId : currentAssigneeId,
        matchedAgent: agentMatch?.matched ? agentMatch.agent : null,
        matchReason: agentMatch?.reason || null,
        priorityOperation: priorityChanged ? 'update' : 'noop',
        customAttributesOperation: customAttributesChanged ? 'update' : 'noop',
        assignmentOperation,
      }
    }

    if (priorityChanged) {
      await updateConversationPriority(normalizedConversationId, targetPriority)
    }

    if (customAttributesChanged) {
      await updateConversationCustomAttributes(normalizedConversationId, nextCustomAttributes)
    }

    if (assignmentChanged) {
      await assignConversation(normalizedConversationId, targetAssigneeId)
    }

    const verifiedConversation = await fetchConversation(normalizedConversationId)

    return {
      enabled: true,
      skipped: false,
      operation: 'update',
      conversationId: normalizedConversationId,
      priority: normalizeConversationPriority(verifiedConversation?.priority),
      assigneeId:
        String(verifiedConversation?.meta?.assignee?.id || verifiedConversation?.assignee_id || '').trim() || null,
      matchedAgent: agentMatch?.matched ? agentMatch.agent : null,
      matchReason: agentMatch?.reason || null,
      priorityOperation: priorityChanged ? 'update' : 'noop',
      customAttributesOperation: customAttributesChanged ? 'update' : 'noop',
      assignmentOperation,
      customAttributes:
        verifiedConversation?.custom_attributes && typeof verifiedConversation.custom_attributes === 'object'
          ? verifiedConversation.custom_attributes
          : {},
    }
  }

  async function syncLinkedConversationMetadata(task, options = {}) {
    const normalizedConversationId = String(options.conversationId || '').trim()
    if (!normalizedConversationId) {
      return {
        enabled: true,
        skipped: true,
        reason: 'conversation_not_found',
        conversationId: null,
        chatContactId: String(options.chatContactId || '').trim() || null,
      }
    }

    const metadataSync = await syncConversationMetadata({
      conversationId: normalizedConversationId,
      task,
      clickupContext: options.clickupContext || null,
      chatAgents: options.chatAgents || [],
      dryRun: Boolean(options.dryRun),
    })

    return {
      ...metadataSync,
      conversationId: normalizedConversationId,
      chatContactId: String(options.chatContactId || '').trim() || null,
    }
  }

  async function hydrateContacts(contacts) {
    const hydrated = []
    const chunkSize = 8

    for (let index = 0; index < contacts.length; index += chunkSize) {
      const chunk = contacts.slice(index, index + chunkSize)
      const resolvedChunk = await Promise.all(
        chunk.map(async (contact) => {
          try {
            return await fetchContactSafely(contact.id)
          } catch {
            return contact
          }
        }),
      )
      hydrated.push(...resolvedChunk.filter(Boolean))
    }

    return hydrated
  }

  function pickBestPartnerContact(matches, normalizedPhone, preferredContactId = null) {
    const preferredId = String(preferredContactId || '').trim()
    const canonicalPhone = normalizePhone(normalizedPhone)

    return [...matches].sort((left, right) => {
      const leftPreferred = preferredId && String(left?.id || '') === preferredId ? 0 : 1
      const rightPreferred = preferredId && String(right?.id || '') === preferredId ? 0 : 1
      if (leftPreferred !== rightPreferred) return leftPreferred - rightPreferred

      const leftExact = normalizePhone(left?.phoneNumber || left?.phone_number) === canonicalPhone ? 0 : 1
      const rightExact = normalizePhone(right?.phoneNumber || right?.phone_number) === canonicalPhone ? 0 : 1
      if (leftExact !== rightExact) return leftExact - rightExact

      return Number(right?.id || 0) - Number(left?.id || 0)
    })[0]
  }

  async function hydrateContactFromPhone(
    phone,
    preferredContactId = null,
    { canonicalName = null, canonicalEmail = null, pageLimit = maxPages } = {},
  ) {
    const normalizedPhone = normalizePhone(phone)
    if (!normalizedPhone) return null

    const contacts = await listPaged('/v2/public-api/v1/contacts', pageLimit)
    const looseMatches = contacts.filter(
      (contact) => phonesMatchLoose(contact.phoneNumber || contact.phone_number, normalizedPhone),
    )
    if (!looseMatches.length) return null

    const hydratedMatches = (
      await Promise.all(looseMatches.map((contact) => fetchContactSafely(contact.id).catch(() => contact)))
    ).filter(Boolean)
    const matches = hydratedMatches.length ? hydratedMatches : looseMatches

    const targetContact = pickBestPartnerContact(matches, normalizedPhone, preferredContactId)

    if (matches.length > 1 && chatEnabled && targetContact?.id) {
      await ensureSingleChatContact({
        baseContactId: targetContact.id,
        phone: normalizedPhone,
        name: canonicalName || targetContact.name || null,
        email: canonicalEmail || targetContact.email || null,
      }).catch((error) => {
        pushLog('warning', 'Falha ao consolidar duplicidades existentes na Bradial', error.message, {
          phone: normalizedPhone,
          preferredContactId: preferredContactId || null,
          baseContactId: targetContact.id,
          duplicateIds: matches.map((contact) => String(contact.id)),
        })
      })
    }

    const refreshedMatches =
      matches.length > 1 ? await listPaged('/v2/public-api/v1/contacts', pageLimit) : contacts
    const remaining = (
      await Promise.all(
        refreshedMatches
          .filter((contact) => phonesMatchLoose(contact.phoneNumber || contact.phone_number, normalizedPhone))
          .map((contact) => fetchContactSafely(contact.id).catch(() => contact)),
      )
    ).filter(Boolean)
    const canonicalContact = pickBestPartnerContact(
      remaining.length ? remaining : matches,
      normalizedPhone,
      targetContact?.id || preferredContactId || null,
    )

    if (!canonicalContact?.id) return null

    return fetchContact(canonicalContact.id).catch(() => canonicalContact)
  }

  async function upsertOpportunityContact(task, existingLead = null, options = {}) {
    const dryRun = Boolean(options.dryRun)
    const allowCreate = options.allowCreate !== false
    const targetStageLabels = Array.isArray(options.targetStageLabels)
      ? normalizeLabels(options.targetStageLabels)
      : resolveBradialStageLabels(task, configuredStageLabelMap)
    const targetStageLabel = targetStageLabels[0] || null
    const payload = buildContactPayload(task, existingLead, {
      includeLabels: syncContactLabels,
      targetStageLabels,
      controlledStageLabels: options.controlledStageLabels,
    })
    const createPayload = buildContactPayload(task, existingLead, {
      includeLabels: syncContactLabels,
      targetStageLabels,
      controlledStageLabels: options.controlledStageLabels,
    })
    if (syncContactLabels) {
      payload.labels = await Promise.all(
        (payload.labels || []).map((label) => resolveAccountLabelTitle(label)),
      )
      createPayload.labels = await Promise.all(
        (createPayload.labels || []).map((label) => resolveAccountLabelTitle(label)),
      )
    }
    const previousStageLabels = pickControlledLabels(
      existingLead?.bradialLabels || existingLead?.raw?.bradialLabels || [],
      options.controlledStageLabels,
    )

    if (!payload.phoneNumber) {
      throw new Error('A task nao possui telefone canonico para criar o contato na Bradial.')
    }

    let contactOperation = 'noop'
    let resolvedContact = existingLead
    const preferredContactId =
      String(
        options.preferredContactId ||
          existingLead?.raw?.bradialContactId ||
          existingLead?.chatContactId ||
          '',
      ).trim() || null
    const preferredChatContactId =
      String(options.preferredChatContactId || existingLead?.chatContactId || '').trim() || null
    const preferredConversationId =
      String(
        options.preferredConversationId ||
          existingLead?.conversationId ||
          existingLead?.chatConversationId ||
          '',
      ).trim() || null
    const linkedSyncTarget = Boolean(preferredConversationId || preferredChatContactId)

    if (linkedSyncTarget) {
      resolvedContact = buildLinkedLeadStub(existingLead, payload, {
        preferredContactId,
        preferredChatContactId,
        preferredConversationId,
      })
    }

    if (!linkedSyncTarget && !resolvedContact?.chatContactId && preferredContactId) {
      const hydratedById = await hydrateContactFromId(preferredContactId, payload.phoneNumber)
      if (hydratedById?.id) {
        resolvedContact = {
          ...(existingLead || {}),
          ...toLead(hydratedById, new Date().toISOString()),
          raw: {
            ...(existingLead?.raw || {}),
            bradialContactId: String(hydratedById.id),
            bradialLabels: normalizeLabels(hydratedById.labels),
            bradialEmail: hydratedById.email ? String(hydratedById.email).trim() : null,
          },
          chatContactId: String(hydratedById.id),
          bradialLabels: normalizeLabels(hydratedById.labels),
          email: hydratedById.email ? String(hydratedById.email).trim() : existingLead?.email || null,
          phone: normalizePhone(hydratedById.phoneNumber || hydratedById.phone_number || payload.phoneNumber),
        }
      }
    }

    if (!linkedSyncTarget && !resolvedContact?.chatContactId && payload.phoneNumber) {
      const hydratedExisting = await hydrateContactFromPhone(
        payload.phoneNumber,
        preferredContactId,
        {
          canonicalName: payload.name || task?.name || existingLead?.name || null,
          canonicalEmail: payload.email || existingLead?.email || null,
          pageLimit: contactSearchPages,
        },
      )

      if (hydratedExisting?.id) {
        resolvedContact = {
          ...(existingLead || {}),
          ...toLead(hydratedExisting, new Date().toISOString()),
          raw: {
            ...(existingLead?.raw || {}),
            bradialContactId: String(hydratedExisting.id),
            bradialLabels: normalizeLabels(hydratedExisting.labels),
            bradialEmail: hydratedExisting.email ? String(hydratedExisting.email).trim() : null,
          },
          chatContactId: String(hydratedExisting.id),
          bradialLabels: normalizeLabels(hydratedExisting.labels),
          email: hydratedExisting.email ? String(hydratedExisting.email).trim() : existingLead?.email || null,
          phone: normalizePhone(hydratedExisting.phoneNumber || hydratedExisting.phone_number || payload.phoneNumber),
        }
      }
    }

    const directContactId = normalizeContactRecordId(
      preferredContactId || resolvePartnerContactId(resolvedContact, existingLead),
    )
    const skipContactLookup = linkedSyncTarget && !directContactId

    if (skipContactLookup) {
      contactOperation = 'noop'
    } else if (resolvedContact && isSamePayload(resolvedContact, payload)) {
      contactOperation = 'noop'
    } else if (dryRun) {
      contactOperation = directContactId || resolvedContact?.chatContactId ? 'update' : 'create'
    } else if (directContactId || resolvedContact?.chatContactId) {
      const targetContactId = String(directContactId || resolvedContact?.chatContactId || '').trim()
      await request(`/v2/public-api/v1/contacts/${targetContactId}`, {
        method: 'PATCH',
        body: payload,
      })

      resolvedContact =
        (await fetchContactSafely(targetContactId)) ||
        buildLinkedLeadStub(resolvedContact, payload, {
          preferredContactId: targetContactId,
          preferredChatContactId,
          preferredConversationId,
        })
      if (!resolvedContact) {
        throw new Error(`Contato Bradial ${targetContactId} nao foi reidratado apos update.`)
      }
      contactOperation = 'update'
    } else if (!allowCreate) {
      contactOperation = 'create_disabled'
    } else {
      try {
        const created = await request('/v2/public-api/v1/contacts', {
          method: 'POST',
          body: createPayload,
        })
        resolvedContact =
          (created && typeof created === 'object' && created.id ? created : null) ||
          (await hydrateContactFromPhone(payload.phoneNumber, preferredContactId, {
            canonicalName: payload.name || task?.name || existingLead?.name || null,
            canonicalEmail: payload.email || existingLead?.email || null,
            pageLimit: contactSearchPages,
          }))
        if (!resolvedContact) {
          throw new Error(`Contato Bradial nao foi localizado apos create para ${payload.phoneNumber}.`)
        }
        contactOperation = 'create'
      } catch (error) {
        if (!isPhoneAlreadyInUseError(error)) {
          throw error
        }

        const recoveredContact =
          (await hydrateContactFromId(preferredContactId, payload.phoneNumber)) ||
          (await hydrateContactFromPhone(payload.phoneNumber, preferredContactId, {
            canonicalName: payload.name || task?.name || existingLead?.name || null,
            canonicalEmail: payload.email || existingLead?.email || null,
            pageLimit: contactSearchPages,
          }))

        if (!recoveredContact?.id) {
          throw error
        }

        if (isSamePayload(toLead(recoveredContact, new Date().toISOString()), payload)) {
          resolvedContact = recoveredContact
          contactOperation = 'noop'
        } else {
          await request(`/v2/public-api/v1/contacts/${recoveredContact.id}`, {
            method: 'PATCH',
            body: payload,
          })

          resolvedContact =
            (await fetchContact(recoveredContact.id).catch(() => null)) ||
            (await hydrateContactFromId(recoveredContact.id, payload.phoneNumber)) ||
            recoveredContact

          contactOperation = 'update'
        }
      }
    }

    const resolvedPartnerContactId =
      normalizeContactRecordId(directContactId || resolvePartnerContactId(resolvedContact, existingLead)) || null

    if (
      !dryRun &&
      resolvedPartnerContactId &&
      !['noop', 'create_disabled'].includes(contactOperation)
    ) {
      const verifiedContact = await fetchContact(resolvedPartnerContactId)
      const verifiedLabels = normalizeLabels(verifiedContact?.labels)
      if (
        syncContactLabels &&
        contactOperation === 'create' &&
        !labelsEqual(verifiedLabels, createPayload.labels || [])
      ) {
        throw new Error(
          `Contato Bradial ${resolvedPartnerContactId} nao convergiu para as etiquetas esperadas.`,
        )
      }
      resolvedContact = verifiedContact
    }

    let consolidatedChatContact = null
    if (!dryRun && resolvedPartnerContactId && !linkedSyncTarget) {
      consolidatedChatContact = await ensureSingleChatContact({
        baseContactId: resolvedPartnerContactId,
        phone: payload.phoneNumber,
        name: payload.name || task?.name || existingLead?.name || null,
        email: payload.email || null,
      }).catch((error) => {
        pushLog('warning', 'Falha ao consolidar contato duplicado no chat', error.message, {
          taskName: task?.name || null,
          phone: payload.phoneNumber,
          contactId: resolvedPartnerContactId || null,
        })
        return null
      })
    }

    let conversationSync = {
      enabled: false,
      skipped: true,
      reason: 'chat_api_not_configured',
    }

    try {
      conversationSync = await syncConversationStageLabels({
        contactId:
          resolvedPartnerContactId ||
          resolvedContact?.chatContactId ||
          preferredChatContactId ||
          existingLead?.chatContactId ||
          null,
        phone: payload.phoneNumber,
        name: payload.name || task?.name || existingLead?.name || null,
        targetStageLabel: targetStageLabel || opportunityLabel,
        targetStageLabels,
        controlledStageLabels: options.controlledStageLabels || [],
        dryRun,
        preferredChatContactId:
          consolidatedChatContact?.contactId || preferredChatContactId || null,
        preferredConversationId: preferredConversationId || null,
      })
    } catch (error) {
      conversationSync = {
        enabled: chatEnabled,
        skipped: true,
        reason: 'conversation_sync_failed',
        error: error.message,
      }
      pushLog(
        'warning',
        'Falha no sync de etiquetas da conversa',
        error.message,
        {
          taskName: task?.name || null,
          phone: payload.phoneNumber,
          chatContactId:
            resolvedPartnerContactId ||
            resolvedContact?.chatContactId ||
            existingLead?.chatContactId ||
            null,
        },
      )
    }

    let metadataSync = {
      enabled: chatEnabled,
      skipped: true,
      reason: 'conversation_not_found',
    }

    try {
      metadataSync = await syncConversationMetadata({
        conversationId: conversationSync?.conversationId || preferredConversationId || null,
        task,
        clickupContext: options.clickupContext || null,
        chatAgents: options.chatAgents || [],
        dryRun,
      })
    } catch (error) {
      metadataSync = {
        enabled: chatEnabled,
        skipped: true,
        reason: 'conversation_metadata_sync_failed',
        error: error.message,
      }
      pushLog(
        'warning',
        'Falha no sync de contexto da conversa',
        error.message,
        {
          taskName: task?.name || null,
          phone: payload.phoneNumber,
          conversationId: conversationSync?.conversationId || preferredConversationId || null,
        },
      )
    }

    return {
      operation:
        contactOperation === 'noop' &&
        (
          conversationSync.operation === 'create' ||
          conversationSync.operation === 'update' ||
          metadataSync.operation === 'update'
        )
          ? 'update'
          : contactOperation,
      contactOperation,
      dryRun,
      payload,
      contact: resolvedContact,
      consolidatedChatContact: consolidatedChatContact || null,
      stageLabel: targetStageLabel || opportunityLabel,
      stageLabels: targetStageLabels,
      previousStageLabels,
      conversationSync,
      metadataSync,
    }
  }

  async function fetchSnapshot(trigger = 'manual') {
    const snapshotAt = new Date().toISOString()
    const normalizedTrigger = String(trigger || '').trim().toLowerCase()
    const shouldHydrateListedContacts =
      normalizedTrigger.includes('startup') ||
      normalizedTrigger.includes('bootstrap') ||
      (normalizedTrigger.includes('manual') &&
        !normalizedTrigger.includes('reconcile') &&
        !normalizedTrigger.includes('deferred'))
    const [contacts, agents, inboxes, chatAgents] = await Promise.all([
      listPaged('/v2/public-api/v1/contacts'),
      listPaged('/v2/public-api/v1/agents'),
      listPaged('/v2/public-api/v1/inboxes'),
      listAssignableChatAgents().catch(() => []),
    ])
    const hydratedContacts = shouldHydrateListedContacts ? await hydrateContacts(contacts) : contacts

    const leads = hydratedContacts.map((contact) => toLead(contact, snapshotAt))
    pushLog(
      'success',
      'Bradial carregado',
      `${leads.length} contatos, ${agents.length} agentes e ${inboxes.length} inboxes importados`,
      { trigger },
    )

    return {
      enabled: true,
      source: 'bradial-partner-api',
      chatEnabled,
      snapshotAt,
      contacts,
      leads,
      agents,
      chatAgents,
      inboxes,
    }
  }

  async function hydrateContactsByIds(contactIds = [], snapshotAt = new Date().toISOString()) {
    const normalizedIds = [...new Set((contactIds || []).map((item) => String(item || '').trim()).filter(Boolean))]
    if (!normalizedIds.length) {
      return {
        contacts: [],
        leads: [],
      }
    }

    const contacts = []
    const chunkSize = 4

    for (let index = 0; index < normalizedIds.length; index += chunkSize) {
      const chunk = normalizedIds.slice(index, index + chunkSize)
      const resolvedChunk = await Promise.all(chunk.map((contactId) => fetchContactSafely(contactId).catch(() => null)))
      contacts.push(...resolvedChunk.filter(Boolean))
    }

    return {
      contacts,
      leads: contacts.map((contact) => toLead(contact, snapshotAt)),
    }
  }

  return {
    fetchSnapshot,
    hydrateContactsByIds,
    upsertOpportunityContact,
    syncLinkedConversationMetadata,
    fetchConversationLabels: listConversationLabels,
    fetchChatContactLabels: listChatContactLabels,
    fetchConversation,
    listChatAccountLabels,
    listAssignableChatAgents,
    opportunityLabel,
    chatEnabled,
  }
}

