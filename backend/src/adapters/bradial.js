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
  resolveBradialStageLabel,
  stripControlledStageLabels,
} from '../services/clickupStageLabels.js'

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
  if (labelsIncludeEquivalent(labels, 'negócio-fechado')) return 'resolved'
  if (labelsIncludeEquivalent(labels, 'em-negociação')) return 'pending'
  if (labelsIncludeEquivalent(labels, 'qualificação')) return 'pending'
  if (labelsIncludeEquivalent(labels, 'reuniao-agendada')) return 'pending'
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
    const targetStageLabel =
      String(
        options.targetStageLabel || resolveBradialStageLabel(task?.status, configuredStageLabelMap) || '',
      ).trim() || null
    const controlledStageLabels = Array.isArray(options.controlledStageLabels)
      ? options.controlledStageLabels
      : []
    const preservedLabels = stripControlledStageLabels(
      existingLead?.bradialLabels || existingLead?.raw?.bradialLabels || [],
      controlledStageLabels,
    )
    const labels = mergeLabels(preservedLabels, [targetStageLabel || opportunityLabel].filter(Boolean))

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

    const chatContact = await resolveChatContact(contactId, phone, preferredChatContactId)
    const fallbackConversation = chatContact?.id
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

    const conversations = chatContact?.id ? await listContactConversations(chatContact.id) : []
    const preferredConversation = preferredConversationId
      ? {
          id: preferredConversationId,
          labels: await listConversationLabels(preferredConversationId).catch(() => []),
          meta: {
            sender: {
              id: chatContact?.id || preferredChatContactId || contactId,
            },
          },
        }
      : null
    const discoveredConversation =
      fallbackConversation ||
      pickBestConversation(conversations, chatInboxId) ||
      (await findConversationByIdentity({
        contactId: chatContact?.id || preferredChatContactId || contactId,
        phone,
        name,
      }))
    const knownConversations = dedupeConversations(
      [preferredConversation, discoveredConversation, ...(conversations || [])].filter(Boolean),
    )
    const conversation = preferredConversation || discoveredConversation || knownConversations[0] || null
    const effectiveChatContactId = String(
      chatContact?.id || conversation?.meta?.sender?.id || preferredChatContactId || contactId,
    )
    const resolvedControlledChatStageLabels = await Promise.all(
      (controlledStageLabels || []).map((label) => resolveAccountLabelTitle(label)),
    )
    const controlledChatStageLabels = resolvedControlledChatStageLabels.map((label) =>
      toConversationLabelValue(label),
    )
    const targetAccountLabel = await resolveAccountLabelTitle(targetStageLabel || opportunityLabel)
    const targetConversationLabel = toConversationLabelValue(targetAccountLabel)
    const currentChatContactLabels = syncContactLabels && effectiveChatContactId
      ? await listChatContactLabels(effectiveChatContactId).catch(() => [])
      : []
    const nextChatContactLabels = syncContactLabels
      ? mergeLabels(
          stripControlledStageLabels(currentChatContactLabels, controlledChatStageLabels),
          [targetConversationLabel].filter(Boolean),
        )
      : []

    let contactLabelOperation = syncContactLabels ? 'noop' : 'disabled'
    let syncedChatContactLabels = currentChatContactLabels

    const contactLabelsChanged = syncContactLabels
      ? !labelsEqual(currentChatContactLabels, nextChatContactLabels)
      : false

    if (contactLabelsChanged) {
      contactLabelOperation = 'update'
      syncedChatContactLabels = dryRun
        ? nextChatContactLabels
        : currentChatContactLabels
    }

    const applyChatContactLabelState = async () => {
      if (!syncContactLabels || !contactLabelsChanged || dryRun || !effectiveChatContactId) {
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

    if (!conversation?.id) {
      syncedChatContactLabels = await applyChatContactLabelState()
      return {
        enabled: true,
        skipped: true,
        reason: 'conversation_not_found',
        operation: 'noop',
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
          [targetConversationLabel].filter(Boolean),
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
        operation: 'noop',
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
        contactLabelOperation === 'update' ||
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
    const targetStageLabel =
      String(
        options.targetStageLabel || resolveBradialStageLabel(task?.status, configuredStageLabelMap) || '',
      ).trim() || null
    const payload = buildContactPayload(task, existingLead, {
      includeLabels: false,
      targetStageLabel,
      controlledStageLabels: options.controlledStageLabels,
    })
    const createPayload = buildContactPayload(task, existingLead, {
      includeLabels: syncContactLabels,
      targetStageLabel,
      controlledStageLabels: options.controlledStageLabels,
    })
    if (syncContactLabels) {
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

    if (!resolvedContact?.chatContactId && preferredContactId) {
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

    if (!resolvedContact?.chatContactId && payload.phoneNumber) {
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

    if (resolvedContact && isSamePayload(resolvedContact, payload)) {
      contactOperation = 'noop'
    } else if (dryRun) {
      contactOperation = resolvedContact?.chatContactId ? 'update' : 'create'
    } else if (resolvedContact?.chatContactId) {
      const targetContactId = resolvedContact.chatContactId
      await request(`/v2/public-api/v1/contacts/${targetContactId}`, {
        method: 'PATCH',
        body: payload,
      })

      resolvedContact =
        (await fetchContact(targetContactId).catch(() => null)) ||
        (await hydrateContactFromPhone(payload.phoneNumber, targetContactId))
      if (!resolvedContact) {
        throw new Error(`Contato Bradial ${targetContactId} nao foi reidratado apos update.`)
      }
      contactOperation = 'update'
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

    const resolvedPartnerContactId = resolvePartnerContactId(resolvedContact, existingLead)

    if (!dryRun && resolvedPartnerContactId && contactOperation !== 'noop') {
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
    if (!dryRun && resolvedPartnerContactId) {
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
          existingLead?.chatContactId ||
          null,
        phone: payload.phoneNumber,
        name: payload.name || task?.name || existingLead?.name || null,
        targetStageLabel: targetStageLabel || opportunityLabel,
        controlledStageLabels: options.controlledStageLabels || [],
        dryRun,
        preferredChatContactId:
          consolidatedChatContact?.contactId || options.preferredChatContactId || null,
        preferredConversationId: options.preferredConversationId || null,
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

    return {
      operation:
        contactOperation === 'noop' && conversationSync.operation === 'update' ? 'update' : contactOperation,
      contactOperation,
      dryRun,
      payload,
      contact: resolvedContact,
      consolidatedChatContact: consolidatedChatContact || null,
      stageLabel: targetStageLabel || opportunityLabel,
      previousStageLabels,
      conversationSync,
    }
  }

  async function fetchSnapshot(trigger = 'manual') {
    const snapshotAt = new Date().toISOString()
    const [contacts, agents, inboxes] = await Promise.all([
      listPaged('/v2/public-api/v1/contacts'),
      listPaged('/v2/public-api/v1/agents'),
      listPaged('/v2/public-api/v1/inboxes'),
    ])
    const hydratedContacts = await hydrateContacts(contacts)

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

    const contacts = (
      await Promise.all(normalizedIds.map((contactId) => fetchContactSafely(contactId)))
    ).filter(Boolean)

    return {
      contacts,
      leads: contacts.map((contact) => toLead(contact, snapshotAt)),
    }
  }

  return {
    fetchSnapshot,
    hydrateContactsByIds,
    upsertOpportunityContact,
    fetchConversationLabels: listConversationLabels,
    fetchChatContactLabels: listChatContactLabels,
    opportunityLabel,
    chatEnabled,
  }
}
