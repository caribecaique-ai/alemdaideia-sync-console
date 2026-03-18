import {
  startTransition,
  useDeferredValue,
  useEffect,
  useEffectEvent,
  useRef,
  useState,
} from 'react'
import './App.css'
import {
  initialConfig,
  initialExceptions,
  initialHealth,
  initialLeads,
  initialLogs,
  mockEventCatalog,
} from './mockState'

const STORAGE_NAMESPACE = 'bradial-clickup-sync-ui-v3'

function readStorage(key, fallback) {
  const fullKey = `${STORAGE_NAMESPACE}:${key}`
  const raw = window.localStorage.getItem(fullKey)
  if (!raw) return fallback

  try {
    return JSON.parse(raw)
  } catch {
    return fallback
  }
}

function writeStorage(key, value) {
  const fullKey = `${STORAGE_NAMESPACE}:${key}`
  window.localStorage.setItem(fullKey, JSON.stringify(value))
}

function usePersistentState(key, fallback) {
  const [state, setState] = useState(() => readStorage(key, fallback))

  useEffect(() => {
    writeStorage(key, state)
  }, [key, state])

  return [state, setState]
}

function normalizeBackendUrl(value) {
  return String(value || '').trim().replace(/\/$/, '')
}

function buildBackendHeaders(config, extraHeaders = {}) {
  const headers = new Headers(extraHeaders)
  const adminApiToken = String(config?.adminApiToken || '').trim()

  if (adminApiToken) {
    headers.set('Authorization', `Bearer ${adminApiToken}`)
  }

  return headers
}

function fetchBackend(config, path, options = {}) {
  const backendUrl = normalizeBackendUrl(config?.backendUrl)
  const requestPath = String(path || '').startsWith('/') ? path : `/${path}`
  return fetch(`${backendUrl}${requestPath}`, {
    ...options,
    headers: buildBackendHeaders(config, options.headers),
  })
}

function buildEventStreamUrl(config) {
  const backendUrl = normalizeBackendUrl(config?.backendUrl)
  const url = new URL(`${backendUrl}/events`)
  const adminApiToken = String(config?.adminApiToken || '').trim()

  if (adminApiToken) {
    url.searchParams.set('adminToken', adminApiToken)
  }

  return url.toString()
}

function formatDate(value) {
  if (!value) return 'sem registro'

  try {
    return new Intl.DateTimeFormat('pt-BR', {
      dateStyle: 'short',
      timeStyle: 'short',
    }).format(new Date(value))
  } catch {
    return value
  }
}

function createLog(level, title, message, leadId = null) {
  return {
    id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
    level,
    title,
    message,
    leadId,
    createdAt: new Date().toISOString(),
  }
}

function toEventTimestamp(value) {
  if (!value) return 0
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function mergeRecentEntries(current = [], incoming = [], limit = 160) {
  const map = new Map()

  for (const item of [...incoming, ...current]) {
    if (!item?.id || map.has(item.id)) continue
    map.set(item.id, item)
  }

  return [...map.values()]
    .sort((left, right) => toEventTimestamp(right.createdAt || right.sentAt) - toEventTimestamp(left.createdAt || left.sentAt))
    .slice(0, limit)
}

function buildActivityLookup(leads = [], pendingContacts = []) {
  const byTaskId = new Map()
  const byPhone = new Map()
  const byConversationId = new Map()
  const byContactId = new Map()

  const register = (map, key, value) => {
    const normalizedKey = String(key || '').trim()
    const normalizedValue = String(value || '').trim()
    if (!normalizedKey || !normalizedValue || map.has(normalizedKey)) return
    map.set(normalizedKey, normalizedValue)
  }

  for (const lead of Array.isArray(leads) ? leads : []) {
    register(byTaskId, lead?.clickupTaskId, lead?.name)
    register(byPhone, normalizeLeadPhone(lead?.phone), lead?.name)
    register(byConversationId, lead?.conversationId || lead?.chatConversationId, lead?.name)
    register(byContactId, lead?.raw?.bradialContactId || lead?.chatContactId, lead?.name)
  }

  for (const item of Array.isArray(pendingContacts) ? pendingContacts : []) {
    register(byTaskId, item?.taskId, item?.taskName || item?.bradialContactName)
    register(byPhone, normalizeLeadPhone(item?.phone), item?.taskName || item?.bradialContactName)
    register(byConversationId, item?.bradialConversationId, item?.taskName || item?.bradialContactName)
    register(byContactId, item?.bradialContactId, item?.bradialContactName || item?.taskName)
  }

  return {
    byTaskId,
    byPhone,
    byConversationId,
    byContactId,
  }
}

function resolveActivityName(entry, lookup = null) {
  const directName =
    entry?.contactName ||
    entry?.taskName ||
    entry?.result?.bradialContactName ||
    entry?.result?.payload?.name ||
    entry?.meta?.contactName ||
    entry?.meta?.taskName ||
    null

  if (directName) return directName
  if (!lookup) return null

  const taskId = entry?.taskId || entry?.result?.taskId || entry?.meta?.taskId || null
  const phone = entry?.phone || entry?.result?.phone || entry?.meta?.phone || null
  const conversationId =
    entry?.conversationId || entry?.result?.conversationSync?.conversationId || entry?.meta?.conversationId || null
  const contactId =
    entry?.chatContactId ||
    entry?.result?.conversationSync?.chatContactId ||
    entry?.result?.bradialContactId ||
    entry?.meta?.chatContactId ||
    entry?.meta?.bradialContactId ||
    null

  return (
    lookup.byTaskId.get(String(taskId || '').trim()) ||
    lookup.byConversationId.get(String(conversationId || '').trim()) ||
    lookup.byContactId.get(String(contactId || '').trim()) ||
    lookup.byPhone.get(String(normalizeLeadPhone(phone) || '').trim()) ||
    null
  )
}

function describeSyncSkipReason(entry) {
  const reason = String(entry?.result?.reason || entry?.reason || '').trim()

  switch (reason) {
    case 'suppressed_clickup_duplicate':
      return `duplicata no ClickUp; o sistema usa a task principal ${entry?.result?.canonicalTaskId || 'nao identificada'}.`
    case 'conversation_required_for_stage_label':
      return 'contato encontrado, mas a conversa ainda nao existe para receber a etiqueta.'
    case 'contact_labels_synced_without_conversation':
      return 'contato encontrado sem conversa; o sistema aguardou a conversa para etiquetar corretamente.'
    case 'missing_phone':
      return 'task sem telefone valido para vinculo.'
    case 'ambiguous_clickup_phone':
      return 'telefone encontrado em mais de uma task ativa do ClickUp.'
    case 'ambiguous_bradial_phone':
      return 'telefone encontrado em mais de um contato no Bradial.'
    case 'task_not_in_scope':
      return 'task fora do escopo monitorado.'
    case 'terminal_status':
      return 'task em status terminal sem destino valido no Bradial.'
    case 'conversation_not_found':
      return 'a conversa ainda nao foi encontrada no Bradial.'
    default:
      return reason || 'sem motivo informado.'
  }
}

function buildActivityStats(syncAudit = []) {
  const attempts = (Array.isArray(syncAudit) ? syncAudit : []).filter((entry) =>
    ['sync_succeeded', 'sync_skipped', 'sync_failed', 'sync_failed_retrying'].includes(entry?.type),
  )
  const succeeded = attempts.filter((entry) => entry.type === 'sync_succeeded').length
  const failed = attempts.filter((entry) => entry.type === 'sync_failed').length
  const retrying = attempts.filter((entry) => entry.type === 'sync_failed_retrying').length
  const skipped = attempts.filter((entry) => entry.type === 'sync_skipped').length
  const duplicateSkips = attempts.filter(
    (entry) => String(entry?.result?.reason || '') === 'suppressed_clickup_duplicate',
  ).length
  const actionableSkips = Math.max(0, skipped - duplicateSkips)
  const usefulBase = succeeded + failed + retrying + actionableSkips

  return {
    attempts: attempts.length,
    succeeded,
    failed,
    retrying,
    skipped,
    duplicateSkips,
    actionableSkips,
    successRate: usefulBase > 0 ? Math.round((succeeded / usefulBase) * 100) : null,
  }
}

function describeSyncSuccess(entry, subject) {
  const stageLabel = entry?.result?.stageLabel || null
  const priority = entry?.result?.metadataSync?.priority || null
  const conversationLabelOperation = entry?.result?.conversationSync?.conversationLabelOperation || null
  const metadataOperation = entry?.result?.metadataSync?.operation || null
  const assignmentOperation = entry?.result?.metadataSync?.assignmentOperation || null
  const fragments = []

  if (stageLabel && conversationLabelOperation === 'update') {
    fragments.push(`recebeu a etiqueta ${stageLabel} na conversa`)
  } else if (stageLabel && conversationLabelOperation === 'noop') {
    fragments.push(`permaneceu com a etiqueta ${stageLabel} na conversa`)
  }

  if (metadataOperation === 'update' && priority) {
    fragments.push(`ficou com prioridade ${priority}`)
  }

  if (assignmentOperation === 'update') {
    fragments.push('foi atribuida ao agente correspondente')
  } else if (assignmentOperation === 'clear') {
    fragments.push('foi desatribuida no Bradial')
  } else if (assignmentOperation === 'unmatched') {
    fragments.push('nao encontrou agente correspondente para o responsavel do ClickUp')
  }

  if (entry?.result?.conversationSync?.reason === 'conversation_required_for_stage_label') {
    fragments.push('segue sem etiqueta porque ainda nao existe conversa vinculada')
  }

  if (!fragments.length) {
    return `${subject} foi verificada e ja estava sincronizada.`
  }

  return `${subject} ${fragments.join(', ')}.`
}

function describeAuditSource(entry) {
  if (String(entry?.source || '').includes('bradial')) return 'Bradial -> ClickUp'
  return 'ClickUp -> Bradial'
}

function describeLogSource(entry) {
  const title = String(entry?.title || '')
  const message = String(entry?.message || '')
  const merged = `${title} ${message}`

  if (/Bradial -> ClickUp/i.test(merged)) return 'Bradial -> ClickUp'
  if (/ClickUp -> Bradial/i.test(merged)) return 'ClickUp -> Bradial'
  if (/Webhook ClickUp/i.test(merged)) return 'ClickUp webhook'
  if (/Webhook Bradial/i.test(merged)) return 'Bradial webhook'
  return 'backend'
}

function summarizeAuditEntry(entry, lookup = null) {
  const taskId = entry?.taskId || entry?.result?.taskId || null
  const conversationId = entry?.conversationId || entry?.result?.conversationSync?.conversationId || null
  const phone = entry?.phone || entry?.result?.phone || null
  const contactName = resolveActivityName(entry, lookup)
  const subject = contactName || `Task ${taskId || 'n/a'}`

  if (entry?.type === 'sync_enqueued') {
    return {
      title: 'Sync enfileirado',
      message: `${subject} entrou na fila por ${entry.trigger || 'evento'}.`,
      reference: taskId || phone || null,
    }
  }

  if (entry?.type === 'sync_started') {
    return {
      title: 'Sync iniciado',
      message: `Processando ${subject} via ${entry.trigger || 'evento'}.`,
      reference: taskId || phone || null,
    }
  }

  if (entry?.type === 'sync_succeeded') {
    return {
      title: 'Sync concluido',
      message: describeSyncSuccess(entry, subject),
      reference: conversationId || taskId || phone || null,
    }
  }

  if (entry?.type === 'sync_skipped') {
    return {
      title: 'Sync ignorado',
      message: `${subject} foi ignorada: ${describeSyncSkipReason(entry)}`,
      reference: taskId || phone || null,
    }
  }

  if (entry?.type === 'sync_failed' || entry?.type === 'sync_failed_retrying') {
    return {
      title: entry.type === 'sync_failed_retrying' ? 'Sync falhou e vai tentar de novo' : 'Sync falhou',
      message: `${subject} falhou: ${entry?.error || 'erro nao informado'}.`,
      reference: taskId || phone || null,
    }
  }

  if (entry?.type === 'sync_bradial_to_clickup') {
    return {
      title: 'Status Bradial -> ClickUp',
      message: `${subject} foi movida para ${entry?.targetStatus || 'status indefinido'} a partir da tag ${entry?.targetLabel || 'n/a'} no Bradial.`,
      reference: taskId || conversationId || null,
    }
  }

  if (entry?.type === 'sync_bradial_priority_to_clickup') {
    return {
      title: 'Prioridade Bradial -> ClickUp',
      message: `${subject} recebeu prioridade ${entry?.targetPriority || 'nenhuma'} a partir do Bradial.`,
      reference: taskId || conversationId || null,
    }
  }

  if (entry?.type === 'sync_bradial_to_clickup_failed' || entry?.type === 'sync_bradial_priority_to_clickup_failed') {
    return {
      title: 'Sync Bradial -> ClickUp falhou',
      message: entry?.error || 'erro nao informado',
      reference: conversationId || taskId || null,
    }
  }

  return {
    title: entry?.type || 'Evento de sync',
    message: entry?.error || entry?.result?.reason || entry?.trigger || 'evento registrado na auditoria',
    reference: taskId || conversationId || phone || null,
  }
}

function buildActivityItems(logs = [], syncAudit = [], leads = [], pendingContacts = []) {
  const lookup = buildActivityLookup(leads, pendingContacts)

  const logItems = (Array.isArray(logs) ? logs : []).map((entry) => ({
    id: `log:${entry.id}`,
    sourceKind: 'log',
    sourceLabel: describeLogSource(entry),
    level: entry.level || 'info',
    title: entry.title || 'Log operacional',
    message: entry.message || '',
    contactName: resolveActivityName(entry, lookup),
    reference:
      entry?.meta?.taskId ||
      entry?.meta?.conversationId ||
      entry?.meta?.phone ||
      entry?.leadId ||
      null,
    createdAt: entry.createdAt || new Date().toISOString(),
  }))

  const auditItems = (Array.isArray(syncAudit) ? syncAudit : []).map((entry) => {
    const summary = summarizeAuditEntry(entry, lookup)
    return {
      id: `audit:${entry.id}`,
      sourceKind: 'audit',
      sourceLabel: describeAuditSource(entry),
      level: entry.level || 'info',
      title: summary.title,
      message: summary.message,
      contactName: resolveActivityName(entry, lookup),
      reference: summary.reference,
      createdAt: entry.createdAt || new Date().toISOString(),
    }
  })

  return [...auditItems, ...logItems].sort(
    (left, right) => toEventTimestamp(right.createdAt) - toEventTimestamp(left.createdAt),
  )
}

function normalizeLeadPhone(value) {
  const digits = String(value || '').replace(/\D/g, '')
  if (!digits) return null
  if (/^55\d{2}9\d{8}$/.test(digits)) return `${digits.slice(0, 4)}${digits.slice(5)}`
  if (/^55\d{2}\d{8}$/.test(digits)) return digits
  return digits
}

function leadHealthRank(value) {
  if (value === 'risk') return 3
  if (value === 'warning') return 2
  if (value === 'healthy') return 1
  return 0
}

function toLeadTimestamp(value) {
  if (!value) return 0
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function leadScore(lead) {
  return (
    (lead?.clickupTaskId ? 200 : 0) +
    (lead?.chatConversationId || lead?.conversationId ? 80 : 0) +
    (lead?.raw?.bradialContactId || lead?.chatContactId ? 60 : 0) +
    ((lead?.bradialLabels || lead?.raw?.bradialLabels || []).length ? 30 : 0) +
    (lead?.clickupStage ? 20 : 0) +
    (lead?.syncEnabled ? 10 : 0) +
    toLeadTimestamp(lead?.lastSyncAt)
  )
}

function chooseLeadValue(leads, selector, fallback = null) {
  for (const lead of leads) {
    const value = selector(lead)
    if (value === undefined || value === null) continue
    if (typeof value === 'string' && !value.trim()) continue
    if (Array.isArray(value) && !value.length) continue
    return value
  }

  return fallback
}

function getLeadKeys(lead) {
  const keys = new Set()
  const phone = normalizeLeadPhone(lead.phone)
  const clickupTaskId = String(lead?.clickupTaskId || '').trim() || null
  const bradialContactId =
    String(lead?.raw?.bradialContactId || lead?.chatContactId || '').trim() || null
  const chatConversationId =
    String(lead?.chatConversationId || lead?.conversationId || '').trim() || null

  if (clickupTaskId) keys.add(`task:${clickupTaskId}`)
  if (phone) keys.add(`phone:${phone}`)
  if (bradialContactId) keys.add(`contact:${bradialContactId}`)
  if (chatConversationId) keys.add(`conversation:${chatConversationId}`)
  if (!keys.size) keys.add(`lead:${lead.id}`)

  return [...keys]
}

function chooseCanonicalLeadId(leads, preferredLead) {
  const clickupTaskId = chooseLeadValue(leads, (lead) => lead.clickupTaskId)
  if (clickupTaskId) return `lead-task-${clickupTaskId}`

  const phone = normalizeLeadPhone(chooseLeadValue(leads, (lead) => lead.phone))
  if (phone) return `lead-phone-${phone}`

  const bradialContactId = chooseLeadValue(
    leads,
    (lead) => lead?.raw?.bradialContactId || lead?.chatContactId,
  )
  if (bradialContactId) return `lead-contact-${bradialContactId}`

  return preferredLead.id
}

function mergeLeadGroup(group) {
  const sorted = [...group].sort((left, right) => leadScore(right) - leadScore(left))
  const preferredLead = sorted[0]
  const canonicalId = chooseCanonicalLeadId(sorted, preferredLead)
  const labels =
    chooseLeadValue(sorted, (lead) => lead.bradialLabels) ||
    chooseLeadValue(sorted, (lead) => lead?.raw?.bradialLabels, [])

  return {
    ...preferredLead,
    id: canonicalId,
    name: chooseLeadValue(sorted, (lead) => lead.name, preferredLead.name),
    phone: chooseLeadValue(sorted, (lead) => lead.phone, preferredLead.phone),
    email: chooseLeadValue(sorted, (lead) => lead.email, preferredLead.email),
    clickupTaskId: chooseLeadValue(sorted, (lead) => lead.clickupTaskId),
    clickupStage: chooseLeadValue(sorted, (lead) => lead.clickupStage),
    clickupTaskUrl: chooseLeadValue(sorted, (lead) => lead.clickupTaskUrl),
    clickupWorkspace: chooseLeadValue(sorted, (lead) => lead.clickupWorkspace),
    clickupListName: chooseLeadValue(sorted, (lead) => lead.clickupListName),
    clickupPhoneMatched: chooseLeadValue(sorted, (lead) => lead.clickupPhoneMatched),
    conversationId: chooseLeadValue(sorted, (lead) => lead.conversationId),
    chatConversationId: chooseLeadValue(sorted, (lead) => lead.chatConversationId),
    chatContactId: chooseLeadValue(sorted, (lead) => lead.chatContactId),
    owner: chooseLeadValue(sorted, (lead) => lead.owner, preferredLead.owner),
    chatStatus: chooseLeadValue(sorted, (lead) => lead.chatStatus, preferredLead.chatStatus),
    syncEnabled: sorted.some((lead) => lead.syncEnabled !== false),
    health:
      [...sorted].sort((left, right) => leadHealthRank(right.health) - leadHealthRank(left.health))[0]
        ?.health || preferredLead.health,
    bradialLabels: labels,
    tags: [...new Set(chooseLeadValue(sorted, (lead) => lead.tags, preferredLead.tags || []))],
    summary: chooseLeadValue(sorted, (lead) => lead.summary, preferredLead.summary),
    lastAction: chooseLeadValue(sorted, (lead) => lead.lastAction, preferredLead.lastAction),
    lastSyncAt: chooseLeadValue(sorted, (lead) => lead.lastSyncAt, preferredLead.lastSyncAt),
    matchCount: Math.max(...sorted.map((lead) => Number(lead.matchCount || 0)), 0),
    raw: {
      ...(preferredLead.raw || {}),
      bradialContactId: chooseLeadValue(
        sorted,
        (lead) => lead?.raw?.bradialContactId || lead?.chatContactId,
      ),
      bradialLabels: labels,
      bradialEmail: chooseLeadValue(sorted, (lead) => lead?.raw?.bradialEmail || lead?.email),
      clickupTaskId: chooseLeadValue(sorted, (lead) => lead?.raw?.clickupTaskId || lead?.clickupTaskId),
      mergedLeadIds: sorted.map((lead) => lead.id),
    },
  }
}

function dedupeLeads(items) {
  const leads = Array.isArray(items) ? items.filter(Boolean) : []
  const groups = []

  for (const lead of leads) {
    const keys = getLeadKeys(lead)
    const matches = groups.filter((group) => keys.some((key) => group.keys.has(key)))

    if (!matches.length) {
      groups.push({
        keys: new Set(keys),
        leads: [lead],
      })
      continue
    }

    const primaryGroup = matches[0]
    primaryGroup.leads.push(lead)
    keys.forEach((key) => primaryGroup.keys.add(key))

    for (const duplicateGroup of matches.slice(1)) {
      duplicateGroup.leads.forEach((item) => primaryGroup.leads.push(item))
      duplicateGroup.keys.forEach((key) => primaryGroup.keys.add(key))
      groups.splice(groups.indexOf(duplicateGroup), 1)
    }
  }

  return groups.map((group) => mergeLeadGroup(group.leads))
}

function App() {
  const bootInLiveMode = initialConfig.mode === 'live'
  const [config, setConfig] = usePersistentState('config', initialConfig)
  const [health, setHealth] = usePersistentState('health', initialHealth)
  const [leads, setLeads] = usePersistentState(
    'leads',
    bootInLiveMode ? [] : dedupeLeads(initialLeads),
  )
  const [pendingContacts, setPendingContacts] = usePersistentState('pendingContacts', [])
  const [webhookRegistry, setWebhookRegistry] = usePersistentState('webhookRegistry', {
    publicBaseUrl: '',
    source: null,
    isPublic: false,
    items: [],
  })
  const [exceptions, setExceptions] = usePersistentState(
    'exceptions',
    bootInLiveMode ? [] : initialExceptions,
  )
  const [logs, setLogs] = usePersistentState('logs', bootInLiveMode ? [] : initialLogs)
  const [syncAudit, setSyncAudit] = usePersistentState('syncAudit', [])
  const [selectedLeadId, setSelectedLeadId] = usePersistentState(
    'selectedLeadId',
    bootInLiveMode ? null : initialLeads[0]?.id ?? null,
  )
  const [leadSearch, setLeadSearch] = useState('')
  const [activityFilter, setActivityFilter] = usePersistentState('activityFilter', 'all')
  const [connectionState, setConnectionState] = useState('idle')
  const [syncingTaskId, setSyncingTaskId] = useState(null)
  const [creatingWebhookUrl, setCreatingWebhookUrl] = useState(false)
  const [liveSessionHydrated, setLiveSessionHydrated] = useState(false)
  const livePullTimeoutRef = useRef(null)

  const deferredLeadSearch = useDeferredValue(leadSearch)
  const unifiedLeads = dedupeLeads(leads)
  const selectedLead =
    unifiedLeads.find((lead) => lead.id === selectedLeadId) ?? unifiedLeads[0] ?? null

  const openExceptions = exceptions.filter((item) => item.status !== 'resolved')
  const syncedToday = unifiedLeads.filter((lead) => lead.syncEnabled).length
  const healthyLeads = unifiedLeads.filter((lead) => lead.health === 'healthy').length
  const warningLeads = unifiedLeads.filter((lead) => lead.health === 'warning').length
  const actionablePendingContacts = pendingContacts.filter((item) => item.syncAllowed).length
  const activityStats = buildActivityStats(syncAudit)
  const activityItems = buildActivityItems(logs, syncAudit, unifiedLeads, pendingContacts)
  const filteredActivityItems = activityItems.filter((item) => {
    if (activityFilter === 'alerts') {
      return ['warning', 'error', 'risk'].includes(item.level)
    }
    if (activityFilter === 'sync') {
      return item.sourceKind === 'audit'
    }
    return true
  })

  const filteredLeads = unifiedLeads.filter((lead) => {
    const search = deferredLeadSearch.trim().toLowerCase()
    if (!search) return true

    return [
      lead.name,
      lead.phone,
      lead.owner,
      lead.clickupStage,
      lead.chatStatus,
    ]
      .join(' ')
      .toLowerCase()
      .includes(search)
  })

  useEffect(() => {
    if (selectedLeadId && unifiedLeads.some((lead) => lead.id === selectedLeadId)) return
    if (!unifiedLeads[0]) return

    setSelectedLeadId(unifiedLeads[0].id)
  }, [selectedLeadId, setSelectedLeadId, unifiedLeads])

  useEffect(() => {
    setLeads((current) => dedupeLeads(current))
  }, [setLeads])

  useEffect(() => {
    if (config.mode !== 'live') {
      setLiveSessionHydrated(false)
      return
    }

    if (liveSessionHydrated) return

    setLeads([])
    setPendingContacts([])
    setSyncAudit([])
    setSelectedLeadId(null)
    setLiveSessionHydrated(true)
  }, [
    config.mode,
    liveSessionHydrated,
    setLeads,
    setPendingContacts,
    setSyncAudit,
    setSelectedLeadId,
  ])

  useEffect(() => {
    if (config.mode !== 'mock') return

    setHealth(initialHealth)
    setLeads(dedupeLeads(initialLeads))
    setPendingContacts([])
    setWebhookRegistry({
      publicBaseUrl: '',
      source: null,
      isPublic: false,
      items: [],
    })
    setExceptions(initialExceptions)
    setLogs(initialLogs)
    setSyncAudit([])
    setSelectedLeadId(dedupeLeads(initialLeads)[0]?.id ?? null)
  }, [config.mode, setExceptions, setHealth, setLeads, setLogs, setPendingContacts, setSelectedLeadId, setSyncAudit, setWebhookRegistry])

  const pushLog = useEffectEvent((entry) => {
    setLogs((current) => [entry, ...current].slice(0, 120))
  })

  const scheduleLivePull = useEffectEvent((reason = 'sse') => {
    if (config.mode !== 'live' || !config.backendUrl.trim()) return

    if (livePullTimeoutRef.current) {
      window.clearTimeout(livePullTimeoutRef.current)
    }

    livePullTimeoutRef.current = window.setTimeout(() => {
      livePullTimeoutRef.current = null
      void pullLiveData(true)
    }, reason === 'snapshot' ? 150 : 700)
  })

  const runMockTick = useEffectEvent(() => {
    const event = mockEventCatalog[Math.floor(Math.random() * mockEventCatalog.length)]
    const candidateLead = unifiedLeads[Math.floor(Math.random() * unifiedLeads.length)]

    if (!event || !candidateLead) return

    if (event.kind === 'sync-ok') {
      setLeads((current) =>
        current.map((lead) =>
          lead.id === candidateLead.id
            ? {
                ...lead,
                health: 'healthy',
                lastSyncAt: new Date().toISOString(),
                lastAction: 'sync concluido',
              }
            : lead,
        ),
      )
    }

    if (event.kind === 'warning') {
      setLeads((current) =>
        current.map((lead) =>
          lead.id === candidateLead.id
            ? {
                ...lead,
                health: 'warning',
                lastAction: 'aguardando retry',
              }
            : lead,
        ),
      )
    }

    if (event.kind === 'exception') {
      const newException = {
        id: `exc-${Date.now()}`,
        status: 'open',
        leadId: candidateLead.id,
        kind: event.exceptionKind,
        source: event.source,
        detectedAt: new Date().toISOString(),
        phone: candidateLead.phone,
        summary: event.message,
      }

      setExceptions((current) => [newException, ...current].slice(0, 30))
      setLeads((current) =>
        current.map((lead) =>
          lead.id === candidateLead.id
            ? {
                ...lead,
                health: 'risk',
                lastAction: 'excecao em aberto',
              }
            : lead,
        ),
      )
    }

    pushLog(
      createLog(event.level, event.title, event.message, candidateLead.id),
    )
  })

  useEffect(() => {
    if (config.mode !== 'mock') return undefined

    const intervalId = window.setInterval(() => {
      runMockTick()
    }, 9000)

    return () => window.clearInterval(intervalId)
  }, [config.mode, runMockTick])

  useEffect(() => {
    if (config.mode !== 'live' || !config.backendUrl.trim()) return undefined

    const eventSource = new EventSource(buildEventStreamUrl(config))

    const handleSnapshot = () => {
      scheduleLivePull('snapshot')
    }

    const handleSyncAudit = (event) => {
      try {
        const payload = JSON.parse(event.data || '{}')
        setSyncAudit((current) => mergeRecentEntries(current, [payload]))
        if (
          [
            'sync_enqueued',
            'sync_started',
            'sync_succeeded',
            'sync_skipped',
            'sync_failed',
            'sync_failed_retrying',
          ].includes(payload?.type)
        ) {
          scheduleLivePull('sync')
        }
      } catch {}
    }

    eventSource.addEventListener('snapshot_refreshed', handleSnapshot)
    eventSource.addEventListener('sync_audit', handleSyncAudit)

    return () => {
      eventSource.removeEventListener('snapshot_refreshed', handleSnapshot)
      eventSource.removeEventListener('sync_audit', handleSyncAudit)
      eventSource.close()
      if (livePullTimeoutRef.current) {
        window.clearTimeout(livePullTimeoutRef.current)
        livePullTimeoutRef.current = null
      }
    }
  }, [config.adminApiToken, config.backendUrl, config.mode, scheduleLivePull, setSyncAudit])

  const saveConfig = () => {
    pushLog(
      createLog(
        'info',
        'Configuracao salva',
        `Modo ${config.mode} configurado para backend ${config.backendUrl || 'nao definido'}`,
      ),
    )
  }

  const refreshBackendSnapshot = async () => {
    if (config.mode !== 'live' || !config.backendUrl.trim()) {
      pushLog(
        createLog(
          'warning',
          'Refresh indisponivel',
          'Defina o backend e use o modo live para atualizar o snapshot.',
        ),
      )
      return
    }

    setConnectionState('loading')

    try {
      const response = await fetchBackend(config, '/refresh', { method: 'POST' })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status} no endpoint /refresh`)
      }

      await pingBackend(true)
      pushLog(
        createLog('success', 'Snapshot atualizado', 'Backend recarregado com dados reais da Bradial.'),
      )
    } catch (error) {
      pushLog(
        createLog(
          'error',
          'Falha ao atualizar snapshot',
          error.message || 'Nao foi possivel recarregar o backend',
        ),
      )
    } finally {
      setConnectionState('idle')
    }
  }

  const pullLiveData = async (silent = false) => {
    if (config.mode !== 'live' || !config.backendUrl.trim()) return

    try {
      const [
        leadsResponse,
        exceptionsResponse,
        logsResponse,
        pendingContactsResponse,
        webhookIntegrationsResponse,
        syncAuditResponse,
      ] = await Promise.all([
        fetchBackend(config, '/leads'),
        fetchBackend(config, '/exceptions'),
        fetchBackend(config, '/logs'),
        fetchBackend(config, '/clickup/pending-contacts'),
        fetchBackend(config, '/clickup/webhook-integrations'),
        fetchBackend(config, '/sync/audit?limit=120'),
      ])

      if (
        !leadsResponse.ok ||
        !exceptionsResponse.ok ||
        !logsResponse.ok ||
        !pendingContactsResponse.ok ||
        !webhookIntegrationsResponse.ok ||
        !syncAuditResponse.ok
      ) {
        throw new Error('Um dos endpoints respondeu com erro')
      }

      const [
        nextLeads,
        nextExceptions,
        nextLogs,
        nextPendingContacts,
        nextWebhookRegistry,
        nextSyncAudit,
      ] = await Promise.all([
        leadsResponse.json(),
        exceptionsResponse.json(),
        logsResponse.json(),
        pendingContactsResponse.json(),
        webhookIntegrationsResponse.json(),
        syncAuditResponse.json(),
      ])

      setLeads(dedupeLeads(Array.isArray(nextLeads) ? nextLeads : []))
      setExceptions(Array.isArray(nextExceptions) ? nextExceptions : [])
      setLogs(Array.isArray(nextLogs) ? nextLogs : [])
      setPendingContacts(Array.isArray(nextPendingContacts) ? nextPendingContacts : [])
      setSyncAudit(Array.isArray(nextSyncAudit) ? nextSyncAudit : [])
      setWebhookRegistry(
        nextWebhookRegistry && typeof nextWebhookRegistry === 'object'
          ? nextWebhookRegistry
          : {
              publicBaseUrl: '',
              source: null,
              isPublic: false,
              items: [],
            },
      )

      if (!silent) {
        pushLog(
          createLog(
            'success',
            'Dados reais carregados',
            `${Array.isArray(nextLeads) ? nextLeads.length : 0} leads, ${
              Array.isArray(nextPendingContacts) ? nextPendingContacts.length : 0
            } pendencias e ${Array.isArray(nextSyncAudit) ? nextSyncAudit.length : 0} eventos importados do backend`,
          ),
        )
      }
    } catch (error) {
      if (!silent) {
        pushLog(
          createLog(
            'error',
            'Falha ao carregar dados reais',
            error.message || 'Nao foi possivel atualizar leads do backend',
          ),
        )
      }
    }
  }

  const pingBackend = async (silent = false) => {
    if (!config.backendUrl.trim()) {
      setHealth({
        status: 'offline',
        detail: 'Defina o backend antes do teste.',
        lastCheckedAt: new Date().toISOString(),
        latencyMs: null,
      })
      if (!silent) {
        pushLog(
          createLog('warning', 'Backend nao configurado', 'Defina a URL do middleware para testar a conexao.'),
        )
      }
      return
    }

    setConnectionState('loading')
    const startedAt = performance.now()
    const controller = new AbortController()
    const timeoutId = window.setTimeout(() => controller.abort(), 15000)

    try {
      const response = await fetchBackend(config, '/health', {
        signal: controller.signal,
      })
      const payload = await response.json().catch(() => ({}))
      const latencyMs = Math.round(performance.now() - startedAt)

      setHealth({
        status: response.ok ? 'online' : 'degraded',
        detail: payload.error || payload.status || `HTTP ${response.status}`,
        lastCheckedAt: new Date().toISOString(),
        latencyMs,
      })
      setConnectionState('idle')

      if (!silent) {
        pushLog(
          createLog(
            response.ok ? 'success' : 'warning',
            'Health check executado',
            response.ok
              ? `Middleware respondeu em ${latencyMs} ms`
              : `Middleware respondeu com HTTP ${response.status}`,
          ),
        )
      }

      if (response.ok) {
        await pullLiveData(silent)
      }
    } catch (error) {
      setHealth({
        status: 'offline',
        detail: error.name === 'AbortError' ? 'timeout apos 15s' : 'falha de conexao',
        lastCheckedAt: new Date().toISOString(),
        latencyMs: null,
      })
      setConnectionState('idle')

      if (!silent) {
        pushLog(
          createLog(
            'error',
            'Falha ao conectar backend',
            error.name === 'AbortError'
              ? 'Timeout no endpoint /health'
              : 'Nao foi possivel falar com o middleware',
          ),
        )
      }
    } finally {
      window.clearTimeout(timeoutId)
    }
  }

  useEffect(() => {
    if (config.mode !== 'live' || !config.autoPoll) return undefined

    const intervalId = window.setInterval(() => {
      void pingBackend(true)
    }, 10000)

    return () => window.clearInterval(intervalId)
  }, [config.adminApiToken, config.autoPoll, config.backendUrl, config.mode])

  useEffect(() => {
    if (config.mode !== 'live') return
    void pingBackend(true)
  }, [config.adminApiToken, config.mode, config.backendUrl])

  const handleResolveException = (exceptionId) => {
    const target = exceptions.find((item) => item.id === exceptionId)
    if (!target) return

    setExceptions((current) =>
      current.map((item) =>
        item.id === exceptionId
          ? { ...item, status: 'resolved', resolvedAt: new Date().toISOString() }
          : item,
      ),
    )

    if (target.leadId) {
      setLeads((current) =>
        current.map((lead) =>
          lead.id === target.leadId
            ? {
                ...lead,
                health: 'healthy',
                lastAction: 'resolvido manualmente',
                lastSyncAt: new Date().toISOString(),
              }
            : lead,
        ),
      )
    }

    pushLog(
      createLog(
        'success',
        'Excecao resolvida',
        `${target.kind} resolvido manualmente`,
        target.leadId,
      ),
    )
  }

  const handleToggleSync = (leadId) => {
    const target = leads.find((lead) => lead.id === leadId)
    if (!target) return

    setLeads((current) =>
      current.map((lead) =>
        lead.id === leadId
          ? {
              ...lead,
              syncEnabled: !lead.syncEnabled,
              lastAction: lead.syncEnabled ? 'sync pausado' : 'sync retomado',
            }
          : lead,
      ),
    )

    pushLog(
      createLog(
        'info',
        target.syncEnabled ? 'Sync pausado' : 'Sync retomado',
        `Lead ${target.name} agora esta ${target.syncEnabled ? 'fora' : 'dentro'} da sincronizacao automatica`,
        leadId,
      ),
    )
  }

  const handleReprocessLead = (leadId) => {
    const target = leads.find((lead) => lead.id === leadId)
    if (!target) return

    setLeads((current) =>
      current.map((lead) =>
        lead.id === leadId
          ? {
              ...lead,
              lastSyncAt: new Date().toISOString(),
              health: 'healthy',
              lastAction: 'reprocessado manualmente',
            }
          : lead,
      ),
    )

    pushLog(
      createLog(
        'success',
        'Reprocessamento executado',
        `Fila de sync reiniciada para ${target.name}`,
        leadId,
      ),
    )
  }

  const handleCreateTestException = () => {
    const targetLead = leads.find((lead) => lead.syncEnabled) ?? leads[0]
    if (!targetLead) return

    const nextException = {
      id: `exc-${Date.now()}`,
      status: 'open',
      leadId: targetLead.id,
      kind: 'manual_review',
      source: 'frontend-test',
      detectedAt: new Date().toISOString(),
      phone: targetLead.phone,
      summary: 'Evento de teste criado pelo painel para validar resolucao manual.',
    }

    setExceptions((current) => [nextException, ...current].slice(0, 30))
    setLeads((current) =>
      current.map((lead) =>
        lead.id === targetLead.id
          ? {
              ...lead,
              health: 'risk',
              lastAction: 'evento de teste criado',
            }
          : lead,
      ),
    )
    pushLog(
      createLog(
        'warning',
        'Excecao de teste criada',
        `Lead ${targetLead.name} entrou em revisao manual`,
        targetLead.id,
      ),
    )
  }

  const handleSelectLead = (leadId) => {
    startTransition(() => {
      setSelectedLeadId(leadId)
    })
  }

  const handleSyncPendingTask = async (pendingTask) => {
    if (config.mode !== 'live' || !config.backendUrl.trim()) {
      pushLog(
        createLog(
          'warning',
          'Sync indisponivel',
          'Use o modo live com backend configurado para criar contatos na Bradial.',
        ),
      )
      return
    }

    setSyncingTaskId(pendingTask.taskId)

    try {
      const response = await fetchBackend(config, `/clickup/tasks/${pendingTask.taskId}/sync-to-bradial`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ dryRun: false }),
      })
      const payload = await response.json().catch(() => ({}))

      if (!response.ok) {
        throw new Error(payload.error || `HTTP ${response.status}`)
      }

      await pingBackend(true)
      pushLog(
        createLog(
          'success',
          payload.operation === 'create' ? 'Contato criado no Bradial' : 'Contato atualizado no Bradial',
          `${pendingTask.taskName} recebeu a tag ${payload.stageLabel || pendingTask.targetStageLabel || 'de etapa'} sem envio de mensagem.`,
          pendingTask.bradialLeadId,
        ),
      )
    } catch (error) {
      pushLog(
        createLog(
          'error',
          'Falha ao sincronizar contato',
          error.message || 'Nao foi possivel criar/atualizar o contato na Bradial.',
          pendingTask.bradialLeadId,
        ),
      )
    } finally {
      setSyncingTaskId(null)
    }
  }

  const handleGenerateWebhookUrl = async () => {
    if (config.mode !== 'live' || !config.backendUrl.trim()) {
      pushLog(
        createLog(
          'warning',
          'Geracao indisponivel',
          'Use o modo live com backend configurado para gerar a URL do webhook.',
        ),
      )
      return
    }

    setCreatingWebhookUrl(true)

    try {
      const response = await fetchBackend(config, '/clickup/webhook-integrations', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({}),
      })
      const payload = await response.json().catch(() => ({}))

      if (!response.ok) {
        throw new Error(payload.error || `HTTP ${response.status}`)
      }

      await pullLiveData(true)
      pushLog(
        createLog(
          payload.warning ? 'warning' : 'success',
          'URL de webhook gerada',
          payload.warning ||
            `Nova URL pronta para cadastro no ClickUp: ${payload.integration?.webhookUrl || 'sem url'}`,
        ),
      )
    } catch (error) {
      pushLog(
        createLog(
          'error',
          'Falha ao gerar URL do webhook',
          error.message || 'Nao foi possivel criar a integracao do ClickUp.',
        ),
      )
    } finally {
      setCreatingWebhookUrl(false)
    }
  }

  const clearLogs = () => {
    setLogs([])
    setSyncAudit([])
  }

  const selectedLeadLogs = logs.filter((entry) => entry.leadId === selectedLead?.id).slice(0, 6)

  return (
    <div className="app-shell">
      <div className="background-orbit background-orbit-left" />
      <div className="background-orbit background-orbit-right" />

      <header className="hero-bar panel">
        <div className="hero-brand">
          <img src="/logo.png" alt="OneClick Logo" className="brand-logo" />
          <div className="brand-text">
            <p className="eyebrow">Painel operacional</p>
            <h1>OneClick Sync Console</h1>
            <p className="hero-copy">
              Central de inteligência para sincronização inteligente entre Bradial e ClickUp.
              Gerencie leads, resolva exceções e economize cliques com automação de ponta.
            </p>
          </div>
        </div>

        <div className="hero-actions">
          <span className={`pill pill-${config.mode}`}>{config.mode === 'mock' ? 'mock mode' : 'live mode'}</span>
          <span className={`pill pill-${health.status}`}>{health.status}</span>
          <button className="ghost-button" type="button" onClick={() => void pingBackend()}>
            Testar backend
          </button>
          {config.mode === 'live' ? (
            <button type="button" onClick={() => void refreshBackendSnapshot()}>
              Atualizar dados
            </button>
          ) : null}
        </div>
      </header>

      <section className="panel section-card">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Movimentacoes</p>
            <h2>Tela em tempo real</h2>
          </div>
          <div className="detail-actions">
            <button
              className={activityFilter === 'all' ? '' : 'ghost-button'}
              type="button"
              onClick={() => setActivityFilter('all')}
            >
              Tudo
            </button>
            <button
              className={activityFilter === 'sync' ? '' : 'ghost-button'}
              type="button"
              onClick={() => setActivityFilter('sync')}
            >
              Sync
            </button>
            <button
              className={activityFilter === 'alerts' ? '' : 'ghost-button'}
              type="button"
              onClick={() => setActivityFilter('alerts')}
            >
              Alertas
            </button>
            {config.mode === 'mock' ? (
              <button type="button" onClick={runMockTick}>
                Gerar evento
              </button>
            ) : (
              <button type="button" onClick={() => void refreshBackendSnapshot()}>
                Atualizar snapshot
              </button>
            )}
            <button className="ghost-button" type="button" onClick={clearLogs}>
              Limpar logs
            </button>
          </div>
        </div>

        <div className="activity-summary-row">
          <span className="pill pill-info">{activityItems.length} eventos</span>
          <span className="pill pill-success">{activityStats.succeeded} sucessos</span>
          <span className="pill pill-warning">{activityStats.duplicateSkips} duplicatas suprimidas</span>
          <span className={`pill ${activityStats.failed || activityStats.retrying ? 'pill-risk' : 'pill-info'}`}>
            {activityStats.failed + activityStats.retrying} falhas
          </span>
          {activityStats.successRate !== null ? (
            <span className="pill pill-info">taxa util {activityStats.successRate}%</span>
          ) : null}
          <span className="activity-summary-copy">
            stream unificado de auditoria de sync e logs operacionais, com duplicatas separadas da taxa util
          </span>
        </div>

        <div className="activity-panel">
          {filteredActivityItems.length === 0 ? (
            <div className="empty-state">
              <strong>Nenhuma movimentacao visivel</strong>
              <p>Gere um evento mock ou conecte um backend real para popular a timeline.</p>
            </div>
          ) : (
            filteredActivityItems.map((entry) => (
              <article key={entry.id} className="activity-row">
                <div className="activity-rail">
                  <span className={`log-dot tone-${entry.level}`} />
                </div>
                <div className="activity-content">
                  <div className="activity-header">
                    <strong>{entry.title}</strong>
                    <div className="activity-badges">
                      <span className={`pill pill-${entry.level}`}>{entry.level}</span>
                      <span className="pill pill-info">{entry.sourceLabel}</span>
                    </div>
                  </div>
                  <p>{entry.message}</p>
                  <div className="activity-meta">
                    <span>{entry.contactName || 'contato nao identificado'}</span>
                    <span>{entry.reference || 'global'}</span>
                    <small>{formatDate(entry.createdAt)}</small>
                  </div>
                </div>
              </article>
            ))
          )}
        </div>
      </section>

      <section className="summary-grid">
        <article className="stat-card panel">
          <span className="stat-label">Leads acompanhados</span>
          <strong>{leads.length}</strong>
          <small>{syncedToday} com sync ativo</small>
        </article>
        <article className="stat-card panel">
          <span className="stat-label">Operacao estavel</span>
          <strong>{healthyLeads}</strong>
          <small>{warningLeads} com alerta</small>
        </article>
        <article className="stat-card panel">
          <span className="stat-label">Excecoes abertas</span>
          <strong>{openExceptions.length}</strong>
          <small>{exceptions.length} registradas</small>
        </article>
        <article className="stat-card panel">
          <span className="stat-label">Pendentes ClickUp</span>
          <strong>{pendingContacts.length}</strong>
          <small>{actionablePendingContacts} podem subir para o Bradial</small>
        </article>
        <article className="stat-card panel">
          <span className="stat-label">Logs em memoria</span>
          <strong>{logs.length}</strong>
          <small>{connectionState === 'loading' ? 'checando backend...' : 'stream local ativa'}</small>
        </article>
      </section>

      <section className="workspace-grid">
        <aside className="column-stack">
          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Conexao</p>
                <h2>Configuracao local</h2>
              </div>
              <button className="ghost-button" type="button" onClick={saveConfig}>
                Salvar
              </button>
            </div>

            <div className="form-grid">
              <label>
                <span>Modo</span>
                <select
                  value={config.mode}
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, mode: event.target.value }))
                  }
                >
                  <option value="mock">Mock</option>
                  <option value="live">Live</option>
                </select>
              </label>

              <label>
                <span>URL do middleware</span>
                <input
                  value={config.backendUrl}
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, backendUrl: event.target.value }))
                  }
                  placeholder="http://localhost:3015"
                />
              </label>

              <label>
                <span>Token admin</span>
                <input
                  type="password"
                  value={config.adminApiToken || ''}
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, adminApiToken: event.target.value }))
                  }
                  placeholder="Bearer token do backend"
                  autoComplete="new-password"
                />
              </label>

              <label>
                <span>Bradial base URL</span>
                <input
                  value={config.bradialBaseUrl}
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, bradialBaseUrl: event.target.value }))
                  }
                  placeholder="https://chat.bradial.com.br"
                />
              </label>

              <label>
                <span>Account ID</span>
                <input
                  value={config.bradialAccountId}
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, bradialAccountId: event.target.value }))
                  }
                  placeholder="6008"
                />
              </label>

              <label className="switch-row">
                <span>Auto-check backend</span>
                <input
                  checked={config.autoPoll}
                  type="checkbox"
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, autoPoll: event.target.checked }))
                  }
                />
              </label>
            </div>
          </section>

          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Webhook</p>
                <h2>URL por integracao</h2>
              </div>
              <button
                className="ghost-button"
                type="button"
                disabled={config.mode !== 'live' || creatingWebhookUrl}
                onClick={() => void handleGenerateWebhookUrl()}
              >
                {creatingWebhookUrl ? 'Gerando...' : 'Gerar URL'}
              </button>
            </div>

            <div className="webhook-box">
              <p>Base publica detectada</p>
              <code>{webhookRegistry.publicBaseUrl || 'nao resolvida'}</code>
              <small>
                {webhookRegistry.source
                  ? `origem: ${webhookRegistry.source}${webhookRegistry.isPublic ? '' : ' (ainda nao publica)'}`
                  : 'gere uma integracao para resolver a URL'}
              </small>
            </div>

            <div className="webhook-list">
              {Array.isArray(webhookRegistry.items) && webhookRegistry.items.length > 0 ? (
                webhookRegistry.items.map((item) => (
                  <article key={item.integrationId} className="webhook-card">
                    <div className="exception-meta">
                      <span className={`pill ${item.status === 'active' ? 'pill-success' : 'pill-risk'}`}>
                        {item.status}
                      </span>
                      <span>{item.authMode}</span>
                    </div>
                    <strong>{item.name}</strong>
                    <code>{item.webhookUrl}</code>
                    <small>{item.workspaceName || 'workspace nao identificado'}</small>
                  </article>
                ))
              ) : (
                <div className="empty-state">
                  <strong>Nenhuma URL gerada</strong>
                  <p>Crie uma integracao para copiar a URL e cadastrar no ClickUp.</p>
                </div>
              )}
            </div>
          </section>

          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Saude</p>
                <h2>Health check</h2>
              </div>
              <button
                className="ghost-button"
                type="button"
                onClick={() => void pingBackend()}
              >
                Executar teste
              </button>
            </div>

            <ul className="status-list">
              <li>
                <span>Status</span>
                <strong className={`tone-${health.status}`}>{health.status}</strong>
              </li>
              <li>
                <span>Detalhe</span>
                <strong>{health.detail}</strong>
              </li>
              <li>
                <span>Latencia</span>
                <strong>{health.latencyMs ? `${health.latencyMs} ms` : 'n/a'}</strong>
              </li>
              <li>
                <span>Ultima checagem</span>
                <strong>{formatDate(health.lastCheckedAt)}</strong>
              </li>
            </ul>

            <div className="endpoint-box">
              <p>Endpoints esperados do backend</p>
              <code>GET /health</code>
              <code>GET /leads</code>
              <code>GET /exceptions</code>
              <code>GET /logs</code>
              <code>GET /clickup/health</code>
              <code>GET /clickup/tasks</code>
              <code>GET /clickup/pending-contacts</code>
              <code>GET /sync/audit</code>
              <code>POST /clickup/tasks/:taskId/sync-to-bradial</code>
              <code>POST /webhooks/bradial/chatwoot</code>
              <code>POST /refresh</code>
            </div>
          </section>

          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Excecoes</p>
                <h2>Fila operacional</h2>
              </div>
              <button
                className="ghost-button"
                type="button"
                disabled={config.mode === 'live'}
                onClick={handleCreateTestException}
              >
                Criar teste
              </button>
            </div>

            <div className="exception-list">
              {openExceptions.length === 0 ? (
                <div className="empty-state">
                  <strong>Nenhuma excecao aberta</strong>
                  <p>O painel esta limpo. Use o modo mock para gerar eventos.</p>
                </div>
              ) : (
                openExceptions.map((item) => (
                  <article key={item.id} className="exception-card">
                    <div className="exception-meta">
                      <span className="pill pill-warning">{item.kind}</span>
                      <span>{formatDate(item.detectedAt)}</span>
                    </div>
                    <strong>{item.phone}</strong>
                    <p>{item.summary}</p>
                    <div className="exception-actions">
                      <span>origem: {item.source}</span>
                      <button
                        type="button"
                        disabled={config.mode === 'live'}
                        onClick={() => handleResolveException(item.id)}
                      >
                        {config.mode === 'live' ? 'Somente leitura' : 'Resolver'}
                      </button>
                    </div>
                  </article>
                ))
              )}
            </div>
          </section>
        </aside>

        <main className="column-stack main-column">
          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">ClickUp</p>
                <h2>Oportunidades para Bradial</h2>
              </div>
              <span className={`pill ${pendingContacts.length ? 'pill-warning' : 'pill-success'}`}>
                {pendingContacts.length} pendentes
              </span>
            </div>

            <div className="opportunity-list">
              {config.mode !== 'live' ? (
                <div className="empty-state">
                  <strong>Disponivel apenas no modo live</strong>
                  <p>Essa fila mostra tasks reais do ClickUp que ainda precisam virar contato no Bradial.</p>
                </div>
              ) : pendingContacts.length === 0 ? (
                <div className="empty-state">
                  <strong>Nenhuma oportunidade pendente</strong>
                  <p>Todas as tasks elegiveis ja possuem contato Bradial com a tag de etapa alinhada ao status atual do ClickUp.</p>
                </div>
              ) : (
                pendingContacts.map((item) => (
                  <article key={item.id} className="opportunity-card">
                    <div className="exception-meta">
                      <span className={`pill ${item.syncAllowed ? 'pill-warning' : 'pill-risk'}`}>
                        {item.syncState}
                      </span>
                      <span>{formatDate(item.dateUpdated)}</span>
                    </div>
                    <strong>{item.taskName}</strong>
                    <p>{item.summary}</p>
                    <div className="opportunity-meta">
                      <span>{item.phone}</span>
                      <span>{item.status}</span>
                      <span>{item.owner || 'sem owner'}</span>
                      <span>{item.listName}</span>
                    </div>
                    <div className="opportunity-actions">
                      <span>
                        {item.bradialContactId
                          ? `Contato Bradial ${item.bradialContactId}`
                          : 'Contato ainda nao existe na Bradial'}
                      </span>
                      <div className="detail-actions">
                        {item.url ? (
                          <a className="inline-link" href={item.url} target="_blank" rel="noreferrer">
                            Abrir task
                          </a>
                        ) : null}
                        <button
                          type="button"
                          disabled={!item.syncAllowed || syncingTaskId === item.taskId}
                          onClick={() => void handleSyncPendingTask(item)}
                        >
                          {syncingTaskId === item.taskId
                            ? 'Processando...'
                            : item.syncState === 'missing_contact'
                              ? 'Criar contato'
                              : 'Atualizar contato'}
                        </button>
                      </div>
                    </div>
                  </article>
                ))
              )}
            </div>
          </section>

          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Leads</p>
                <h2>Explorador de vinculos</h2>
              </div>
              <input
                className="search-input"
                value={leadSearch}
                onChange={(event) => setLeadSearch(event.target.value)}
                placeholder="Buscar por nome, telefone, stage ou status"
              />
            </div>

            <div className="lead-grid">
              <div className="lead-list">
                {filteredLeads.map((lead) => (
                  <button
                    key={lead.id}
                    className={`lead-list-item ${selectedLead?.id === lead.id ? 'selected' : ''}`}
                    type="button"
                    onClick={() => handleSelectLead(lead.id)}
                  >
                    <div className="lead-line">
                      <strong>{lead.name}</strong>
                      <span className={`pill pill-${lead.health}`}>{lead.health}</span>
                    </div>
                    <div className="lead-line">
                      <span>{lead.phone}</span>
                      <span>{lead.clickupStage}</span>
                    </div>
                    <small>{lead.lastAction}</small>
                  </button>
                ))}
              </div>

              {selectedLead ? (
                <article className="lead-detail">
                  <div className="lead-detail-header">
                    <div>
                      <p className="eyebrow">Lead selecionado</p>
                      <h3>{selectedLead.name}</h3>
                    </div>
                    <div className="detail-actions">
                      <button
                        type="button"
                        disabled={config.mode === 'live'}
                        onClick={() => handleReprocessLead(selectedLead.id)}
                      >
                        Reprocessar
                      </button>
                      <button
                        className="ghost-button"
                        type="button"
                        disabled={config.mode === 'live'}
                        onClick={() => handleToggleSync(selectedLead.id)}
                      >
                        {selectedLead.syncEnabled ? 'Pausar sync' : 'Retomar sync'}
                      </button>
                    </div>
                  </div>

                  <div className="detail-grid">
                    <div>
                      <span className="detail-label">Telefone</span>
                      <strong>{selectedLead.phone}</strong>
                    </div>
                    <div>
                      <span className="detail-label">ClickUp</span>
                      <strong>{selectedLead.clickupTaskId ?? 'nao identificado'}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Bradial contact</span>
                      <strong>{selectedLead.chatContactId}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Conversa ativa</span>
                      <strong>{selectedLead.chatConversationId ?? selectedLead.conversationId ?? 'nao identificado'}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Etapa comercial</span>
                      <strong>{selectedLead.clickupStage ?? 'nao identificado'}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Status da conversa</span>
                      <strong>{selectedLead.chatStatus}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Owner</span>
                      <strong>{selectedLead.owner ?? 'nao atribuido'}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Ultimo sync</span>
                      <strong>{formatDate(selectedLead.lastSyncAt)}</strong>
                    </div>
                  </div>

                  <div className="tag-row">
                    {(selectedLead.tags ?? []).map((tag) => (
                      <span key={tag} className="tag-chip">
                        {tag}
                      </span>
                    ))}
                  </div>

                  <div className="lead-notes">
                    <p>{selectedLead.summary}</p>
                    <small>Ultima acao: {selectedLead.lastAction}</small>
                  </div>

                  <div className="mini-log-list">
                    <div className="section-heading compact">
                      <h4>Ultimos eventos deste lead</h4>
                    </div>
                    {selectedLeadLogs.length === 0 ? (
                      <p className="empty-inline">Nenhum evento local para este lead ainda.</p>
                    ) : (
                      selectedLeadLogs.map((entry) => (
                        <div key={entry.id} className="mini-log-item">
                          <span className={`log-dot tone-${entry.level}`} />
                          <div>
                            <strong>{entry.title}</strong>
                            <p>{entry.message}</p>
                          </div>
                          <small>{formatDate(entry.createdAt)}</small>
                        </div>
                      ))
                    )}
                  </div>
                </article>
              ) : null}
            </div>
          </section>

        </main>
      </section>
    </div>
  )
}

export default App
