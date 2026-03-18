import crypto from 'node:crypto'
import path from 'node:path'
import Fastify from 'fastify'
import cors from '@fastify/cors'
import dotenv from 'dotenv'
import { createBradialAdapter } from './adapters/bradial.js'
import { createClickupAdapter } from './adapters/clickup.js'
import { createClickupIntegrationStore } from './services/clickupIntegrations.js'
import { createSyncAuditStore } from './services/syncAuditStore.js'
import { createLeadLinkStore } from './services/leadLinkStore.js'
import { createSyncJobStore } from './services/syncJobStore.js'
import {
  buildStageLabelMap,
  listControlledStageLabels,
  normalizeLabelKey,
  pickControlledLabels,
  resolveBradialStageLabel,
  resolveBradialStageLabels,
  resolveClickupStatusFromLabel,
} from './services/clickupStageLabels.js'
import {
  resolveClickupPhoneConflict,
} from './services/clickupTaskResolution.js'
import { buildConsolidatedSnapshot } from './services/consolidation.js'
import {
  extractClickupWebhookContext,
  isClosedOpportunityTask,
  normalizeChatPriority,
} from './services/clickupLeadContext.js'
import { normalizePhone, phonesMatchLoose } from './utils/normalizers.js'

dotenv.config()

const PORT = Number(process.env.PORT || 3015)
const ADMIN_API_TOKEN = String(
  process.env.ADMIN_API_TOKEN || process.env.BACKEND_ADMIN_TOKEN || '',
).trim()

// CORS_ORIGINS aceita lista separada por vírgula. Use '*' para liberar tudo (dev only).
// Fallback: FRONTEND_ORIGIN (compatibilidade retroativa) ou localhost:4180
const CORS_ORIGINS = (() => {
  const raw = String(
    process.env.CORS_ORIGINS || process.env.FRONTEND_ORIGIN || 'http://localhost:4180',
  ).trim()
  if (raw === '*') return true // libera tudo
  return [
    ...new Set(
      raw.split(',').map((s) => s.trim()).filter(Boolean),
    ),
  ]
})()
const BRADIAL_REFRESH_MS = Math.max(5_000, Number(process.env.BRADIAL_REFRESH_MS || 60_000))
const BRADIAL_OPPORTUNITY_LABEL =
  String(process.env.BRADIAL_OPPORTUNITY_LABEL || 'OPORTUNIDADE').trim() || 'OPORTUNIDADE'
const BRADIAL_SYNC_CONVERSATION_LABELS = !['0', 'false', 'no', 'off'].includes(
  String(process.env.BRADIAL_SYNC_CONVERSATION_LABELS || 'true').trim().toLowerCase(),
)
const BRADIAL_SYNC_CONTACT_LABELS_RAW = !['0', 'false', 'no', 'off'].includes(
  String(process.env.BRADIAL_SYNC_CONTACT_LABELS || 'false').trim().toLowerCase(),
)
const BRADIAL_STAGE_LABEL_SCOPE =
  String(process.env.BRADIAL_STAGE_LABEL_SCOPE || 'conversation').trim().toLowerCase() === 'contact'
    ? 'contact'
    : 'conversation'
const BRADIAL_SYNC_CONTACT_LABELS =
  BRADIAL_STAGE_LABEL_SCOPE === 'contact' && BRADIAL_SYNC_CONTACT_LABELS_RAW
const BRADIAL_CHAT_AUTO_CREATE_CONVERSATIONS = !['0', 'false', 'no', 'off'].includes(
  String(process.env.BRADIAL_CHAT_AUTO_CREATE_CONVERSATIONS || 'false').trim().toLowerCase(),
)
const SYNC_MAX_ATTEMPTS = Math.max(1, Number(process.env.SYNC_MAX_ATTEMPTS || 4))
const SYNC_RETRY_BASE_MS = Math.max(500, Number(process.env.SYNC_RETRY_BASE_MS || 1_500))
const SYNC_CONCURRENCY = Math.max(1, Number(process.env.SYNC_CONCURRENCY || 6))
const SYNC_RECONCILE_MS = Math.max(10_000, Number(process.env.SYNC_RECONCILE_MS || 90_000))
const SYNC_RECONCILE_BATCH_SIZE = Math.max(1, Number(process.env.SYNC_RECONCILE_BATCH_SIZE || 5))
const SYNC_RECONCILE_LOOKBACK_MS = Math.max(
  60_000,
  Number(process.env.SYNC_RECONCILE_LOOKBACK_MS || 20 * 60 * 1000),
)
const DEFERRED_REFRESH_MS = Math.max(100, Number(process.env.DEFERRED_REFRESH_MS || 150))
const CLICKUP_STAGE_LABEL_MAP = buildStageLabelMap(process.env.CLICKUP_STAGE_LABEL_MAP)
const CLICKUP_WEBHOOK_SECRET = String(process.env.CLICKUP_WEBHOOK_SECRET || '').trim()
const CLICKUP_URGENCY_FIELD_NAMES = String(process.env.CLICKUP_URGENCY_FIELD_NAMES || '').trim()
const CLICKUP_CLOSED_STAGE_LABELS =
  String(process.env.CLICKUP_CLOSED_STAGE_LABELS || 'negocio-fechado').trim() || 'negocio-fechado'
const BRADIAL_CHAT_WEBHOOK_SECRET = String(process.env.BRADIAL_CHAT_WEBHOOK_SECRET || '').trim()
const BRADIAL_CHAT_WEBHOOK_MAX_AGE_SEC = Math.max(
  0,
  Number(process.env.BRADIAL_CHAT_WEBHOOK_MAX_AGE_SEC || 300),
)
const BRADIAL_STAGE_SUPPRESSION_WINDOW_MS = Math.max(
  30_000,
  Number(process.env.BRADIAL_STAGE_SUPPRESSION_WINDOW_MS || 120_000),
)
const CLICKUP_RECONCILE_SUPPRESSION_WINDOW_MS = Math.max(
  30_000,
  Number(process.env.CLICKUP_RECONCILE_SUPPRESSION_WINDOW_MS || 120_000),
)
const PUBLIC_BASE_URL = String(process.env.PUBLIC_BASE_URL || '').trim()
const BRADIAL_AGENT_ALIAS_MAP = String(process.env.BRADIAL_AGENT_ALIAS_MAP || '').trim()
const BRADIAL_SYNC_CONVERSATION_PRIORITY = !['0', 'false', 'no', 'off'].includes(
  String(process.env.BRADIAL_SYNC_CONVERSATION_PRIORITY || 'true').trim().toLowerCase(),
)
const BRADIAL_SYNC_CONVERSATION_ASSIGNMENT = !['0', 'false', 'no', 'off'].includes(
  String(process.env.BRADIAL_SYNC_CONVERSATION_ASSIGNMENT || 'true').trim().toLowerCase(),
)
const BRADIAL_SYNC_CLOSED_BY_ASSIGNMENT = !['0', 'false', 'no', 'off'].includes(
  String(process.env.BRADIAL_SYNC_CLOSED_BY_ASSIGNMENT || 'true').trim().toLowerCase(),
)
const BRADIAL_SYNC_CLOSED_BY_ATTRIBUTES = !['0', 'false', 'no', 'off'].includes(
  String(process.env.BRADIAL_SYNC_CLOSED_BY_ATTRIBUTES || 'true').trim().toLowerCase(),
)
const CLICKUP_INTEGRATIONS_PATH =
  String(process.env.CLICKUP_INTEGRATIONS_PATH || '').trim() ||
  path.resolve(process.cwd(), 'runtime-data', 'clickup-integrations.json')
const SYNC_AUDIT_PATH =
  String(process.env.SYNC_AUDIT_PATH || '').trim() ||
  path.resolve(process.cwd(), 'runtime-data', 'sync-audit-log.json')
const LEAD_LINKS_PATH =
  String(process.env.LEAD_LINKS_PATH || '').trim() ||
  path.resolve(process.cwd(), 'runtime-data', 'lead-links.json')
const SYNC_JOBS_PATH =
  String(process.env.SYNC_JOBS_PATH || '').trim() ||
  path.resolve(process.cwd(), 'runtime-data', 'sync-jobs.json')

const app = Fastify({ logger: false })

app.addContentTypeParser('application/json', { parseAs: 'string' }, (request, body, done) => {
  request.rawBody = body

  if (!body) {
    done(null, {})
    return
  }

  try {
    done(null, JSON.parse(body))
  } catch (error) {
    error.statusCode = 400
    done(error)
  }
})

await app.register(cors, {
  origin: CORS_ORIGINS,
  allowedHeaders: [
    'Authorization',
    'Content-Type',
    'X-Admin-Token',
    'X-Signature',
    'X-Chatwoot-Signature',
    'X-Chatwoot-Timestamp',
    'X-Hub-Signature-256',
    'X-Request-Timestamp',
  ],
})

const runtime = {
  lastRefreshAt: null,
  errors: {
    bradial: null,
    clickup: null,
  },
  cache: {
    overview: null,
    leads: [],
    exceptions: [],
    agents: [],
    chatAgents: [],
    inboxes: [],
    clickup: {
      health: null,
      workspaces: [],
      navigation: null,
      tasks: [],
      pendingContacts: [],
    },
  },
  logs: [],
  processedWebhookKeys: [],
  clickupTaskStateIndex: new Map(),
  queuedStageReconcileTaskIds: new Set(),
  queuedStageReconcileContextByTaskId: new Map(),
  sync: {
    queue: [],
    queuedByTaskId: new Map(),
    activeByTaskId: new Map(),
    deferredByTaskId: new Map(),
    retryTimersByTaskId: new Map(),
    retryJobMetaByTaskId: new Map(),
    audit: [],
    workerCount: 0,
    metrics: {
      processed: 0,
      succeeded: 0,
      failed: 0,
      retried: 0,
      enqueued: 0,
      reconciled: 0,
    },
  },
}

function extractBearerToken(headerValue) {
  const match = String(headerValue || '').trim().match(/^Bearer\s+(.+)$/i)
  return match?.[1]?.trim() || null
}

function extractAdminToken(request) {
  const headerToken =
    extractBearerToken(request.headers.authorization) ||
    String(request.headers['x-admin-token'] || '').trim() ||
    null

  if (headerToken) return headerToken

  const rawUrl = String(request.raw.url || request.url || '')
  const queryString = rawUrl.includes('?') ? rawUrl.slice(rawUrl.indexOf('?') + 1) : ''
  const queryToken = new URLSearchParams(queryString).get('adminToken')
  return String(queryToken || '').trim() || null
}

function isValidAdminToken(candidate) {
  if (!ADMIN_API_TOKEN) return true
  if (!candidate || candidate.length !== ADMIN_API_TOKEN.length) return false

  return crypto.timingSafeEqual(Buffer.from(candidate), Buffer.from(ADMIN_API_TOKEN))
}

function isPublicRequestPath(requestPath) {
  return requestPath === '/healthz' || requestPath.startsWith('/webhooks/')
}

function resolveSseAllowedOrigin(request) {
  const requestOrigin = String(request.headers.origin || '').trim()

  if (CORS_ORIGINS === true) {
    return requestOrigin || '*'
  }

  if (requestOrigin && CORS_ORIGINS.includes(requestOrigin)) {
    return requestOrigin
  }

  return CORS_ORIGINS[0] || 'http://localhost:4180'
}

app.addHook('onRequest', async (request, reply) => {
  if (!ADMIN_API_TOKEN || request.method === 'OPTIONS') return

  const requestPath = String(request.raw.url || request.url || '').split('?')[0]
  if (isPublicRequestPath(requestPath)) return

  if (isValidAdminToken(extractAdminToken(request))) return

  reply.code(401)
  return reply.send({
    error: 'Acesso administrativo nao autorizado.',
  })
})
let inflightRefresh = null
let inflightStageReconcile = null
let deferredRefreshTimer = null
let syncStatePersistTimer = null
const sseClients = new Set()
const recentStageSyncSuppressions = new Map()
const recentClickupReconcileSuppressions = new Map()

function broadcastSse(event, payload = {}) {
  const message = `event: ${event}\ndata: ${JSON.stringify({
    sentAt: new Date().toISOString(),
    ...payload,
  })}\n\n`

  for (const client of [...sseClients]) {
    try {
      client.write(message)
    } catch {
      sseClients.delete(client)
    }
  }
}

function pushLog(level, title, message, meta = {}) {
  runtime.logs.unshift({
    id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
    level,
    title,
    message,
    meta,
    createdAt: new Date().toISOString(),
  })
  runtime.logs = runtime.logs.slice(0, 250)
}

const syncAuditStore = createSyncAuditStore(
  {
    storePath: SYNC_AUDIT_PATH,
    maxItems: 1500,
  },
  pushLog,
)

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

function pushSyncAudit(entry = {}) {
  const auditEntry = {
    id: entry.id || `${Date.now()}-${Math.random().toString(16).slice(2, 10)}`,
    createdAt: entry.createdAt || new Date().toISOString(),
    ...entry,
  }

  runtime.sync.audit.unshift(auditEntry)
  runtime.sync.audit = runtime.sync.audit.slice(0, 300)
  syncAuditStore.record(auditEntry).catch((error) => {
    pushLog('warning', 'Falha ao persistir auditoria de sync', error.message, {
      type: auditEntry.type || 'sync_event',
      taskId: auditEntry.taskId || null,
    })
  })
  broadcastSse('sync_audit', auditEntry)

  return auditEntry
}

const bradial = createBradialAdapter(
  {
    baseUrl: process.env.BRADIAL_BASE_URL,
    apiToken: process.env.BRADIAL_API_TOKEN,
    accountId: process.env.BRADIAL_ACCOUNT_ID,
    chatBaseUrl: process.env.BRADIAL_CHAT_BASE_URL,
    chatApiToken: process.env.BRADIAL_CHAT_API_TOKEN,
    chatAccountId: process.env.BRADIAL_CHAT_ACCOUNT_ID || process.env.BRADIAL_ACCOUNT_ID,
    chatInboxId: process.env.BRADIAL_CHAT_INBOX_ID,
    maxPages: process.env.BRADIAL_MAX_PAGES,
    opportunityLabel: BRADIAL_OPPORTUNITY_LABEL,
    stageLabelMap: CLICKUP_STAGE_LABEL_MAP,
    syncConversationLabels: BRADIAL_SYNC_CONVERSATION_LABELS,
    syncContactLabels: BRADIAL_SYNC_CONTACT_LABELS,
    syncConversationPriority: BRADIAL_SYNC_CONVERSATION_PRIORITY,
    syncConversationAssignment: BRADIAL_SYNC_CONVERSATION_ASSIGNMENT,
    syncClosedByAssignment: BRADIAL_SYNC_CLOSED_BY_ASSIGNMENT,
    syncClosedByAttributes: BRADIAL_SYNC_CLOSED_BY_ATTRIBUTES,
    autoCreateConversations: BRADIAL_CHAT_AUTO_CREATE_CONVERSATIONS,
    agentAliasMap: BRADIAL_AGENT_ALIAS_MAP,
    urgencyFieldNames: CLICKUP_URGENCY_FIELD_NAMES,
    closedStageLabels: CLICKUP_CLOSED_STAGE_LABELS,
    requestMaxAttempts: process.env.BRADIAL_REQUEST_MAX_ATTEMPTS,
    requestRetryBaseMs: process.env.BRADIAL_REQUEST_RETRY_BASE_MS,
    conversationSearchPages: process.env.BRADIAL_CONVERSATION_SEARCH_PAGES,
    labelVerifyAttempts: process.env.BRADIAL_LABEL_VERIFY_ATTEMPTS,
    labelVerifyDelayMs: process.env.BRADIAL_LABEL_VERIFY_DELAY_MS,
  },
  pushLog,
)

const clickup = createClickupAdapter(
  {
    apiKey: process.env.CLICKUP_API_KEY,
    workspaceId: process.env.CLICKUP_WORKSPACE_ID,
    workspaceName: process.env.CLICKUP_WORKSPACE_NAME,
    commercialSpaceName: process.env.CLICKUP_COMMERCIAL_SPACE_NAME,
    commercialFolderName: process.env.CLICKUP_COMMERCIAL_FOLDER_NAME,
    commercialListName: process.env.CLICKUP_COMMERCIAL_LIST_NAME,
    urgencyFieldNames: CLICKUP_URGENCY_FIELD_NAMES,
    maxListPages: process.env.CLICKUP_MAX_LIST_PAGES,
    backupClientName: process.env.CLICKUP_BACKUP_CLIENT_NAME,
    clientsBackupPath: process.env.CLICKUP_CLIENTS_BACKUP_PATH,
  },
  pushLog,
)

const clickupIntegrations = createClickupIntegrationStore(
  {
    storePath: CLICKUP_INTEGRATIONS_PATH,
  },
  pushLog,
)

const leadLinkStore = createLeadLinkStore(
  {
    storePath: LEAD_LINKS_PATH,
  },
  pushLog,
)

const syncJobStore = createSyncJobStore(
  {
    storePath: SYNC_JOBS_PATH,
  },
  pushLog,
)

function getControlledStageLabels(tasks = runtime.cache.clickup.tasks) {
  return listControlledStageLabels(tasks, CLICKUP_STAGE_LABEL_MAP)
}

function buildTrackedClickupTaskState(task = {}) {
  const assigneesSignature = Array.isArray(task.assignees)
    ? [...task.assignees].map((item) => String(item || '').trim()).filter(Boolean).sort().join('|')
    : ''

  return {
    status: String(task.status || '').trim(),
    statusType: String(task.statusType || '').trim(),
    phone: normalizePhone(task.phone),
    priority: String(task.priority || '').trim(),
    urgency: String(task.urgency || '').trim(),
    eventInviteConfirmed: task.eventInviteConfirmed === true ? 'true' : 'false',
    owner: String(task.owner || '').trim(),
    assigneesSignature,
    dateClosed: String(task.dateClosed || '').trim(),
  }
}

function buildClickupTaskStateIndex(tasks = []) {
  const index = new Map()

  for (const task of tasks || []) {
    index.set(String(task.id), buildTrackedClickupTaskState(task))
  }

  return index
}

function buildTrackedClickupTaskStateSignature(task = {}) {
  const state = buildTrackedClickupTaskState(task)
  return [
    state.status,
    state.statusType,
    state.phone,
    state.priority,
    state.urgency,
    state.eventInviteConfirmed,
    state.owner,
    state.assigneesSignature,
    state.dateClosed,
  ].join('|')
}

function collectChangedClickupTaskIds(tasks = [], previousIndex = new Map()) {
  if (!previousIndex.size) return []

  const changedTasks = []

  for (const task of tasks || []) {
    const currentTaskId = String(task.id)
    const previous = previousIndex.get(currentTaskId)
    if (!previous) continue

    const current = buildTrackedClickupTaskState(task)
    const statusChanged = previous.status !== current.status || previous.statusType !== current.statusType
    const phoneChanged = previous.phone !== current.phone
    const priorityChanged = previous.priority !== current.priority || previous.urgency !== current.urgency
    const eventInviteConfirmedChanged = previous.eventInviteConfirmed !== current.eventInviteConfirmed
    const ownerChanged =
      previous.owner !== current.owner ||
      previous.assigneesSignature !== current.assigneesSignature ||
      previous.dateClosed !== current.dateClosed

    if (statusChanged || phoneChanged || priorityChanged || eventInviteConfirmedChanged || ownerChanged) {
      changedTasks.push({
        taskId: currentTaskId,
        clickupContext: {
          event: 'snapshot_reconcile',
          actor: null,
          field:
            priorityChanged && !statusChanged && !phoneChanged && !eventInviteConfirmedChanged && !ownerChanged
              ? 'priority'
              : eventInviteConfirmedChanged &&
                  !statusChanged &&
                  !phoneChanged &&
                  !priorityChanged &&
                  !ownerChanged
                ? 'confirmou invite para o evento'
                : null,
          customFieldName: null,
          isStatusEvent: statusChanged,
          isPriorityEvent: priorityChanged,
        },
      })
    }
  }

  return changedTasks
}

function scheduleStageReconcile(taskIds = [], trigger = 'refresh') {
  for (const item of taskIds) {
    const taskId = typeof item === 'string' ? item : item?.taskId
    if (!taskId) continue

    const normalizedTaskId = String(taskId)
    runtime.queuedStageReconcileTaskIds.add(normalizedTaskId)

    if (item?.clickupContext && typeof item.clickupContext === 'object') {
      runtime.queuedStageReconcileContextByTaskId.set(normalizedTaskId, item.clickupContext)
    }
  }

  if (!runtime.queuedStageReconcileTaskIds.size || inflightStageReconcile) return

  inflightStageReconcile = queueStageReconcile(trigger).finally(() => {
    inflightStageReconcile = null
    if (runtime.queuedStageReconcileTaskIds.size) {
      scheduleStageReconcile([], `${trigger}-followup`)
    }
  })
}

async function queueStageReconcile(trigger) {
  const taskIds = [...runtime.queuedStageReconcileTaskIds]
  runtime.queuedStageReconcileTaskIds.clear()

  if (!taskIds.length) return

  const controlledStageLabels = getControlledStageLabels()
  let enqueuedCount = 0

  for (const taskId of taskIds) {
    const clickupContext = runtime.queuedStageReconcileContextByTaskId.get(String(taskId)) || null
    runtime.queuedStageReconcileContextByTaskId.delete(String(taskId))
    const task = runtime.cache.clickup.tasks.find((item) => String(item.id) === String(taskId))
    if (!task) continue

    const normalizedPhone = normalizePhone(task.phone)
    if (normalizedPhone) {
      const clickupMatches = runtime.cache.clickup.tasks.filter((item) =>
        phonesMatchLoose(item.phone, normalizedPhone),
      )
      const clickupResolution = resolveClickupPhoneConflict(clickupMatches, normalizedPhone)

      if (
        clickupResolution.canonicalTask?.id &&
        String(clickupResolution.canonicalTask.id) !== String(taskId)
      ) {
        continue
      }
    }

    const pendingItem = runtime.cache.clickup.pendingContacts.find((item) => item.taskId === taskId)
    if (pendingItem && pendingItem.syncAllowed === false) continue
    const leadLink = findLeadLink(taskId, task.phone)
    const existingLead = findLeadMatchesByPhone(task.phone)[0] || null
    const hasSyncTarget = Boolean(
      pendingItem?.bradialContactId ||
        leadLink?.bradialContactId ||
        leadLink?.chatContactId ||
        leadLink?.conversationId ||
        existingLead,
    )
    if (!hasSyncTarget) continue

    const pendingSyncState = String(pendingItem?.syncState || '')
    const stagePending =
      ['missing_stage_label', 'stage_label_outdated'].includes(pendingSyncState) ||
      (BRADIAL_CHAT_AUTO_CREATE_CONVERSATIONS &&
        pendingSyncState === 'conversation_required_for_stage_label')
    const taskStateSignature = buildTrackedClickupTaskStateSignature(task)

    if (!stagePending && shouldSuppressClickupReconcile(taskId, taskStateSignature)) {
      continue
    }

    const queued = enqueueSyncJob({
      taskId,
      dryRun: false,
      trigger: `stage-reconcile-${trigger}`,
      source: 'stage-reconcile',
      refreshBefore: false,
      refreshAfter: false,
      directFetchTask: true,
      backgroundRefresh: !(clickupContext?.isPriorityEvent && !clickupContext?.isStatusEvent),
      controlledStageLabels,
      reason: stagePending ? pendingItem.syncState : 'task_state_changed',
      clickupContext,
      taskStateSignature,
    })

    if (queued.ok) {
      enqueuedCount += 1
    }
  }

  if (enqueuedCount > 0) {
    runtime.sync.metrics.reconciled += enqueuedCount
    pushLog(
      'info',
      'Reconcile ClickUp enfileirado',
      `${enqueuedCount} task(s) com mudancas persistidas foram enviadas para a fila de sync.`,
      { trigger },
    )
  }
}

function isActionableStagePendingItem(item) {
  if (!item || item.syncAllowed === false) return false

  return ['missing_stage_label', 'stage_label_outdated'].includes(String(item.syncState || '')) || (
    BRADIAL_CHAT_AUTO_CREATE_CONVERSATIONS &&
    String(item.syncState || '') === 'conversation_required_for_stage_label'
  )
}

function hasPendingStageTransition(current, previous = null) {
  if (!isActionableStagePendingItem(current)) return false
  if (!previous) return false

  return (
    String(previous.syncState || '') !== String(current.syncState || '') ||
    String(previous.bradialConversationId || '') !== String(current.bradialConversationId || '') ||
    String(previous.targetStageLabel || '') !== String(current.targetStageLabel || '') ||
    !sameNormalizedLabels(previous.currentControlledLabels || [], current.currentControlledLabels || [])
  )
}

function collectNewlyActionablePendingStageTasks(nextPendingContacts = [], previousPendingContacts = []) {
  const previousByTaskId = new Map(
    (previousPendingContacts || []).map((item) => [String(item?.taskId || ''), item]).filter(([taskId]) => taskId),
  )

  return (nextPendingContacts || [])
    .filter((item) => isActionableStagePendingItem(item))
    .filter((item) => hasPendingStageTransition(item, previousByTaskId.get(String(item.taskId))))
    .map((item) => ({
      taskId: item.taskId,
      clickupContext: {
        event: 'conversation_stage_backfill',
        actor: null,
        field: null,
        customFieldName: null,
        isStatusEvent: false,
        isPriorityEvent: false,
      },
    }))
}

async function overlayConversationStageLabels(bradialSnapshot, trigger = 'manual') {
  if (BRADIAL_SYNC_CONTACT_LABELS || !bradial.chatEnabled) {
    return bradialSnapshot
  }

  const links = leadLinkStore.listLinks()
  if (!Array.isArray(bradialSnapshot?.leads) || !bradialSnapshot.leads.length) {
    return bradialSnapshot
  }

  const linksByContactId = new Map()
  const linksByPhone = new Map()
  for (const link of links) {
    const contactId = String(link?.bradialContactId || '').trim()
    const phone = normalizePhone(link?.phone)
    if (contactId && !linksByContactId.has(contactId)) {
      linksByContactId.set(contactId, link)
    }
    if (phone && !linksByPhone.has(phone)) {
      linksByPhone.set(phone, link)
    }
  }

  const cachedLabelsByConversationId = new Map()
  const cachedLabelsByContactId = new Map()
  const cachedLabelsByPhone = new Map()

  for (const lead of runtime.cache.leads || []) {
    const labels = normalizeWebhookLabels(lead?.bradialLabels || lead?.raw?.bradialLabels || [])
    const conversationId = String(lead?.conversationId || lead?.chatConversationId || '').trim()
    const contactId = String(lead?.raw?.bradialContactId || lead?.chatContactId || '').trim()
    const phone = normalizePhone(lead?.phone)

    if (conversationId && labels.length && !cachedLabelsByConversationId.has(conversationId)) {
      cachedLabelsByConversationId.set(conversationId, labels)
    }
    if (contactId && labels.length && !cachedLabelsByContactId.has(contactId)) {
      cachedLabelsByContactId.set(contactId, labels)
    }
    if (phone && labels.length && !cachedLabelsByPhone.has(phone)) {
      cachedLabelsByPhone.set(phone, labels)
    }
  }

  const conversationIdsToFetch = [
    ...new Set(
      bradialSnapshot.leads
        .map((lead) => {
          const contactId = String(lead?.raw?.bradialContactId || lead?.chatContactId || '').trim()
          const phone = normalizePhone(lead?.phone)
          const link = linksByContactId.get(contactId) || linksByPhone.get(phone) || null
          return String(link?.conversationId || '').trim()
        })
        .filter((conversationId) => conversationId && !cachedLabelsByConversationId.has(conversationId)),
    ),
  ]

  const fetchedLabelsByConversationId = new Map()
  const chunkSize = 8
  for (let index = 0; index < conversationIdsToFetch.length; index += chunkSize) {
    const chunk = conversationIdsToFetch.slice(index, index + chunkSize)
    const rows = await Promise.all(
      chunk.map(async (conversationId) => {
        try {
          const labels = normalizeWebhookLabels(await bradial.fetchConversationLabels(conversationId))
          return [conversationId, labels]
        } catch (error) {
          pushLog(
            'warning',
            'Falha ao sobrepor labels de conversa no snapshot',
            error.message,
            { trigger, conversationId },
          )
          return [conversationId, []]
        }
      }),
    )

    for (const [conversationId, labels] of rows) {
      fetchedLabelsByConversationId.set(conversationId, labels)
    }
  }

  return {
    ...bradialSnapshot,
    leads: bradialSnapshot.leads.map((lead) => {
      const contactId = String(lead?.raw?.bradialContactId || lead?.chatContactId || '').trim()
      const phone = normalizePhone(lead?.phone)
      const link = linksByContactId.get(contactId) || linksByPhone.get(phone) || null
      const conversationId =
        String(link?.conversationId || lead?.chatConversationId || lead?.conversationId || '').trim() || null
      const conversationLabels =
        (conversationId && fetchedLabelsByConversationId.get(conversationId)) ||
        (conversationId && cachedLabelsByConversationId.get(conversationId)) ||
        cachedLabelsByContactId.get(contactId) ||
        cachedLabelsByPhone.get(phone) ||
        []
      const normalizedLabels = normalizeWebhookLabels(conversationLabels)

      return {
        ...lead,
        bradialLabels: normalizedLabels,
        tags: mergeLeadDisplayTags(lead.tags, normalizedLabels),
        conversationId: conversationId || lead.conversationId,
        chatConversationId: conversationId || lead.chatConversationId,
        raw: {
          ...(lead.raw || {}),
          partnerContactLabels: normalizeWebhookLabels(lead?.raw?.bradialLabels || lead?.bradialLabels || []),
          bradialLabels: normalizedLabels,
        },
      }
    }),
  }
}

async function refreshSnapshot(trigger = 'manual') {
  pushLog('info', 'Atualizando snapshot integrado', `Inicio do refresh por ${trigger}`)

  const [bradialResult, clickupResult] = await Promise.allSettled([
    bradial.fetchSnapshot(trigger),
    clickup.fetchSnapshot(trigger),
  ])

  if (bradialResult.status !== 'fulfilled') {
    runtime.errors.bradial = bradialResult.reason.message
    pushLog('error', 'Falha no refresh Bradial', bradialResult.reason.message)
    throw bradialResult.reason
  }

  runtime.errors.bradial = null

  const snapshotAt = new Date().toISOString()
  const clickupSnapshot =
    clickupResult.status === 'fulfilled'
      ? clickupResult.value
      : {
          enabled: false,
          snapshotAt,
          error: clickupResult.reason.message,
          tokenSource: null,
          workspace: null,
          workspaces: [],
          navigation: null,
          tasks: [],
        }

  runtime.errors.clickup = clickupResult.status === 'fulfilled' ? null : clickupResult.reason.message

  if (runtime.errors.clickup) {
    pushLog('warning', 'ClickUp degradado', runtime.errors.clickup)
  }

  let bradialSnapshot = bradialResult.value
  const linkedBradialContactIds = [
    ...new Set(
      leadLinkStore
        .listLinks()
        .map((item) => String(item?.bradialContactId || '').trim())
        .filter(Boolean),
    ),
  ]
  const knownBradialContactIds = new Set(
    (bradialSnapshot?.contacts || []).map((item) => String(item?.id || '').trim()).filter(Boolean),
  )
  const missingLinkedContactIds = linkedBradialContactIds.filter((contactId) => !knownBradialContactIds.has(contactId))

  if (missingLinkedContactIds.length) {
    try {
      const hydratedLinked = await bradial.hydrateContactsByIds(
        missingLinkedContactIds,
        bradialSnapshot?.snapshotAt || snapshotAt,
      )

      if (hydratedLinked.contacts.length || hydratedLinked.leads.length) {
        bradialSnapshot = {
          ...bradialSnapshot,
          contacts: [...(bradialSnapshot.contacts || []), ...hydratedLinked.contacts],
          leads: [...(bradialSnapshot.leads || []), ...hydratedLinked.leads],
        }
      }
    } catch (error) {
      pushLog(
        'warning',
        'Falha ao reidratar contatos vinculados fora do snapshot da Bradial',
        error.message,
        {
          trigger,
          missingLinkedContactIds,
        },
      )
    }
  }

  bradialSnapshot = await overlayConversationStageLabels(bradialSnapshot, trigger)

  const previousTaskStateIndex = runtime.clickupTaskStateIndex
  const previousPendingContacts = Array.isArray(runtime.cache?.clickup?.pendingContacts)
    ? runtime.cache.clickup.pendingContacts
    : []

  runtime.lastRefreshAt = snapshotAt
  runtime.cache = buildConsolidatedSnapshot({
    bradialSnapshot,
    clickupSnapshot,
    accountId: process.env.BRADIAL_ACCOUNT_ID || null,
    preferredInboxId: process.env.BRADIAL_INBOX_ID || null,
    lastRefreshAt: snapshotAt,
    stageLabelMap: CLICKUP_STAGE_LABEL_MAP,
    controlledStageLabels: getControlledStageLabels(clickupSnapshot.tasks || []),
    conversationLabelsOnly: !BRADIAL_SYNC_CONTACT_LABELS,
    autoCreateConversations: BRADIAL_CHAT_AUTO_CREATE_CONVERSATIONS,
  })
  runtime.clickupTaskStateIndex = buildClickupTaskStateIndex(runtime.cache.clickup.tasks)

  const changedTaskIds = collectChangedClickupTaskIds(
    runtime.cache.clickup.tasks,
    previousTaskStateIndex,
  )
  const newlyActionableStageTasks = collectNewlyActionablePendingStageTasks(
    runtime.cache.clickup.pendingContacts,
    previousPendingContacts,
  )

  if (changedTaskIds.length || newlyActionableStageTasks.length) {
    scheduleStageReconcile([...changedTaskIds, ...newlyActionableStageTasks], trigger)
  }

  pushLog(
    'success',
    'Snapshot consolidado',
    `${runtime.cache.leads.length} leads Bradial e ${runtime.cache.clickup.tasks.length} tasks ClickUp processados`,
    { trigger },
  )
  broadcastSse('snapshot_refreshed', {
    trigger,
    lastRefreshAt: snapshotAt,
    leadCount: runtime.cache.leads.length,
    taskCount: runtime.cache.clickup.tasks.length,
  })

  return runtime.cache
}

function ensureSnapshot(trigger = 'manual') {
  if (inflightRefresh) return inflightRefresh
  inflightRefresh = refreshSnapshot(trigger).finally(() => {
    inflightRefresh = null
  })
  return inflightRefresh
}

function buildPersistedSyncState() {
  return {
    queued: runtime.sync.queue,
    deferred: [...runtime.sync.deferredByTaskId.values()],
    active: [...runtime.sync.activeByTaskId.values()],
    retries: [...runtime.sync.retryJobMetaByTaskId.values()],
  }
}

function scheduleSyncStatePersist() {
  if (syncStatePersistTimer) return

  syncStatePersistTimer = setTimeout(() => {
    syncStatePersistTimer = null
    syncJobStore.replaceState(buildPersistedSyncState()).catch((error) => {
      pushLog('warning', 'Falha ao persistir fila de sync', error.message, {
        storePath: syncJobStore.storePath,
      })
    })
  }, 100)
  syncStatePersistTimer.unref?.()
}

function scheduleDeferredSnapshotRefresh(trigger = 'deferred-sync', delayMs = DEFERRED_REFRESH_MS) {
  if (deferredRefreshTimer) return

  deferredRefreshTimer = setTimeout(() => {
    deferredRefreshTimer = null
    ensureSnapshot(trigger).catch((error) => {
      pushLog('warning', 'Falha no refresh deferido', error.message, { trigger })
    })
  }, Math.max(250, delayMs))
  deferredRefreshTimer.unref?.()
}

function upsertRuntimeClickupTask(task) {
  if (!task?.id) return

  const tasks = Array.isArray(runtime.cache.clickup.tasks) ? [...runtime.cache.clickup.tasks] : []
  const currentIndex = tasks.findIndex((item) => String(item.id) === String(task.id))
  if (currentIndex >= 0) {
    tasks[currentIndex] = {
      ...tasks[currentIndex],
      ...task,
    }
  } else {
    tasks.push(task)
  }

  runtime.cache.clickup.tasks = tasks.sort((left, right) => Number(right.dateUpdated || 0) - Number(left.dateUpdated || 0))
}

async function resolveClickupTask(taskId, options = {}) {
  const preferFresh = options.preferFresh !== false
  const trigger = String(options.trigger || 'manual').trim() || 'manual'

  if (preferFresh) {
    const freshTask = await clickup.fetchTaskById(taskId, trigger)
    if (freshTask) {
      upsertRuntimeClickupTask(freshTask)
      return freshTask
    }
  }

  return runtime.cache.clickup.tasks.find((task) => String(task.id) === String(taskId)) || null
}

function findLeadMatchesByPhone(phone) {
  const normalizedPhone = normalizePhone(phone)
  if (!normalizedPhone) return []

  return runtime.cache.leads.filter((lead) => phonesMatchLoose(lead.phone, normalizedPhone))
}

function mergeLeadDisplayTags(existingTags = [], controlledLabels = []) {
  const controlledKeys = new Set(
    normalizeWebhookLabels(controlledLabels).map((label) => normalizeLabelKey(label)).filter(Boolean),
  )
  const preserved = Array.isArray(existingTags)
    ? existingTags.filter((tag) => !controlledKeys.has(normalizeLabelKey(tag)))
    : []

  return [...new Set([...preserved, ...normalizeWebhookLabels(controlledLabels)])]
}

function patchRuntimeClickupTask(updatedTask = {}) {
  const taskId = String(updatedTask?.id || '').trim()
  if (!taskId) return false

  const tasks = Array.isArray(runtime.cache.clickup.tasks) ? [...runtime.cache.clickup.tasks] : []
  const index = tasks.findIndex((task) => String(task.id) === taskId)
  if (index < 0) return false

  tasks[index] = {
    ...tasks[index],
    ...updatedTask,
  }

  runtime.cache.clickup.tasks = tasks.sort(
    (left, right) => Number(right.dateUpdated || 0) - Number(left.dateUpdated || 0),
  )

  if (Array.isArray(runtime.cache.clickup.pendingContacts)) {
    runtime.cache.clickup.pendingContacts = runtime.cache.clickup.pendingContacts.map((item) =>
      String(item.taskId) === taskId
        ? {
            ...item,
            status: updatedTask.status || item.status,
            targetStageLabels:
              resolveBradialStageLabels(updatedTask, CLICKUP_STAGE_LABEL_MAP) ||
              item.targetStageLabels ||
              [],
            stageLabel:
              resolveBradialStageLabel(updatedTask.status, CLICKUP_STAGE_LABEL_MAP) ||
              item.stageLabel ||
              null,
          }
        : item,
    )
  }

  return true
}

function patchRuntimeLeadState({
  taskId = null,
  phone = null,
  chatContactId = null,
  conversationId = null,
  clickupStage = null,
  clickupPriority = undefined,
  clickupUrgency = undefined,
  bradialLabels = null,
  lastSyncAt = null,
} = {}) {
  const normalizedTaskId = String(taskId || '').trim()
  const normalizedPhone = normalizePhone(phone)
  const normalizedChatContactId = String(chatContactId || '').trim()
  const normalizedConversationId = String(conversationId || '').trim()
  const normalizedLabels = Array.isArray(bradialLabels) ? normalizeWebhookLabels(bradialLabels) : null
  const syncAt = lastSyncAt || new Date().toISOString()
  let changed = false

  runtime.cache.leads = runtime.cache.leads.map((lead) => {
    const leadTaskId = String(lead.clickupTaskId || '').trim()
    const leadPhone = normalizePhone(lead.phone)
    const leadChatContactId = String(lead.chatContactId || '').trim()
    const leadConversationId = String(lead.conversationId || lead.chatConversationId || '').trim()

    const matches =
      (normalizedTaskId && leadTaskId === normalizedTaskId) ||
      (normalizedPhone && leadPhone && phonesMatchLoose(leadPhone, normalizedPhone)) ||
      (normalizedChatContactId && leadChatContactId === normalizedChatContactId) ||
      (normalizedConversationId && leadConversationId === normalizedConversationId)

    if (!matches) return lead

    changed = true
    const nextRaw = {
      ...(lead.raw || {}),
    }

    if (normalizedLabels) {
      nextRaw.bradialLabels = normalizedLabels
    }

    return {
      ...lead,
      clickupStage: clickupStage || lead.clickupStage,
      clickupPriority: clickupPriority === undefined ? lead.clickupPriority : clickupPriority,
      clickupUrgency: clickupUrgency === undefined ? lead.clickupUrgency : clickupUrgency,
      bradialLabels: normalizedLabels || lead.bradialLabels,
      tags: normalizedLabels ? mergeLeadDisplayTags(lead.tags, normalizedLabels) : lead.tags,
      chatContactId: normalizedChatContactId || lead.chatContactId,
      conversationId: normalizedConversationId || lead.conversationId,
      chatConversationId: normalizedConversationId || lead.chatConversationId,
      lastSyncAt: syncAt,
      raw: nextRaw,
    }
  })

  return changed
}

function parseBooleanFlag(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase())
}

function findLeadLink(taskId, phone = null) {
  return leadLinkStore.findLink({
    taskId: taskId ? String(taskId) : null,
    phone: phone ? String(phone) : null,
  })
}

function resolveBradialContactId(contact, fallbackLead = null) {
  return (
    contact?.raw?.bradialContactId ||
    contact?.id ||
    contact?.chatContactId ||
    fallbackLead?.raw?.bradialContactId ||
    fallbackLead?.id ||
    fallbackLead?.chatContactId ||
    null
  )
}

function buildClickupWebhookKey(payload) {
  const taskId = String(payload?.task_id || payload?.taskId || payload?.task?.id || 'no-task').trim()
  const event = String(payload?.event || 'unknown').trim()
  const historyItemId = Array.isArray(payload?.history_items)
    ? payload.history_items[0]?.id || payload.history_items[0]?.field || 'no-history'
    : 'no-history'

  return [payload?.webhook_id || 'clickup', event, taskId, historyItemId].join(':')
}

function extractClickupWebhookEnvelope(payload) {
  const automationTaskId = String(payload?.payload?.id || '').trim()
  if (automationTaskId) {
    return {
      mode: 'automation',
      event: 'automation_call_webhook',
      taskId: automationTaskId,
      webhookKey: [
        payload?.auto_id || 'automation',
        payload?.trigger_id || 'trigger',
        payload?.date || 'no-date',
        automationTaskId,
      ].join(':'),
    }
  }

  return {
    mode: 'api',
    event: String(payload?.event || '').trim(),
    taskId: String(payload?.task_id || payload?.taskId || payload?.task?.id || '').trim(),
    webhookKey: buildClickupWebhookKey(payload),
  }
}

function buildIntegrationWebhookKey(integrationId, payload) {
  return `${integrationId}:${buildClickupWebhookKey(payload)}`
}

function normalizeWebhookLabels(labels) {
  if (Array.isArray(labels)) {
    return [
      ...new Set(
        labels
          .map((item) => {
            if (typeof item === 'string') return item.trim()
            if (item && typeof item === 'object') {
              return String(item.title || item.name || item.label || '').trim()
            }
            return ''
          })
          .filter(Boolean),
      ),
    ]
  }

  if (typeof labels === 'string') {
    return labels
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
  }

  return []
}

function normalizedLabelSet(labels = []) {
  return [...new Set(normalizeWebhookLabels(labels).map((label) => normalizeLabelKey(label)).filter(Boolean))].sort()
}

function sameNormalizedLabels(left = [], right = []) {
  const leftSet = normalizedLabelSet(left)
  const rightSet = normalizedLabelSet(right)
  return leftSet.length === rightSet.length && leftSet.every((item, index) => item === rightSet[index])
}

function selectPrimaryControlledStageLabels(labels = [], controlledStageLabels = []) {
  const currentControlledLabels = pickControlledLabels(labels, controlledStageLabels)
  const supplementalKeys = new Set([normalizeLabelKey('confirmado')])
  const primaryControlledLabels = currentControlledLabels.filter(
    (label) => !supplementalKeys.has(normalizeLabelKey(label)),
  )

  return {
    currentControlledLabels,
    primaryControlledLabels: primaryControlledLabels.length ? primaryControlledLabels : currentControlledLabels,
  }
}

function extractBradialChangedLabels(payload = {}) {
  const changedAttributes = Array.isArray(payload.changed_attributes)
    ? payload.changed_attributes
    : Array.isArray(payload.changedAttributes)
      ? payload.changedAttributes
      : []

  for (const item of changedAttributes) {
    const key = normalizeLabelKey(
      item?.key || item?.attribute_key || item?.field || item?.name || '',
    )

    if (!['label', 'labels'].includes(key)) continue

    return normalizeWebhookLabels(
      item?.current_value ?? item?.currentValue ?? item?.value ?? item?.new_value ?? [],
    )
  }

  return []
}

function extractBradialChangedPriority(payload = {}) {
  const changedAttributes = Array.isArray(payload.changed_attributes)
    ? payload.changed_attributes
    : Array.isArray(payload.changedAttributes)
      ? payload.changedAttributes
      : []

  for (const item of changedAttributes) {
    const key = normalizeLabelKey(
      item?.key || item?.attribute_key || item?.field || item?.name || '',
    )

    if (!['priority', 'conversationpriority'].includes(key)) continue

    return {
      changed: true,
      priority: normalizeChatPriority(
        item?.current_value ?? item?.currentValue ?? item?.value ?? item?.new_value ?? item?.newValue ?? null,
      ),
    }
  }

  return {
    changed: false,
    priority: null,
  }
}

function extractBradialWebhookEnvelope(payload = {}) {
  const conversation =
    payload?.conversation && typeof payload.conversation === 'object' ? payload.conversation : payload
  const sender = conversation?.meta?.sender || payload?.contact || {}
  const changedPriorityState = extractBradialChangedPriority(payload)
  const changedLabels = extractBradialChangedLabels(payload)
  const labels = changedLabels.length
    ? changedLabels
    : normalizeWebhookLabels(
        conversation?.labels || payload?.labels || payload?.contact?.labels || sender?.labels || [],
      )

  return {
    event: String(payload?.event || '').trim(),
    conversationId: String(conversation?.id || payload?.conversation_id || '').trim() || null,
    chatContactId:
      String(
        payload?.contact?.id ||
          sender?.id ||
          conversation?.contact_id ||
          conversation?.meta?.sender?.id ||
          '',
      ).trim() || null,
    phone:
      normalizePhone(
        payload?.contact?.phone_number ||
          payload?.contact?.phoneNumber ||
          sender?.phone_number ||
          sender?.phoneNumber ||
          '',
      ) || null,
    labels,
    changedLabels,
    priority: normalizeChatPriority(conversation?.priority ?? payload?.priority ?? null),
    changedPriority: changedPriorityState.priority,
    priorityChanged: changedPriorityState.changed,
    changedAttributes: Array.isArray(payload?.changed_attributes) ? payload.changed_attributes : [],
  }
}

function buildBradialWebhookKey(request, envelope, payload = {}) {
  return [
    'bradial-chat',
    String(request.headers['x-chatwoot-delivery'] || request.headers['x-request-id'] || '').trim() ||
      String(payload?.id || envelope.conversationId || envelope.chatContactId || 'no-identity').trim(),
    envelope.event || 'unknown',
    envelope.conversationId || envelope.chatContactId || envelope.phone || 'no-target',
    normalizeWebhookLabels(envelope.labels).join('|') || 'no-labels',
    envelope.priorityChanged ? envelope.changedPriority ?? 'priority-none' : 'priority-unchanged',
  ].join(':')
}

function cleanupStageSyncSuppressions() {
  const now = Date.now()
  for (const [key, value] of recentStageSyncSuppressions.entries()) {
    if (!value || Number(value.expiresAt || 0) <= now) {
      recentStageSyncSuppressions.delete(key)
    }
  }
}

function cleanupClickupReconcileSuppressions() {
  const now = Date.now()
  for (const [key, value] of recentClickupReconcileSuppressions.entries()) {
    if (!value || Number(value.expiresAt || 0) <= now) {
      recentClickupReconcileSuppressions.delete(key)
    }
  }
}

function buildClickupReconcileSuppressionKey(taskId, taskStateSignature) {
  const normalizedTaskId = String(taskId || '').trim()
  const normalizedSignature = String(taskStateSignature || '').trim()
  if (!normalizedTaskId || !normalizedSignature) return null
  return `${normalizedTaskId}:${normalizedSignature}`
}

function rememberClickupReconcileSuppression(taskId, taskStateSignature) {
  const key = buildClickupReconcileSuppressionKey(taskId, taskStateSignature)
  if (!key) return

  cleanupClickupReconcileSuppressions()
  recentClickupReconcileSuppressions.set(key, {
    expiresAt: Date.now() + CLICKUP_RECONCILE_SUPPRESSION_WINDOW_MS,
  })
}

function shouldSuppressClickupReconcile(taskId, taskStateSignature) {
  const key = buildClickupReconcileSuppressionKey(taskId, taskStateSignature)
  if (!key) return false

  cleanupClickupReconcileSuppressions()
  const hit = recentClickupReconcileSuppressions.get(key)
  return Boolean(hit && Number(hit.expiresAt || 0) > Date.now())
}

function buildStageSyncSuppressionKeys({ taskId = null, conversationId = null, chatContactId = null, label = null } = {}) {
  const normalizedLabel = normalizeLabelKey(label)
  if (!normalizedLabel) return []

  const keys = []
  if (taskId) keys.push(`task:${String(taskId).trim()}:${normalizedLabel}`)
  if (conversationId) keys.push(`conversation:${String(conversationId).trim()}:${normalizedLabel}`)
  if (chatContactId) keys.push(`chat-contact:${String(chatContactId).trim()}:${normalizedLabel}`)
  return keys
}

function rememberOutboundBradialStageSync(identity = {}) {
  const keys = buildStageSyncSuppressionKeys(identity)
  if (!keys.length) return

  cleanupStageSyncSuppressions()
  const expiresAt = Date.now() + BRADIAL_STAGE_SUPPRESSION_WINDOW_MS

  for (const key of keys) {
    recentStageSyncSuppressions.set(key, {
      ...identity,
      expiresAt,
    })
  }
}

function shouldSuppressBradialStageSync(identity = {}) {
  const keys = buildStageSyncSuppressionKeys(identity)
  if (!keys.length) return false

  cleanupStageSyncSuppressions()
  const now = Date.now()
  return keys.some((key) => {
    const hit = recentStageSyncSuppressions.get(key)
    return hit && Number(hit.expiresAt || 0) > now
  })
}

function rememberWebhookKey(webhookKey) {
  if (!webhookKey) return false
  if (runtime.processedWebhookKeys.includes(webhookKey)) return true

  runtime.processedWebhookKeys.unshift(webhookKey)
  runtime.processedWebhookKeys = runtime.processedWebhookKeys.slice(0, 200)
  return false
}

function buildSyncJobId(taskId) {
  return `sync-${String(taskId)}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`
}

function getSyncJobPriority(source = '') {
  const normalizedSource = String(source || '').trim().toLowerCase()

  if (normalizedSource.includes('clickup-integration-webhook')) return 0
  if (normalizedSource.includes('clickup-api-webhook')) return 0
  if (normalizedSource.includes('stage-reconcile')) return 1
  if (normalizedSource.includes('manual')) return 1
  if (normalizedSource.includes('retry')) return 2
  if (normalizedSource.includes('reconciler')) return 9

  return 5
}

function sortSyncQueue() {
  runtime.sync.queue.sort((left, right) => {
    const priorityDelta = Number(left.priority || 0) - Number(right.priority || 0)
    if (priorityDelta !== 0) return priorityDelta

    return Date.parse(left.requestedAt || 0) - Date.parse(right.requestedAt || 0)
  })
}

function calculateRetryDelay(attempt) {
  return SYNC_RETRY_BASE_MS * Math.max(1, 2 ** Math.max(0, attempt - 1))
}

function normalizeSyncJob(input = {}) {
  const taskId = String(input.taskId || '').trim()
  return {
    id: input.id || buildSyncJobId(taskId),
    taskId,
    trigger: String(input.trigger || 'queue').trim() || 'queue',
    source: String(input.source || input.trigger || 'queue').trim() || 'queue',
    priority:
      input.priority === undefined || input.priority === null
        ? getSyncJobPriority(input.source || input.trigger || 'queue')
        : Number(input.priority),
    dryRun: Boolean(input.dryRun),
    refreshBefore: Boolean(input.refreshBefore),
    refreshAfter: Boolean(input.refreshAfter),
    directFetchTask: input.directFetchTask !== false,
    backgroundRefresh: input.backgroundRefresh !== false,
    attempt: Math.max(1, Number(input.attempt || 1)),
    maxAttempts: Math.max(1, Number(input.maxAttempts || SYNC_MAX_ATTEMPTS)),
    controlledStageLabels: Array.isArray(input.controlledStageLabels) ? input.controlledStageLabels : null,
    requestedAt: input.requestedAt || new Date().toISOString(),
    reason: input.reason || null,
    event: input.event || null,
    integrationId: input.integrationId || null,
    clickupContext:
      input.clickupContext && typeof input.clickupContext === 'object' ? input.clickupContext : null,
    taskStateSignature: String(input.taskStateSignature || '').trim() || null,
    parentJobId: input.parentJobId || null,
  }
}

function mergeSyncJobs(current, incoming) {
  return {
    ...current,
    trigger: incoming.trigger || current.trigger,
    source:
      Number(incoming.priority || Number.POSITIVE_INFINITY) <=
      Number(current.priority || Number.POSITIVE_INFINITY)
        ? incoming.source || current.source
        : current.source || incoming.source,
    priority: Math.min(
      Number.isFinite(Number(current.priority)) ? Number(current.priority) : Number.POSITIVE_INFINITY,
      Number.isFinite(Number(incoming.priority)) ? Number(incoming.priority) : Number.POSITIVE_INFINITY,
    ),
    refreshBefore: current.refreshBefore || incoming.refreshBefore,
    refreshAfter: current.refreshAfter || incoming.refreshAfter,
    directFetchTask: current.directFetchTask || incoming.directFetchTask,
    backgroundRefresh: current.backgroundRefresh || incoming.backgroundRefresh,
    dryRun: current.dryRun && incoming.dryRun,
    attempt: Math.min(current.attempt, incoming.attempt),
    maxAttempts: Math.max(current.maxAttempts, incoming.maxAttempts),
    controlledStageLabels: incoming.controlledStageLabels || current.controlledStageLabels,
    requestedAt: incoming.requestedAt || current.requestedAt,
    reason: incoming.reason || current.reason,
    event: incoming.event || current.event,
    integrationId: incoming.integrationId || current.integrationId,
    clickupContext: incoming.clickupContext || current.clickupContext,
    taskStateSignature: incoming.taskStateSignature || current.taskStateSignature || null,
    parentJobId: incoming.parentJobId || current.parentJobId,
  }
}

function enqueueSyncJob(input = {}) {
  const job = normalizeSyncJob(input)

  if (!job.taskId) {
    return {
      ok: false,
      reason: 'missing_task_id',
    }
  }

  const queued = runtime.sync.queuedByTaskId.get(job.taskId)
  if (queued) {
    const merged = mergeSyncJobs(queued, job)
    runtime.sync.queuedByTaskId.set(job.taskId, merged)
    const queueIndex = runtime.sync.queue.findIndex((item) => item.taskId === job.taskId)
    if (queueIndex >= 0) runtime.sync.queue[queueIndex] = merged
    sortSyncQueue()
    pushSyncAudit({
      type: 'sync_queue_merged',
      level: 'info',
      taskId: job.taskId,
      trigger: merged.trigger,
      source: merged.source,
      attempt: merged.attempt,
    })
    scheduleSyncStatePersist()
    return {
      ok: true,
      queued: true,
      merged: true,
      job: merged,
    }
  }

  const active = runtime.sync.activeByTaskId.get(job.taskId)
  if (active) {
    const deferred = runtime.sync.deferredByTaskId.get(job.taskId)
    const merged = mergeSyncJobs(deferred || active, job)
    runtime.sync.deferredByTaskId.set(job.taskId, merged)
    pushSyncAudit({
      type: 'sync_queue_deferred',
      level: 'info',
      taskId: job.taskId,
      trigger: merged.trigger,
      source: merged.source,
      attempt: merged.attempt,
      activeJobId: active.id,
    })
    scheduleSyncStatePersist()
    return {
      ok: true,
      queued: true,
      deferred: true,
      job: merged,
    }
  }

  runtime.sync.queue.push(job)
  sortSyncQueue()
  runtime.sync.queuedByTaskId.set(job.taskId, job)
  runtime.sync.metrics.enqueued += 1
  pushSyncAudit({
    type: 'sync_enqueued',
    level: 'info',
    taskId: job.taskId,
    trigger: job.trigger,
    source: job.source,
    attempt: job.attempt,
  })
  scheduleSyncStatePersist()
  processSyncQueue()

  return {
    ok: true,
    queued: true,
    job,
  }
}

function scheduleSyncRetry(job, error) {
  if (job.attempt >= job.maxAttempts) return false

  const nextAttempt = job.attempt + 1
  const delayMs = calculateRetryDelay(nextAttempt)
  const taskId = job.taskId

  const existingTimer = runtime.sync.retryTimersByTaskId.get(taskId)
  if (existingTimer) clearTimeout(existingTimer)

  const timer = setTimeout(() => {
    runtime.sync.retryTimersByTaskId.delete(taskId)
    runtime.sync.retryJobMetaByTaskId.delete(taskId)
    scheduleSyncStatePersist()
    enqueueSyncJob({
      ...job,
      id: buildSyncJobId(taskId),
      attempt: nextAttempt,
      trigger: `${job.trigger}-retry`,
      source: 'retry',
      dryRun: false,
      refreshBefore: false,
      refreshAfter: false,
      directFetchTask: true,
      backgroundRefresh: true,
      parentJobId: job.id,
      reason: error?.message || job.reason || null,
    })
  }, delayMs)
  timer.unref?.()

  runtime.sync.retryTimersByTaskId.set(taskId, timer)
  runtime.sync.retryJobMetaByTaskId.set(taskId, {
    ...job,
    taskId,
    id: buildSyncJobId(taskId),
    attempt: nextAttempt,
    trigger: `${job.trigger}-retry`,
    source: 'retry',
    dryRun: false,
    refreshBefore: false,
    refreshAfter: false,
    directFetchTask: true,
    backgroundRefresh: true,
    parentJobId: job.id,
    reason: error?.message || job.reason || null,
    dueAt: new Date(Date.now() + delayMs).toISOString(),
  })
  runtime.sync.metrics.retried += 1
  scheduleSyncStatePersist()
  pushSyncAudit({
    type: 'sync_retry_scheduled',
    level: 'warning',
    taskId,
    trigger: job.trigger,
    source: 'retry',
    attempt: nextAttempt,
    delayMs,
    error: error?.message || null,
    parentJobId: job.id,
  })

  return true
}

function scheduleConversationRecovery(job, result) {
  const hadKnownConversation = Boolean(result?.conversationSync?.knownConversationId)
  if (!hadKnownConversation) return false
  if (job.attempt >= job.maxAttempts) return false

  const taskId = job.taskId
  if (runtime.sync.retryTimersByTaskId.has(taskId)) return false

  const nextAttempt = job.attempt + 1
  const delayMs = Math.min(15_000, 2_000 * nextAttempt)
  const timer = setTimeout(() => {
    runtime.sync.retryTimersByTaskId.delete(taskId)
    runtime.sync.retryJobMetaByTaskId.delete(taskId)
    scheduleSyncStatePersist()
    enqueueSyncJob({
      ...job,
      id: buildSyncJobId(taskId),
      attempt: nextAttempt,
      trigger: `${job.trigger}-conversation-recovery`,
      source: 'retry',
      dryRun: false,
      refreshBefore: false,
      refreshAfter: false,
      directFetchTask: true,
      backgroundRefresh: true,
      parentJobId: job.id,
      reason: result?.conversationSync?.reason || 'conversation_not_found',
    })
  }, delayMs)
  timer.unref?.()

  runtime.sync.retryTimersByTaskId.set(taskId, timer)
  runtime.sync.retryJobMetaByTaskId.set(taskId, {
    ...job,
    taskId,
    id: buildSyncJobId(taskId),
    attempt: nextAttempt,
    trigger: `${job.trigger}-conversation-recovery`,
    source: 'retry',
    dryRun: false,
    refreshBefore: false,
    refreshAfter: false,
    directFetchTask: true,
    backgroundRefresh: true,
    parentJobId: job.id,
    reason: result?.conversationSync?.reason || 'conversation_not_found',
    dueAt: new Date(Date.now() + delayMs).toISOString(),
  })
  runtime.sync.metrics.retried += 1
  scheduleSyncStatePersist()
  pushSyncAudit({
    type: 'sync_conversation_recovery_scheduled',
    level: 'warning',
    taskId,
    trigger: job.trigger,
    source: 'retry',
    attempt: nextAttempt,
    delayMs,
    parentJobId: job.id,
    reason: result?.conversationSync?.reason || 'conversation_not_found',
  })
  return true
}

function releaseDeferredSyncJob(taskId) {
  const deferred = runtime.sync.deferredByTaskId.get(taskId)
  if (!deferred) return
  runtime.sync.deferredByTaskId.delete(taskId)
  scheduleSyncStatePersist()
  enqueueSyncJob({
    ...deferred,
    id: buildSyncJobId(taskId),
    parentJobId: deferred.id,
  })
}

async function runSyncJob(job) {
  runtime.sync.activeByTaskId.set(job.taskId, job)
  scheduleSyncStatePersist()
  pushSyncAudit({
    type: 'sync_started',
    level: 'info',
    taskId: job.taskId,
    trigger: job.trigger,
    source: job.source,
    attempt: job.attempt,
    jobId: job.id,
  })

  try {
    const result = await syncClickupTaskToBradial(job.taskId, {
      dryRun: job.dryRun,
      trigger: job.trigger,
      refreshBefore: job.refreshBefore,
      refreshAfter: job.refreshAfter,
      directFetchTask: job.directFetchTask,
      backgroundRefresh: job.backgroundRefresh,
      controlledStageLabels: job.controlledStageLabels,
      clickupContext: job.clickupContext,
    })

    runtime.sync.metrics.processed += 1
    if (result.ok) runtime.sync.metrics.succeeded += 1

    const recoveredWithoutConversation =
      result.contactOperation === 'create' ||
      result.contactOperation === 'update' ||
      result.conversationSync?.contactLabelOperation === 'update' ||
      result.conversationSync?.reason === 'contact_labels_synced_without_conversation'

    const recoveringConversation =
      !result.skipped &&
      !recoveredWithoutConversation &&
      result.conversationSync?.enabled &&
      result.conversationSync?.reason === 'conversation_not_found'

    if (recoveringConversation) {
      scheduleConversationRecovery(job, result)
    }

    const noopStageReconcile =
      job.source === 'stage-reconcile' &&
      job.reason === 'task_state_changed' &&
      !result.skipped &&
      result.operation === 'noop' &&
      result.conversationSync?.operation !== 'update' &&
      result.metadataSync?.operation !== 'update'

    if (noopStageReconcile && job.taskStateSignature) {
      rememberClickupReconcileSuppression(job.taskId, job.taskStateSignature)
    }

    pushSyncAudit({
      type: result.skipped
        ? 'sync_skipped'
        : recoveringConversation
          ? 'sync_partial_retrying'
          : 'sync_succeeded',
      level: result.skipped ? 'warning' : recoveringConversation ? 'warning' : 'success',
      taskId: job.taskId,
      trigger: job.trigger,
      source: job.source,
      attempt: job.attempt,
      jobId: job.id,
      result,
    })
  } catch (error) {
    runtime.sync.metrics.processed += 1
    const willRetry = scheduleSyncRetry(job, error)
    if (!willRetry) runtime.sync.metrics.failed += 1

    pushSyncAudit({
      type: willRetry ? 'sync_failed_retrying' : 'sync_failed',
      level: 'error',
      taskId: job.taskId,
      trigger: job.trigger,
      source: job.source,
      attempt: job.attempt,
      jobId: job.id,
      error: error.message,
    })
  } finally {
    runtime.sync.activeByTaskId.delete(job.taskId)
    scheduleSyncStatePersist()
    releaseDeferredSyncJob(job.taskId)
    runtime.sync.workerCount = Math.max(0, runtime.sync.workerCount - 1)
    processSyncQueue()
  }
}

function processSyncQueue() {
  while (runtime.sync.workerCount < SYNC_CONCURRENCY && runtime.sync.queue.length) {
    const job = runtime.sync.queue.shift()
    if (!job?.taskId) continue

    runtime.sync.queuedByTaskId.delete(job.taskId)

    if (runtime.sync.activeByTaskId.has(job.taskId)) {
      const active = runtime.sync.activeByTaskId.get(job.taskId)
      runtime.sync.deferredByTaskId.set(job.taskId, mergeSyncJobs(active, job))
      scheduleSyncStatePersist()
      continue
    }

    runtime.sync.workerCount += 1
    void runSyncJob(job)
  }
}

function restoreRetryJob(meta) {
  const taskId = String(meta?.taskId || '').trim()
  if (!taskId) return

  const dueAt = Date.parse(meta?.dueAt || 0)
  const delayMs = Number.isFinite(dueAt) ? Math.max(0, dueAt - Date.now()) : 0
  const timer = setTimeout(() => {
    runtime.sync.retryTimersByTaskId.delete(taskId)
    runtime.sync.retryJobMetaByTaskId.delete(taskId)
    scheduleSyncStatePersist()
    enqueueSyncJob({
      ...meta,
      id: buildSyncJobId(taskId),
      requestedAt: new Date().toISOString(),
    })
  }, delayMs)
  timer.unref?.()

  runtime.sync.retryTimersByTaskId.set(taskId, timer)
  runtime.sync.retryJobMetaByTaskId.set(taskId, meta)
}

async function restorePersistedSyncState() {
  const persisted = await syncJobStore.readState()
  const pendingJobs = [
    ...(persisted.active || []),
    ...(persisted.deferred || []),
    ...(persisted.queued || []),
  ]

  for (const job of pendingJobs) {
    enqueueSyncJob({
      ...job,
      id: buildSyncJobId(job.taskId),
      requestedAt: job.requestedAt || new Date().toISOString(),
    })
  }

  for (const retryMeta of persisted.retries || []) {
    restoreRetryJob(retryMeta)
  }

  if (pendingJobs.length || (persisted.retries || []).length) {
    pushLog(
      'warning',
      'Fila de sync restaurada',
      `${pendingJobs.length} job(s) pendentes e ${(persisted.retries || []).length} retry(s) foram restaurados apos reinicio.`,
      { storePath: syncJobStore.storePath },
    )
  }
}

function validateClickupWebhookSignature(rawBody, signature, secret) {
  if (!secret) {
    return {
      ok: false,
      reason: 'missing_secret',
    }
  }

  if (!signature) {
    return {
      ok: false,
      reason: 'missing_signature',
    }
  }

  const expected = crypto
    .createHmac('sha256', secret)
    .update(rawBody || '')
    .digest('hex')

  if (signature.length !== expected.length) {
    return {
      ok: false,
      reason: 'invalid_signature',
    }
  }

  return {
    ok: crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected)),
    reason: 'invalid_signature',
  }
}

async function resolvePublicBaseUrl() {
  if (PUBLIC_BASE_URL) {
    return {
      publicBaseUrl: PUBLIC_BASE_URL.replace(/\/$/, ''),
      source: 'env:PUBLIC_BASE_URL',
      isPublic: !/localhost|127\.0\.0\.1|0\.0\.0\.0/.test(PUBLIC_BASE_URL),
    }
  }

  try {
    const response = await fetch('http://127.0.0.1:4040/api/tunnels')
    if (response.ok) {
      const payload = await response.json()
      const tunnels = Array.isArray(payload?.tunnels) ? payload.tunnels : []
      const httpsTunnel =
        tunnels.find((item) => String(item.proto || '').toLowerCase() === 'https') || tunnels[0]

      if (httpsTunnel?.public_url) {
        return {
          publicBaseUrl: String(httpsTunnel.public_url).replace(/\/$/, ''),
          source: 'ngrok',
          isPublic: true,
        }
      }
    }
  } catch {}

  return {
    publicBaseUrl: `http://localhost:${PORT}`,
    source: 'localhost',
    isPublic: false,
  }
}

function validateClickupIntegrationRequest(request, integration) {
  const signature = String(request.headers['x-signature'] || '').trim()

  if (integration?.clickupSecret) {
    return validateClickupWebhookSignature(request.rawBody || '', signature, integration.clickupSecret)
  }

  return {
    ok: true,
    reason: 'token_only',
  }
}

function validateBradialChatWebhookRequest(request) {
  if (!BRADIAL_CHAT_WEBHOOK_SECRET) {
    return {
      ok: true,
      reason: 'unsigned',
    }
  }

  const signature = String(
    request.headers['x-chatwoot-signature'] || request.headers['x-hub-signature-256'] || '',
  ).trim()
  const timestamp = String(
    request.headers['x-chatwoot-timestamp'] || request.headers['x-request-timestamp'] || '',
  ).trim()

  if (!signature) {
    return {
      ok: false,
      reason: 'missing_signature',
    }
  }

  if (BRADIAL_CHAT_WEBHOOK_MAX_AGE_SEC > 0 && timestamp) {
    const timestampSeconds = Number(timestamp)
    if (!Number.isFinite(timestampSeconds)) {
      return {
        ok: false,
        reason: 'invalid_timestamp',
      }
    }

    const ageSeconds = Math.abs(Math.floor(Date.now() / 1000) - timestampSeconds)
    if (ageSeconds > BRADIAL_CHAT_WEBHOOK_MAX_AGE_SEC) {
      return {
        ok: false,
        reason: 'stale_timestamp',
      }
    }
  }

  const rawBody = request.rawBody || ''
  const expectedCandidates = []

  if (timestamp) {
    expectedCandidates.push(
      crypto.createHmac('sha256', BRADIAL_CHAT_WEBHOOK_SECRET).update(`${timestamp}.${rawBody}`).digest('hex'),
    )
  }

  expectedCandidates.push(
    crypto.createHmac('sha256', BRADIAL_CHAT_WEBHOOK_SECRET).update(rawBody).digest('hex'),
  )

  const normalizedSignature = signature.replace(/^sha256=/i, '')

  const matched = expectedCandidates.some((expected) => {
    if (normalizedSignature.length !== expected.length) return false
    return crypto.timingSafeEqual(Buffer.from(normalizedSignature), Buffer.from(expected))
  })

  return {
    ok: matched,
    reason: matched ? 'validated' : 'invalid_signature',
  }
}

function findClickupTaskFromBradialEnvelope(envelope) {
  const link = leadLinkStore.findLinkByIdentity({
    conversationId: envelope.conversationId,
    chatContactId: envelope.chatContactId,
    phone: envelope.phone,
  })

  if (link?.taskId) {
    const linkedTask =
      runtime.cache.clickup.tasks.find((task) => String(task.id) === String(link.taskId)) || null

    return {
      task: linkedTask,
      link,
      resolution: linkedTask ? 'lead_link' : 'stale_lead_link',
    }
  }

  if (!envelope.phone) {
    return {
      task: null,
      link: null,
      resolution: 'missing_phone',
    }
  }

  const matches = runtime.cache.clickup.tasks.filter((task) => phonesMatchLoose(task.phone, envelope.phone))
  const clickupResolution = resolveClickupPhoneConflict(matches, envelope.phone)

  if (clickupResolution.shouldBlock) {
    return {
      task: null,
      link: null,
      resolution: 'ambiguous_clickup_phone',
      matchCount: clickupResolution.activeMatches.length,
    }
  }

  return {
    task: clickupResolution.canonicalTask || matches[0] || null,
    link: null,
    resolution: clickupResolution.canonicalTask ? 'phone_match' : 'not_found',
  }
}

async function enqueueMissingStageLabelSyncFromBradialWebhook(payload, options = {}) {
  const trigger = String(options.trigger || 'bradial-webhook').trim() || 'bradial-webhook'
  if (!runtime.lastRefreshAt) {
    await ensureSnapshot(`${trigger}-bootstrap`)
  }

  if (!BRADIAL_SYNC_CONVERSATION_LABELS || !bradial.chatEnabled) {
    return {
      ok: true,
      skipped: true,
      reason: 'conversation_label_sync_disabled',
    }
  }

  const envelope = extractBradialWebhookEnvelope(payload)
  const supportedEvents = ['conversation_created', 'conversation_updated']
  if (!supportedEvents.includes(envelope.event)) {
    return {
      ok: true,
      skipped: true,
      reason: 'unsupported_event',
      event: envelope.event,
    }
  }

  const controlledStageLabels = getControlledStageLabels()
  let labels = normalizeWebhookLabels(envelope.labels)

  if (envelope.conversationId) {
    try {
      labels = normalizeWebhookLabels(await bradial.fetchConversationLabels(envelope.conversationId))
    } catch {}
  }

  const currentControlledLabels = pickControlledLabels(labels, controlledStageLabels)
  if (currentControlledLabels.length) {
    return {
      ok: true,
      skipped: true,
      reason: 'conversation_already_tagged',
      conversationId: envelope.conversationId,
      labels: currentControlledLabels,
    }
  }

  const located = findClickupTaskFromBradialEnvelope({
    ...envelope,
    labels,
  })

  if (!located.task) {
    return {
      ok: true,
      skipped: true,
      reason: located.resolution || 'clickup_task_not_found',
      phone: envelope.phone,
      conversationId: envelope.conversationId,
      chatContactId: envelope.chatContactId,
      matchCount: located.matchCount || 0,
    }
  }

  leadLinkStore.upsertLink({
    taskId: located.task.id,
    phone: envelope.phone || located.task.phone || null,
    bradialContactId: located.link?.bradialContactId || null,
    chatContactId: envelope.chatContactId || located.link?.chatContactId || null,
    conversationId: envelope.conversationId || located.link?.conversationId || null,
  })

  const queued = enqueueSyncJob({
    taskId: located.task.id,
    dryRun: false,
    trigger: `${trigger}-stage-backfill`,
    source: 'bradial-chat-webhook-backfill',
    refreshBefore: false,
    refreshAfter: false,
    directFetchTask: true,
    backgroundRefresh: true,
    controlledStageLabels,
    reason: 'conversation_missing_stage_label',
  })

  return {
    ok: queued.ok,
    skipped: !queued.ok,
    queued: queued.ok,
    reason: queued.ok ? 'enqueued' : queued.reason || 'enqueue_failed',
    taskId: located.task.id,
    taskName: located.task.name,
    conversationId: envelope.conversationId,
    chatContactId: envelope.chatContactId,
  }
}

async function syncBradialStageToClickup(payload, options = {}) {
  const trigger = String(options.trigger || 'bradial-webhook').trim() || 'bradial-webhook'
  if (!runtime.lastRefreshAt) {
    await ensureSnapshot(`${trigger}-bootstrap`)
  }

  const envelope = extractBradialWebhookEnvelope(payload)
  const supportedEvents = ['conversation_created', 'conversation_updated']
  if (!supportedEvents.includes(envelope.event)) {
    return {
      ok: true,
      skipped: true,
      reason: 'unsupported_event',
      event: envelope.event,
    }
  }

  const controlledStageLabels = getControlledStageLabels()
  let labels = normalizeWebhookLabels(envelope.labels)

  if (envelope.conversationId && bradial.chatEnabled) {
    const changedControlledLabels = pickControlledLabels(envelope.changedLabels || [], controlledStageLabels)
    const fallbackLabels = changedControlledLabels.length
      ? normalizeWebhookLabels(envelope.changedLabels)
      : labels

    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const liveConversationLabels = await bradial.fetchConversationLabels(envelope.conversationId).catch(() => [])
      const liveControlledLabels = pickControlledLabels(liveConversationLabels, controlledStageLabels)

      if (liveControlledLabels.length) {
        labels = liveConversationLabels
        if (!changedControlledLabels.length || sameNormalizedLabels(liveControlledLabels, changedControlledLabels)) {
          break
        }
      } else if (changedControlledLabels.length) {
        labels = fallbackLabels
      }

      if (attempt < 3) {
        await sleep(150 * attempt)
      }
    }
  }

  const { currentControlledLabels, primaryControlledLabels } = selectPrimaryControlledStageLabels(
    labels,
    controlledStageLabels,
  )

  if (!currentControlledLabels.length) {
    return {
      ok: true,
      skipped: true,
      reason: 'no_controlled_stage_label',
      event: envelope.event,
      conversationId: envelope.conversationId,
    }
  }

  if (primaryControlledLabels.length > 1) {
    return {
      ok: true,
      skipped: true,
      reason: 'ambiguous_bradial_stage_label',
      labels: primaryControlledLabels,
      conversationId: envelope.conversationId,
    }
  }

  const targetLabel = primaryControlledLabels[0]
  const located = findClickupTaskFromBradialEnvelope({
    ...envelope,
    labels,
  })

  if (!located.task) {
    return {
      ok: true,
      skipped: true,
      reason: located.resolution || 'clickup_task_not_found',
      phone: envelope.phone,
      conversationId: envelope.conversationId,
      chatContactId: envelope.chatContactId,
      matchCount: located.matchCount || 0,
      targetLabel,
    }
  }

  if (
    shouldSuppressBradialStageSync({
      taskId: located.task.id,
      conversationId: envelope.conversationId,
      chatContactId: envelope.chatContactId,
      label: targetLabel,
    })
  ) {
    return {
      ok: true,
      skipped: true,
      reason: 'suppressed_self_echo',
      taskId: located.task.id,
      targetLabel,
      conversationId: envelope.conversationId,
    }
  }

  const targetStatus = resolveClickupStatusFromLabel(
    targetLabel,
    process.env.CLICKUP_STAGE_LABEL_MAP || null,
  )
  if (!targetStatus) {
    return {
      ok: true,
      skipped: true,
      reason: 'unmapped_bradial_stage_label',
      targetLabel,
      taskId: located.task.id,
    }
  }

  if (normalizeLabelKey(located.task.status) === normalizeLabelKey(targetStatus)) {
    if (envelope.phone || envelope.conversationId || envelope.chatContactId) {
      leadLinkStore.upsertLink({
        taskId: located.task.id,
        phone: envelope.phone || located.task.phone || null,
        bradialContactId: located.link?.bradialContactId || null,
        chatContactId: envelope.chatContactId || located.link?.chatContactId || null,
        conversationId: envelope.conversationId || located.link?.conversationId || null,
      })
    }

    return {
      ok: true,
      skipped: false,
      operation: 'noop',
      taskId: located.task.id,
      targetStatus,
      targetLabel,
      reason: 'already_in_sync',
    }
  }

  const updatedTask = await clickup.updateTaskStatus(located.task.id, targetStatus, trigger)
  patchRuntimeClickupTask(updatedTask)
  leadLinkStore.upsertLink({
    taskId: located.task.id,
    phone: normalizePhone(updatedTask?.phone || envelope.phone || located.task.phone || ''),
    bradialContactId: located.link?.bradialContactId || null,
    chatContactId: envelope.chatContactId || located.link?.chatContactId || null,
    conversationId: envelope.conversationId || located.link?.conversationId || null,
  })

  pushLog(
    'success',
    'Sync Bradial -> ClickUp',
    `${located.task.name} movida para ${targetStatus} a partir da tag ${targetLabel} no Bradial.`,
    {
      taskId: located.task.id,
      conversationId: envelope.conversationId,
      chatContactId: envelope.chatContactId,
      targetLabel,
      targetStatus,
      trigger,
    },
  )

  pushSyncAudit({
    type: 'sync_bradial_to_clickup',
    level: 'success',
    taskId: located.task.id,
    trigger,
    source: 'bradial-chat-webhook',
    conversationId: envelope.conversationId,
    chatContactId: envelope.chatContactId,
    targetLabel,
    targetStatus,
  })

  patchRuntimeLeadState({
    taskId: located.task.id,
    phone: normalizePhone(updatedTask?.phone || envelope.phone || located.task.phone || ''),
    chatContactId: envelope.chatContactId || located.link?.chatContactId || null,
    conversationId: envelope.conversationId || located.link?.conversationId || null,
    clickupStage: targetStatus,
    bradialLabels: [targetLabel],
  })

  scheduleDeferredSnapshotRefresh(`${trigger}-deferred`)

  return {
    ok: true,
    skipped: false,
    operation: 'update',
    taskId: located.task.id,
    targetStatus,
    targetLabel,
    conversationId: envelope.conversationId,
    chatContactId: envelope.chatContactId,
  }
}

async function syncBradialPriorityToClickup(payload, options = {}) {
  const trigger = String(options.trigger || 'bradial-webhook').trim() || 'bradial-webhook'
  if (!runtime.lastRefreshAt) {
    await ensureSnapshot(`${trigger}-bootstrap`)
  }

  const envelope = extractBradialWebhookEnvelope(payload)
  const supportedEvents = ['conversation_updated']
  if (!supportedEvents.includes(envelope.event)) {
    return {
      ok: true,
      skipped: true,
      reason: 'unsupported_event',
      event: envelope.event,
    }
  }

  if (!envelope.priorityChanged) {
    return {
      ok: true,
      skipped: true,
      reason: 'no_priority_change',
      event: envelope.event,
      conversationId: envelope.conversationId,
    }
  }

  const located = findClickupTaskFromBradialEnvelope(envelope)
  if (!located.task) {
    return {
      ok: true,
      skipped: true,
      reason: located.resolution || 'clickup_task_not_found',
      phone: envelope.phone,
      conversationId: envelope.conversationId,
      chatContactId: envelope.chatContactId,
      matchCount: located.matchCount || 0,
      targetPriority: envelope.changedPriority,
    }
  }

  const currentTaskPriority = normalizeChatPriority(located.task.priority)
  const targetPriority = normalizeChatPriority(envelope.changedPriority)

  if (currentTaskPriority === targetPriority) {
    return {
      ok: true,
      skipped: false,
      operation: 'noop',
      taskId: located.task.id,
      currentPriority: currentTaskPriority,
      targetPriority,
      reason: 'already_in_sync',
    }
  }

  const updatedTask = await clickup.updateTaskPriority(located.task.id, targetPriority, trigger)
  patchRuntimeClickupTask(updatedTask)
  leadLinkStore.upsertLink({
    taskId: located.task.id,
    phone: normalizePhone(updatedTask?.phone || envelope.phone || located.task.phone || ''),
    bradialContactId: located.link?.bradialContactId || null,
    chatContactId: envelope.chatContactId || located.link?.chatContactId || null,
    conversationId: envelope.conversationId || located.link?.conversationId || null,
  })

  pushLog(
    'success',
    'Prioridade Bradial -> ClickUp',
    `${located.task.name} atualizada para prioridade ${targetPriority || 'nenhuma'} a partir do Bradial.`,
    {
      taskId: located.task.id,
      conversationId: envelope.conversationId,
      chatContactId: envelope.chatContactId,
      targetPriority,
      trigger,
    },
  )

  pushSyncAudit({
    type: 'sync_bradial_priority_to_clickup',
    level: 'success',
    taskId: located.task.id,
    trigger,
    source: 'bradial-chat-webhook',
    conversationId: envelope.conversationId,
    chatContactId: envelope.chatContactId,
    targetPriority,
  })

  patchRuntimeLeadState({
    taskId: located.task.id,
    phone: normalizePhone(updatedTask?.phone || envelope.phone || located.task.phone || ''),
    chatContactId: envelope.chatContactId || located.link?.chatContactId || null,
    conversationId: envelope.conversationId || located.link?.conversationId || null,
    clickupPriority: updatedTask?.priority ?? targetPriority,
    clickupUrgency: updatedTask?.urgency ?? null,
    lastSyncAt: new Date().toISOString(),
  })

  return {
    ok: true,
    skipped: false,
    operation: 'update',
    taskId: located.task.id,
    targetPriority,
    conversationId: envelope.conversationId,
    chatContactId: envelope.chatContactId,
  }
}

async function syncClickupTaskToBradial(taskId, options = {}) {
  const dryRun = Boolean(options.dryRun)
  const trigger = String(options.trigger || 'manual').trim() || 'manual'
  const refreshBefore = Boolean(options.refreshBefore)
  const refreshAfter = Boolean(options.refreshAfter)
  const directFetchTask = options.directFetchTask !== false
  const backgroundRefresh = options.backgroundRefresh !== false
  const clickupContext =
    options.clickupContext && typeof options.clickupContext === 'object' ? options.clickupContext : null
  const controlledStageLabels = Array.isArray(options.controlledStageLabels)
    ? options.controlledStageLabels
    : getControlledStageLabels()

  if (refreshBefore) {
    await refreshSnapshot(`${trigger}-preload`)
  } else if (!runtime.lastRefreshAt) {
    await ensureSnapshot(`${trigger}-bootstrap`)
  }

  const task = await resolveClickupTask(taskId, {
    preferFresh: directFetchTask,
    trigger,
  })
  if (!task) {
    return {
      ok: false,
      skipped: true,
      reason: 'task_not_in_scope',
      taskId,
    }
  }

  const normalizedPhone = normalizePhone(task.phone)
  if (!normalizedPhone) {
    return {
      ok: false,
      skipped: true,
      reason: 'missing_phone',
      taskId,
    }
  }

  const clickupMatches = runtime.cache.clickup.tasks.filter(
    (item) => phonesMatchLoose(item.phone, normalizedPhone),
  )
  const clickupResolution = resolveClickupPhoneConflict(clickupMatches, normalizedPhone)

  if (clickupResolution.ambiguous) {
    pushLog(
      'warning',
      'Sync ClickUp bloqueado por telefone ambiguo',
      `${task.name} compartilha telefone com outras tasks ativas no ClickUp e precisa ser saneada antes do sync.`,
      {
        taskId,
        phone: normalizedPhone,
        matchCount: clickupResolution.activeMatches.length,
      },
    )
    return {
      ok: false,
      skipped: true,
      reason: 'ambiguous_clickup_phone',
      taskId,
      phone: normalizedPhone,
      matchCount: clickupResolution.activeMatches.length,
    }
  }

  if (
    clickupResolution.canonicalTask?.id &&
    String(clickupResolution.canonicalTask.id) !== String(taskId)
  ) {
    pushLog(
      'info',
      'Task duplicada do ClickUp suprimida',
      `${task.name} foi reconhecida como duplicata; a task canonica ${clickupResolution.canonicalTask.id} segue como referencia do telefone.`,
      {
        taskId,
        canonicalTaskId: String(clickupResolution.canonicalTask.id),
        phone: normalizedPhone,
        matchCount: clickupResolution.activeMatches.length,
      },
    )
    return {
      ok: false,
      skipped: true,
      reason: 'suppressed_clickup_duplicate',
      taskId,
      phone: normalizedPhone,
      canonicalTaskId: String(clickupResolution.canonicalTask.id),
      matchCount: clickupResolution.activeMatches.length,
    }
  }

  const bradialMatches = findLeadMatchesByPhone(normalizedPhone)
  const leadLink = findLeadLink(taskId, normalizedPhone)
  const existingLead =
    bradialMatches.find((lead) => String(resolveBradialContactId(null, lead) || '') === String(leadLink?.bradialContactId || '')) ||
    bradialMatches.find((lead) => normalizePhone(lead.phone) === normalizedPhone) ||
    bradialMatches[0] ||
    null
  const knownConversationId =
    String(leadLink?.conversationId || existingLead?.conversationId || existingLead?.chatConversationId || '').trim() ||
    null
  const knownChatContactId =
    String(leadLink?.chatContactId || existingLead?.chatContactId || resolveBradialContactId(null, existingLead) || '').trim() ||
    null
  const metadataOnly = Boolean(clickupContext?.isPriorityEvent) && !clickupContext?.isStatusEvent
  const terminalStatus = isClosedOpportunityTask(task, {
    stageLabelMap: CLICKUP_STAGE_LABEL_MAP,
    closedStageLabels: CLICKUP_CLOSED_STAGE_LABELS,
  })

  if (
    terminalStatus &&
    !existingLead &&
    !leadLink?.bradialContactId &&
    !leadLink?.chatContactId &&
    !leadLink?.conversationId
  ) {
    return {
      ok: false,
      skipped: true,
      reason: 'terminal_status',
      taskId,
      statusType: String(task.statusType || '').toLowerCase(),
    }
  }

  try {
    const result =
      metadataOnly && knownConversationId
        ? {
            payload: {
              name: task.name,
              phoneNumber: normalizedPhone,
            },
            contact: existingLead,
            stageLabel: resolveBradialStageLabel(task.status, CLICKUP_STAGE_LABEL_MAP),
            previousStageLabels: normalizeWebhookLabels(
              existingLead?.bradialLabels || existingLead?.raw?.bradialLabels || [],
            ),
            conversationSync: {
              enabled: bradial.chatEnabled,
              skipped: false,
              operation: 'noop',
              conversationId: knownConversationId,
              knownConversationId,
              chatContactId: knownChatContactId,
              labels: normalizeWebhookLabels(existingLead?.bradialLabels || existingLead?.raw?.bradialLabels || []),
              contactLabels: normalizeWebhookLabels(existingLead?.raw?.bradialLabels || existingLead?.bradialLabels || []),
              contactLabelOperation: 'noop',
              conversationLabelOperation: 'noop',
            },
            metadataSync: await bradial.syncLinkedConversationMetadata(task, {
              conversationId: knownConversationId,
              chatContactId: knownChatContactId,
              clickupContext,
              chatAgents: runtime.cache.chatAgents || [],
              dryRun,
            }),
          }
        : await bradial.upsertOpportunityContact(task, existingLead, {
            dryRun,
            allowCreate: !terminalStatus,
            controlledStageLabels,
            preferredContactId: leadLink?.bradialContactId || null,
            preferredChatContactId: leadLink?.chatContactId || null,
            preferredConversationId: leadLink?.conversationId || null,
            chatAgents: runtime.cache.chatAgents || [],
            clickupContext,
          })

    if (metadataOnly && knownConversationId) {
      result.operation = result.metadataSync?.operation === 'update' ? 'update' : 'noop'
    }

    if (!dryRun) {
      const canonicalContactId =
        result.consolidatedChatContact?.contactId ||
        result.conversationSync?.chatContactId ||
        resolveBradialContactId(result.contact, existingLead)
      const runtimeStageLabels =
        Array.isArray(result.conversationSync?.labels) && result.conversationSync.conversationId
          ? result.conversationSync.labels
          : BRADIAL_SYNC_CONTACT_LABELS && Array.isArray(result.conversationSync?.contactLabels)
            ? result.conversationSync.contactLabels
            : null

      if (result.consolidatedChatContact?.mergedIds?.length && canonicalContactId) {
        leadLinkStore.rebindContactIds({
          fromIds: result.consolidatedChatContact.mergedIds,
          toId: canonicalContactId,
        })
      }

      leadLinkStore.upsertLink({
        taskId,
        phone: normalizedPhone,
        bradialContactId: resolveBradialContactId(result.contact, existingLead),
        chatContactId: canonicalContactId || leadLink?.chatContactId || null,
        conversationId:
          result.conversationSync?.conversationId || leadLink?.conversationId || null,
      })

      rememberOutboundBradialStageSync({
        taskId,
        conversationId: result.conversationSync?.conversationId || leadLink?.conversationId || null,
        chatContactId: canonicalContactId || leadLink?.chatContactId || null,
        label: result.stageLabel,
      })

      patchRuntimeLeadState({
        taskId,
        phone: normalizedPhone,
        chatContactId: canonicalContactId || leadLink?.chatContactId || null,
        conversationId: result.conversationSync?.conversationId || leadLink?.conversationId || null,
        clickupStage: task.status,
        clickupPriority: task.priority,
        clickupUrgency: task.urgency,
        bradialLabels: runtimeStageLabels,
      })

      runtime.cache.clickup.pendingContacts = (runtime.cache.clickup.pendingContacts || []).filter(
        (item) => String(item.taskId) !== String(taskId),
      )
    }

    pushLog(
      result.operation === 'create' || result.operation === 'update' || result.operation === 'noop'
        ? 'success'
        : 'info',
      dryRun ? 'Dry-run ClickUp -> Bradial' : 'Sync ClickUp -> Bradial',
      `${task.name} processada com operacao ${result.operation}${
        result.conversationSync?.enabled && !result.conversationSync?.skipped
          ? ` e conversa ${result.conversationSync.operation}.`
          : ''
      }.`,
      {
        taskId,
        phone: normalizedPhone,
        dryRun,
        bradialContactId: resolveBradialContactId(result.contact, existingLead),
        bradialConversationId: result.conversationSync?.conversationId || null,
        conversationOperation: result.conversationSync?.operation || null,
        metadataOperation: result.metadataSync?.operation || null,
      },
    )

    if (result.metadataSync?.assignmentOperation === 'unmatched') {
      pushLog(
        'warning',
        'Responsavel ClickUp sem agente Bradial',
        `${task.name} nao encontrou agente correspondente para o responsavel atual do ClickUp.`,
        {
          taskId,
          phone: normalizedPhone,
          owner: task.owner || null,
          ownerEmail: task.ownerEmail || null,
          assignees: task.assignees || [],
          matchReason: result.metadataSync?.matchReason || 'no_match',
        },
      )
    }

    const shouldRefreshAfterSync =
      result.operation !== 'noop' ||
      result.contactOperation === 'create' ||
      result.contactOperation === 'update' ||
      result.conversationSync?.operation === 'create' ||
      result.conversationSync?.operation === 'update' ||
      result.metadataSync?.operation === 'update'

    if (!dryRun && refreshAfter && shouldRefreshAfterSync) {
      await refreshSnapshot(`${trigger}-${taskId}`)
    } else if (!dryRun && backgroundRefresh && shouldRefreshAfterSync) {
      scheduleDeferredSnapshotRefresh(`${trigger}-deferred`)
    }

    return {
      ok: true,
      skipped: false,
      dryRun,
      taskId,
      phone: normalizedPhone,
      operation: result.operation,
      opportunityLabel: BRADIAL_OPPORTUNITY_LABEL,
      chatSyncEnabled: bradial.chatEnabled,
      stageLabel: result.stageLabel,
      previousStageLabels: result.previousStageLabels || [],
      conversationSync: result.conversationSync || null,
      metadataSync: result.metadataSync || null,
      bradialContactId: resolveBradialContactId(result.contact, existingLead),
      bradialContactName: result.contact?.name || existingLead?.name || null,
      payload: result.payload,
    }
  } catch (error) {
    pushLog('error', 'Falha no sync ClickUp -> Bradial', error.message, {
      taskId,
      phone: normalizedPhone,
      trigger,
    })
    throw error
  }
}

function enqueuePendingSyncCandidates(trigger = 'reconcile-interval', limit = SYNC_RECONCILE_BATCH_SIZE) {
  const batchLimit = Math.max(1, Number(limit || SYNC_RECONCILE_BATCH_SIZE))
  const queueBusy =
    runtime.sync.queue.length > 0 ||
    runtime.sync.activeByTaskId.size > 0 ||
    runtime.sync.deferredByTaskId.size > 0 ||
    runtime.sync.retryTimersByTaskId.size > 0

  if (queueBusy) {
    pushLog(
      'info',
      'Reconcile adiado',
      'Fila ocupada com syncs em andamento; reconcile automatico foi adiado para priorizar eventos em tempo real.',
      { trigger },
    )
    return 0
  }

  const candidates = (runtime.cache.clickup.pendingContacts || [])
    .filter((item) => {
      if (!item.syncAllowed) return false
      const syncState = String(item.syncState || '')
      return ['missing_contact', 'missing_stage_label', 'stage_label_outdated'].includes(syncState) || (
        BRADIAL_CHAT_AUTO_CREATE_CONVERSATIONS &&
        syncState === 'conversation_required_for_stage_label'
      )
    })
    .slice(0, batchLimit)

  let queuedCount = 0

  for (const item of candidates) {
    const queued = enqueueSyncJob({
      taskId: item.taskId,
      dryRun: false,
      trigger,
      source: 'reconciler',
      refreshBefore: false,
      refreshAfter: false,
      directFetchTask: true,
      backgroundRefresh: true,
      reason: item.syncState,
    })
    if (queued.ok) queuedCount += 1
  }

  if (queuedCount > 0) {
    runtime.sync.metrics.reconciled += queuedCount
    pushLog(
      'info',
      'Pendencias re-enfileiradas',
      `${queuedCount} task(s) pendentes foram adicionadas na fila de reconciliacao.`,
      { trigger },
    )
  }

  return queuedCount
}

app.get('/healthz', async () => ({
  status: 'ok',
  source: 'bradial-clickup-sync-backend',
  started: true,
}))

app.get('/health', async () => {
  if (!runtime.lastRefreshAt) {
    await ensureSnapshot('health-bootstrap')
  }

  const status = runtime.errors.bradial
    ? 'degraded'
    : runtime.errors.clickup
      ? 'degraded'
      : 'ok'

  return {
    status,
    source: 'bradial-clickup-sync-backend',
    lastRefreshAt: runtime.lastRefreshAt,
    bradial: {
      status: runtime.errors.bradial ? 'degraded' : 'ok',
      apiBaseUrl: process.env.BRADIAL_BASE_URL,
      accountId: process.env.BRADIAL_ACCOUNT_ID || null,
      inboxId: process.env.BRADIAL_INBOX_ID || null,
      opportunityLabel: BRADIAL_OPPORTUNITY_LABEL,
      chatSyncEnabled: bradial.chatEnabled,
      contactLabelSyncEnabled: BRADIAL_SYNC_CONTACT_LABELS,
      stageLabelScope: BRADIAL_STAGE_LABEL_SCOPE,
      conversationAssignmentSyncEnabled: BRADIAL_SYNC_CONVERSATION_ASSIGNMENT,
      conversationAutoCreateEnabled: BRADIAL_CHAT_AUTO_CREATE_CONVERSATIONS,
      chatBaseUrl: process.env.BRADIAL_CHAT_BASE_URL || null,
      chatAccountId: process.env.BRADIAL_CHAT_ACCOUNT_ID || process.env.BRADIAL_ACCOUNT_ID || null,
      chatInboxId: process.env.BRADIAL_CHAT_INBOX_ID || null,
      controlledStageLabels: getControlledStageLabels(),
      cachedLeads: runtime.cache.leads.length,
      lastError: runtime.errors.bradial,
    },
    sync: {
      queueSize: runtime.sync.queue.length,
      activeCount: runtime.sync.activeByTaskId.size,
      deferredCount: runtime.sync.deferredByTaskId.size,
      retryingCount: runtime.sync.retryTimersByTaskId.size,
      metrics: runtime.sync.metrics,
      auditStorePath: syncAuditStore.storePath,
    },
    clickup: runtime.cache.clickup.health || {
      enabled: false,
      status: runtime.errors.clickup ? 'degraded' : 'disabled',
      lastError: runtime.errors.clickup,
    },
  }
})

app.get('/overview', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('overview-bootstrap')
  return runtime.cache.overview
})

app.get('/leads', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('leads-bootstrap')
  return runtime.cache.leads
})

app.get('/leads/:id', async (request, reply) => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('lead-bootstrap')
  const lead = runtime.cache.leads.find((item) => item.id === request.params.id)
  if (!lead) {
    reply.code(404)
    return { error: 'Lead nao encontrado' }
  }
  return lead
})

app.get('/exceptions', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('exceptions-bootstrap')
  return runtime.cache.exceptions
})

app.get('/logs', async () => runtime.logs)

app.get('/events', async (request, reply) => {
  reply.hijack()
  reply.raw.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
    'Access-Control-Allow-Origin': resolveSseAllowedOrigin(request),
    Vary: 'Origin',
  })

  reply.raw.write(
    `event: connected\ndata: ${JSON.stringify({
      connectedAt: new Date().toISOString(),
      lastRefreshAt: runtime.lastRefreshAt,
    })}\n\n`,
  )

  sseClients.add(reply.raw)

  request.raw.on('close', () => {
    sseClients.delete(reply.raw)
  })
})

app.get('/sync/health', async () => ({
  queueSize: runtime.sync.queue.length,
  activeJobs: [...runtime.sync.activeByTaskId.values()],
  deferredJobs: [...runtime.sync.deferredByTaskId.values()],
  retryingTaskIds: [...runtime.sync.retryTimersByTaskId.keys()],
  metrics: runtime.sync.metrics,
  lastRefreshAt: runtime.lastRefreshAt,
  auditStorePath: syncAuditStore.storePath,
}))

app.get('/sync/queue', async () => ({
  queued: runtime.sync.queue,
  active: [...runtime.sync.activeByTaskId.values()],
  deferred: [...runtime.sync.deferredByTaskId.values()],
  retryingTaskIds: [...runtime.sync.retryTimersByTaskId.keys()],
}))

app.get('/sync/audit', async (request) => {
  const limit = Math.max(1, Number(request.query?.limit || 100))
  return syncAuditStore.listRecent(limit)
})

app.post('/sync/reconcile', async (request) => {
  const limit = Math.max(
    1,
    Number(request.query?.limit || request.body?.limit || SYNC_RECONCILE_BATCH_SIZE),
  )
  const queuedCount = enqueuePendingSyncCandidates('manual-reconcile', limit)
  return {
    ok: true,
    queuedCount,
    limit,
  }
})

app.get('/agents', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('agents-bootstrap')
  return runtime.cache.agents
})

app.get('/chat/agents', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('chat-agents-bootstrap')
  return runtime.cache.chatAgents || []
})

app.get('/inboxes', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('inboxes-bootstrap')
  return runtime.cache.inboxes
})

app.get('/clickup/health', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('clickup-health-bootstrap')
  return runtime.cache.clickup.health
})

app.get('/clickup/workspaces', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('clickup-workspaces-bootstrap')
  return runtime.cache.clickup.workspaces
})

app.get('/clickup/navigation', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('clickup-navigation-bootstrap')
  return runtime.cache.clickup.navigation
})

app.get('/clickup/tasks', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('clickup-tasks-bootstrap')
  return runtime.cache.clickup.tasks
})

app.get('/clickup/pending-contacts', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('clickup-pending-contacts-bootstrap')
  return runtime.cache.clickup.pendingContacts
})

app.get('/clickup/webhook-integrations', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('clickup-webhook-integrations-bootstrap')

  const publicUrl = await resolvePublicBaseUrl()

  return {
    publicBaseUrl: publicUrl.publicBaseUrl,
    source: publicUrl.source,
    isPublic: publicUrl.isPublic,
    items: clickupIntegrations.listIntegrations(),
  }
})

app.post('/clickup/webhook-integrations', async (request) => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('clickup-webhook-integration-create-bootstrap')

  const publicUrl = await resolvePublicBaseUrl()
  const lists = runtime.cache.clickup.navigation?.lists || []
  const body = request.body || {}

  const integration = clickupIntegrations.createIntegration({
    name:
      body.name ||
      `${runtime.cache.clickup.health?.workspaceName || 'ClickUp'} webhook ${new Date().toLocaleDateString('pt-BR')}`,
    publicBaseUrl: String(body.publicBaseUrl || publicUrl.publicBaseUrl).trim(),
    workspaceId: body.workspaceId || runtime.cache.clickup.health?.workspaceId || null,
    workspaceName: body.workspaceName || runtime.cache.clickup.health?.workspaceName || null,
    lists,
    clickupSecret: body.clickupSecret || null,
  })

  return {
    ok: true,
    integration,
    source: publicUrl.source,
    isPublic: publicUrl.isPublic,
    warning: publicUrl.isPublic
      ? null
      : 'A URL gerada ainda aponta para localhost. Suba um tunel ou configure PUBLIC_BASE_URL antes de usar no ClickUp.',
  }
})

app.patch('/clickup/webhook-integrations/:integrationId', async (request, reply) => {
  const integrationId = String(request.params.integrationId || '').trim()
  const body = request.body || {}
  const updated = clickupIntegrations.updateIntegration(integrationId, {
    clickupSecret: body.clickupSecret,
    status: body.status,
    publicBaseUrl: body.publicBaseUrl,
    name: body.name,
  })

  if (!updated) {
    reply.code(404)
    return { error: 'Integracao nao encontrada.' }
  }

  return {
    ok: true,
    integration: updated,
  }
})

app.post('/clickup/tasks/:taskId/sync-to-bradial', async (request, reply) => {
  const taskId = String(request.params.taskId || '').trim()
  const dryRun = parseBooleanFlag(request.query?.dryRun || request.body?.dryRun)
  let result = await syncClickupTaskToBradial(taskId, {
    dryRun,
    trigger: 'manual-endpoint',
  })

  if (result.skipped) {
    if (result.reason === 'task_not_in_scope') {
      reply.code(404)
      return { error: 'Task do ClickUp nao encontrada no snapshot atual.', taskId }
    }

    if (result.reason === 'missing_phone') {
      reply.code(400)
      return { error: 'A task nao possui telefone canonico para criar/atualizar contato na Bradial.', taskId }
    }

    if (result.reason === 'terminal_status') {
      reply.code(409)
      return {
        error: 'A task esta em status terminal e nao entra no fluxo de oportunidade.',
        taskId,
        statusType: result.statusType,
      }
    }

    if (result.reason === 'ambiguous_clickup_phone') {
      reply.code(409)
      return {
        error: 'Telefone ambiguo no ClickUp. Resolva a duplicidade antes de sincronizar.',
        taskId,
        phone: result.phone,
        matchCount: result.matchCount,
      }
    }

    if (result.reason === 'suppressed_clickup_duplicate') {
      if (result.canonicalTaskId) {
        pushLog(
          'info',
          'Sync manual redirecionado para task canonica',
          `Task ${taskId} foi reconhecida como duplicata; a task canonica ${result.canonicalTaskId} sera priorizada.`,
          {
            taskId,
            canonicalTaskId: result.canonicalTaskId,
            phone: result.phone,
          },
        )

        const canonicalResult = await syncClickupTaskToBradial(result.canonicalTaskId, {
          dryRun,
          trigger: 'manual-endpoint-canonical',
        })

        return {
          ...canonicalResult,
          requestedTaskId: taskId,
          followedCanonicalTaskId: result.canonicalTaskId,
          duplicateTaskSuppressed: true,
          message:
            'Task duplicada detectada; a sincronizacao foi executada usando a task canonica do mesmo telefone.',
        }
      }

      return {
        ok: true,
        skipped: true,
        reason: result.reason,
        taskId,
        phone: result.phone,
        canonicalTaskId: result.canonicalTaskId,
        message: 'Task duplicada suprimida automaticamente; outra task canonica do mesmo telefone foi priorizada.',
      }
    }

    if (result.reason === 'ambiguous_bradial_phone') {
      reply.code(409)
      return {
        error: 'Telefone ambiguo na Bradial. Resolva a duplicidade antes de sincronizar.',
        taskId,
        phone: result.phone,
        matchCount: result.matchCount,
      }
    }
  }

  return result
})

app.post('/webhooks/clickup', async (request, reply) => {
  const signatureCheck = validateClickupWebhookSignature(
    request.rawBody || '',
    String(request.headers['x-signature'] || '').trim(),
    CLICKUP_WEBHOOK_SECRET,
  )

  if (!signatureCheck.ok) {
    reply.code(signatureCheck.reason === 'missing_secret' ? 503 : 401)
    return {
      error:
        signatureCheck.reason === 'missing_secret'
          ? 'Configure CLICKUP_WEBHOOK_SECRET antes de habilitar webhooks do ClickUp.'
          : 'Assinatura X-Signature invalida para webhook do ClickUp.',
    }
  }

  const payload = request.body || {}
  const webhookKey = buildClickupWebhookKey(payload)

  if (rememberWebhookKey(webhookKey)) {
    return {
      ok: true,
      duplicate: true,
      event: payload.event || null,
      taskId: payload.task_id || payload.taskId || payload.task?.id || null,
    }
  }

  const event = String(payload.event || '').trim()
  const taskId = String(payload.task_id || payload.taskId || payload.task?.id || '').trim()

  reply.code(202).send({
    ok: true,
    accepted: true,
    event,
    taskId: taskId || null,
  })

  queueMicrotask(() => {
    if (!['taskCreated', 'taskUpdated', 'taskStatusUpdated', 'taskPriorityUpdated', 'taskAssigneeUpdated'].includes(event)) {
      pushLog('info', 'Webhook ClickUp ignorado', `Evento ${event} fora do escopo de sync.`, {
        event,
      })
      return
    }

    if (!taskId) {
      pushLog('warning', 'Webhook ClickUp sem task_id', 'Payload recebido sem task_id.', {
        event,
      })
      return
    }

    const clickupContext = extractClickupWebhookContext(payload, {
      urgencyFieldNames: CLICKUP_URGENCY_FIELD_NAMES,
      stageLabelMap: CLICKUP_STAGE_LABEL_MAP,
      closedStageLabels: CLICKUP_CLOSED_STAGE_LABELS,
    })

    enqueueSyncJob({
      taskId,
      dryRun: false,
      trigger: `clickup-webhook-${event}`,
      source: 'clickup-api-webhook',
      refreshBefore: false,
      refreshAfter: false,
      directFetchTask: true,
      backgroundRefresh: !clickupContext.isPriorityEvent,
      event,
      clickupContext,
    })
  })

  return reply
})

app.post('/webhooks/clickup/:integrationId/:webhookToken', async (request, reply) => {
  const integrationId = String(request.params.integrationId || '').trim()
  const webhookToken = String(request.params.webhookToken || '').trim()
  const integration = clickupIntegrations.findByWebhookPath(integrationId, webhookToken)

  if (!integration) {
    reply.code(404)
    return {
      error: 'Integracao ClickUp nao encontrada ou inativa.',
    }
  }

  const signatureCheck = validateClickupIntegrationRequest(request, integration)

  if (!signatureCheck.ok) {
    reply.code(401)
    return {
      error: 'Assinatura X-Signature invalida para esta integracao do ClickUp.',
    }
  }

  const payload = request.body || {}
  const envelope = extractClickupWebhookEnvelope(payload)
  const webhookKey = buildIntegrationWebhookKey(integration.integrationId, {
    webhook_id: integration.integrationId,
    event: envelope.event,
    task_id: envelope.taskId,
    history_items: [{ id: envelope.webhookKey }],
  })

  if (rememberWebhookKey(webhookKey)) {
    return {
      ok: true,
      duplicate: true,
      integrationId: integration.integrationId,
      event: envelope.event || null,
      taskId: envelope.taskId || null,
    }
  }

  const event = envelope.event
  const taskId = envelope.taskId

  clickupIntegrations.markIntegrationEvent(integration.integrationId)

  reply.code(202).send({
    ok: true,
    accepted: true,
    integrationId: integration.integrationId,
    authMode: integration.authMode,
    mode: envelope.mode,
    event,
    taskId: taskId || null,
  })

  queueMicrotask(() => {
    const supportedEvents = [
      'taskCreated',
      'taskUpdated',
      'taskStatusUpdated',
      'taskPriorityUpdated',
      'taskAssigneeUpdated',
      'automation_call_webhook',
    ]

    if (!supportedEvents.includes(event)) {
      pushLog('info', 'Webhook ClickUp ignorado', `Evento ${event} fora do escopo de sync.`, {
        event,
        integrationId: integration.integrationId,
      })
      return
    }

    if (!taskId) {
      pushLog('warning', 'Webhook ClickUp sem task_id', 'Payload recebido sem task_id.', {
        event,
        integrationId: integration.integrationId,
      })
      return
    }

    const clickupContext = extractClickupWebhookContext(payload, {
      urgencyFieldNames: CLICKUP_URGENCY_FIELD_NAMES,
      stageLabelMap: CLICKUP_STAGE_LABEL_MAP,
      closedStageLabels: CLICKUP_CLOSED_STAGE_LABELS,
    })

    enqueueSyncJob({
      taskId,
      dryRun: false,
      trigger: `clickup-webhook-${integration.integrationId}-${event}`,
      source: 'clickup-integration-webhook',
      refreshBefore: false,
      refreshAfter: false,
      directFetchTask: true,
      backgroundRefresh: !clickupContext.isPriorityEvent,
      event,
      integrationId: integration.integrationId,
      clickupContext,
    })
  })

  return reply
})

app.post('/webhooks/bradial/chatwoot', async (request, reply) => {
  const signatureCheck = validateBradialChatWebhookRequest(request)
  if (!signatureCheck.ok) {
    reply.code(401)
    return {
      error: 'Assinatura invalida para webhook do Bradial/Chat.',
      reason: signatureCheck.reason,
    }
  }

  const payload = request.body || {}
  const envelope = extractBradialWebhookEnvelope(payload)
  const webhookKey = buildBradialWebhookKey(request, envelope, payload)

  if (rememberWebhookKey(webhookKey)) {
    return {
      ok: true,
      duplicate: true,
      event: envelope.event,
      conversationId: envelope.conversationId,
      chatContactId: envelope.chatContactId,
    }
  }

  reply.code(202).send({
    ok: true,
    accepted: true,
    event: envelope.event,
    conversationId: envelope.conversationId,
    chatContactId: envelope.chatContactId,
    signed: signatureCheck.reason !== 'unsigned',
  })

  queueMicrotask(() => {
    void enqueueMissingStageLabelSyncFromBradialWebhook(payload, {
      trigger: `bradial-chat-webhook-${envelope.event || 'unknown'}`,
    })
      .then((result) => {
        if (result?.queued) {
          pushLog(
            'info',
            'Backfill de etiqueta enfileirado',
            `${result.taskName || `Task ${result.taskId}`} entrou na fila para aplicar a etiqueta atual do ClickUp na conversa.`,
            {
              taskId: result.taskId,
              conversationId: result.conversationId,
              chatContactId: result.chatContactId,
            },
          )
        }
      })
      .catch((error) => {
        pushLog(
          'warning',
          'Falha ao enfileirar backfill de etiqueta',
          error.message,
          {
            event: envelope.event,
            conversationId: envelope.conversationId,
            chatContactId: envelope.chatContactId,
          },
        )
      })

    void syncBradialStageToClickup(payload, {
      trigger: `bradial-chat-webhook-${envelope.event || 'unknown'}`,
    })
      .then((result) => {
        if (result?.skipped) {
          pushLog(
            'info',
            'Webhook Bradial ignorado',
            `Evento ${envelope.event || 'unknown'} ignorado: ${result.reason}.`,
            {
              conversationId: envelope.conversationId,
              chatContactId: envelope.chatContactId,
              targetLabel: result.targetLabel || null,
            },
          )
        }
      })
      .catch((error) => {
        pushLog(
          'error',
          'Falha no sync Bradial -> ClickUp',
          error.message,
          {
            event: envelope.event,
            conversationId: envelope.conversationId,
            chatContactId: envelope.chatContactId,
          },
        )
        pushSyncAudit({
          type: 'sync_bradial_to_clickup_failed',
          level: 'error',
          trigger: `bradial-chat-webhook-${envelope.event || 'unknown'}`,
          source: 'bradial-chat-webhook',
          conversationId: envelope.conversationId,
          chatContactId: envelope.chatContactId,
          error: error.message,
        })
      })

    void syncBradialPriorityToClickup(payload, {
      trigger: `bradial-chat-webhook-priority-${envelope.event || 'unknown'}`,
    }).catch((error) => {
      pushLog(
        'error',
        'Falha no sync de prioridade Bradial -> ClickUp',
        error.message,
        {
          event: envelope.event,
          conversationId: envelope.conversationId,
          chatContactId: envelope.chatContactId,
        },
      )
      pushSyncAudit({
        type: 'sync_bradial_priority_to_clickup_failed',
        level: 'error',
        trigger: `bradial-chat-webhook-priority-${envelope.event || 'unknown'}`,
        source: 'bradial-chat-webhook',
        conversationId: envelope.conversationId,
        chatContactId: envelope.chatContactId,
        error: error.message,
      })
    })
  })

  return reply
})

app.post('/refresh', async () => {
  await ensureSnapshot('manual-endpoint')
  return { ok: true, lastRefreshAt: runtime.lastRefreshAt }
})

setInterval(() => {
  if (!runtime.lastRefreshAt) return
  enqueuePendingSyncCandidates('reconcile-interval')
}, SYNC_RECONCILE_MS).unref()

setInterval(() => {
  for (const client of [...sseClients]) {
    try {
      client.write(`: heartbeat ${Date.now()}\n\n`)
    } catch {
      sseClients.delete(client)
    }
  }
}, 15_000).unref()

setInterval(() => {
  ensureSnapshot('interval').catch(() => {})
}, BRADIAL_REFRESH_MS).unref()

runtime.sync.audit = await syncAuditStore.listRecent(100)
await restorePersistedSyncState()

await app.listen({ host: '0.0.0.0', port: PORT })
pushLog('success', 'Backend iniciado', `Servidor ouvindo na porta ${PORT}`)
ensureSnapshot('startup').catch((error) => {
  pushLog('error', 'Falha no bootstrap', error.message)
})
