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
} from './services/clickupStageLabels.js'
import { buildConsolidatedSnapshot } from './services/consolidation.js'
import { normalizePhone, phonesMatchLoose } from './utils/normalizers.js'

dotenv.config()

const PORT = Number(process.env.PORT || 3015)
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || 'http://localhost:4180'
const BRADIAL_REFRESH_MS = Math.max(15_000, Number(process.env.BRADIAL_REFRESH_MS || 60_000))
const BRADIAL_OPPORTUNITY_LABEL =
  String(process.env.BRADIAL_OPPORTUNITY_LABEL || 'OPORTUNIDADE').trim() || 'OPORTUNIDADE'
const BRADIAL_SYNC_CONVERSATION_LABELS = !['0', 'false', 'no', 'off'].includes(
  String(process.env.BRADIAL_SYNC_CONVERSATION_LABELS || 'true').trim().toLowerCase(),
)
const SYNC_MAX_ATTEMPTS = Math.max(1, Number(process.env.SYNC_MAX_ATTEMPTS || 4))
const SYNC_RETRY_BASE_MS = Math.max(500, Number(process.env.SYNC_RETRY_BASE_MS || 1_500))
const SYNC_CONCURRENCY = Math.max(1, Number(process.env.SYNC_CONCURRENCY || 6))
const SYNC_RECONCILE_MS = Math.max(30_000, Number(process.env.SYNC_RECONCILE_MS || 90_000))
const SYNC_RECONCILE_BATCH_SIZE = Math.max(1, Number(process.env.SYNC_RECONCILE_BATCH_SIZE || 5))
const SYNC_RECONCILE_LOOKBACK_MS = Math.max(
  60_000,
  Number(process.env.SYNC_RECONCILE_LOOKBACK_MS || 20 * 60 * 1000),
)
const DEFERRED_REFRESH_MS = Math.max(100, Number(process.env.DEFERRED_REFRESH_MS || 150))
const CLICKUP_STAGE_LABEL_MAP = buildStageLabelMap(process.env.CLICKUP_STAGE_LABEL_MAP)
const CLICKUP_WEBHOOK_SECRET = String(process.env.CLICKUP_WEBHOOK_SECRET || '').trim()
const PUBLIC_BASE_URL = String(process.env.PUBLIC_BASE_URL || '').trim()
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
  origin: [FRONTEND_ORIGIN, 'http://127.0.0.1:4180'],
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
let inflightRefresh = null
let inflightStageReconcile = null
let deferredRefreshTimer = null
let syncStatePersistTimer = null
const sseClients = new Set()

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

function buildClickupTaskStateIndex(tasks = []) {
  const index = new Map()

  for (const task of tasks || []) {
    index.set(String(task.id), {
      status: String(task.status || '').trim(),
      phone: normalizePhone(task.phone),
    })
  }

  return index
}

function collectChangedClickupTaskIds(tasks = [], previousIndex = new Map()) {
  if (!previousIndex.size) return []

  const changedTaskIds = []

  for (const task of tasks || []) {
    const currentTaskId = String(task.id)
    const previous = previousIndex.get(currentTaskId)
    if (!previous) continue

    const currentStatus = String(task.status || '').trim()
    const currentPhone = normalizePhone(task.phone)

    if (previous.status !== currentStatus || previous.phone !== currentPhone) {
      changedTaskIds.push(currentTaskId)
    }
  }

  return changedTaskIds
}

function scheduleStageReconcile(taskIds = [], trigger = 'refresh') {
  for (const taskId of taskIds) {
    if (taskId) runtime.queuedStageReconcileTaskIds.add(String(taskId))
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
    const pendingItem = runtime.cache.clickup.pendingContacts.find((item) => item.taskId === taskId)
    if (!pendingItem) continue
    if (!pendingItem.bradialContactId) continue
    if (!['missing_stage_label', 'stage_label_outdated'].includes(pendingItem.syncState)) continue

    const queued = enqueueSyncJob({
      taskId,
      dryRun: false,
      trigger: `stage-reconcile-${trigger}`,
      source: 'stage-reconcile',
      refreshBefore: false,
      refreshAfter: false,
      directFetchTask: true,
      backgroundRefresh: true,
      controlledStageLabels,
      reason: pendingItem.syncState,
    })

    if (queued.ok) {
      enqueuedCount += 1
    }
  }

  if (enqueuedCount > 0) {
    runtime.sync.metrics.reconciled += enqueuedCount
    pushLog(
      'info',
      'Reconcile de etapa enfileirado',
      `${enqueuedCount} task(s) foram enviadas para a fila de sync de etapa.`,
      { trigger },
    )
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

  const previousTaskStateIndex = runtime.clickupTaskStateIndex

  runtime.lastRefreshAt = snapshotAt
  runtime.cache = buildConsolidatedSnapshot({
    bradialSnapshot: bradialResult.value,
    clickupSnapshot,
    accountId: process.env.BRADIAL_ACCOUNT_ID || null,
    preferredInboxId: process.env.BRADIAL_INBOX_ID || null,
    lastRefreshAt: snapshotAt,
    stageLabelMap: CLICKUP_STAGE_LABEL_MAP,
    controlledStageLabels: getControlledStageLabels(clickupSnapshot.tasks || []),
  })
  runtime.clickupTaskStateIndex = buildClickupTaskStateIndex(runtime.cache.clickup.tasks)

  const changedTaskIds = collectChangedClickupTaskIds(
    runtime.cache.clickup.tasks,
    previousTaskStateIndex,
  )

  if (changedTaskIds.length) {
    scheduleStageReconcile(changedTaskIds, trigger)
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
  runtime.clickupTaskStateIndex.set(String(task.id), {
    status: String(task.status || '').trim(),
    phone: normalizePhone(task.phone),
  })
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
    contact?.chatContactId ||
    contact?.raw?.bradialContactId ||
    fallbackLead?.chatContactId ||
    fallbackLead?.raw?.bradialContactId ||
    contact?.id ||
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
    })

    runtime.sync.metrics.processed += 1
    if (result.ok) runtime.sync.metrics.succeeded += 1

    const recoveringConversation =
      !result.skipped &&
      result.conversationSync?.enabled &&
      result.conversationSync?.reason === 'conversation_not_found'

    if (recoveringConversation) {
      scheduleConversationRecovery(job, result)
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

async function syncClickupTaskToBradial(taskId, options = {}) {
  const dryRun = Boolean(options.dryRun)
  const trigger = String(options.trigger || 'manual').trim() || 'manual'
  const refreshBefore = Boolean(options.refreshBefore)
  const refreshAfter = Boolean(options.refreshAfter)
  const directFetchTask = options.directFetchTask !== false
  const backgroundRefresh = options.backgroundRefresh !== false
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

  const statusType = String(task.statusType || '').toLowerCase()
  if (statusType === 'done' || statusType === 'closed') {
    return {
      ok: false,
      skipped: true,
      reason: 'terminal_status',
      taskId,
      statusType,
    }
  }

  const clickupMatches = runtime.cache.clickup.tasks.filter(
    (item) => phonesMatchLoose(item.phone, normalizedPhone),
  )
  if (clickupMatches.length > 1) {
    return {
      ok: false,
      skipped: true,
      reason: 'ambiguous_clickup_phone',
      taskId,
      phone: normalizedPhone,
      matchCount: clickupMatches.length,
    }
  }

  const bradialMatches = findLeadMatchesByPhone(normalizedPhone)
  const leadLink = findLeadLink(taskId, normalizedPhone)
  const existingLead =
    bradialMatches.find((lead) => String(resolveBradialContactId(null, lead) || '') === String(leadLink?.bradialContactId || '')) ||
    bradialMatches.find((lead) => normalizePhone(lead.phone) === normalizedPhone) ||
    bradialMatches[0] ||
    null

  try {
    const result = await bradial.upsertOpportunityContact(task, existingLead, {
      dryRun,
      controlledStageLabels,
      preferredChatContactId: leadLink?.chatContactId || null,
      preferredConversationId: leadLink?.conversationId || null,
    })

    if (!dryRun) {
      const canonicalContactId =
        result.consolidatedChatContact?.contactId ||
        result.conversationSync?.chatContactId ||
        resolveBradialContactId(result.contact, existingLead)

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
      },
    )

    if (!dryRun && refreshAfter) {
      await refreshSnapshot(`${trigger}-${taskId}`)
    } else if (!dryRun && backgroundRefresh) {
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

function enqueuePendingSyncCandidates(trigger = 'reconcile-interval') {
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
      return ['missing_contact', 'missing_stage_label', 'stage_label_outdated'].includes(
        String(item.syncState || ''),
      )
    })
    .slice(0, SYNC_RECONCILE_BATCH_SIZE)

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
    'Access-Control-Allow-Origin': FRONTEND_ORIGIN,
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

app.post('/sync/reconcile', async () => {
  const queuedCount = enqueuePendingSyncCandidates('manual-reconcile')
  return {
    ok: true,
    queuedCount,
  }
})

app.get('/agents', async () => {
  if (!runtime.lastRefreshAt) await ensureSnapshot('agents-bootstrap')
  return runtime.cache.agents
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
  const result = await syncClickupTaskToBradial(taskId, {
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
    if (!['taskCreated', 'taskUpdated', 'taskStatusUpdated'].includes(event)) {
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

    enqueueSyncJob({
      taskId,
      dryRun: false,
      trigger: `clickup-webhook-${event}`,
      source: 'clickup-api-webhook',
      refreshBefore: false,
      refreshAfter: false,
      directFetchTask: true,
      backgroundRefresh: true,
      event,
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
    const supportedEvents = ['taskCreated', 'taskUpdated', 'taskStatusUpdated', 'automation_call_webhook']

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

    enqueueSyncJob({
      taskId,
      dryRun: false,
      trigger: `clickup-webhook-${integration.integrationId}-${event}`,
      source: 'clickup-integration-webhook',
      refreshBefore: false,
      refreshAfter: false,
      directFetchTask: true,
      backgroundRefresh: true,
      event,
      integrationId: integration.integrationId,
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
