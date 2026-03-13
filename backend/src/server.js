import crypto from 'node:crypto'
import path from 'node:path'
import Fastify from 'fastify'
import cors from '@fastify/cors'
import dotenv from 'dotenv'
import { createBradialAdapter } from './adapters/bradial.js'
import { createClickupAdapter } from './adapters/clickup.js'
import { createClickupIntegrationStore } from './services/clickupIntegrations.js'
import { buildConsolidatedSnapshot } from './services/consolidation.js'
import { normalizePhone } from './utils/normalizers.js'

dotenv.config()

const PORT = Number(process.env.PORT || 3015)
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || 'http://localhost:4180'
const BRADIAL_REFRESH_MS = Math.max(15_000, Number(process.env.BRADIAL_REFRESH_MS || 60_000))
const BRADIAL_OPPORTUNITY_LABEL =
  String(process.env.BRADIAL_OPPORTUNITY_LABEL || 'OPORTUNIDADE').trim() || 'OPORTUNIDADE'
const CLICKUP_WEBHOOK_SECRET = String(process.env.CLICKUP_WEBHOOK_SECRET || '').trim()
const PUBLIC_BASE_URL = String(process.env.PUBLIC_BASE_URL || '').trim()
const CLICKUP_INTEGRATIONS_PATH =
  String(process.env.CLICKUP_INTEGRATIONS_PATH || '').trim() ||
  path.resolve(process.cwd(), 'runtime-data', 'clickup-integrations.json')

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
}
let inflightRefresh = null

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

const bradial = createBradialAdapter(
  {
    baseUrl: process.env.BRADIAL_BASE_URL,
    apiToken: process.env.BRADIAL_API_TOKEN,
    maxPages: process.env.BRADIAL_MAX_PAGES,
    opportunityLabel: BRADIAL_OPPORTUNITY_LABEL,
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

  runtime.lastRefreshAt = snapshotAt
  runtime.cache = buildConsolidatedSnapshot({
    bradialSnapshot: bradialResult.value,
    clickupSnapshot,
    accountId: process.env.BRADIAL_ACCOUNT_ID || null,
    preferredInboxId: process.env.BRADIAL_INBOX_ID || null,
    lastRefreshAt: snapshotAt,
    opportunityLabel: BRADIAL_OPPORTUNITY_LABEL,
  })

  pushLog(
    'success',
    'Snapshot consolidado',
    `${runtime.cache.leads.length} leads Bradial e ${runtime.cache.clickup.tasks.length} tasks ClickUp processados`,
    { trigger },
  )

  return runtime.cache
}

function ensureSnapshot(trigger = 'manual') {
  if (inflightRefresh) return inflightRefresh
  inflightRefresh = refreshSnapshot(trigger).finally(() => {
    inflightRefresh = null
  })
  return inflightRefresh
}

function findTaskById(taskId) {
  return runtime.cache.clickup.tasks.find((task) => String(task.id) === String(taskId)) || null
}

function findLeadMatchesByPhone(phone) {
  const normalizedPhone = normalizePhone(phone)
  if (!normalizedPhone) return []

  return runtime.cache.leads.filter((lead) => normalizePhone(lead.phone) === normalizedPhone)
}

function parseBooleanFlag(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase())
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

  if (refreshBefore) {
    await refreshSnapshot(`${trigger}-preload`)
  } else if (!runtime.lastRefreshAt) {
    await ensureSnapshot(`${trigger}-bootstrap`)
  }

  const task = findTaskById(taskId)
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
    (item) => normalizePhone(item.phone) === normalizedPhone,
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
  if (bradialMatches.length > 1) {
    return {
      ok: false,
      skipped: true,
      reason: 'ambiguous_bradial_phone',
      taskId,
      phone: normalizedPhone,
      matchCount: bradialMatches.length,
    }
  }

  const existingLead = bradialMatches[0] || null

  try {
    const result = await bradial.upsertOpportunityContact(task, existingLead, { dryRun })

    pushLog(
      result.operation === 'create' || result.operation === 'update' || result.operation === 'noop'
        ? 'success'
        : 'info',
      dryRun ? 'Dry-run ClickUp -> Bradial' : 'Sync ClickUp -> Bradial',
      `${task.name} processada com operacao ${result.operation}.`,
      {
        taskId,
        phone: normalizedPhone,
        dryRun,
        bradialContactId: resolveBradialContactId(result.contact, existingLead),
      },
    )

    if (!dryRun) {
      await refreshSnapshot(`${trigger}-${taskId}`)
    }

    return {
      ok: true,
      skipped: false,
      dryRun,
      taskId,
      phone: normalizedPhone,
      operation: result.operation,
      opportunityLabel: BRADIAL_OPPORTUNITY_LABEL,
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
      cachedLeads: runtime.cache.leads.length,
      lastError: runtime.errors.bradial,
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

    syncClickupTaskToBradial(taskId, {
      dryRun: false,
      trigger: `clickup-webhook-${event}`,
      refreshBefore: true,
    })
      .then((result) => {
        if (result.skipped) {
          pushLog('info', 'Webhook ClickUp ignorado', `Task ${taskId} ignorada: ${result.reason}.`, {
            event,
            taskId,
            reason: result.reason,
          })
        }
      })
      .catch((error) => {
        pushLog('error', 'Falha no webhook ClickUp', error.message, {
          event,
          taskId,
        })
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

    syncClickupTaskToBradial(taskId, {
      dryRun: false,
      trigger: `clickup-webhook-${integration.integrationId}-${event}`,
      refreshBefore: true,
    })
      .then((result) => {
        if (result.skipped) {
          pushLog('info', 'Webhook ClickUp ignorado', `Task ${taskId} ignorada: ${result.reason}.`, {
            event,
            taskId,
            reason: result.reason,
            integrationId: integration.integrationId,
          })
        }
      })
      .catch((error) => {
        pushLog('error', 'Falha no webhook ClickUp', error.message, {
          event,
          taskId,
          integrationId: integration.integrationId,
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
  ensureSnapshot('interval').catch(() => {})
}, BRADIAL_REFRESH_MS).unref()

await app.listen({ host: '0.0.0.0', port: PORT })
pushLog('success', 'Backend iniciado', `Servidor ouvindo na porta ${PORT}`)
ensureSnapshot('startup').catch((error) => {
  pushLog('error', 'Falha no bootstrap', error.message)
})
