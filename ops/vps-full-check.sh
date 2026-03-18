#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_URL="${BASE_URL:-http://127.0.0.1:3015}"
TOKEN="${TOKEN:-}"
AUDIT_LIMIT="${AUDIT_LIMIT:-400}"

if [[ -z "${TOKEN}" ]]; then
  echo "Defina TOKEN com ADMIN_API_TOKEN antes de executar."
  echo "Exemplo: export TOKEN='seu_token'"
  exit 1
fi

node --input-type=module - "${BASE_URL}" "${TOKEN}" "${ROOT_DIR}" "${AUDIT_LIMIT}" <<'NODE'
import path from 'node:path'
import { pathToFileURL } from 'node:url'

const [baseUrl, token, rootDir, auditLimitRaw] = process.argv.slice(2)
const auditLimit = Math.max(50, Number(auditLimitRaw || 400))

const labelsModuleUrl = pathToFileURL(
  path.join(rootDir, 'backend', 'src', 'services', 'clickupStageLabels.js'),
).href
const clickupResolutionModuleUrl = pathToFileURL(
  path.join(rootDir, 'backend', 'src', 'services', 'clickupTaskResolution.js'),
).href
const normalizersModuleUrl = pathToFileURL(
  path.join(rootDir, 'backend', 'src', 'utils', 'normalizers.js'),
).href
const {
  resolveBradialStageLabel,
  labelsIncludeEquivalent,
  pickControlledLabels,
} = await import(labelsModuleUrl)
const { resolveClickupPhoneConflict, isActiveClickupTask } = await import(clickupResolutionModuleUrl)
const { normalizePhone } = await import(normalizersModuleUrl)

async function api(endpoint, { auth = true } = {}) {
  try {
    const response = await fetch(`${baseUrl}${endpoint}`, {
      headers: auth
        ? {
            Authorization: `Bearer ${token}`,
          }
        : {},
    })

    let payload = null
    try {
      payload = await response.json()
    } catch {
      payload = null
    }

    return {
      ok: response.ok,
      status: response.status,
      data: payload,
      error: response.ok ? null : payload?.error || `HTTP ${response.status}`,
    }
  } catch (error) {
    return {
      ok: false,
      status: 0,
      data: null,
      error: error.message,
    }
  }
}

function countBy(items = [], selector) {
  const counts = {}
  for (const item of items) {
    const key = selector(item)
    counts[key] = (counts[key] || 0) + 1
  }
  return counts
}

function normalizeLeadName(value) {
  return String(value || '').trim() || 'contato nao identificado'
}

function toTimestamp(value) {
  const parsed = Date.parse(value || 0)
  return Number.isFinite(parsed) ? parsed : 0
}

const endpointResults = Object.fromEntries(
  await Promise.all([
    ['healthz', api('/healthz', { auth: false })],
    ['health', api('/health')],
    ['syncHealth', api('/sync/health')],
    ['pending', api('/clickup/pending-contacts')],
    ['leads', api('/leads')],
    ['exceptions', api('/exceptions')],
    ['logs', api('/logs')],
    ['audit', api(`/sync/audit?limit=${auditLimit}`)],
    ['integrations', api('/clickup/webhook-integrations')],
    ['chatAgents', api('/chat/agents')],
    ['clickupTasks', api('/clickup/tasks')],
  ].map(async ([key, promise]) => [key, await promise]),
  ),
)

const criticalFindings = []
const warningFindings = []

if (!endpointResults.healthz.ok) {
  criticalFindings.push(`/healthz falhou: ${endpointResults.healthz.error}`)
}
if (!endpointResults.health.ok) {
  criticalFindings.push(`/health falhou: ${endpointResults.health.error}`)
}

const health = endpointResults.health.data || {}
const bradialHealth = health.bradial || {}
const syncHealth = endpointResults.syncHealth.data || {}
const controlledStageLabels = Array.isArray(bradialHealth.controlledStageLabels)
  ? bradialHealth.controlledStageLabels
  : []

if (health.status && health.status !== 'ok') {
  criticalFindings.push(`health.status=${health.status}`)
}
if (bradialHealth.contactLabelSyncEnabled) {
  warningFindings.push('sync de etiqueta de contato esta habilitado')
}
if (bradialHealth.stageLabelScope !== 'conversation') {
  warningFindings.push(`stageLabelScope atual: ${bradialHealth.stageLabelScope || 'desconhecido'}`)
}

const pendingItems = Array.isArray(endpointResults.pending.data) ? endpointResults.pending.data : []
const pendingByState = countBy(pendingItems, (item) => String(item?.syncState || 'unknown'))
const actionablePending = pendingItems.filter((item) =>
  item?.syncAllowed &&
  ['missing_contact', 'missing_stage_label', 'stage_label_outdated'].includes(String(item?.syncState || '')),
)
const blockedNoConversation = pendingItems.filter(
  (item) => String(item?.syncState || '') === 'conversation_required_for_stage_label',
)

const leads = Array.isArray(endpointResults.leads.data) ? endpointResults.leads.data : []
const clickupTasks = Array.isArray(endpointResults.clickupTasks.data) ? endpointResults.clickupTasks.data : []
const clickupTasksByPhone = new Map()
for (const task of clickupTasks) {
  const phone = normalizePhone(task?.phone)
  if (!phone || !isActiveClickupTask(task)) continue
  if (!clickupTasksByPhone.has(phone)) clickupTasksByPhone.set(phone, [])
  clickupTasksByPhone.get(phone).push(task)
}

const suppressedDuplicateTaskIds = new Set()
const canonicalTaskIdsBySuppressedTaskId = new Map()
for (const [phone, tasks] of clickupTasksByPhone.entries()) {
  const resolution = resolveClickupPhoneConflict(tasks, phone)
  for (const suppressedTaskId of resolution.suppressedTaskIds || []) {
    suppressedDuplicateTaskIds.add(String(suppressedTaskId))
    if (resolution.canonicalTask?.id) {
      canonicalTaskIdsBySuppressedTaskId.set(String(suppressedTaskId), String(resolution.canonicalTask.id))
    }
  }
}

const monitoredLeads = leads.filter((lead) => String(lead?.clickupTaskId || '').trim())
const leadsWithConversation = monitoredLeads.filter(
  (lead) => String(lead?.conversationId || lead?.chatConversationId || '').trim(),
)
const leadsWithoutConversation = monitoredLeads.filter(
  (lead) => !String(lead?.conversationId || lead?.chatConversationId || '').trim(),
)

const conversationMismatches = []
const duplicateSuppressedMismatches = []
const leadsWithMultipleControlledLabels = []

for (const lead of leadsWithConversation) {
  const expectedLabel = resolveBradialStageLabel(lead?.clickupStage)
  const labels = pickControlledLabels(
    lead?.bradialLabels || lead?.raw?.bradialLabels || [],
    controlledStageLabels,
  )

  if (labels.length > 1) {
    leadsWithMultipleControlledLabels.push({
      taskId: lead?.clickupTaskId || null,
      name: normalizeLeadName(lead?.name),
      labels,
      conversationId: lead?.conversationId || lead?.chatConversationId || null,
    })
  }

  if (!expectedLabel) continue
  if (!labelsIncludeEquivalent(labels, expectedLabel)) {
    const mismatch = {
      taskId: lead?.clickupTaskId || null,
      name: normalizeLeadName(lead?.name),
      clickupStage: lead?.clickupStage || null,
      expectedLabel,
      actualLabels: labels,
      conversationId: lead?.conversationId || lead?.chatConversationId || null,
      canonicalTaskId: canonicalTaskIdsBySuppressedTaskId.get(String(lead?.clickupTaskId || '')) || null,
    }

    if (suppressedDuplicateTaskIds.has(String(lead?.clickupTaskId || ''))) {
      duplicateSuppressedMismatches.push(mismatch)
    } else {
      conversationMismatches.push(mismatch)
    }
  }
}

if (conversationMismatches.length) {
  criticalFindings.push(`${conversationMismatches.length} lead(s) com conversa e etiqueta divergente`)
}
if (leadsWithMultipleControlledLabels.length) {
  warningFindings.push(
    `${leadsWithMultipleControlledLabels.length} lead(s) com mais de uma etiqueta controlada`,
  )
}
if (duplicateSuppressedMismatches.length) {
  warningFindings.push(
    `${duplicateSuppressedMismatches.length} divergencia(s) pertencem a tasks duplicadas suprimidas no ClickUp`,
  )
}

const exceptions = Array.isArray(endpointResults.exceptions.data) ? endpointResults.exceptions.data : []
const openExceptions = exceptions.filter((item) => item?.status !== 'resolved')
const exceptionsByKind = countBy(openExceptions, (item) => String(item?.kind || 'unknown'))

const auditItems = Array.isArray(endpointResults.audit.data) ? endpointResults.audit.data : []
const attemptItems = auditItems.filter((item) =>
  ['sync_succeeded', 'sync_skipped', 'sync_failed', 'sync_failed_retrying'].includes(item?.type),
)
const auditStats = {
  attempts: attemptItems.length,
  succeeded: attemptItems.filter((item) => item.type === 'sync_succeeded').length,
  skipped: attemptItems.filter((item) => item.type === 'sync_skipped').length,
  failed: attemptItems.filter((item) => item.type === 'sync_failed').length,
  retrying: attemptItems.filter((item) => item.type === 'sync_failed_retrying').length,
  duplicateSkips: attemptItems.filter(
    (item) => String(item?.result?.reason || '') === 'suppressed_clickup_duplicate',
  ).length,
}
const latestAuditAt = auditItems.reduce((latest, item) => {
  const current = toTimestamp(item?.createdAt)
  return current > latest ? current : latest
}, 0)
const noisyCutoff = latestAuditAt > 0 ? latestAuditAt - 5 * 60 * 1000 : 0

if (auditStats.failed > 0) {
  criticalFindings.push(`${auditStats.failed} falha(s) de sync no recorte recente`)
}
if (auditStats.retrying > 0) {
  warningFindings.push(`${auditStats.retrying} sync(s) em retry no recorte recente`)
}

const noopStageReconcile = auditItems.filter((item) => {
  return (
    toTimestamp(item?.createdAt) >= noisyCutoff &&
    item?.type === 'sync_succeeded' &&
    item?.source === 'stage-reconcile' &&
    item?.result?.operation === 'noop' &&
    item?.result?.conversationSync?.conversationLabelOperation !== 'update' &&
    item?.result?.metadataSync?.operation !== 'update'
  )
})

const noisyStageReconcileTasks = Object.values(
  noopStageReconcile.reduce((acc, item) => {
    const taskId = String(item?.taskId || '').trim()
    if (!taskId) return acc
    if (!acc[taskId]) {
      acc[taskId] = {
        taskId,
        name: normalizeLeadName(item?.result?.bradialContactName || item?.result?.payload?.name),
        count: 0,
        lastCreatedAt: item?.createdAt || null,
      }
    }
    acc[taskId].count += 1
    if (toTimestamp(item?.createdAt) > toTimestamp(acc[taskId].lastCreatedAt)) {
      acc[taskId].lastCreatedAt = item.createdAt
    }
    return acc
  }, {}),
)
  .filter((item) => item.count >= 3)
  .sort((left, right) => right.count - left.count)
  .slice(0, 10)

if (noisyStageReconcileTasks.length) {
  warningFindings.push(
    `${noisyStageReconcileTasks.length} task(s) com repeticao suspeita de stage-reconcile noop`,
  )
}

const logs = Array.isArray(endpointResults.logs.data) ? endpointResults.logs.data : []
const logStats = {
  total: logs.length,
  error: logs.filter((item) => item?.level === 'error').length,
  warning: logs.filter((item) => item?.level === 'warning').length,
}
if (logStats.error > 0) {
  warningFindings.push(`${logStats.error} log(s) de erro ainda presentes na janela recente`)
}

const integrations = Array.isArray(endpointResults.integrations.data?.items)
  ? endpointResults.integrations.data.items
  : Array.isArray(endpointResults.integrations.data)
    ? endpointResults.integrations.data
    : []
const activeIntegrations = integrations.filter((item) => item?.status === 'active')
if (!activeIntegrations.length) {
  warningFindings.push('nenhuma integracao webhook ativa do ClickUp')
}

const chatAgents = Array.isArray(endpointResults.chatAgents.data) ? endpointResults.chatAgents.data : []
if (!chatAgents.length) {
  warningFindings.push('nenhum agente de chat carregado no snapshot')
}

const summary = {
  checkedAt: new Date().toISOString(),
  baseUrl,
  health: {
    healthzOk: endpointResults.healthz.ok,
    healthOk: endpointResults.health.ok,
    status: health.status || null,
    lastRefreshAt: health.lastRefreshAt || null,
    stageLabelScope: bradialHealth.stageLabelScope || null,
    contactLabelSyncEnabled: Boolean(bradialHealth.contactLabelSyncEnabled),
    conversationAssignmentSyncEnabled: Boolean(bradialHealth.conversationAssignmentSyncEnabled),
  },
  sync: {
    queueSize: syncHealth.queueSize ?? health.sync?.queueSize ?? null,
    activeCount: syncHealth.activeCount ?? health.sync?.activeCount ?? null,
    deferredCount: syncHealth.deferredCount ?? health.sync?.deferredCount ?? null,
    retryingCount: syncHealth.retryingCount ?? health.sync?.retryingCount ?? null,
    metrics: syncHealth.metrics || health.sync?.metrics || null,
  },
  clickup: {
    taskCount: health.clickup?.taskCount ?? null,
    pendingContactCount: health.clickup?.pendingContactCount ?? null,
    webhookIntegrationsActive: activeIntegrations.length,
  },
  leads: {
    total: monitoredLeads.length,
    withConversation: leadsWithConversation.length,
    withoutConversation: leadsWithoutConversation.length,
    conversationMismatches: conversationMismatches.length,
    duplicateSuppressedMismatches: duplicateSuppressedMismatches.length,
    multipleControlledLabels: leadsWithMultipleControlledLabels.length,
  },
  pending: {
    total: pendingItems.length,
    actionable: actionablePending.length,
    blockedNoConversation: blockedNoConversation.length,
    bySyncState: pendingByState,
  },
  exceptions: {
    open: openExceptions.length,
    byKind: exceptionsByKind,
  },
  audit: {
    ...auditStats,
    noisyStageReconcileTasks,
  },
  logs: logStats,
  findings: {
    critical: criticalFindings,
    warning: warningFindings,
  },
  samples: {
    conversationMismatches: conversationMismatches.slice(0, 10),
    duplicateSuppressedMismatches: duplicateSuppressedMismatches.slice(0, 10),
    multipleControlledLabels: leadsWithMultipleControlledLabels.slice(0, 10),
    actionablePending: actionablePending.slice(0, 10).map((item) => ({
      taskId: item?.taskId || null,
      taskName: item?.taskName || null,
      syncState: item?.syncState || null,
      targetStageLabel: item?.targetStageLabel || null,
      summary: item?.summary || null,
    })),
  },
}

console.log('== Saude ==')
console.log(JSON.stringify(summary.health, null, 2))
console.log('')

console.log('== Fila e Sync ==')
console.log(JSON.stringify(summary.sync, null, 2))
console.log('')

console.log('== Pendencias ClickUp ==')
console.log(JSON.stringify(summary.pending, null, 2))
console.log('')

console.log('== Leads ==')
console.log(JSON.stringify(summary.leads, null, 2))
console.log('')

console.log('== Excecoes ==')
console.log(JSON.stringify(summary.exceptions, null, 2))
console.log('')

console.log('== Auditoria ==')
console.log(JSON.stringify(summary.audit, null, 2))
console.log('')

console.log('== Findings ==')
console.log(JSON.stringify(summary.findings, null, 2))
console.log('')

console.log('== Samples ==')
console.log(JSON.stringify(summary.samples, null, 2))
console.log('')

console.log('== Resumo JSON ==')
console.log(JSON.stringify(summary, null, 2))

if (criticalFindings.length > 0) {
  process.exitCode = 1
}
NODE
