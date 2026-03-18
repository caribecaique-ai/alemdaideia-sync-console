import { normalizeText } from '../utils/normalizers.js'
import { normalizeLabelKey, resolveBradialStageLabel } from './clickupStageLabels.js'

const DEFAULT_URGENCY_FIELD_NAMES = ['urgencia', 'nivel de urgencia', 'nivel urgencia']
const DEFAULT_CLOSED_STAGE_LABELS = ['negocio-fechado']
const DEFAULT_EVENT_CONFIRMED_FIELD_NAMES = [
  'confirmou invite para o evento',
  'confirmado',
  'evento confirmado',
  'convite confirmado',
  'convidado confirmado'
]

function toList(input, fallback = []) {
  if (Array.isArray(input)) {
    return input.map((item) => String(item || '').trim()).filter(Boolean)
  }

  const raw = String(input || '').trim()
  if (!raw) return [...fallback]

  return raw
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function normalizeUrgencyKey(value) {
  return normalizeText(value)
}

function extractPriorityLabel(priority) {
  if (!priority) return null
  if (typeof priority === 'string') return priority

  return (
    priority.priority ||
    priority.label ||
    priority.name ||
    priority.value ||
    priority.orderindex ||
    null
  )
}

function findUrgencyField(customFields = [], candidateFieldNames = []) {
  const normalizedCandidates = new Set(
    toList(candidateFieldNames, DEFAULT_URGENCY_FIELD_NAMES).map((item) => normalizeText(item)).filter(Boolean),
  )

  return (customFields || []).find((field) => {
    const fieldName = normalizeText(field?.name)
    if (!fieldName) return false
    if (normalizedCandidates.has(fieldName)) return true
    return [...normalizedCandidates].some((candidate) => fieldName.includes(candidate))
  }) || null
}

function findNamedCustomField(customFields = [], candidateFieldNames = []) {
  const normalizedCandidates = new Set(
    toList(candidateFieldNames, DEFAULT_EVENT_CONFIRMED_FIELD_NAMES)
      .map((item) => normalizeText(item))
      .filter(Boolean),
  )

  return (
    (customFields || []).find((field) => {
      const fieldName = normalizeText(field?.name)
      if (!fieldName) return false
      if (normalizedCandidates.has(fieldName)) return true
      return [...normalizedCandidates].some((candidate) => fieldName.includes(candidate))
    }) || null
  )
}

function parseCheckboxValue(value) {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0

  const normalized = normalizeText(value)
  if (!normalized) return false

  return ['true', '1', 'yes', 'sim', 'checked', 'marcado'].includes(normalized)
}

export function mapUrgencyToChatPriority(value) {
  const normalized = normalizeUrgencyKey(value)
  if (!normalized) return null

  if (['urgente', 'urgent', 'critica', 'critico', 'critical'].includes(normalized)) return 'urgent'
  if (['alta', 'alto', 'high'].includes(normalized)) return 'high'
  if (['media', 'medio', 'medium', 'normal'].includes(normalized)) return 'medium'
  if (['baixa', 'baixo', 'low'].includes(normalized)) return 'low'
  if (['nenhuma', 'none', 'sem prioridade'].includes(normalized)) return null

  return null
}

export function normalizeChatPriority(value) {
  const normalized = normalizeUrgencyKey(value)
  if (!normalized) return null

  if (['urgent', 'urgente'].includes(normalized)) return 'urgent'
  if (['high', 'alta', 'alto'].includes(normalized)) return 'high'
  if (['medium', 'media', 'medio', 'normal'].includes(normalized)) return 'medium'
  if (['low', 'baixa', 'baixo'].includes(normalized)) return 'low'
  if (['none', 'nenhuma', 'sem prioridade'].includes(normalized)) return null

  return null
}

export function mapChatPriorityToClickupPriorityValue(value) {
  const normalized = normalizeChatPriority(value)
  if (!normalized) return null

  if (normalized === 'urgent') return 1
  if (normalized === 'high') return 2
  if (normalized === 'medium') return 3
  if (normalized === 'low') return 4

  return null
}

export function resolveTaskUrgency(task = {}, options = {}) {
  const urgencyField = findUrgencyField(task.customFields || task.custom_fields || [], options.urgencyFieldNames)
  const customFieldValue = urgencyField?.value ?? null
  const nativePriorityValue = extractPriorityLabel(task.priority)
  const rawValue = customFieldValue ?? nativePriorityValue ?? null

  return {
    rawValue: rawValue == null ? null : String(rawValue).trim(),
    source: customFieldValue != null ? 'custom_field' : nativePriorityValue != null ? 'native_priority' : null,
    fieldName: urgencyField?.name || null,
    chatPriority: mapUrgencyToChatPriority(rawValue),
  }
}

export function resolveTaskEventConfirmation(task = {}, options = {}) {
  const confirmationField = findNamedCustomField(
    task.customFields || task.custom_fields || [],
    options.confirmationFieldNames,
  )

  return {
    checked: parseCheckboxValue(confirmationField?.value),
    fieldName: confirmationField?.name || null,
    source: confirmationField ? 'custom_field' : null,
  }
}

function extractActorUser(payload = {}) {
  const historyItem = Array.isArray(payload.history_items) ? payload.history_items[0] || null : null
  const user = historyItem?.user || payload?.user || payload?.triggered_by || null
  if (!user || typeof user !== 'object') return null

  const email = String(user.email || '').trim() || null
  const username = String(user.username || user.name || '').trim() || null

  return {
    id: user.id == null ? null : String(user.id),
    username,
    email,
    name: username || (email ? email.split('@')[0] : null),
  }
}

export function isClosedOpportunityTask(task = {}, options = {}) {
  const statusType = normalizeText(task.statusType)
  if (statusType === 'done' || statusType === 'closed') return true

  const stageLabel = resolveBradialStageLabel(task.status, options.stageLabelMap)
  if (!stageLabel) return false

  const closedStageKeys = new Set(
    toList(options.closedStageLabels, DEFAULT_CLOSED_STAGE_LABELS)
      .map((item) => normalizeLabelKey(item))
      .filter(Boolean),
  )

  return closedStageKeys.has(normalizeLabelKey(stageLabel))
}

export function extractClickupWebhookContext(payload = {}, options = {}) {
  const event = String(payload?.event || '').trim()
  const historyItem = Array.isArray(payload?.history_items) ? payload.history_items[0] || null : null
  const field = normalizeText(historyItem?.field || '')
  const actor = extractActorUser(payload)
  const customFieldName =
    String(
      historyItem?.custom_field?.name ||
        historyItem?.field_data?.name ||
        historyItem?.field_name ||
        '',
    ).trim() || null

  const urgencyFieldNames = toList(options.urgencyFieldNames, DEFAULT_URGENCY_FIELD_NAMES)
  const normalizedUrgencyFieldNames = new Set(urgencyFieldNames.map((item) => normalizeText(item)).filter(Boolean))
  const normalizedCustomFieldName = normalizeText(customFieldName)

  return {
    event,
    actor,
    field,
    customFieldName,
    isStatusEvent: event === 'taskStatusUpdated',
    isAssigneeEvent:
      event === 'taskAssigneeUpdated' ||
      field === 'assignees' ||
      field === 'assignee' ||
      field === 'owner',
    isPriorityEvent:
      event === 'taskPriorityUpdated' ||
      field === 'priority' ||
      (['custom_field', 'custom field'].includes(field) &&
        normalizedUrgencyFieldNames.has(normalizedCustomFieldName)),
  }
}
