import { normalizeText } from '../utils/normalizers.js'

const DEFAULT_CLICKUP_STAGE_LABEL_MAP = {
  oportunidade: 'oportunidade',
  'em qualificação': 'qualificacao',
  'reunião agendada': 'reuniao-agendada',
  followup: 'follow-up',
  'convidado para o evento': 'convidado-evento',
  confirmado: 'confirmado',
  compareceu: 'compareceu',
  'em negociação': 'em-negociacao',
  'negócio fechado': 'negocio-fechado',
  desqualificado: 'desqualificado',
  perdido: 'perdido',
}

export function normalizeLabelKey(value) {
  return normalizeText(value).replace(/[_\-\s]+/g, '')
}

function toBradialLabelFallback(value) {
  return normalizeText(value).replace(/\s+/g, '-')
}

function parseStageLabelMap(input) {
  if (!input) return {}
  if (typeof input === 'object' && !Array.isArray(input)) return input

  try {
    const parsed = JSON.parse(String(input))
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {}
  } catch {
    return {}
  }
}

export function buildStageLabelMap(overrides = null) {
  const merged = {
    ...DEFAULT_CLICKUP_STAGE_LABEL_MAP,
    ...parseStageLabelMap(overrides),
  }

  const map = new Map()

  for (const [status, label] of Object.entries(merged)) {
    const normalizedStatus = normalizeLabelKey(status)
    const normalizedLabel = String(label || '').trim()
    if (!normalizedStatus || !normalizedLabel) continue
    map.set(normalizedStatus, normalizedLabel)
  }

  return map
}

export function buildLabelStatusMap(overrides = null) {
  const merged = {
    ...DEFAULT_CLICKUP_STAGE_LABEL_MAP,
    ...parseStageLabelMap(overrides),
  }

  const map = new Map()

  for (const [status, label] of Object.entries(merged)) {
    const normalizedLabel = normalizeLabelKey(label)
    const normalizedStatus = String(status || '').trim()
    if (!normalizedLabel || !normalizedStatus) continue
    map.set(normalizedLabel, normalizedStatus)
  }

  return map
}

export function resolveBradialStageLabel(status, stageLabelMap = null) {
  const normalizedStatus = normalizeLabelKey(status)
  if (!normalizedStatus) return null

  const map = stageLabelMap instanceof Map ? stageLabelMap : buildStageLabelMap(stageLabelMap)
  return map.get(normalizedStatus) || toBradialLabelFallback(status)
}

export function resolveClickupStatusFromLabel(label, stageLabelMap = null) {
  const normalizedLabel = normalizeLabelKey(label)
  if (!normalizedLabel) return null

  const map =
    stageLabelMap instanceof Map
      ? buildLabelStatusMap(Object.fromEntries(stageLabelMap.entries()))
      : buildLabelStatusMap(stageLabelMap)

  return map.get(normalizedLabel) || null
}

export function listControlledStageLabels(tasksOrStatuses = [], stageLabelMap = null) {
  const labels = new Map()

  for (const item of tasksOrStatuses || []) {
    const status = typeof item === 'string' ? item : item?.status
    const label = resolveBradialStageLabel(status, stageLabelMap)
    if (!label) continue
    labels.set(normalizeLabelKey(label), label)
  }

  return [...labels.values()]
}

export function labelsIncludeEquivalent(labels = [], targetLabel) {
  const targetKey = normalizeLabelKey(targetLabel)
  if (!targetKey) return false

  return (labels || []).some((label) => normalizeLabelKey(label) === targetKey)
}

export function pickControlledLabels(labels = [], controlledStageLabels = []) {
  const controlledKeys = new Set(
    (controlledStageLabels || []).map((label) => normalizeLabelKey(label)).filter(Boolean),
  )

  return (labels || []).filter((label) => controlledKeys.has(normalizeLabelKey(label)))
}

export function stripControlledStageLabels(labels = [], controlledStageLabels = []) {
  const controlledKeys = new Set(
    (controlledStageLabels || []).map((label) => normalizeLabelKey(label)).filter(Boolean),
  )

  return (labels || []).filter((label) => !controlledKeys.has(normalizeLabelKey(label)))
}
