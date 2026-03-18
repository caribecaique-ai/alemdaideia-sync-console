import { normalizeText } from '../utils/normalizers.js'

const DEFAULT_CLICKUP_STAGE_LABEL_MAP = {
  oportunidade: 'oportunidade',
  'em qualificacao': 'qualificacao',
  'em-qualificacao': 'qualificacao',
  'reuniao agendada': 'reuniao-agendada',
  'reuniao-agendada': 'reuniao-agendada',
  followup: 'follow-up',
  'follow-up': 'follow-up',
  'convidado para o evento': 'convidado-evento',
  'convidado-para-o-evento': 'convidado-evento',
  'convidado-evento': 'convidado-evento',
  confirmado: 'confirmado',
  compareceu: 'compareceu',
  'em negociacao': 'em-negociacao',
  'em-negociacao': 'em-negociacao',
  'negocio fechado': 'negocio-fechado',
  'negocio-fechado': 'negocio-fechado',
  desqualificado: 'desqualificado',
  perdido: 'perdido',
}

const LEGACY_CLICKUP_LABEL_STATUS_MAP = {
  'em qualificacao': 'em qualifica\u00e7\u00e3o',
  'em-qualificacao': 'em qualifica\u00e7\u00e3o',
  qualificacao: 'em qualifica\u00e7\u00e3o',
  'reuniao agendada': 'reuni\u00e3o agendada',
  'reuniao-agendada': 'reuni\u00e3o agendada',
  'follow-up': 'followup',
  'convidado para o evento': 'convidado para o evento',
  'convidado-para-o-evento': 'convidado para o evento',
  'convidado-evento': 'convidado para o evento',
  'em negociacao': 'em negocia\u00e7\u00e3o',
  'em-negociacao': 'em negocia\u00e7\u00e3o',
  'negocio fechado': 'negocio fechado',
  'negocio-fechado': 'negocio fechado',
}

const LEGACY_CONTROLLED_STAGE_LABELS = [
  'em qualificacao',
  'em-qualificacao',
  'reuniao agendada',
  'convidado para o evento',
  'convidado-para-o-evento',
  'em negociacao',
  'negocio fechado',
]

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

  for (const [label, status] of Object.entries(LEGACY_CLICKUP_LABEL_STATUS_MAP)) {
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

export function resolveBradialStageLabels(taskOrStatus, stageLabelMap = null, options = {}) {
  const task =
    taskOrStatus && typeof taskOrStatus === 'object' && !Array.isArray(taskOrStatus)
      ? taskOrStatus
      : null
  const status = task ? task.status : taskOrStatus
  const confirmedLabel = String(options.confirmedLabel || 'confirmado').trim() || 'confirmado'
  const labels = []
  const primaryLabel = resolveBradialStageLabel(status, stageLabelMap)

  if (primaryLabel) {
    labels.push(primaryLabel)
  }

  if (task?.eventInviteConfirmed) {
    labels.push(confirmedLabel)
  }

  return [...new Map(labels.map((label) => [normalizeLabelKey(label), label])).values()]
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
  const map = stageLabelMap instanceof Map ? stageLabelMap : buildStageLabelMap(stageLabelMap)
  const labels = new Map()

  for (const label of map.values()) {
    const normalizedLabel = normalizeLabelKey(label)
    if (!normalizedLabel) continue
    labels.set(normalizedLabel, label)
  }

  for (const label of [...Object.keys(LEGACY_CLICKUP_LABEL_STATUS_MAP), ...LEGACY_CONTROLLED_STAGE_LABELS]) {
    const normalizedLabel = normalizeLabelKey(label)
    if (!normalizedLabel) continue
    labels.set(normalizedLabel, label)
  }

  for (const item of tasksOrStatuses || []) {
    const status = typeof item === 'string' ? item : item?.status
    const normalizedStatus = normalizeLabelKey(status)
    const label = map.get(normalizedStatus)
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
