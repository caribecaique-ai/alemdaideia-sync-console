import { normalizeText, phonesMatchLoose } from '../utils/normalizers.js'

export function isActiveClickupTask(task) {
  const statusType = String(task?.statusType || '').toLowerCase()
  return statusType !== 'done' && statusType !== 'closed'
}

function sameNormalizedName(tasks = []) {
  const names = [...new Set(tasks.map((task) => normalizeText(task?.name)).filter(Boolean))]
  return names.length === 1
}

function sortMostRecent(tasks = []) {
  return [...tasks].sort((left, right) => {
    const updatedDelta = Number(right?.dateUpdated || 0) - Number(left?.dateUpdated || 0)
    if (updatedDelta !== 0) return updatedDelta
    return String(right?.id || '').localeCompare(String(left?.id || ''))
  })
}

export function resolveClickupPhoneConflict(tasks = [], phone = null) {
  const activeMatches = (tasks || []).filter(
    (task) => isActiveClickupTask(task) && (!phone || phonesMatchLoose(task?.phone, phone)),
  )

  if (activeMatches.length <= 1) {
    return {
      ambiguous: false,
      mode: activeMatches.length === 1 ? 'unique_active' : 'no_active_match',
      activeMatches,
      canonicalTask: activeMatches[0] || null,
      suppressedTaskIds: [],
    }
  }

  if (sameNormalizedName(activeMatches)) {
    const sorted = sortMostRecent(activeMatches)
    return {
      ambiguous: false,
      mode: 'same_name_latest',
      activeMatches,
      canonicalTask: sorted[0] || null,
      suppressedTaskIds: sorted.slice(1).map((task) => String(task.id)),
    }
  }

  return {
    ambiguous: true,
    mode: 'ambiguous_active_phone',
    activeMatches,
    canonicalTask: null,
    suppressedTaskIds: [],
  }
}
