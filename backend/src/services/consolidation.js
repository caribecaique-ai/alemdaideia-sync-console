import { normalizePhone } from '../utils/normalizers.js'

function buildBradialExceptions(leads, snapshotAt) {
  const groupedByPhone = new Map()
  const exceptions = []

  for (const lead of leads) {
    if (!groupedByPhone.has(lead.phone)) groupedByPhone.set(lead.phone, [])
    groupedByPhone.get(lead.phone).push(lead)

    if (lead.phone === 'sem telefone') {
      exceptions.push({
        id: `missing-phone-${lead.id}`,
        status: 'open',
        leadId: lead.id,
        kind: 'missing_phone',
        source: 'bradial-contacts',
        detectedAt: snapshotAt,
        phone: lead.phone,
        summary: 'Contato sem telefone canonico.',
      })
    }
  }

  for (const [phone, items] of groupedByPhone.entries()) {
    if (phone === 'sem telefone' || items.length <= 1) continue
    exceptions.push({
      id: `duplicate-bradial-phone-${phone}`,
      status: 'open',
      leadId: items[0].id,
      kind: 'ambiguous_phone',
      source: 'bradial-contacts',
      detectedAt: snapshotAt,
      phone,
      summary: `Telefone encontrado em ${items.length} contatos importados da Bradial.`,
    })
  }

  return exceptions
}

function buildClickupExceptions(taskIndex, snapshotAt) {
  const exceptions = []

  for (const [phone, tasks] of taskIndex.entries()) {
    if (!phone || tasks.length <= 1) continue
    exceptions.push({
      id: `duplicate-clickup-phone-${phone}`,
      status: 'open',
      leadId: null,
      kind: 'ambiguous_clickup_phone',
      source: 'clickup-workspace',
      detectedAt: snapshotAt,
      phone,
      summary: `Telefone encontrado em ${tasks.length} tasks do ClickUp.`,
    })
  }

  return exceptions
}

function statusRank(task) {
  const type = String(task?.statusType || '').toLowerCase()
  if (type === 'closed') return 3
  if (type === 'done') return 2
  if (type === 'custom') return 1
  return 0
}

function pickBestTask(tasks) {
  return [...tasks].sort((left, right) => {
    const rankDelta = statusRank(left) - statusRank(right)
    if (rankDelta !== 0) return rankDelta
    return Number(right.dateUpdated || 0) - Number(left.dateUpdated || 0)
  })[0]
}

function hasOpportunityLabel(lead, opportunityLabel) {
  const target = String(opportunityLabel || '').trim().toLowerCase()
  if (!target) return false

  return (lead?.bradialLabels || lead?.raw?.bradialLabels || []).some(
    (label) => String(label || '').trim().toLowerCase() === target,
  )
}

function enrichLead(lead, matchingTasks, clickupSnapshot) {
  const bestTask = matchingTasks.length ? pickBestTask(matchingTasks) : null
  const tags = new Set(Array.isArray(lead.tags) ? lead.tags : [])

  if (bestTask) tags.add('clickup_match')
  if (!bestTask && clickupSnapshot?.enabled) tags.add('clickup_pending')

  let summary = lead.summary
  if (bestTask) {
    summary = `${lead.summary} Match ClickUp: ${bestTask.name}.`
  }

  return {
    ...lead,
    clickupTaskId: bestTask?.id || null,
    clickupStage: bestTask?.status || null,
    clickupTaskUrl: bestTask?.url || null,
    clickupWorkspace: clickupSnapshot?.workspace?.name || null,
    clickupListName: bestTask?.listName || null,
    clickupPhoneMatched: bestTask?.phone || null,
    owner: bestTask?.owner || lead.owner,
    health: matchingTasks.length > 1 ? 'risk' : bestTask ? 'healthy' : lead.health,
    tags: [...tags],
    summary,
    lastAction: bestTask ? 'match clickup por telefone' : lead.lastAction,
    matchCount: matchingTasks.length,
    raw: {
      ...lead.raw,
      clickupTaskId: bestTask?.id || null,
    },
  }
}

function buildOverview({ leads, exceptions, agents, inboxes, clickupSnapshot, accountId, preferredInboxId, lastRefreshAt }) {
  return {
    leadCount: leads.length,
    syncEnabledCount: leads.filter((lead) => lead.syncEnabled).length,
    healthyCount: leads.filter((lead) => lead.health === 'healthy').length,
    warningCount: leads.filter((lead) => lead.health === 'warning').length,
    riskCount: leads.filter((lead) => lead.health === 'risk').length,
    openExceptions: exceptions.filter((item) => item.status !== 'resolved').length,
    agentsCount: agents.length,
    inboxesCount: inboxes.length,
    clickupTaskCount: Array.isArray(clickupSnapshot?.tasks) ? clickupSnapshot.tasks.length : 0,
    clickupMatchedCount: leads.filter((lead) => lead.clickupTaskId).length,
    clickupPendingContactCount: Array.isArray(clickupSnapshot?.pendingContacts)
      ? clickupSnapshot.pendingContacts.length
      : 0,
    accountId,
    preferredInboxId,
    clickupWorkspaceId: clickupSnapshot?.workspace?.id || null,
    clickupWorkspaceName: clickupSnapshot?.workspace?.name || null,
    lastRefreshAt,
  }
}

function buildClickupHealth(clickupSnapshot) {
  return {
    enabled: Boolean(clickupSnapshot?.enabled),
    status: clickupSnapshot?.error ? 'degraded' : clickupSnapshot?.enabled ? 'ok' : 'disabled',
    tokenSource: clickupSnapshot?.tokenSource || null,
    workspaceId: clickupSnapshot?.workspace?.id || null,
    workspaceName: clickupSnapshot?.workspace?.name || null,
    taskCount: Array.isArray(clickupSnapshot?.tasks) ? clickupSnapshot.tasks.length : 0,
    listCount: Array.isArray(clickupSnapshot?.navigation?.lists) ? clickupSnapshot.navigation.lists.length : 0,
    pendingContactCount: Array.isArray(clickupSnapshot?.pendingContacts)
      ? clickupSnapshot.pendingContacts.length
      : 0,
    lastError: clickupSnapshot?.error || null,
  }
}

function buildPendingClickupContacts(taskIndex, leads, clickupSnapshot, opportunityLabel) {
  const bradialIndex = new Map()

  for (const lead of leads) {
    const phone = normalizePhone(lead.phone)
    if (!phone) continue
    if (!bradialIndex.has(phone)) bradialIndex.set(phone, [])
    bradialIndex.get(phone).push(lead)
  }

  return (clickupSnapshot?.tasks || [])
    .filter((task) => {
      if (!normalizePhone(task.phone)) return false
      const statusType = String(task.statusType || '').toLowerCase()
      return statusType !== 'done' && statusType !== 'closed'
    })
    .map((task) => {
      const phone = normalizePhone(task.phone)
      const clickupMatches = phone ? taskIndex.get(phone) || [] : []
      const bradialMatches = phone ? bradialIndex.get(phone) || [] : []
      const matchedLead = bradialMatches[0] || null
      const alreadyTagged = matchedLead ? hasOpportunityLabel(matchedLead, opportunityLabel) : false

      let syncState = null
      let syncAllowed = false
      let summary = ''

      if (clickupMatches.length > 1) {
        syncState = 'ambiguous_clickup_phone'
        summary = `Telefone usado em ${clickupMatches.length} tasks do ClickUp.`
      } else if (bradialMatches.length > 1) {
        syncState = 'ambiguous_bradial_phone'
        summary = `Telefone usado em ${bradialMatches.length} contatos da Bradial.`
      } else if (!matchedLead) {
        syncState = 'missing_contact'
        syncAllowed = true
        summary = 'Task pronta para criar um novo contato no Bradial.'
      } else if (!alreadyTagged) {
        syncState = 'missing_opportunity_label'
        syncAllowed = true
        summary = `Contato encontrado no Bradial, mas ainda sem a label ${opportunityLabel}.`
      }

      if (!syncState) return null

      return {
        id: `pending-clickup-${task.id}`,
        taskId: task.id,
        taskName: task.name,
        phone,
        email: task.email || null,
        owner: task.owner || null,
        status: task.status,
        statusType: task.statusType,
        listName: task.listName,
        url: task.url || null,
        dateUpdated: task.dateUpdated || null,
        bradialLeadId: matchedLead?.id || null,
        bradialContactId: matchedLead?.chatContactId || null,
        bradialContactName: matchedLead?.name || null,
        bradialLabels: matchedLead?.bradialLabels || matchedLead?.raw?.bradialLabels || [],
        bradialMatchCount: bradialMatches.length,
        clickupMatchCount: clickupMatches.length,
        syncState,
        syncAllowed,
        summary,
      }
    })
    .filter(Boolean)
    .sort((left, right) => Number(right.dateUpdated || 0) - Number(left.dateUpdated || 0))
}

export function buildConsolidatedSnapshot({
  bradialSnapshot,
  clickupSnapshot,
  accountId,
  preferredInboxId,
  lastRefreshAt,
  opportunityLabel,
}) {
  const taskIndex = new Map()
  const snapshotAt = lastRefreshAt || new Date().toISOString()

  for (const task of clickupSnapshot?.tasks || []) {
    const phone = normalizePhone(task.phone)
    if (!phone) continue
    if (!taskIndex.has(phone)) taskIndex.set(phone, [])
    taskIndex.get(phone).push(task)
  }

  const leads = (bradialSnapshot?.leads || []).map((lead) => {
    const phone = normalizePhone(lead.phone)
    const matchingTasks = phone ? taskIndex.get(phone) || [] : []
    return enrichLead(lead, matchingTasks, clickupSnapshot)
  })

  const pendingContacts = buildPendingClickupContacts(
    taskIndex,
    leads,
    clickupSnapshot,
    opportunityLabel,
  )

  const exceptions = [
    ...buildBradialExceptions(bradialSnapshot?.leads || [], snapshotAt),
    ...buildClickupExceptions(taskIndex, snapshotAt),
  ]

  return {
    overview: buildOverview({
      leads,
      exceptions,
      agents: bradialSnapshot?.agents || [],
      inboxes: bradialSnapshot?.inboxes || [],
      clickupSnapshot: {
        ...clickupSnapshot,
        pendingContacts,
      },
      accountId,
      preferredInboxId,
      lastRefreshAt: snapshotAt,
    }),
    leads,
    exceptions,
    agents: bradialSnapshot?.agents || [],
    inboxes: bradialSnapshot?.inboxes || [],
    clickup: {
      health: buildClickupHealth({
        ...clickupSnapshot,
        pendingContacts,
      }),
      workspaces: clickupSnapshot?.workspaces || [],
      navigation: clickupSnapshot?.navigation || null,
      tasks: clickupSnapshot?.tasks || [],
      pendingContacts,
    },
  }
}
