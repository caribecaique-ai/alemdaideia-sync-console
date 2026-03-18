import { normalizePhone, normalizePhoneLooseKey } from '../utils/normalizers.js'
import { isActiveClickupTask, resolveClickupPhoneConflict } from './clickupTaskResolution.js'
import {
  labelsIncludeEquivalent,
  pickControlledLabels,
  resolveBradialStageLabel,
  resolveBradialStageLabels,
} from './clickupStageLabels.js'

function buildBradialExceptions(leads, snapshotAt) {
  const groupedByPhone = new Map()
  const exceptions = []

  for (const lead of leads) {
    const phoneKey = normalizeLeadPhone(lead.phone) || lead.phone
    if (!groupedByPhone.has(phoneKey)) groupedByPhone.set(phoneKey, [])
    groupedByPhone.get(phoneKey).push(lead)

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
    const resolution = resolveClickupPhoneConflict(tasks, phone)
    if (!phone || !resolution.ambiguous) continue
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

function resolvePreferredClickupTask(matchingTasks = []) {
  if (!Array.isArray(matchingTasks) || !matchingTasks.length) {
    return {
      task: null,
      ambiguous: false,
      activeMatchCount: 0,
      suppressedTaskIds: [],
    }
  }

  const activeTasks = matchingTasks.filter((task) => isActiveClickupTask(task))
  if (!activeTasks.length) {
    return {
      task: pickBestTask(matchingTasks) || null,
      ambiguous: false,
      activeMatchCount: 0,
      suppressedTaskIds: [],
    }
  }

  const normalizedPhone = normalizePhone(activeTasks[0]?.phone)
  const resolution = resolveClickupPhoneConflict(activeTasks, normalizedPhone)

  if (resolution.ambiguous) {
    return {
      task: null,
      ambiguous: true,
      activeMatchCount: activeTasks.length,
      suppressedTaskIds: [],
    }
  }

  return {
    task: resolution.canonicalTask || pickBestTask(activeTasks) || null,
    ambiguous: false,
    activeMatchCount: activeTasks.length,
    suppressedTaskIds: resolution.suppressedTaskIds || [],
  }
}

function hasStageLabels(lead, targetLabels = []) {
  const effectiveTargetLabels = (targetLabels || []).filter(Boolean)
  if (!effectiveTargetLabels.length) return false

  return effectiveTargetLabels.every((targetLabel) =>
    labelsIncludeEquivalent(lead?.bradialLabels || lead?.raw?.bradialLabels || [], targetLabel),
  )
}

function normalizeLeadPhone(value) {
  return normalizePhoneLooseKey(value) || normalizePhone(value) || null
}

function toTimestamp(value) {
  if (!value) return 0
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function healthRank(value) {
  if (value === 'risk') return 3
  if (value === 'warning') return 2
  if (value === 'healthy') return 1
  return 0
}

function getLeadKeys(lead) {
  const keys = new Set()
  const phone = normalizeLeadPhone(lead.phone)
  const bradialContactId =
    String(lead?.raw?.bradialContactId || lead?.chatContactId || '').trim() || null
  const chatConversationId =
    String(lead?.chatConversationId || lead?.conversationId || '').trim() || null
  const clickupTaskId = String(lead?.clickupTaskId || '').trim() || null

  if (clickupTaskId) keys.add(`task:${clickupTaskId}`)
  if (phone) keys.add(`phone:${phone}`)
  if (bradialContactId) keys.add(`contact:${bradialContactId}`)
  if (chatConversationId) keys.add(`conversation:${chatConversationId}`)
  if (!keys.size) keys.add(`lead:${lead.id}`)

  return [...keys]
}

function scoreLead(lead) {
  return (
    (lead?.clickupTaskId ? 200 : 0) +
    (lead?.chatConversationId || lead?.conversationId ? 80 : 0) +
    (lead?.raw?.bradialContactId || lead?.chatContactId ? 60 : 0) +
    ((lead?.bradialLabels || lead?.raw?.bradialLabels || []).length ? 30 : 0) +
    (lead?.clickupStage ? 20 : 0) +
    (lead?.syncEnabled ? 10 : 0) +
    toTimestamp(lead?.lastSyncAt)
  )
}

function choosePreferredValue(leads, selector, fallback = null) {
  for (const lead of leads) {
    const value = selector(lead)
    if (value === undefined || value === null) continue
    if (typeof value === 'string' && !value.trim()) continue
    if (Array.isArray(value) && !value.length) continue
    return value
  }

  return fallback
}

function chooseCanonicalLeadId(leads, preferredLead) {
  const clickupTaskId = choosePreferredValue(leads, (lead) => lead.clickupTaskId)
  if (clickupTaskId) return `lead-task-${clickupTaskId}`

  const phone = normalizeLeadPhone(choosePreferredValue(leads, (lead) => lead.phone))
  if (phone) return `lead-phone-${phone.replace(/[^\d]+/g, '')}`

  const bradialContactId = choosePreferredValue(
    leads,
    (lead) => lead?.raw?.bradialContactId || lead?.chatContactId,
  )
  if (bradialContactId) return `lead-contact-${bradialContactId}`

  return preferredLead.id
}

function mergeLeadGroup(group) {
  const members = Array.isArray(group?.leads) ? group.leads : Array.isArray(group) ? group : []
  const sorted = [...members].sort((left, right) => scoreLead(right) - scoreLead(left))
  const preferredLead = sorted[0]
  const canonicalId = chooseCanonicalLeadId(sorted, preferredLead)
  const primaryLabels =
    choosePreferredValue(sorted, (lead) => lead.bradialLabels) ||
    choosePreferredValue(sorted, (lead) => lead?.raw?.bradialLabels, [])

  return {
    ...preferredLead,
    id: canonicalId,
    name: choosePreferredValue(sorted, (lead) => lead.name, preferredLead.name),
    phone: choosePreferredValue(sorted, (lead) => lead.phone, preferredLead.phone),
    email: choosePreferredValue(sorted, (lead) => lead.email, preferredLead.email),
    clickupTaskId: choosePreferredValue(sorted, (lead) => lead.clickupTaskId),
    clickupStage: choosePreferredValue(sorted, (lead) => lead.clickupStage),
    clickupPriority: choosePreferredValue(sorted, (lead) => lead.clickupPriority),
    clickupUrgency: choosePreferredValue(sorted, (lead) => lead.clickupUrgency),
    clickupTaskUrl: choosePreferredValue(sorted, (lead) => lead.clickupTaskUrl),
    clickupWorkspace: choosePreferredValue(sorted, (lead) => lead.clickupWorkspace),
    clickupListName: choosePreferredValue(sorted, (lead) => lead.clickupListName),
    clickupPhoneMatched: choosePreferredValue(sorted, (lead) => lead.clickupPhoneMatched),
    conversationId: choosePreferredValue(sorted, (lead) => lead.conversationId),
    chatConversationId: choosePreferredValue(sorted, (lead) => lead.chatConversationId),
    chatContactId: choosePreferredValue(sorted, (lead) => lead.chatContactId),
    owner: choosePreferredValue(sorted, (lead) => lead.owner, preferredLead.owner),
    chatStatus: choosePreferredValue(sorted, (lead) => lead.chatStatus, preferredLead.chatStatus),
    syncEnabled: sorted.some((lead) => lead.syncEnabled !== false),
    health: [...sorted].sort((left, right) => healthRank(right.health) - healthRank(left.health))[0]?.health || preferredLead.health,
    bradialLabels: primaryLabels,
    tags: [...new Set(choosePreferredValue(sorted, (lead) => lead.tags, preferredLead.tags || []))],
    summary: choosePreferredValue(sorted, (lead) => lead.summary, preferredLead.summary),
    lastAction: choosePreferredValue(sorted, (lead) => lead.lastAction, preferredLead.lastAction),
    lastSyncAt: choosePreferredValue(sorted, (lead) => lead.lastSyncAt, preferredLead.lastSyncAt),
    matchCount: Math.max(...sorted.map((lead) => Number(lead.matchCount || 0)), 0),
    raw: {
      ...(preferredLead.raw || {}),
      bradialContactId: choosePreferredValue(
        sorted,
        (lead) => lead?.raw?.bradialContactId || lead?.chatContactId,
      ),
      bradialLabels: primaryLabels,
      bradialEmail: choosePreferredValue(sorted, (lead) => lead?.raw?.bradialEmail || lead?.email),
      clickupTaskId: choosePreferredValue(sorted, (lead) => lead?.raw?.clickupTaskId || lead?.clickupTaskId),
      mergedLeadIds: members.map((lead) => lead.id),
    },
  }
}

function consolidateLeads(leads) {
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

  return groups.map((group) => mergeLeadGroup(group))
}

function enrichLead(lead, matchingTasks, clickupSnapshot) {
  const preferredTask = resolvePreferredClickupTask(matchingTasks)
  const bestTask = preferredTask.task
  const tags = new Set(Array.isArray(lead.tags) ? lead.tags : [])

  if (bestTask) tags.add('clickup_match')
  if (!bestTask && clickupSnapshot?.enabled) tags.add('clickup_pending')
  if (preferredTask.ambiguous) tags.add('clickup_ambiguous_phone')
  if ((preferredTask.suppressedTaskIds || []).length) tags.add('clickup_duplicate_suppressed')

  let summary = lead.summary
  if (bestTask) {
    summary = `${lead.summary} Match ClickUp: ${bestTask.name}.`
  } else if (preferredTask.ambiguous) {
    summary = `${lead.summary} Telefone ambiguo no ClickUp; lead mantido sem task canonica vinculada.`
  }

  return {
    ...lead,
    clickupTaskId: bestTask?.id || null,
    clickupStage: bestTask?.status || null,
    clickupPriority: bestTask?.priority || null,
    clickupUrgency: bestTask?.urgency || null,
    clickupTaskUrl: bestTask?.url || null,
    clickupWorkspace: clickupSnapshot?.workspace?.name || null,
    clickupListName: bestTask?.listName || null,
    clickupPhoneMatched: bestTask?.phone || null,
    owner: bestTask?.owner || lead.owner,
    health: preferredTask.ambiguous || matchingTasks.length > 1 ? 'risk' : bestTask ? 'healthy' : lead.health,
    tags: [...tags],
    summary,
    lastAction: bestTask ? 'match clickup por telefone' : lead.lastAction,
    matchCount: matchingTasks.length,
    raw: {
      ...lead.raw,
      clickupTaskId: bestTask?.id || null,
      suppressedClickupTaskIds: preferredTask.suppressedTaskIds || [],
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

function buildPendingClickupContacts(
  taskIndex,
  leads,
  clickupSnapshot,
  stageLabelMap,
  controlledStageLabels,
  rawLeads = leads,
  options = {},
) {
  const conversationLabelsOnly = options.conversationLabelsOnly === true
  const autoCreateConversations = options.autoCreateConversations === true
  const bradialIndex = new Map()
  const rawBradialIndex = new Map()

  for (const lead of leads) {
    const phone = normalizePhone(lead.phone)
    if (!phone) continue
    if (!bradialIndex.has(phone)) bradialIndex.set(phone, [])
    bradialIndex.get(phone).push(lead)
  }

  for (const lead of rawLeads) {
    const phone = normalizePhone(lead.phone)
    if (!phone) continue
    if (!rawBradialIndex.has(phone)) rawBradialIndex.set(phone, [])
    rawBradialIndex.get(phone).push(lead)
  }

  return (clickupSnapshot?.tasks || [])
    .filter((task) => normalizePhone(task.phone) && isActiveClickupTask(task))
    .map((task) => {
      const phone = normalizePhone(task.phone)
      const clickupMatches = phone ? taskIndex.get(phone) || [] : []
      const clickupResolution = resolveClickupPhoneConflict(clickupMatches, phone)
      const activeClickupMatches = clickupResolution.activeMatches || []
      const canonicalTaskId = String(clickupResolution.canonicalTask?.id || '')

      if (
        !clickupResolution.ambiguous &&
        activeClickupMatches.length > 1 &&
        canonicalTaskId &&
        canonicalTaskId !== String(task.id)
      ) {
        return null
      }

      const bradialMatches = phone ? rawBradialIndex.get(phone) || [] : []
      const matchedLead = phone ? (bradialIndex.get(phone) || [])[0] || null : null
      const matchedConversationId =
        String(matchedLead?.conversationId || matchedLead?.chatConversationId || '').trim() || null
      const targetStageLabel = resolveBradialStageLabel(task.status, stageLabelMap)
      const targetStageLabels = resolveBradialStageLabels(task, stageLabelMap)
      const alreadyTagged = matchedLead ? hasStageLabels(matchedLead, targetStageLabels) : false
      const currentControlledLabels = matchedLead
        ? pickControlledLabels(
            matchedLead?.bradialLabels || matchedLead?.raw?.bradialLabels || [],
            controlledStageLabels,
          )
        : []

      let syncState = null
      let syncAllowed = false
      let summary = ''

      if (clickupResolution.ambiguous) {
        syncState = 'ambiguous_clickup_phone'
        summary = `Telefone usado em ${activeClickupMatches.length} tasks ativas do ClickUp.`
      } else if (bradialMatches.length > 1) {
        syncState = 'ambiguous_bradial_phone'
        summary = `Telefone usado em ${bradialMatches.length} contatos da Bradial.`
      } else if (!matchedLead) {
        syncState = 'missing_contact'
        syncAllowed = true
        summary = `Task pronta para criar um novo contato no Bradial com a tag ${targetStageLabels.join(', ') || targetStageLabel || 'de etapa'}.`
      } else if (conversationLabelsOnly && !matchedConversationId) {
        syncState = 'conversation_required_for_stage_label'
        syncAllowed = autoCreateConversations
        summary = autoCreateConversations
          ? 'Contato encontrado no Bradial sem conversa vinculada; o sistema pode iniciar uma conversa vazia automaticamente para aplicar a etiqueta e o responsavel.'
          : 'Contato encontrado no Bradial, mas ainda sem conversa vinculada para sincronizar a etiqueta de etapa.'
      } else if (!alreadyTagged) {
        syncState = currentControlledLabels.length ? 'stage_label_outdated' : 'missing_stage_label'
        syncAllowed = true
        summary = currentControlledLabels.length
          ? `Contato encontrado no Bradial com tag de etapa ${currentControlledLabels.join(', ')}, mas a etapa atual do ClickUp exige ${targetStageLabels.join(', ') || targetStageLabel}.`
          : `Contato encontrado no Bradial, mas ainda sem a tag de etapa ${targetStageLabels.join(', ') || targetStageLabel}.`
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
        priority: task.priority || null,
        urgency: task.urgency || null,
        listName: task.listName,
        url: task.url || null,
        dateUpdated: task.dateUpdated || null,
        bradialLeadId: matchedLead?.id || null,
        bradialContactId: matchedLead?.chatContactId || null,
        bradialConversationId: matchedConversationId,
        bradialContactName: matchedLead?.name || null,
        bradialLabels: matchedLead?.bradialLabels || matchedLead?.raw?.bradialLabels || [],
        currentControlledLabels,
        targetStageLabel,
        targetStageLabels,
        bradialMatchCount: bradialMatches.length,
        clickupMatchCount: activeClickupMatches.length,
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
  stageLabelMap,
  controlledStageLabels,
  conversationLabelsOnly = false,
  autoCreateConversations = false,
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
  const consolidatedLeads = consolidateLeads(leads)

  const pendingContacts = buildPendingClickupContacts(
    taskIndex,
    consolidatedLeads,
    clickupSnapshot,
    stageLabelMap,
    controlledStageLabels,
    leads,
    { conversationLabelsOnly, autoCreateConversations },
  )

  const exceptions = [
    ...buildBradialExceptions(bradialSnapshot?.leads || [], snapshotAt),
    ...buildClickupExceptions(taskIndex, snapshotAt),
  ]

  return {
    overview: buildOverview({
      leads: consolidatedLeads,
      exceptions,
      agents: bradialSnapshot?.agents || [],
      chatAgents: bradialSnapshot?.chatAgents || [],
      inboxes: bradialSnapshot?.inboxes || [],
      clickupSnapshot: {
        ...clickupSnapshot,
        pendingContacts,
      },
      accountId,
      preferredInboxId,
      lastRefreshAt: snapshotAt,
    }),
    leads: consolidatedLeads,
    exceptions,
    agents: bradialSnapshot?.agents || [],
    chatAgents: bradialSnapshot?.chatAgents || [],
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
