import fs from 'node:fs'
import { normalizePhone, normalizeText, phoneFieldLooksRelevant } from '../utils/normalizers.js'

function normalizeScopeName(value) {
  return normalizeText(value).replace(/^\d+\s*[\.\-\)]\s*/, '')
}

function resolveFieldValue(field) {
  if (!field) return null
  if (field.type === 'drop_down') {
    const options = Array.isArray(field.type_config?.options) ? field.type_config.options : []
    const selected = options.find((option) => String(option.orderindex) === String(field.value))
    return selected?.name || field.value || null
  }
  return field.value ?? null
}

function normalizeTaskStatusType(statusType) {
  const normalized = normalizeText(statusType)
  if (normalized === 'closed') return 3
  if (normalized === 'done') return 2
  if (normalized === 'custom') return 1
  return 0
}

export function createClickupAdapter(config, pushLog) {
  const apiBaseUrl = String(config.apiBaseUrl || 'https://api.clickup.com/api/v2').replace(/\/$/, '')
  const explicitApiKey = String(config.apiKey || '').trim()
  const workspaceName = normalizeText(config.workspaceName || 'alem da ideia')
  const workspaceId = String(config.workspaceId || '').trim()
  const commercialSpaceName = normalizeScopeName(config.commercialSpaceName || 'comercial')
  const commercialFolderName = normalizeScopeName(config.commercialFolderName || 'area de vendas')
  const commercialListName = normalizeScopeName(config.commercialListName || '')
  const maxListPages = Math.max(1, Number(config.maxListPages || 4))
  const backupClientName = normalizeText(config.backupClientName || 'Stev')
  const backupPath = String(config.clientsBackupPath || '').trim()
  let resolvedContext = null
  let resolvedNavigation = null

  function readBackupCandidates() {
    if (!backupPath || !fs.existsSync(backupPath)) return []

    try {
      const raw = fs.readFileSync(backupPath, 'utf8').replace(/^\uFEFF/, '')
      const parsed = JSON.parse(raw)
      const clients = Array.isArray(parsed) ? parsed : []
      return clients
        .filter((client) => normalizeText(client?.name) === backupClientName && client?.clickupToken)
        .map((client) => ({
          label: `backup:${client.name}`,
          token: String(client.clickupToken || '').trim(),
        }))
    } catch (error) {
      pushLog('warning', 'Backup ClickUp ignorado', error.message, { backupPath })
      return []
    }
  }

  function buildTokenCandidates() {
    const candidates = []

    if (explicitApiKey) {
      candidates.push({
        label: 'env:CLICKUP_API_KEY',
        token: explicitApiKey,
      })
    }

    candidates.push(...readBackupCandidates())
    return candidates.filter((candidate) => candidate.token)
  }

  async function request(resourcePath, token, params = {}, init = {}) {
    const url = new URL(`${apiBaseUrl}${resourcePath}`)
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        url.searchParams.set(key, String(value))
      }
    })

    const method = String(init.method || 'GET').toUpperCase()
    const hasBody = init.body !== undefined
    const response = await fetch(url, {
      method,
      headers: {
        Authorization: token,
        'Content-Type': 'application/json',
        ...(init.headers || {}),
      },
      body: hasBody ? JSON.stringify(init.body) : undefined,
    })

    if (!response.ok) {
      const body = await response.text()
      throw new Error(`ClickUp HTTP ${response.status}: ${body.slice(0, 300)}`)
    }

    return response.json()
  }

  function matchWorkspace(teams) {
    if (workspaceId) {
      return teams.find((team) => String(team.id) === workspaceId) || null
    }

    return (
      teams.find((team) => normalizeText(team.name) === workspaceName) ||
      teams.find((team) => normalizeText(team.name).includes(workspaceName)) ||
      null
    )
  }

  function matchByScopeName(items, targetName) {
    if (!targetName) return null

    return (
      items.find((item) => normalizeScopeName(item.name) === targetName) ||
      items.find((item) => normalizeScopeName(item.name).includes(targetName)) ||
      null
    )
  }

  async function resolveWorkspaceContext(force = false) {
    if (resolvedContext && !force) return resolvedContext

    const candidates = buildTokenCandidates()
    if (!candidates.length) {
      throw new Error('Nenhum token ClickUp disponivel para este projeto.')
    }

    let lastError = null

    for (const candidate of candidates) {
      try {
        const payload = await request('/team', candidate.token)
        const teams = Array.isArray(payload?.teams) ? payload.teams : []
        const workspace = matchWorkspace(teams)
        if (!workspace) {
          throw new Error(`Workspace alvo nao encontrado entre: ${teams.map((team) => team.name).join(', ')}`)
        }

        resolvedContext = {
          token: candidate.token,
          tokenSource: candidate.label,
          workspace: {
            id: String(workspace.id),
            name: String(workspace.name || '').trim(),
          },
          visibleWorkspaces: teams.map((team) => ({
            id: String(team.id),
            name: String(team.name || '').trim(),
          })),
        }

        pushLog(
          'success',
          'ClickUp autenticado',
          `Workspace ${resolvedContext.workspace.name} resolvido via ${candidate.label}`,
          { workspaceId: resolvedContext.workspace.id },
        )
        return resolvedContext
      } catch (error) {
        lastError = error
        pushLog(
          'warning',
          'Tentativa de auth ClickUp falhou',
          `${candidate.label} nao conseguiu resolver o workspace alvo`,
        )
      }
    }

    throw lastError || new Error('Falha ao resolver o workspace do ClickUp.')
  }

  async function fetchCommercialNavigation(context) {
    const spacesPayload = await request(`/team/${context.workspace.id}/space`, context.token, {
      archived: false,
    })
    const spaces = Array.isArray(spacesPayload?.spaces) ? spacesPayload.spaces : []
    const targetSpace = matchByScopeName(spaces, commercialSpaceName)
    if (!targetSpace) {
      throw new Error(`Space comercial nao encontrado no workspace ${context.workspace.name}.`)
    }

    const foldersPayload = await request(`/space/${targetSpace.id}/folder`, context.token, {
      archived: false,
    })
    const folders = Array.isArray(foldersPayload?.folders) ? foldersPayload.folders : []
    const targetFolder = matchByScopeName(folders, commercialFolderName)
    if (!targetFolder) {
      throw new Error(`Pasta comercial nao encontrada no space ${targetSpace.name}.`)
    }

    const listsPayload = await request(`/folder/${targetFolder.id}/list`, context.token, {
      archived: false,
    })
    const lists = Array.isArray(listsPayload?.lists) ? listsPayload.lists : []
    const selectedLists = commercialListName
      ? lists.filter((list) => normalizeScopeName(list.name) === commercialListName)
      : lists

    if (!selectedLists.length) {
      throw new Error(`Nenhuma lista comercial encontrada na pasta ${targetFolder.name}.`)
    }

    return {
      space: {
        id: String(targetSpace.id),
        name: String(targetSpace.name || '').trim(),
      },
      folder: {
        id: String(targetFolder.id),
        name: String(targetFolder.name || '').trim(),
      },
      lists: selectedLists.map((list) => ({
        id: String(list.id),
        name: String(list.name || '').trim(),
        taskCount: Number(list.task_count || 0),
      })),
    }
  }

  async function resolveCommercialNavigation(context, force = false) {
    if (resolvedNavigation && !force) return resolvedNavigation
    resolvedNavigation = await fetchCommercialNavigation(context)
    return resolvedNavigation
  }

  function mapTask(task, navigationList = null, navigation = null) {
    const customFields = Array.isArray(task.custom_fields)
      ? task.custom_fields.map((field) => ({
          id: String(field.id),
          name: String(field.name || '').trim(),
          type: field.type,
          value: resolveFieldValue(field),
        }))
      : []

    const phoneField = customFields.find((field) => phoneFieldLooksRelevant(field.name))
    const emailField = customFields.find((field) => normalizeText(field.name) === 'e-mail')
    const assignees = Array.isArray(task.assignees)
      ? task.assignees.map((assignee) => String(assignee.username || assignee.email || '').trim()).filter(Boolean)
      : []

    return {
      id: String(task.id),
      name: String(task.name || '').trim(),
      phone: normalizePhone(phoneField?.value),
      email: emailField?.value ? String(emailField.value).trim() : null,
      status: String(task.status?.status || 'sem status').trim(),
      statusType: String(task.status?.type || 'unknown').trim(),
      owner: assignees[0] || null,
      assignees,
      listId: String(navigationList?.id || task.list?.id || task.list_id || ''),
      listName: String(navigationList?.name || task.list?.name || '').trim(),
      folderId: String(navigation?.folder?.id || task.folder?.id || task.folder_id || ''),
      folderName: String(navigation?.folder?.name || task.folder?.name || '').trim(),
      spaceId: String(navigation?.space?.id || task.space?.id || task.space_id || ''),
      spaceName: String(navigation?.space?.name || task.space?.name || '').trim(),
      tags: Array.isArray(task.tags)
        ? task.tags.map((tag) => String(tag.name || '').trim()).filter(Boolean)
        : [],
      customFields,
      url: String(task.url || '').trim() || null,
      dateUpdated: task.date_updated || task.date_updated_local || null,
    }
  }

  async function fetchTasksForList(context, navigationList, navigation) {
    const rows = []

    for (let page = 0; page < maxListPages; page += 1) {
      const payload = await request(`/list/${navigationList.id}/task`, context.token, {
        page,
        include_closed: true,
        subtasks: true,
      })
      const tasks = Array.isArray(payload?.tasks) ? payload.tasks : []
      if (!tasks.length) break
      rows.push(...tasks)
      if (tasks.length < 100) break
    }

    return rows.map((task) => mapTask(task, navigationList, navigation))
  }

  async function fetchTaskById(taskId, trigger = 'manual-task') {
    const normalizedTaskId = String(taskId || '').trim()
    if (!normalizedTaskId) return null

    const context = await resolveWorkspaceContext()
    const navigation = await resolveCommercialNavigation(context)
    const task = await request(`/task/${normalizedTaskId}`, context.token)
    const navigationList =
      navigation.lists.find((list) => String(list.id) === String(task?.list?.id || task?.list_id || '')) || null

    if (!navigationList) {
      pushLog(
        'info',
        'Task ClickUp fora do escopo',
        `Task ${normalizedTaskId} ignorada porque nao pertence as listas monitoradas.`,
        { trigger },
      )
      return null
    }

    return mapTask(task, navigationList, navigation)
  }

  async function updateTaskStatus(taskId, status, trigger = 'manual-status-update') {
    const normalizedTaskId = String(taskId || '').trim()
    const normalizedStatus = String(status || '').trim()
    if (!normalizedTaskId || !normalizedStatus) {
      throw new Error('Task e status do ClickUp sao obrigatorios para atualizar a etapa.')
    }

    const context = await resolveWorkspaceContext()
    await resolveCommercialNavigation(context)

    await request(
      `/task/${normalizedTaskId}`,
      context.token,
      {},
      {
        method: 'PUT',
        body: {
          status: normalizedStatus,
        },
      },
    )

    pushLog(
      'success',
      'Status ClickUp atualizado',
      `Task ${normalizedTaskId} movida para ${normalizedStatus}.`,
      {
        trigger,
        taskId: normalizedTaskId,
        status: normalizedStatus,
      },
    )

    return fetchTaskById(normalizedTaskId, trigger)
  }

  async function fetchSnapshot(trigger = 'manual') {
    const snapshotAt = new Date().toISOString()
    const context = await resolveWorkspaceContext()
    const navigation = await resolveCommercialNavigation(context)
    const taskGroups = await Promise.all(
      navigation.lists.map((list) => fetchTasksForList(context, list, navigation)),
    )
    const tasks = taskGroups.flat().sort((left, right) => {
      const statusDelta = normalizeTaskStatusType(left.statusType) - normalizeTaskStatusType(right.statusType)
      if (statusDelta !== 0) return statusDelta
      return Number(right.dateUpdated || 0) - Number(left.dateUpdated || 0)
    })

    pushLog(
      'success',
      'ClickUp carregado',
      `${tasks.length} tasks importadas do workspace ${context.workspace.name}`,
      {
        trigger,
        workspaceId: context.workspace.id,
        lists: navigation.lists.length,
      },
    )

    return {
      enabled: true,
      snapshotAt,
      tokenSource: context.tokenSource,
      workspace: context.workspace,
      workspaces: context.visibleWorkspaces,
      navigation,
      tasks,
    }
  }

  return {
    fetchSnapshot,
    fetchTaskById,
    updateTaskStatus,
  }
}
