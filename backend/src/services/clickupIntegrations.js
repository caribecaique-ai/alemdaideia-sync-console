import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'

function ensureStoreFile(storePath) {
  const directory = path.dirname(storePath)
  if (!fs.existsSync(directory)) {
    fs.mkdirSync(directory, { recursive: true })
  }

  if (!fs.existsSync(storePath)) {
    fs.writeFileSync(
      storePath,
      JSON.stringify({ version: 1, items: [] }, null, 2),
      'utf8',
    )
  }
}

function readStore(storePath) {
  ensureStoreFile(storePath)
  const raw = fs.readFileSync(storePath, 'utf8').replace(/^\uFEFF/, '')
  const parsed = JSON.parse(raw)

  return {
    version: 1,
    items: Array.isArray(parsed?.items) ? parsed.items : [],
  }
}

function writeStore(storePath, payload) {
  ensureStoreFile(storePath)
  fs.writeFileSync(storePath, JSON.stringify(payload, null, 2), 'utf8')
}

function buildWebhookUrl(publicBaseUrl, integrationId, webhookToken) {
  const base = String(publicBaseUrl || '').trim().replace(/\/$/, '')
  return `${base}/webhooks/clickup/${integrationId}/${webhookToken}`
}

export function createClickupIntegrationStore(config, pushLog) {
  const storePath = String(config.storePath || '').trim()

  if (!storePath) {
    throw new Error('Defina um caminho para armazenar as integracoes do ClickUp.')
  }

  function listIntegrations() {
    return readStore(storePath).items
  }

  function findIntegration(integrationId) {
    return listIntegrations().find((item) => item.integrationId === integrationId) || null
  }

  function findByWebhookPath(integrationId, webhookToken) {
    return (
      listIntegrations().find(
        (item) =>
          item.integrationId === integrationId &&
          item.webhookToken === webhookToken &&
          item.status === 'active',
      ) || null
    )
  }

  function createIntegration(input) {
    const store = readStore(storePath)
    const now = new Date().toISOString()
    const integrationId = `int_${crypto.randomBytes(6).toString('hex')}`
    const webhookToken = `whk_${crypto.randomBytes(12).toString('hex')}`
    const publicBaseUrl = String(input.publicBaseUrl || '').trim().replace(/\/$/, '')

    const record = {
      integrationId,
      provider: 'clickup',
      name: String(input.name || 'ClickUp Integration').trim(),
      publicBaseUrl,
      webhookToken,
      webhookUrl: buildWebhookUrl(publicBaseUrl, integrationId, webhookToken),
      workspaceId: input.workspaceId ? String(input.workspaceId).trim() : null,
      workspaceName: input.workspaceName ? String(input.workspaceName).trim() : null,
      lists: Array.isArray(input.lists) ? input.lists : [],
      clickupSecret: input.clickupSecret ? String(input.clickupSecret).trim() : null,
      authMode: input.clickupSecret ? 'token+signature' : 'token',
      status: 'active',
      createdAt: now,
      updatedAt: now,
      lastEventAt: null,
    }

    store.items.unshift(record)
    writeStore(storePath, store)
    pushLog('success', 'Integracao ClickUp criada', `${record.name} pronta para receber webhooks.`, {
      integrationId: record.integrationId,
      authMode: record.authMode,
    })

    return record
  }

  function updateIntegration(integrationId, changes = {}) {
    const store = readStore(storePath)
    const index = store.items.findIndex((item) => item.integrationId === integrationId)

    if (index === -1) return null

    const current = store.items[index]
    const updated = {
      ...current,
      ...changes,
      clickupSecret:
        changes.clickupSecret !== undefined
          ? String(changes.clickupSecret || '').trim() || null
          : current.clickupSecret,
      authMode:
        changes.clickupSecret !== undefined
          ? String(changes.clickupSecret || '').trim()
            ? 'token+signature'
            : 'token'
          : current.authMode,
      updatedAt: new Date().toISOString(),
    }

    if (changes.publicBaseUrl !== undefined) {
      updated.publicBaseUrl = String(changes.publicBaseUrl || '').trim().replace(/\/$/, '')
      updated.webhookUrl = buildWebhookUrl(updated.publicBaseUrl, updated.integrationId, updated.webhookToken)
    }

    store.items[index] = updated
    writeStore(storePath, store)
    return updated
  }

  function markIntegrationEvent(integrationId) {
    return updateIntegration(integrationId, {
      lastEventAt: new Date().toISOString(),
    })
  }

  return {
    listIntegrations,
    findIntegration,
    findByWebhookPath,
    createIntegration,
    updateIntegration,
    markIntegrationEvent,
    storePath,
  }
}
