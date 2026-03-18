import crypto from 'node:crypto'
import { readJsonStoreSync, writeJsonStoreAtomicSync } from '../utils/jsonStore.js'

function createEmptyStore() {
  return { version: 1, items: [] }
}

function normalizeStore(parsed) {
  return {
    version: 1,
    items: Array.isArray(parsed?.items) ? parsed.items : [],
  }
}

function serializeIntegration(record, options = {}) {
  const includeSecrets = options.includeSecrets === true
  if (!record || typeof record !== 'object') return null

  const serialized = {
    integrationId: record.integrationId,
    provider: record.provider,
    name: record.name,
    publicBaseUrl: record.publicBaseUrl,
    webhookUrl: record.webhookUrl,
    workspaceId: record.workspaceId,
    workspaceName: record.workspaceName,
    lists: Array.isArray(record.lists) ? record.lists : [],
    authMode: record.authMode,
    status: record.status,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
    lastEventAt: record.lastEventAt,
    clickupSecretConfigured: Boolean(record.clickupSecret),
  }

  if (includeSecrets) {
    serialized.webhookToken = record.webhookToken
    serialized.clickupSecret = record.clickupSecret
  }

  return serialized
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

  function readStore() {
    return normalizeStore(
      readJsonStoreSync(storePath, createEmptyStore, {
        onCorrupt(error, corruptPath) {
          pushLog(
            'warning',
            'Store de integracoes do ClickUp restaurada',
            corruptPath
              ? `Arquivo corrompido arquivado em ${corruptPath}.`
              : `Falha ao ler ${storePath}; store recriada.`,
            { storePath, corruptPath, error: error.message },
          )
        },
      }),
    )
  }

  function writeStore(payload) {
    writeJsonStoreAtomicSync(storePath, normalizeStore(payload))
  }

  function listRawIntegrations() {
    return readStore().items
  }

  function listIntegrations(options = {}) {
    return listRawIntegrations().map((item) => serializeIntegration(item, options))
  }

  function findIntegration(integrationId, options = {}) {
    const item = listRawIntegrations().find((record) => record.integrationId === integrationId) || null
    return item ? serializeIntegration(item, options) : null
  }

  function findByWebhookPath(integrationId, webhookToken) {
    return (
      listRawIntegrations().find(
        (item) =>
          item.integrationId === integrationId &&
          item.webhookToken === webhookToken &&
          item.status === 'active',
      ) || null
    )
  }

  function createIntegration(input) {
    const store = readStore()
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
    writeStore(store)
    pushLog('success', 'Integracao ClickUp criada', `${record.name} pronta para receber webhooks.`, {
      integrationId: record.integrationId,
      authMode: record.authMode,
    })

    return serializeIntegration(record)
  }

  function updateIntegration(integrationId, changes = {}) {
    const store = readStore()
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
    writeStore(store)
    return serializeIntegration(updated)
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
