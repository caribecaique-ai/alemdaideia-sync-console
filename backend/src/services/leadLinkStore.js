import fs from 'node:fs'
import path from 'node:path'

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

export function createLeadLinkStore(config, pushLog = () => {}) {
  const storePath = String(config.storePath || '').trim()

  if (!storePath) {
    throw new Error('Defina um caminho para armazenar os vinculos de lead.')
  }

  function listLinks() {
    return readStore(storePath).items
  }

  function findLink({ taskId = null, phone = null } = {}) {
    const normalizedTaskId = taskId ? String(taskId).trim() : null
    const normalizedPhone = phone ? String(phone).trim() : null

    return (
      listLinks().find(
        (item) =>
          (normalizedTaskId && String(item.taskId || '').trim() === normalizedTaskId) ||
          (normalizedPhone && String(item.phone || '').trim() === normalizedPhone),
      ) || null
    )
  }

  function findLinkByIdentity({
    taskId = null,
    phone = null,
    bradialContactId = null,
    chatContactId = null,
    conversationId = null,
  } = {}) {
    const normalizedTaskId = taskId ? String(taskId).trim() : null
    const normalizedPhone = phone ? String(phone).trim() : null
    const normalizedBradialContactId = bradialContactId ? String(bradialContactId).trim() : null
    const normalizedChatContactId = chatContactId ? String(chatContactId).trim() : null
    const normalizedConversationId = conversationId ? String(conversationId).trim() : null

    return (
      listLinks().find((item) => {
        const itemTaskId = String(item.taskId || '').trim()
        const itemPhone = String(item.phone || '').trim()
        const itemBradialContactId = String(item.bradialContactId || '').trim()
        const itemChatContactId = String(item.chatContactId || '').trim()
        const itemConversationId = String(item.conversationId || '').trim()

        return (
          (normalizedTaskId && itemTaskId === normalizedTaskId) ||
          (normalizedPhone && itemPhone === normalizedPhone) ||
          (normalizedBradialContactId && itemBradialContactId === normalizedBradialContactId) ||
          (normalizedChatContactId && itemChatContactId === normalizedChatContactId) ||
          (normalizedConversationId && itemConversationId === normalizedConversationId)
        )
      }) || null
    )
  }

  function upsertLink(input = {}) {
    const normalizedTaskId = String(input.taskId || '').trim() || null
    const normalizedPhone = String(input.phone || '').trim() || null

    if (!normalizedTaskId && !normalizedPhone) {
      return null
    }

    const store = readStore(storePath)
    const index = store.items.findIndex(
      (item) =>
        (normalizedTaskId && String(item.taskId || '').trim() === normalizedTaskId) ||
        (normalizedPhone && String(item.phone || '').trim() === normalizedPhone),
    )

    const current = index >= 0 ? store.items[index] : {}
    const record = {
      ...current,
      taskId: normalizedTaskId || current.taskId || null,
      phone: normalizedPhone || current.phone || null,
      bradialContactId:
        String(input.bradialContactId || current.bradialContactId || '').trim() || null,
      chatContactId: String(input.chatContactId || current.chatContactId || '').trim() || null,
      conversationId:
        String(input.conversationId || current.conversationId || '').trim() || null,
      updatedAt: new Date().toISOString(),
      createdAt: current.createdAt || new Date().toISOString(),
    }

    if (index >= 0) {
      store.items[index] = record
    } else {
      store.items.unshift(record)
    }

    writeStore(storePath, store)

    pushLog(
      'info',
      'Vinculo de lead atualizado',
      `Lead ${record.taskId || record.phone} vinculado a contato ${record.bradialContactId || 'n/a'} e conversa ${record.conversationId || 'n/a'}.`,
      {
        taskId: record.taskId,
        phone: record.phone,
        bradialContactId: record.bradialContactId,
        chatContactId: record.chatContactId,
        conversationId: record.conversationId,
      },
    )

    return record
  }

  function rebindContactIds({ fromIds = [], toId = null } = {}) {
    const normalizedFromIds = [...new Set((fromIds || []).map((item) => String(item || '').trim()).filter(Boolean))]
    const normalizedToId = String(toId || '').trim() || null

    if (!normalizedFromIds.length || !normalizedToId) {
      return 0
    }

    const store = readStore(storePath)
    let updatedCount = 0

    store.items = store.items.map((item) => {
      const currentBradialContactId = String(item.bradialContactId || '').trim()
      const currentChatContactId = String(item.chatContactId || '').trim()
      const shouldUpdateBradial = normalizedFromIds.includes(currentBradialContactId)
      const shouldUpdateChat = normalizedFromIds.includes(currentChatContactId)

      if (!shouldUpdateBradial && !shouldUpdateChat) {
        return item
      }

      updatedCount += 1
      return {
        ...item,
        bradialContactId: shouldUpdateBradial ? normalizedToId : item.bradialContactId,
        chatContactId: shouldUpdateChat ? normalizedToId : item.chatContactId,
        updatedAt: new Date().toISOString(),
      }
    })

    if (updatedCount > 0) {
      writeStore(storePath, store)
      pushLog(
        'info',
        'Vinculos de lead reamarrados',
        `${updatedCount} vinculo(s) atualizaram contato(s) mesclado(s) para ${normalizedToId}.`,
        {
          fromIds: normalizedFromIds,
          toId: normalizedToId,
        },
      )
    }

    return updatedCount
  }

  return {
    storePath,
    listLinks,
    findLink,
    findLinkByIdentity,
    upsertLink,
    rebindContactIds,
  }
}
