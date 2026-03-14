import fs from 'node:fs/promises'
import path from 'node:path'

function normalizeItems(items) {
  return Array.isArray(items) ? items.filter((item) => item && typeof item === 'object') : []
}

export function createSyncAuditStore(config = {}, pushLog = () => {}) {
  const storePath =
    String(config.storePath || '').trim() ||
    path.resolve(process.cwd(), 'runtime-data', 'sync-audit-log.json')
  const maxItems = Math.max(100, Number(config.maxItems || 1000))
  let writeChain = Promise.resolve()

  async function ensureStoreDir() {
    await fs.mkdir(path.dirname(storePath), { recursive: true })
  }

  async function readItems() {
    try {
      const raw = await fs.readFile(storePath, 'utf8')
      return normalizeItems(JSON.parse(raw))
    } catch (error) {
      if (error.code === 'ENOENT') return []
      pushLog('warning', 'Falha ao ler auditoria de sync', error.message, {
        storePath,
      })
      return []
    }
  }

  async function writeItems(items) {
    await ensureStoreDir()
    await fs.writeFile(storePath, JSON.stringify(normalizeItems(items).slice(0, maxItems), null, 2), 'utf8')
  }

  function record(entry = {}) {
    const auditEntry = {
      id: entry.id || `${Date.now()}-${Math.random().toString(16).slice(2, 10)}`,
      type: entry.type || 'sync_event',
      level: entry.level || 'info',
      createdAt: entry.createdAt || new Date().toISOString(),
      ...entry,
    }

    writeChain = writeChain
      .catch(() => {})
      .then(async () => {
        const current = await readItems()
        current.unshift(auditEntry)
        await writeItems(current)
      })

    return writeChain.then(() => auditEntry)
  }

  async function listRecent(limit = 100) {
    const current = await readItems()
    return current.slice(0, Math.max(1, Number(limit || 100)))
  }

  async function clear() {
    writeChain = writeChain
      .catch(() => {})
      .then(async () => {
        await writeItems([])
      })

    return writeChain
  }

  return {
    storePath,
    record,
    listRecent,
    clear,
  }
}
