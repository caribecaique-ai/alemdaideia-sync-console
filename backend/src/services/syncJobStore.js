import fs from 'node:fs/promises'
import path from 'node:path'

function normalizeJobs(items) {
  return Array.isArray(items)
    ? items.filter((item) => item && typeof item === 'object' && item.taskId)
    : []
}

export function createSyncJobStore(config = {}, pushLog = () => {}) {
  const storePath =
    String(config.storePath || '').trim() ||
    path.resolve(process.cwd(), 'runtime-data', 'sync-jobs.json')
  let writeChain = Promise.resolve()

  async function ensureStoreDir() {
    await fs.mkdir(path.dirname(storePath), { recursive: true })
  }

  async function readState() {
    try {
      const raw = await fs.readFile(storePath, 'utf8')
      const parsed = JSON.parse(raw)
      return {
        version: Number(parsed?.version || 1),
        updatedAt: parsed?.updatedAt || null,
        queued: normalizeJobs(parsed?.queued),
        deferred: normalizeJobs(parsed?.deferred),
        active: normalizeJobs(parsed?.active),
        retries: normalizeJobs(parsed?.retries),
      }
    } catch (error) {
      if (error.code === 'ENOENT') {
        return {
          version: 1,
          updatedAt: null,
          queued: [],
          deferred: [],
          active: [],
          retries: [],
        }
      }

      pushLog('warning', 'Falha ao ler jobs de sync persistidos', error.message, {
        storePath,
      })
      return {
        version: 1,
        updatedAt: null,
        queued: [],
        deferred: [],
        active: [],
        retries: [],
      }
    }
  }

  async function writeState(state = {}) {
    const payload = {
      version: 1,
      updatedAt: new Date().toISOString(),
      queued: normalizeJobs(state.queued),
      deferred: normalizeJobs(state.deferred),
      active: normalizeJobs(state.active),
      retries: normalizeJobs(state.retries),
    }

    await ensureStoreDir()
    await fs.writeFile(storePath, JSON.stringify(payload, null, 2), 'utf8')
    return payload
  }

  function replaceState(state = {}) {
    writeChain = writeChain
      .catch(() => {})
      .then(() => writeState(state))

    return writeChain
  }

  return {
    storePath,
    readState,
    replaceState,
  }
}
