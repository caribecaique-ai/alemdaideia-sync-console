import path from 'node:path'
import { readJsonStore, writeJsonStoreAtomic } from '../utils/jsonStore.js'

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

  function createEmptyState() {
    return {
      version: 1,
      updatedAt: null,
      queued: [],
      deferred: [],
      active: [],
      retries: [],
    }
  }

  async function readState() {
    const parsed = await readJsonStore(storePath, createEmptyState, {
      onCorrupt(error, corruptPath) {
        pushLog(
          'warning',
          'Fila persistida de sync restaurada',
          corruptPath
            ? `Arquivo corrompido arquivado em ${corruptPath}.`
            : `Falha ao ler ${storePath}; fila recriada.`,
          { storePath, corruptPath, error: error.message },
        )
      },
    })

    return {
      version: Number(parsed?.version || 1),
      updatedAt: parsed?.updatedAt || null,
      queued: normalizeJobs(parsed?.queued),
      deferred: normalizeJobs(parsed?.deferred),
      active: normalizeJobs(parsed?.active),
      retries: normalizeJobs(parsed?.retries),
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

    await writeJsonStoreAtomic(storePath, payload)
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
