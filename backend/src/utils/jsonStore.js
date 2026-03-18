import fs from 'node:fs'
import fsPromises from 'node:fs/promises'
import path from 'node:path'

function stripBom(value) {
  return String(value || '').replace(/^\uFEFF/, '')
}

function buildCorruptBackupPath(storePath) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  return `${storePath}.corrupt-${timestamp}`
}

function ensureDirSync(storePath) {
  fs.mkdirSync(path.dirname(storePath), { recursive: true })
}

async function ensureDir(storePath) {
  await fsPromises.mkdir(path.dirname(storePath), { recursive: true })
}

function writeFileAtomicSync(storePath, payload) {
  ensureDirSync(storePath)
  const tempPath = `${storePath}.${process.pid}.${Date.now()}.tmp`
  fs.writeFileSync(tempPath, payload, 'utf8')

  try {
    fs.renameSync(tempPath, storePath)
  } catch (error) {
    if (!['EEXIST', 'EPERM'].includes(error?.code)) {
      try {
        fs.rmSync(tempPath, { force: true })
      } catch {}
      throw error
    }

    fs.rmSync(storePath, { force: true })
    fs.renameSync(tempPath, storePath)
  }
}

async function writeFileAtomic(storePath, payload) {
  await ensureDir(storePath)
  const tempPath = `${storePath}.${process.pid}.${Date.now()}.tmp`
  await fsPromises.writeFile(tempPath, payload, 'utf8')

  try {
    await fsPromises.rename(tempPath, storePath)
  } catch (error) {
    if (!['EEXIST', 'EPERM'].includes(error?.code)) {
      await fsPromises.rm(tempPath, { force: true }).catch(() => {})
      throw error
    }

    await fsPromises.rm(storePath, { force: true }).catch(() => {})
    await fsPromises.rename(tempPath, storePath)
  }
}

function archiveCorruptStoreSync(storePath) {
  const corruptPath = buildCorruptBackupPath(storePath)
  fs.renameSync(storePath, corruptPath)
  return corruptPath
}

async function archiveCorruptStore(storePath) {
  const corruptPath = buildCorruptBackupPath(storePath)
  await fsPromises.rename(storePath, corruptPath)
  return corruptPath
}

export function readJsonStoreSync(storePath, buildFallback, options = {}) {
  const fallbackFactory = typeof buildFallback === 'function' ? buildFallback : () => buildFallback
  const onCorrupt = typeof options.onCorrupt === 'function' ? options.onCorrupt : null

  ensureDirSync(storePath)

  if (!fs.existsSync(storePath)) {
    const fallback = fallbackFactory()
    writeFileAtomicSync(storePath, JSON.stringify(fallback, null, 2))
    return fallback
  }

  try {
    return JSON.parse(stripBom(fs.readFileSync(storePath, 'utf8')))
  } catch (error) {
    let corruptPath = null

    if (error instanceof SyntaxError) {
      try {
        corruptPath = archiveCorruptStoreSync(storePath)
      } catch {}
    }

    onCorrupt?.(error, corruptPath)

    const fallback = fallbackFactory()
    writeFileAtomicSync(storePath, JSON.stringify(fallback, null, 2))
    return fallback
  }
}

export async function readJsonStore(storePath, buildFallback, options = {}) {
  const fallbackFactory = typeof buildFallback === 'function' ? buildFallback : () => buildFallback
  const onCorrupt = typeof options.onCorrupt === 'function' ? options.onCorrupt : null

  await ensureDir(storePath)

  try {
    const raw = await fsPromises.readFile(storePath, 'utf8')
    return JSON.parse(stripBom(raw))
  } catch (error) {
    if (error?.code !== 'ENOENT' && !(error instanceof SyntaxError)) {
      onCorrupt?.(error, null)
      const fallback = fallbackFactory()
      await writeFileAtomic(storePath, JSON.stringify(fallback, null, 2))
      return fallback
    }

    let corruptPath = null
    if (error instanceof SyntaxError) {
      try {
        corruptPath = await archiveCorruptStore(storePath)
      } catch {}
    }

    onCorrupt?.(error, corruptPath)

    const fallback = fallbackFactory()
    await writeFileAtomic(storePath, JSON.stringify(fallback, null, 2))
    return fallback
  }
}

export function writeJsonStoreAtomicSync(storePath, payload) {
  writeFileAtomicSync(storePath, JSON.stringify(payload, null, 2))
}

export async function writeJsonStoreAtomic(storePath, payload) {
  await writeFileAtomic(storePath, JSON.stringify(payload, null, 2))
}
