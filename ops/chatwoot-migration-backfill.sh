#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:3015}"
TOKEN="${TOKEN:-}"
MAX_ITEMS="${MAX_ITEMS:-400}"
SLEEP_MS="${SLEEP_MS:-80}"

if [[ -z "${TOKEN}" ]]; then
  echo "Defina TOKEN com ADMIN_API_TOKEN antes de executar."
  echo "Exemplo: export TOKEN='seu_token'"
  exit 1
fi

node --input-type=module - "${BASE_URL}" "${TOKEN}" "${MAX_ITEMS}" "${SLEEP_MS}" <<'NODE'
const baseUrl = process.argv[2]
const token = process.argv[3]
const maxItems = Number(process.argv[4] || 400)
const sleepMs = Number(process.argv[5] || 80)

async function api(path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  })

  let payload = null
  try {
    payload = await response.json()
  } catch {
    payload = null
  }

  return { response, payload }
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

const pendingRes = await api('/clickup/pending-contacts')
if (!pendingRes.response.ok || !Array.isArray(pendingRes.payload)) {
  console.error('Falha ao carregar /clickup/pending-contacts')
  process.exit(1)
}

const candidates = pendingRes.payload
  .filter((item) => item && item.syncAllowed)
  .filter((item) => ['missing_contact', 'missing_stage_label', 'stage_label_outdated'].includes(String(item.syncState || '')))
  .slice(0, maxItems)

console.log(`candidatos: ${candidates.length}`)

let success = 0
let skipped = 0
let failed = 0

for (const item of candidates) {
  const { response, payload } = await api(`/clickup/tasks/${encodeURIComponent(item.taskId)}/sync-to-bradial`, {
    method: 'POST',
  })

  if (!response.ok) {
    failed += 1
    console.log(`[erro] ${item.taskId} | ${response.status} | ${payload?.error || 'sem detalhe'}`)
  } else if (payload?.skipped) {
    skipped += 1
    console.log(`[skip] ${item.taskId} | ${payload?.reason || 'skipped'}`)
  } else {
    success += 1
    const stage = payload?.stageLabel || payload?.result?.stageLabel || item?.targetStageLabel || 'sem-etapa'
    const name = payload?.bradialContactName || payload?.payload?.name || item?.taskName || item?.taskId
    console.log(`[ok] ${item.taskId} | ${name} | etapa ${stage}`)
  }

  if (sleepMs > 0) await delay(sleepMs)
}

console.log('')
console.log(JSON.stringify({
  total: candidates.length,
  success,
  skipped,
  failed,
}, null, 2))
NODE
