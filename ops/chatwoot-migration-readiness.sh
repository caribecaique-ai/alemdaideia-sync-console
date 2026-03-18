#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_PATH="${ROOT_DIR}/backend/.env"
BASE_URL="${BASE_URL:-http://127.0.0.1:3015}"
TOKEN="${TOKEN:-}"

if [[ ! -f "${ENV_PATH}" ]]; then
  echo "Arquivo nao encontrado: ${ENV_PATH}"
  exit 1
fi

required_env=(
  "BRADIAL_CHAT_BASE_URL"
  "BRADIAL_CHAT_ACCOUNT_ID"
  "BRADIAL_CHAT_API_TOKEN"
  "BRADIAL_CHAT_INBOX_ID"
  "BRADIAL_CHAT_WEBHOOK_SECRET"
  "BRADIAL_STAGE_LABEL_SCOPE"
  "BRADIAL_SYNC_CONVERSATION_LABELS"
)

echo "== Variaveis obrigatorias =="
missing=0
for key in "${required_env[@]}"; do
  value="$(sed -n "s/^${key}=//p" "${ENV_PATH}" | tail -n 1)"
  if [[ -z "${value}" ]]; then
    echo "[missing] ${key}"
    missing=$((missing + 1))
  else
    echo "[ok]      ${key}"
  fi
done

if [[ -z "${TOKEN}" ]]; then
  echo
  echo "Defina TOKEN para validar endpoints protegidos."
  echo "Exemplo: export TOKEN='seu_admin_api_token'"
  exit 1
fi

echo
echo "== Endpoints =="

if curl -fsS "${BASE_URL}/healthz" >/dev/null; then
  echo "[ok]      /healthz"
else
  echo "[error]   /healthz"
fi

if curl -fsS -H "Authorization: Bearer ${TOKEN}" "${BASE_URL}/health" >/dev/null; then
  echo "[ok]      /health (auth)"
else
  echo "[error]   /health (auth)"
fi

echo
echo "== Pendencias ClickUp =="
node --input-type=module - "${BASE_URL}" "${TOKEN}" <<'NODE'
const baseUrl = process.argv[2]
const token = process.argv[3]

const response = await fetch(`${baseUrl}/clickup/pending-contacts`, {
  headers: { Authorization: `Bearer ${token}` },
})

if (!response.ok) {
  console.error(`Falha ao consultar pending-contacts: HTTP ${response.status}`)
  process.exit(1)
}

const items = await response.json()
const counts = {}
for (const item of Array.isArray(items) ? items : []) {
  const key = String(item?.syncState || 'unknown')
  counts[key] = (counts[key] || 0) + 1
}

console.log(JSON.stringify({
  total: Array.isArray(items) ? items.length : 0,
  bySyncState: counts,
}, null, 2))
NODE

echo
if [[ "${missing}" -eq 0 ]]; then
  echo "Readiness: variaveis obrigatorias presentes."
else
  echo "Readiness: faltam ${missing} variavel(is) obrigatoria(s)."
fi
