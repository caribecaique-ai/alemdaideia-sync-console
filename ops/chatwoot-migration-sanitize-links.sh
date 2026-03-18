#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STORE_PATH="${ROOT_DIR}/backend/runtime-data/lead-links.json"
APPLY=false

for arg in "$@"; do
  case "${arg}" in
    --apply) APPLY=true ;;
    --help|-h)
      cat <<'HELP'
Uso:
  ./ops/chatwoot-migration-sanitize-links.sh          # dry-run
  ./ops/chatwoot-migration-sanitize-links.sh --apply  # aplica e salva

O script remove somente os IDs de chat antigo:
  - chatContactId
  - conversationId

Mantem taskId, phone e bradialContactId para preservar o vinculo comercial.
HELP
      exit 0
      ;;
    *)
      echo "Argumento invalido: ${arg}"
      exit 1
      ;;
  esac
done

if [[ ! -f "${STORE_PATH}" ]]; then
  echo "Arquivo nao encontrado: ${STORE_PATH}"
  exit 1
fi

TMP_FILE="$(mktemp)"
node - "${STORE_PATH}" "${TMP_FILE}" <<'NODE'
const fs = require('node:fs')
const sourcePath = process.argv[2]
const outputPath = process.argv[3]

const payload = JSON.parse(fs.readFileSync(sourcePath, 'utf8'))
const items = Array.isArray(payload?.items) ? payload.items : []
const now = new Date().toISOString()

let changed = 0
let withConversationBefore = 0
let withChatContactBefore = 0

const sanitized = items.map((item) => {
  const hadConversation = String(item?.conversationId || '').trim().length > 0
  const hadChatContact = String(item?.chatContactId || '').trim().length > 0
  if (hadConversation) withConversationBefore += 1
  if (hadChatContact) withChatContactBefore += 1
  if (!hadConversation && !hadChatContact) return item

  changed += 1
  return {
    ...item,
    chatContactId: null,
    conversationId: null,
    updatedAt: now,
  }
})

const nextPayload = {
  version: 1,
  items: sanitized,
}

fs.writeFileSync(outputPath, JSON.stringify({
  summary: {
    total: items.length,
    changed,
    withConversationBefore,
    withChatContactBefore,
  },
  nextPayload,
}, null, 2))
NODE

SUMMARY_JSON="$(cat "${TMP_FILE}")"

node - "${TMP_FILE}" <<'NODE'
const fs = require('node:fs')
const payload = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'))
const summary = payload.summary || {}
console.log(`total: ${summary.total || 0}`)
console.log(`serao saneados: ${summary.changed || 0}`)
console.log(`com conversationId hoje: ${summary.withConversationBefore || 0}`)
console.log(`com chatContactId hoje: ${summary.withChatContactBefore || 0}`)
NODE

if [[ "${APPLY}" != "true" ]]; then
  echo
  echo "[dry-run] Nenhuma alteracao aplicada."
  rm -f "${TMP_FILE}"
  exit 0
fi

BACKUP_PATH="${STORE_PATH}.pre-chatwoot-switch.$(date +%Y%m%d-%H%M%S).bak"
cp "${STORE_PATH}" "${BACKUP_PATH}"

node - "${STORE_PATH}" "${TMP_FILE}" <<'NODE'
const fs = require('node:fs')
const targetPath = process.argv[2]
const payload = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'))
fs.writeFileSync(targetPath, JSON.stringify(payload.nextPayload, null, 2))
NODE

rm -f "${TMP_FILE}"
echo
echo "[ok] lead-links saneado com sucesso."
echo "backup: ${BACKUP_PATH}"
