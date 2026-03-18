#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP_DIR="${ROOT_DIR}/migration-backups/${STAMP}"

mkdir -p "${BACKUP_DIR}"

copy_if_exists() {
  local rel="$1"
  local src="${ROOT_DIR}/${rel}"
  local dst="${BACKUP_DIR}/${rel}"
  if [[ -f "${src}" ]]; then
    mkdir -p "$(dirname "${dst}")"
    cp "${src}" "${dst}"
    echo "[backup] ${rel}"
  else
    echo "[skip]   ${rel} (nao encontrado)"
  fi
}

copy_if_exists "backend/.env"
copy_if_exists "backend/runtime-data/lead-links.json"
copy_if_exists "backend/runtime-data/clickup-integrations.json"
copy_if_exists "backend/runtime-data/sync-jobs.json"
copy_if_exists "backend/runtime-data/sync-audit-log.json"

echo
echo "Backup criado em: ${BACKUP_DIR}"

if [[ -f "${BACKUP_DIR}/backend/runtime-data/lead-links.json" ]]; then
  node - "${BACKUP_DIR}/backend/runtime-data/lead-links.json" <<'NODE'
const fs = require('node:fs')
const filePath = process.argv[2]
const payload = JSON.parse(fs.readFileSync(filePath, 'utf8'))
const items = Array.isArray(payload?.items) ? payload.items : []
const withConversation = items.filter((item) => String(item?.conversationId || '').trim()).length
const withChatContact = items.filter((item) => String(item?.chatContactId || '').trim()).length
console.log(`lead-links: ${items.length} total | com conversationId: ${withConversation} | com chatContactId: ${withChatContact}`)
NODE
fi
