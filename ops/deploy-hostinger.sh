#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/alemdaideia-sync-console}"
BRANCH="${BRANCH:-main}"

echo "[deploy] app dir: ${APP_DIR}"
cd "${APP_DIR}"

echo "[deploy] git fetch"
git fetch --all --prune
git checkout "${BRANCH}"
git pull --ff-only origin "${BRANCH}"

echo "[deploy] backend dependencies"
cd "${APP_DIR}/backend"
npm ci --omit=dev

echo "[deploy] frontend build"
cd "${APP_DIR}/frontend"
npm ci
npm run build

echo "[deploy] pm2 reload"
cd "${APP_DIR}"
pm2 startOrReload ecosystem.config.cjs --update-env
pm2 save

echo "[deploy] done"
