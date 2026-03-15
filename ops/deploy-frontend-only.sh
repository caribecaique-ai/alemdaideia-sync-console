#!/bin/bash
# deploy-frontend-only.sh
# Atualiza os arquivos do frontend OneClick na VPS e realiza o rebuild.
# Execute na VPS como root: bash /var/www/alemdaideia-sync-console/ops/deploy-frontend-only.sh

set -e

APP_DIR="/var/www/alemdaideia-sync-console"
FRONTEND_DIR="$APP_DIR/frontend"
NGINX_HTML="/var/www/html"
REPO_RAW_URL="https://raw.githubusercontent.com/julia-alemdaideia/alemdaideia-sync-console/main"

echo "========================================="
echo "  Deploy: OneClick Rebranding (Frontend)"
echo "========================================="

# 1. Download dos arquivos atualizados (Contornando falta de .git na VPS)
echo ">>> [1/3] Baixando novos arquivos do GitHub..."

mkdir -p "$FRONTEND_DIR/public"
mkdir -p "$FRONTEND_DIR/src"

# Baixando arquivos principais
curl -s -L "$REPO_RAW_URL/frontend/public/logo.png" -o "$FRONTEND_DIR/public/logo.png"
curl -s -L "$REPO_RAW_URL/frontend/src/App.jsx" -o "$FRONTEND_DIR/src/App.jsx"
curl -s -L "$REPO_RAW_URL/frontend/src/App.css" -o "$FRONTEND_DIR/src/App.css"
curl -s -L "$REPO_RAW_URL/frontend/src/index.css" -o "$FRONTEND_DIR/src/index.css"
curl -s -L "$REPO_RAW_URL/frontend/index.html" -o "$FRONTEND_DIR/index.html"
curl -s -L "$REPO_RAW_URL/backend/src/server.js" -o "$APP_DIR/backend/src/server.js"

echo "    Arquivos baixados."

# 2. Rebuild do Frontend
echo ""
echo ">>> [2/3] Rebuilding frontend..."
cd "$FRONTEND_DIR"

# Garante que a URL do backend está correta para a VPS
sed -i "s|backendUrl: 'http://localhost:3015'|backendUrl: 'http://168.231.97.169:3015'|g" src/mockState.js

npm run build
echo "    Build concluído."

# 3. Deploy para Nginx
echo ""
echo ">>> [3/3] Copiando para diretorio publico..."
cp -r dist/. "$NGINX_HTML/"

# Restart backend para garantir que o server.js novo (CORS flexível) entre em vigor
pm2 restart alemdaideia-sync-backend

echo ""
echo "========================================="
echo "  ✅ OneClick Lite Deploy concluído!"
echo "========================================="
echo "  Acesse: http://168.231.97.169"
echo "  (Dica: Se não mudar a logo, limpe o cache do browser ou Ctrl+F5)"
