#!/bin/bash
# fix-backend-cors-vps.sh
# Corrige o problema de backend "offline" causado por CORS bloqueando o IP da VPS.
# Execute na VPS como root:
#   bash /var/www/alemdaideia-sync-console/ops/fix-backend-cors-vps.sh

set -e

APP_DIR="/var/www/alemdaideia-sync-console"
BACKEND_ENV="$APP_DIR/backend/.env"
FRONTEND_DIST="$APP_DIR/frontend/dist"
NGINX_HTML="/var/www/html"

echo "========================================="
echo "  Fix: Backend offline (CORS + URL)"
echo "========================================="

# -------------------------------------------
# 1. Atualiza CORS no .env do backend
# -------------------------------------------
echo ""
echo ">>> [1/4] Atualizando CORS_ORIGINS no .env do backend..."

# Remove FRONTEND_ORIGIN legada se existir
sed -i '/^FRONTEND_ORIGIN=/d' "$BACKEND_ENV"

# Remove CORS_ORIGINS antiga se existir
sed -i '/^CORS_ORIGINS=/d' "$BACKEND_ENV"

# Adiciona a nova com IP da VPS + localhost + ngrok
echo 'CORS_ORIGINS=http://168.231.97.169,http://168.231.97.169:4180,http://localhost:4180,http://127.0.0.1:4180,https://realizable-jacquelyne-pseudodramatic.ngrok-free.dev' >> "$BACKEND_ENV"

# Atualiza PUBLIC_BASE_URL
sed -i '/^PUBLIC_BASE_URL=/d' "$BACKEND_ENV"
echo 'PUBLIC_BASE_URL=https://realizable-jacquelyne-pseudodramatic.ngrok-free.dev' >> "$BACKEND_ENV"

echo "    Configurações atuais:"
grep -E "^(CORS_ORIGINS|PUBLIC_BASE_URL)=" "$BACKEND_ENV"

# -------------------------------------------
# 2. Rebuild do frontend com novo backendUrl
# -------------------------------------------
echo ""
echo ">>> [2/4] Rebuilding frontend..."
cd "$APP_DIR/frontend"
npm run build
echo "    Build concluído: $(du -sh dist/ | cut -f1)"

# -------------------------------------------
# 3. Deploy do frontend para nginx
# -------------------------------------------
echo ""
echo ">>> [3/4] Copiando build para $NGINX_HTML..."
cp -r "$FRONTEND_DIST/." "$NGINX_HTML/"
echo "    Arquivos copiados."

# -------------------------------------------
# 4. Restart do backend via PM2
# -------------------------------------------
echo ""
echo ">>> [4/4] Reiniciando backend via PM2..."
pm2 restart alemdaideia-sync-backend
sleep 3

echo ""
echo ">>> Status PM2:"
pm2 status alemdaideia-sync-backend

echo ""
echo ">>> Testando /health local:"
curl -s http://localhost:3015/health

echo ""
echo ""
echo "========================================="
echo "  ✅ Fix aplicado com sucesso!"
echo "========================================="
echo ""
echo "  >> Agora acesse no browser:"
echo "     http://168.231.97.169"
echo ""
echo "  >> Se o painel ainda mostrar 'offline',"
echo "     abra o console do browser (F12) e execute:"
echo "     localStorage.clear()  + F5"
echo ""
echo "  >> Isso limpa o backendUrl antigo (localhost:3015)"
echo "     que ficou salvo no localStorage."
echo ""
