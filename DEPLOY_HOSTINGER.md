# Deploy na VPS Hostinger

## Desenho

- `backend`: Node.js via `pm2`, porta `3015`
- `frontend`: build estático em `frontend/dist`
- `nginx`: entrega o frontend e faz proxy para o backend

## 1. Preparar a VPS

```bash
apt update && apt upgrade -y
apt install -y git nginx
npm install -g pm2
```

Use um template com Node.js já instalado, ou instale Node 22 antes.

## 2. Clonar o repositório

```bash
cd /var/www
git clone git@github.com:caribecaique-ai/alemdaideia-sync-console.git
cd alemdaideia-sync-console
```

## 3. Configurar backend

```bash
cd /var/www/alemdaideia-sync-console/backend
cp .env.example .env
```

Preencha pelo menos:

- `PORT=3015`
- `FRONTEND_ORIGIN=https://sync.seudominio.com`
- `PUBLIC_BASE_URL=https://sync.seudominio.com`
- `BRADIAL_BASE_URL`
- `BRADIAL_ACCOUNT_ID`
- `BRADIAL_API_TOKEN`
- `BRADIAL_CHAT_BASE_URL`
- `BRADIAL_CHAT_ACCOUNT_ID`
- `BRADIAL_CHAT_API_TOKEN`
- `BRADIAL_CHAT_INBOX_ID`
- `CLICKUP_API_KEY`
- `CLICKUP_WORKSPACE_NAME`
- `CLICKUP_COMMERCIAL_SPACE_NAME`
- `CLICKUP_COMMERCIAL_FOLDER_NAME`

## 4. Instalar e buildar

```bash
cd /var/www/alemdaideia-sync-console/backend
npm ci --omit=dev

cd /var/www/alemdaideia-sync-console/frontend
npm ci
npm run build
```

## 5. Subir o backend com PM2

```bash
cd /var/www/alemdaideia-sync-console
pm2 start ecosystem.config.cjs
pm2 save
pm2 startup
```

## 6. Configurar o nginx

Copie o template:

```bash
cp /var/www/alemdaideia-sync-console/ops/nginx/alemdaideia-sync-console.conf /etc/nginx/sites-available/alemdaideia-sync-console.conf
ln -s /etc/nginx/sites-available/alemdaideia-sync-console.conf /etc/nginx/sites-enabled/alemdaideia-sync-console.conf
nginx -t
systemctl reload nginx
```

Edite `server_name` no arquivo para o domínio real.

## 7. SSL

Se usar Certbot:

```bash
apt install -y certbot python3-certbot-nginx
certbot --nginx -d sync.seudominio.com
```

## 8. Deploy futuro

Quando fizer push no GitHub:

```bash
cd /var/www/alemdaideia-sync-console
bash ops/deploy-hostinger.sh
```

## 9. Trocar os webhooks

Depois da VPS no ar:

- ClickUp: troque a URL pública do webhook para `https://sync.seudominio.com/webhooks/clickup/...`
- Bradial: troque para `https://sync.seudominio.com/webhooks/bradial/chatwoot`

## 10. Checks finais

```bash
curl https://sync.seudominio.com/health
pm2 status
systemctl status nginx
```
