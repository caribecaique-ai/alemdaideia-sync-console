# Alem da Ideia Sync Console

Projeto local para consolidar dados da Bradial com o ClickUp do workspace `Alem da Ideia`.

## Estrutura

- `frontend/`: painel operacional em React + Vite
- `backend/`: middleware local em Fastify para ingestao, merge e observabilidade

## O que ja faz

- conecta na Bradial Partner API
- suporta sync opcional de etiquetas da conversa via API Chatwoot/Bradial quando houver token de conversa
- conecta no ClickUp com escopo do workspace `Alem da Ideia`
- limita a leitura comercial ao space `COMERCIAL` e pasta `Area de Vendas`
- cruza contatos e tasks por telefone normalizado em `E.164`
- exibe dashboard, excecoes, logs e leads enriquecidos
- lista tasks do ClickUp que ainda precisam virar contato no Bradial
- cria ou atualiza contato no Bradial sem enviar mensagem
- espelha a etapa do ClickUp em uma tag controlada no Bradial, removendo a tag de etapa anterior
- permite sync reverso de etapa via webhook do Bradial/Chat para atualizar o status no ClickUp
- gera URL por integracao para cadastro de webhook no ClickUp

## Seguranca

- o frontend nao recebe tokens
- a leitura da Bradial depende apenas do que a `x-api-key` autenticada pode acessar
- a leitura do ClickUp depende apenas do que o token autenticado pode acessar
- os segredos ficam fora do Git em `.env`
- o fluxo de cadastro no Bradial bloqueia telefones ambiguos no ClickUp e na Bradial

## Setup

### Backend

```powershell
cd backend
Copy-Item .env.example .env
npm install
npm run dev
```

Variaveis principais:

- `BRADIAL_BASE_URL`
- `BRADIAL_ACCOUNT_ID`
- `BRADIAL_API_TOKEN`
- `BRADIAL_INBOX_ID`
- `BRADIAL_CHAT_BASE_URL`
- `BRADIAL_CHAT_ACCOUNT_ID`
- `BRADIAL_CHAT_API_TOKEN`
- `BRADIAL_CHAT_INBOX_ID`
- `BRADIAL_CHAT_WEBHOOK_SECRET`
- `BRADIAL_OPPORTUNITY_LABEL`
- `PUBLIC_BASE_URL`
- `CLICKUP_API_KEY`
- `CLICKUP_WORKSPACE_NAME`
- `CLICKUP_COMMERCIAL_SPACE_NAME`
- `CLICKUP_COMMERCIAL_FOLDER_NAME`
- `CLICKUP_STAGE_LABEL_MAP`
- `CLICKUP_WEBHOOK_SECRET`
- `CLICKUP_INTEGRATIONS_PATH`

Opcionalmente, em vez de `CLICKUP_API_KEY`, o backend pode usar:

- `CLICKUP_CLIENTS_BACKUP_PATH`
- `CLICKUP_BACKUP_CLIENT_NAME`

### Frontend

```powershell
cd frontend
npm install
npm run dev
```

## Modo Local Blindado

Para manter o stack local de pe enquanto a maquina estiver ligada:

```powershell
cd ops
.\Start-LocalStackSupervisor.ps1
```

O supervisor:

- mantem backend na porta `3015`
- mantem frontend na porta `4180`
- mantem o `ngrok` apontando para `3015`
- reinicia automaticamente backend, frontend ou tunnel se cair
- valida a URL publica esperada antes de considerar o stack saudavel

Para parar o supervisor:

```powershell
cd ops
.\Stop-LocalStackSupervisor.ps1
```

Para parar tambem backend, frontend e ngrok:

```powershell
cd ops
.\Stop-LocalStackSupervisor.ps1 -StopServices
```

## Portas padrao

- frontend: `4180`
- preview frontend: `4181`
- backend: `3015`

## Endpoints principais

- `GET /health`
- `GET /overview`
- `GET /leads`
- `GET /exceptions`
- `GET /logs`
- `GET /clickup/health`
- `GET /clickup/navigation`
- `GET /clickup/tasks`
- `GET /clickup/pending-contacts`
- `GET /clickup/webhook-integrations`
- `POST /clickup/webhook-integrations`
- `PATCH /clickup/webhook-integrations/:integrationId`
- `POST /clickup/tasks/:taskId/sync-to-bradial`
- `POST /webhooks/clickup`
- `POST /webhooks/clickup/:integrationId/:webhookToken`
- `POST /webhooks/bradial/chatwoot`
- `POST /refresh`
