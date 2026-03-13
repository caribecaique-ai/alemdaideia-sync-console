# Alem da Ideia Sync Console

Projeto local para consolidar dados da Bradial com o ClickUp do workspace `Alem da Ideia`.

## Estrutura

- `frontend/`: painel operacional em React + Vite
- `backend/`: middleware local em Fastify para ingestao, merge e observabilidade

## O que ja faz

- conecta na Bradial Partner API
- conecta no ClickUp com escopo do workspace `Alem da Ideia`
- limita a leitura comercial ao space `COMERCIAL` e pasta `Area de Vendas`
- cruza contatos e tasks por telefone normalizado em `E.164`
- exibe dashboard, excecoes, logs e leads enriquecidos
- lista tasks do ClickUp que ainda precisam virar contato no Bradial
- cria ou atualiza contato no Bradial com a label `OPORTUNIDADE`, sem enviar mensagem
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
- `BRADIAL_OPPORTUNITY_LABEL`
- `PUBLIC_BASE_URL`
- `CLICKUP_API_KEY`
- `CLICKUP_WORKSPACE_NAME`
- `CLICKUP_COMMERCIAL_SPACE_NAME`
- `CLICKUP_COMMERCIAL_FOLDER_NAME`
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
- `POST /refresh`
