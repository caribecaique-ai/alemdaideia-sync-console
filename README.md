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
- sincroniza urgencia do ClickUp para a prioridade nativa da conversa no Bradial/Chat
- registra o fechador vindo do webhook oficial do ClickUp e pode atribuir a conversa ao agente correspondente no chat
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
- `BRADIAL_SYNC_CONVERSATION_PRIORITY`
- `BRADIAL_SYNC_CLOSED_BY_ASSIGNMENT`
- `BRADIAL_SYNC_CLOSED_BY_ATTRIBUTES`
- `BRADIAL_AGENT_ALIAS_MAP`
- `PUBLIC_BASE_URL`
- `CLICKUP_API_KEY`
- `CLICKUP_WORKSPACE_NAME`
- `CLICKUP_COMMERCIAL_SPACE_NAME`
- `CLICKUP_COMMERCIAL_FOLDER_NAME`
- `CLICKUP_STAGE_LABEL_MAP`
- `CLICKUP_URGENCY_FIELD_NAMES`
- `CLICKUP_CLOSED_STAGE_LABELS`
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
- `GET /chat/agents`
- `GET /clickup/pending-contacts`
- `GET /clickup/webhook-integrations`
- `POST /clickup/webhook-integrations`
- `PATCH /clickup/webhook-integrations/:integrationId`
- `POST /clickup/tasks/:taskId/sync-to-bradial`
- `POST /webhooks/clickup`
- `POST /webhooks/clickup/:integrationId/:webhookToken`
- `POST /webhooks/bradial/chatwoot`
- `POST /refresh`

## Produção na VPS

Arquivos prontos no repositório:

- `ecosystem.config.cjs`: processo do backend no `pm2`
- `ops/nginx/alemdaideia-sync-console.conf`: template do `nginx`
- `ops/deploy-hostinger.sh`: atualização rápida na VPS
- `DEPLOY_HOSTINGER.md`: passo a passo de publicação

Fluxo recomendado:

1. subir a VPS com Node.js
2. clonar o repositório
3. configurar `backend/.env`
4. rodar o build do frontend
5. subir o backend com `pm2`
6. apontar o `nginx` para `frontend/dist` e para `localhost:3015`
7. trocar `PUBLIC_BASE_URL` e os webhooks para o domínio final

## Migracao de Chatwoot

Kit de migracao preparado em `ops/`:

- `ops/CHATWOOT_MIGRATION_RUNBOOK.md`
- `ops/chatwoot-migration-backup.sh`
- `ops/chatwoot-migration-readiness.sh`
- `ops/chatwoot-migration-sanitize-links.sh`
- `ops/chatwoot-migration-backfill.sh`
- `ops/vps-full-check.sh`
- `ops/VPS_FULL_CHECK.md`

Observacao:
- com `BRADIAL_STAGE_LABEL_SCOPE=conversation`, contato sem conversa nao recebe etiqueta de etapa.

## Match de agentes

Para atribuir no Bradial quem fechou no ClickUp, o backend usa os membros do inbox do chat e tenta casar nesta ordem:

- email do usuario do ClickUp
- nome normalizado do usuario do ClickUp
- alias manual via `BRADIAL_AGENT_ALIAS_MAP`

Exemplo:

```env
BRADIAL_AGENT_ALIAS_MAP={"abimael bueno":"abimael.bueno@hotmail.com","diego stev":"diego@seudominio.com"}
```

## Mapa de urgencia

O sync de urgencia usa a prioridade nativa da conversa no Bradial/Chat:

- ClickUp `Urgente` -> Bradial `Urgente`
- ClickUp `Alta` -> Bradial `Alta`
- ClickUp `Normal` -> Bradial `Media`
- ClickUp `Baixa` -> Bradial `Baixa`
- ClickUp `Limpar` -> Bradial `Nenhuma`

Se a urgencia estiver em campo personalizado, configure `CLICKUP_URGENCY_FIELD_NAMES`.
