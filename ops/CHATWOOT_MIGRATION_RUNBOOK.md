# Runbook de Migracao de Chat (Bradial/Chatwoot)

Objetivo: trocar o chat conectado (`BRADIAL_CHAT_*`) sem perder o fluxo ClickUp -> contato -> conversa -> etiqueta.

## Premissas

- O backend ja esta com `ADMIN_API_TOKEN` configurado.
- O novo Chatwoot ja existe e voce vai criar as mesmas etiquetas comerciais:
  - `oportunidade`
  - `qualificacao`
  - `reuniao-agendada`
  - `follow-up`
  - `convidado-evento`
  - `confirmado`
  - `compareceu`
  - `em-negociacao`
  - `negocio-fechado`
  - `desqualificado`
  - `perdido`
- A regra de etapa segue `BRADIAL_STAGE_LABEL_SCOPE=conversation`.

## Scripts deste kit

- `ops/chatwoot-migration-backup.sh`
  - backup de `.env` e runtime-data
- `ops/chatwoot-migration-readiness.sh`
  - valida variaveis de chat e pendencias antes da virada
- `ops/chatwoot-migration-sanitize-links.sh`
  - limpa `chatContactId` e `conversationId` antigos de `lead-links.json`
- `ops/chatwoot-migration-backfill.sh`
  - processa pendentes do ClickUp no novo chat em lote

## Fase 1 - Pre-migracao (antes da janela)

1. Conferir novo chat:
   - inbox criada
   - agentes no inbox
   - etiquetas comerciais criadas

2. Backup completo:

```bash
cd /var/www/alemdaideia-sync-console
bash ops/chatwoot-migration-backup.sh
```

3. Validar baseline e prontidao:

```bash
export TOKEN='SEU_ADMIN_API_TOKEN'
bash ops/chatwoot-migration-readiness.sh
curl -s http://127.0.0.1:3015/healthz
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:3015/health
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:3015/clickup/pending-contacts
```

## Fase 2 - Dia da virada

1. Atualizar variaveis de chat no `backend/.env`:
   - `BRADIAL_CHAT_BASE_URL`
   - `BRADIAL_CHAT_ACCOUNT_ID`
   - `BRADIAL_CHAT_API_TOKEN`
   - `BRADIAL_CHAT_INBOX_ID`
   - `BRADIAL_CHAT_WEBHOOK_SECRET`

2. Saneamento dos vinculos antigos (obrigatorio):

```bash
cd /var/www/alemdaideia-sync-console
bash ops/chatwoot-migration-sanitize-links.sh --apply
```

3. Recarregar backend:

```bash
cd /var/www/alemdaideia-sync-console
pm2 startOrReload ecosystem.config.cjs --update-env
pm2 save
```

4. Snapshot inicial:

```bash
export TOKEN='SEU_ADMIN_API_TOKEN'
curl -s -H "Authorization: Bearer $TOKEN" -X POST http://127.0.0.1:3015/refresh
```

5. Backfill de pendentes no novo chat:

```bash
cd /var/www/alemdaideia-sync-console
export TOKEN='SEU_ADMIN_API_TOKEN'
bash ops/chatwoot-migration-backfill.sh
```

## Fase 3 - Validacao pos-virada

1. Confirmar saude:

```bash
export TOKEN='SEU_ADMIN_API_TOKEN'
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:3015/health
```

2. Confirmar eventos recentes:

```bash
export TOKEN='SEU_ADMIN_API_TOKEN'
curl -s -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:3015/sync/audit?limit=40"
curl -s -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:3015/logs"
```

3. Teste funcional minimo:
   - mudar status de uma task no ClickUp e confirmar etiqueta de conversa
   - mudar prioridade de uma task no ClickUp e confirmar prioridade no chat
   - trocar responsavel no ClickUp e confirmar agente da conversa

## Rollback (se necessario)

1. Restaurar `backend/.env` do backup.
2. Restaurar `backend/runtime-data/lead-links.json` do backup.
3. Recarregar backend:

```bash
cd /var/www/alemdaideia-sync-console
pm2 startOrReload ecosystem.config.cjs --update-env
pm2 save
```

## Observacoes importantes

- Com `stageLabelScope=conversation`, lead sem conversa nao recebe etiqueta de etapa.
- O sistema atual nao "abre conversa do zero" para todo contato por default; ele sincroniza etiqueta quando a conversa existe.
- Se quiser cobertura visual de 100% durante migracao, habilite fallback temporario em contato (decisao operacional).
