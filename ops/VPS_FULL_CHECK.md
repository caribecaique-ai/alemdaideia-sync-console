# VPS Full Check

Checagem operacional completa do ambiente na VPS.

## Uso

Na VPS:

```bash
cd /var/www/alemdaideia-sync-console
chmod +x ops/vps-full-check.sh
export TOKEN='SEU_ADMIN_API_TOKEN'
bash ops/vps-full-check.sh
```

Opcional:

```bash
AUDIT_LIMIT=600 bash ops/vps-full-check.sh
```

## O que valida

- `healthz`, `health` e `sync/health`
- configuracao efetiva de `stageLabelScope` e `contactLabelSyncEnabled`
- fila ativa, retries e metricas do sync
- pendencias do ClickUp por `syncState`
- leads com conversa mas etiqueta divergente
- leads com mais de uma etiqueta controlada
- excecoes abertas
- falhas recentes e retries no `sync/audit`
- repeticao suspeita de `stage-reconcile noop`
- integracoes webhook do ClickUp ativas

## Leitura rapida

- `critical`: problema real a corrigir
- `warning`: ponto de atencao operacional
- `conversationMismatches`: lead com conversa e etiqueta errada
- `blockedNoConversation`: lead sem conversa; no modo `conversation`, ele nao recebe etiqueta ainda
