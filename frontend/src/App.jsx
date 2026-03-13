import {
  startTransition,
  useDeferredValue,
  useEffect,
  useEffectEvent,
  useState,
} from 'react'
import './App.css'
import {
  initialConfig,
  initialExceptions,
  initialHealth,
  initialLeads,
  initialLogs,
  mockEventCatalog,
} from './mockState'

const STORAGE_NAMESPACE = 'bradial-clickup-sync-ui-v2'

function readStorage(key, fallback) {
  const fullKey = `${STORAGE_NAMESPACE}:${key}`
  const raw = window.localStorage.getItem(fullKey)
  if (!raw) return fallback

  try {
    return JSON.parse(raw)
  } catch {
    return fallback
  }
}

function writeStorage(key, value) {
  const fullKey = `${STORAGE_NAMESPACE}:${key}`
  window.localStorage.setItem(fullKey, JSON.stringify(value))
}

function usePersistentState(key, fallback) {
  const [state, setState] = useState(() => readStorage(key, fallback))

  useEffect(() => {
    writeStorage(key, state)
  }, [key, state])

  return [state, setState]
}

function formatDate(value) {
  if (!value) return 'sem registro'

  try {
    return new Intl.DateTimeFormat('pt-BR', {
      dateStyle: 'short',
      timeStyle: 'short',
    }).format(new Date(value))
  } catch {
    return value
  }
}

function createLog(level, title, message, leadId = null) {
  return {
    id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
    level,
    title,
    message,
    leadId,
    createdAt: new Date().toISOString(),
  }
}

function App() {
  const bootInLiveMode = initialConfig.mode === 'live'
  const [config, setConfig] = usePersistentState('config', initialConfig)
  const [health, setHealth] = usePersistentState('health', initialHealth)
  const [leads, setLeads] = usePersistentState('leads', bootInLiveMode ? [] : initialLeads)
  const [pendingContacts, setPendingContacts] = usePersistentState('pendingContacts', [])
  const [webhookRegistry, setWebhookRegistry] = usePersistentState('webhookRegistry', {
    publicBaseUrl: '',
    source: null,
    isPublic: false,
    items: [],
  })
  const [exceptions, setExceptions] = usePersistentState(
    'exceptions',
    bootInLiveMode ? [] : initialExceptions,
  )
  const [logs, setLogs] = usePersistentState('logs', bootInLiveMode ? [] : initialLogs)
  const [selectedLeadId, setSelectedLeadId] = usePersistentState(
    'selectedLeadId',
    bootInLiveMode ? null : initialLeads[0]?.id ?? null,
  )
  const [leadSearch, setLeadSearch] = useState('')
  const [connectionState, setConnectionState] = useState('idle')
  const [syncingTaskId, setSyncingTaskId] = useState(null)
  const [creatingWebhookUrl, setCreatingWebhookUrl] = useState(false)

  const deferredLeadSearch = useDeferredValue(leadSearch)
  const selectedLead =
    leads.find((lead) => lead.id === selectedLeadId) ?? leads[0] ?? null

  const openExceptions = exceptions.filter((item) => item.status !== 'resolved')
  const syncedToday = leads.filter((lead) => lead.syncEnabled).length
  const healthyLeads = leads.filter((lead) => lead.health === 'healthy').length
  const warningLeads = leads.filter((lead) => lead.health === 'warning').length
  const actionablePendingContacts = pendingContacts.filter((item) => item.syncAllowed).length

  const filteredLeads = leads.filter((lead) => {
    const search = deferredLeadSearch.trim().toLowerCase()
    if (!search) return true

    return [
      lead.name,
      lead.phone,
      lead.owner,
      lead.clickupStage,
      lead.chatStatus,
    ]
      .join(' ')
      .toLowerCase()
      .includes(search)
  })

  useEffect(() => {
    if (selectedLeadId) return
    if (!leads[0]) return

    setSelectedLeadId(leads[0].id)
  }, [leads, selectedLeadId, setSelectedLeadId])

  useEffect(() => {
    if (config.mode !== 'mock') return

    setHealth(initialHealth)
    setLeads(initialLeads)
    setPendingContacts([])
    setWebhookRegistry({
      publicBaseUrl: '',
      source: null,
      isPublic: false,
      items: [],
    })
    setExceptions(initialExceptions)
    setLogs(initialLogs)
    setSelectedLeadId(initialLeads[0]?.id ?? null)
  }, [config.mode, setExceptions, setHealth, setLeads, setLogs, setPendingContacts, setSelectedLeadId, setWebhookRegistry])

  const pushLog = useEffectEvent((entry) => {
    setLogs((current) => [entry, ...current].slice(0, 120))
  })

  const runMockTick = useEffectEvent(() => {
    const event = mockEventCatalog[Math.floor(Math.random() * mockEventCatalog.length)]
    const candidateLead = leads[Math.floor(Math.random() * leads.length)]

    if (!event || !candidateLead) return

    if (event.kind === 'sync-ok') {
      setLeads((current) =>
        current.map((lead) =>
          lead.id === candidateLead.id
            ? {
                ...lead,
                health: 'healthy',
                lastSyncAt: new Date().toISOString(),
                lastAction: 'sync concluido',
              }
            : lead,
        ),
      )
    }

    if (event.kind === 'warning') {
      setLeads((current) =>
        current.map((lead) =>
          lead.id === candidateLead.id
            ? {
                ...lead,
                health: 'warning',
                lastAction: 'aguardando retry',
              }
            : lead,
        ),
      )
    }

    if (event.kind === 'exception') {
      const newException = {
        id: `exc-${Date.now()}`,
        status: 'open',
        leadId: candidateLead.id,
        kind: event.exceptionKind,
        source: event.source,
        detectedAt: new Date().toISOString(),
        phone: candidateLead.phone,
        summary: event.message,
      }

      setExceptions((current) => [newException, ...current].slice(0, 30))
      setLeads((current) =>
        current.map((lead) =>
          lead.id === candidateLead.id
            ? {
                ...lead,
                health: 'risk',
                lastAction: 'excecao em aberto',
              }
            : lead,
        ),
      )
    }

    pushLog(
      createLog(event.level, event.title, event.message, candidateLead.id),
    )
  })

  useEffect(() => {
    if (config.mode !== 'mock') return undefined

    const intervalId = window.setInterval(() => {
      runMockTick()
    }, 9000)

    return () => window.clearInterval(intervalId)
  }, [config.mode, runMockTick])

  const saveConfig = () => {
    pushLog(
      createLog(
        'info',
        'Configuracao salva',
        `Modo ${config.mode} configurado para backend ${config.backendUrl || 'nao definido'}`,
      ),
    )
  }

  const refreshBackendSnapshot = async () => {
    if (config.mode !== 'live' || !config.backendUrl.trim()) {
      pushLog(
        createLog(
          'warning',
          'Refresh indisponivel',
          'Defina o backend e use o modo live para atualizar o snapshot.',
        ),
      )
      return
    }

    setConnectionState('loading')

    try {
      const baseUrl = config.backendUrl.replace(/\/$/, '')
      const response = await fetch(`${baseUrl}/refresh`, { method: 'POST' })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status} no endpoint /refresh`)
      }

      await pingBackend(true)
      pushLog(
        createLog('success', 'Snapshot atualizado', 'Backend recarregado com dados reais da Bradial.'),
      )
    } catch (error) {
      pushLog(
        createLog(
          'error',
          'Falha ao atualizar snapshot',
          error.message || 'Nao foi possivel recarregar o backend',
        ),
      )
    } finally {
      setConnectionState('idle')
    }
  }

  const pullLiveData = async (silent = false) => {
    if (config.mode !== 'live' || !config.backendUrl.trim()) return

    try {
      const baseUrl = config.backendUrl.replace(/\/$/, '')
      const [
        leadsResponse,
        exceptionsResponse,
        logsResponse,
        pendingContactsResponse,
        webhookIntegrationsResponse,
      ] = await Promise.all([
        fetch(`${baseUrl}/leads`),
        fetch(`${baseUrl}/exceptions`),
        fetch(`${baseUrl}/logs`),
        fetch(`${baseUrl}/clickup/pending-contacts`),
        fetch(`${baseUrl}/clickup/webhook-integrations`),
      ])

      if (
        !leadsResponse.ok ||
        !exceptionsResponse.ok ||
        !logsResponse.ok ||
        !pendingContactsResponse.ok ||
        !webhookIntegrationsResponse.ok
      ) {
        throw new Error('Um dos endpoints respondeu com erro')
      }

      const [
        nextLeads,
        nextExceptions,
        nextLogs,
        nextPendingContacts,
        nextWebhookRegistry,
      ] = await Promise.all([
        leadsResponse.json(),
        exceptionsResponse.json(),
        logsResponse.json(),
        pendingContactsResponse.json(),
        webhookIntegrationsResponse.json(),
      ])

      setLeads(Array.isArray(nextLeads) ? nextLeads : [])
      setExceptions(Array.isArray(nextExceptions) ? nextExceptions : [])
      setLogs(Array.isArray(nextLogs) ? nextLogs : [])
      setPendingContacts(Array.isArray(nextPendingContacts) ? nextPendingContacts : [])
      setWebhookRegistry(
        nextWebhookRegistry && typeof nextWebhookRegistry === 'object'
          ? nextWebhookRegistry
          : {
              publicBaseUrl: '',
              source: null,
              isPublic: false,
              items: [],
            },
      )

      if (!silent) {
        pushLog(
          createLog(
            'success',
            'Dados reais carregados',
            `${Array.isArray(nextLeads) ? nextLeads.length : 0} leads e ${
              Array.isArray(nextPendingContacts) ? nextPendingContacts.length : 0
            } pendencias importados do backend`,
          ),
        )
      }
    } catch (error) {
      if (!silent) {
        pushLog(
          createLog(
            'error',
            'Falha ao carregar dados reais',
            error.message || 'Nao foi possivel atualizar leads do backend',
          ),
        )
      }
    }
  }

  const pingBackend = async (silent = false) => {
    if (!config.backendUrl.trim()) {
      setHealth({
        status: 'offline',
        detail: 'Defina o backend antes do teste.',
        lastCheckedAt: new Date().toISOString(),
        latencyMs: null,
      })
      if (!silent) {
        pushLog(
          createLog('warning', 'Backend nao configurado', 'Defina a URL do middleware para testar a conexao.'),
        )
      }
      return
    }

    setConnectionState('loading')
    const startedAt = performance.now()
    const controller = new AbortController()
    const timeoutId = window.setTimeout(() => controller.abort(), 15000)

    try {
      const response = await fetch(`${config.backendUrl.replace(/\/$/, '')}/health`, {
        signal: controller.signal,
      })
      const payload = await response.json().catch(() => ({}))
      const latencyMs = Math.round(performance.now() - startedAt)

      setHealth({
        status: response.ok ? 'online' : 'degraded',
        detail: payload.status || `HTTP ${response.status}`,
        lastCheckedAt: new Date().toISOString(),
        latencyMs,
      })
      setConnectionState('idle')

      if (!silent) {
        pushLog(
          createLog(
            response.ok ? 'success' : 'warning',
            'Health check executado',
            response.ok
              ? `Middleware respondeu em ${latencyMs} ms`
              : `Middleware respondeu com HTTP ${response.status}`,
          ),
        )
      }

      if (response.ok) {
        await pullLiveData(silent)
      }
    } catch (error) {
      setHealth({
        status: 'offline',
        detail: error.name === 'AbortError' ? 'timeout apos 15s' : 'falha de conexao',
        lastCheckedAt: new Date().toISOString(),
        latencyMs: null,
      })
      setConnectionState('idle')

      if (!silent) {
        pushLog(
          createLog(
            'error',
            'Falha ao conectar backend',
            error.name === 'AbortError'
              ? 'Timeout no endpoint /health'
              : 'Nao foi possivel falar com o middleware',
          ),
        )
      }
    } finally {
      window.clearTimeout(timeoutId)
    }
  }

  useEffect(() => {
    if (config.mode !== 'live' || !config.autoPoll) return undefined

    const intervalId = window.setInterval(() => {
      void pingBackend(true)
    }, 30000)

    return () => window.clearInterval(intervalId)
  }, [config.autoPoll, config.backendUrl, config.mode])

  useEffect(() => {
    if (config.mode !== 'live') return
    void pingBackend(true)
  }, [config.mode, config.backendUrl])

  const handleResolveException = (exceptionId) => {
    const target = exceptions.find((item) => item.id === exceptionId)
    if (!target) return

    setExceptions((current) =>
      current.map((item) =>
        item.id === exceptionId
          ? { ...item, status: 'resolved', resolvedAt: new Date().toISOString() }
          : item,
      ),
    )

    if (target.leadId) {
      setLeads((current) =>
        current.map((lead) =>
          lead.id === target.leadId
            ? {
                ...lead,
                health: 'healthy',
                lastAction: 'resolvido manualmente',
                lastSyncAt: new Date().toISOString(),
              }
            : lead,
        ),
      )
    }

    pushLog(
      createLog(
        'success',
        'Excecao resolvida',
        `${target.kind} resolvido manualmente`,
        target.leadId,
      ),
    )
  }

  const handleToggleSync = (leadId) => {
    const target = leads.find((lead) => lead.id === leadId)
    if (!target) return

    setLeads((current) =>
      current.map((lead) =>
        lead.id === leadId
          ? {
              ...lead,
              syncEnabled: !lead.syncEnabled,
              lastAction: lead.syncEnabled ? 'sync pausado' : 'sync retomado',
            }
          : lead,
      ),
    )

    pushLog(
      createLog(
        'info',
        target.syncEnabled ? 'Sync pausado' : 'Sync retomado',
        `Lead ${target.name} agora esta ${target.syncEnabled ? 'fora' : 'dentro'} da sincronizacao automatica`,
        leadId,
      ),
    )
  }

  const handleReprocessLead = (leadId) => {
    const target = leads.find((lead) => lead.id === leadId)
    if (!target) return

    setLeads((current) =>
      current.map((lead) =>
        lead.id === leadId
          ? {
              ...lead,
              lastSyncAt: new Date().toISOString(),
              health: 'healthy',
              lastAction: 'reprocessado manualmente',
            }
          : lead,
      ),
    )

    pushLog(
      createLog(
        'success',
        'Reprocessamento executado',
        `Fila de sync reiniciada para ${target.name}`,
        leadId,
      ),
    )
  }

  const handleCreateTestException = () => {
    const targetLead = leads.find((lead) => lead.syncEnabled) ?? leads[0]
    if (!targetLead) return

    const nextException = {
      id: `exc-${Date.now()}`,
      status: 'open',
      leadId: targetLead.id,
      kind: 'manual_review',
      source: 'frontend-test',
      detectedAt: new Date().toISOString(),
      phone: targetLead.phone,
      summary: 'Evento de teste criado pelo painel para validar resolucao manual.',
    }

    setExceptions((current) => [nextException, ...current].slice(0, 30))
    setLeads((current) =>
      current.map((lead) =>
        lead.id === targetLead.id
          ? {
              ...lead,
              health: 'risk',
              lastAction: 'evento de teste criado',
            }
          : lead,
      ),
    )
    pushLog(
      createLog(
        'warning',
        'Excecao de teste criada',
        `Lead ${targetLead.name} entrou em revisao manual`,
        targetLead.id,
      ),
    )
  }

  const handleSelectLead = (leadId) => {
    startTransition(() => {
      setSelectedLeadId(leadId)
    })
  }

  const handleSyncPendingTask = async (pendingTask) => {
    if (config.mode !== 'live' || !config.backendUrl.trim()) {
      pushLog(
        createLog(
          'warning',
          'Sync indisponivel',
          'Use o modo live com backend configurado para criar contatos na Bradial.',
        ),
      )
      return
    }

    setSyncingTaskId(pendingTask.taskId)

    try {
      const baseUrl = config.backendUrl.replace(/\/$/, '')
      const response = await fetch(`${baseUrl}/clickup/tasks/${pendingTask.taskId}/sync-to-bradial`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ dryRun: false }),
      })
      const payload = await response.json().catch(() => ({}))

      if (!response.ok) {
        throw new Error(payload.error || `HTTP ${response.status}`)
      }

      await pingBackend(true)
      pushLog(
        createLog(
          'success',
          payload.operation === 'create' ? 'Contato criado no Bradial' : 'Contato atualizado no Bradial',
          `${pendingTask.taskName} recebeu a label ${payload.opportunityLabel || 'OPORTUNIDADE'} sem envio de mensagem.`,
          pendingTask.bradialLeadId,
        ),
      )
    } catch (error) {
      pushLog(
        createLog(
          'error',
          'Falha ao sincronizar contato',
          error.message || 'Nao foi possivel criar/atualizar o contato na Bradial.',
          pendingTask.bradialLeadId,
        ),
      )
    } finally {
      setSyncingTaskId(null)
    }
  }

  const handleGenerateWebhookUrl = async () => {
    if (config.mode !== 'live' || !config.backendUrl.trim()) {
      pushLog(
        createLog(
          'warning',
          'Geracao indisponivel',
          'Use o modo live com backend configurado para gerar a URL do webhook.',
        ),
      )
      return
    }

    setCreatingWebhookUrl(true)

    try {
      const baseUrl = config.backendUrl.replace(/\/$/, '')
      const response = await fetch(`${baseUrl}/clickup/webhook-integrations`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({}),
      })
      const payload = await response.json().catch(() => ({}))

      if (!response.ok) {
        throw new Error(payload.error || `HTTP ${response.status}`)
      }

      await pullLiveData(true)
      pushLog(
        createLog(
          payload.warning ? 'warning' : 'success',
          'URL de webhook gerada',
          payload.warning ||
            `Nova URL pronta para cadastro no ClickUp: ${payload.integration?.webhookUrl || 'sem url'}`,
        ),
      )
    } catch (error) {
      pushLog(
        createLog(
          'error',
          'Falha ao gerar URL do webhook',
          error.message || 'Nao foi possivel criar a integracao do ClickUp.',
        ),
      )
    } finally {
      setCreatingWebhookUrl(false)
    }
  }

  const clearLogs = () => {
    setLogs([])
  }

  const selectedLeadLogs = logs.filter((entry) => entry.leadId === selectedLead?.id).slice(0, 6)

  return (
    <div className="app-shell">
      <div className="background-orbit background-orbit-left" />
      <div className="background-orbit background-orbit-right" />

      <header className="hero-bar panel">
        <div>
          <p className="eyebrow">Painel operacional</p>
          <h1>Bradial x ClickUp Sync Console</h1>
          <p className="hero-copy">
            Controle a leitura real de Bradial e ClickUp, acompanhe excecoes de identificacao
            e cadastre oportunidades no Bradial sem disparar mensagens automaticas.
          </p>
        </div>

        <div className="hero-actions">
          <span className={`pill pill-${config.mode}`}>{config.mode === 'mock' ? 'mock mode' : 'live mode'}</span>
          <span className={`pill pill-${health.status}`}>{health.status}</span>
          <button className="ghost-button" type="button" onClick={() => void pingBackend()}>
            Testar backend
          </button>
          {config.mode === 'live' ? (
            <button type="button" onClick={() => void refreshBackendSnapshot()}>
              Atualizar dados
            </button>
          ) : null}
        </div>
      </header>

      <section className="summary-grid">
        <article className="stat-card panel">
          <span className="stat-label">Leads acompanhados</span>
          <strong>{leads.length}</strong>
          <small>{syncedToday} com sync ativo</small>
        </article>
        <article className="stat-card panel">
          <span className="stat-label">Operacao estavel</span>
          <strong>{healthyLeads}</strong>
          <small>{warningLeads} com alerta</small>
        </article>
        <article className="stat-card panel">
          <span className="stat-label">Excecoes abertas</span>
          <strong>{openExceptions.length}</strong>
          <small>{exceptions.length} registradas</small>
        </article>
        <article className="stat-card panel">
          <span className="stat-label">Pendentes ClickUp</span>
          <strong>{pendingContacts.length}</strong>
          <small>{actionablePendingContacts} podem subir para o Bradial</small>
        </article>
        <article className="stat-card panel">
          <span className="stat-label">Logs em memoria</span>
          <strong>{logs.length}</strong>
          <small>{connectionState === 'loading' ? 'checando backend...' : 'stream local ativa'}</small>
        </article>
      </section>

      <section className="workspace-grid">
        <aside className="column-stack">
          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Conexao</p>
                <h2>Configuracao local</h2>
              </div>
              <button className="ghost-button" type="button" onClick={saveConfig}>
                Salvar
              </button>
            </div>

            <div className="form-grid">
              <label>
                <span>Modo</span>
                <select
                  value={config.mode}
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, mode: event.target.value }))
                  }
                >
                  <option value="mock">Mock</option>
                  <option value="live">Live</option>
                </select>
              </label>

              <label>
                <span>URL do middleware</span>
                <input
                  value={config.backendUrl}
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, backendUrl: event.target.value }))
                  }
                  placeholder="http://localhost:3015"
                />
              </label>

              <label>
                <span>Bradial base URL</span>
                <input
                  value={config.bradialBaseUrl}
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, bradialBaseUrl: event.target.value }))
                  }
                  placeholder="https://chat.bradial.com.br"
                />
              </label>

              <label>
                <span>Account ID</span>
                <input
                  value={config.bradialAccountId}
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, bradialAccountId: event.target.value }))
                  }
                  placeholder="6008"
                />
              </label>

              <label className="switch-row">
                <span>Auto-check backend</span>
                <input
                  checked={config.autoPoll}
                  type="checkbox"
                  onChange={(event) =>
                    setConfig((current) => ({ ...current, autoPoll: event.target.checked }))
                  }
                />
              </label>
            </div>
          </section>

          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Webhook</p>
                <h2>URL por integracao</h2>
              </div>
              <button
                className="ghost-button"
                type="button"
                disabled={config.mode !== 'live' || creatingWebhookUrl}
                onClick={() => void handleGenerateWebhookUrl()}
              >
                {creatingWebhookUrl ? 'Gerando...' : 'Gerar URL'}
              </button>
            </div>

            <div className="webhook-box">
              <p>Base publica detectada</p>
              <code>{webhookRegistry.publicBaseUrl || 'nao resolvida'}</code>
              <small>
                {webhookRegistry.source
                  ? `origem: ${webhookRegistry.source}${webhookRegistry.isPublic ? '' : ' (ainda nao publica)'}`
                  : 'gere uma integracao para resolver a URL'}
              </small>
            </div>

            <div className="webhook-list">
              {Array.isArray(webhookRegistry.items) && webhookRegistry.items.length > 0 ? (
                webhookRegistry.items.map((item) => (
                  <article key={item.integrationId} className="webhook-card">
                    <div className="exception-meta">
                      <span className={`pill ${item.status === 'active' ? 'pill-success' : 'pill-risk'}`}>
                        {item.status}
                      </span>
                      <span>{item.authMode}</span>
                    </div>
                    <strong>{item.name}</strong>
                    <code>{item.webhookUrl}</code>
                    <small>{item.workspaceName || 'workspace nao identificado'}</small>
                  </article>
                ))
              ) : (
                <div className="empty-state">
                  <strong>Nenhuma URL gerada</strong>
                  <p>Crie uma integracao para copiar a URL e cadastrar no ClickUp.</p>
                </div>
              )}
            </div>
          </section>

          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Saude</p>
                <h2>Health check</h2>
              </div>
              <button
                className="ghost-button"
                type="button"
                onClick={() => void pingBackend()}
              >
                Executar teste
              </button>
            </div>

            <ul className="status-list">
              <li>
                <span>Status</span>
                <strong className={`tone-${health.status}`}>{health.status}</strong>
              </li>
              <li>
                <span>Detalhe</span>
                <strong>{health.detail}</strong>
              </li>
              <li>
                <span>Latencia</span>
                <strong>{health.latencyMs ? `${health.latencyMs} ms` : 'n/a'}</strong>
              </li>
              <li>
                <span>Ultima checagem</span>
                <strong>{formatDate(health.lastCheckedAt)}</strong>
              </li>
            </ul>

            <div className="endpoint-box">
              <p>Endpoints esperados do backend</p>
              <code>GET /health</code>
              <code>GET /leads</code>
              <code>GET /exceptions</code>
              <code>GET /logs</code>
              <code>GET /clickup/health</code>
              <code>GET /clickup/tasks</code>
              <code>GET /clickup/pending-contacts</code>
              <code>POST /clickup/tasks/:taskId/sync-to-bradial</code>
              <code>POST /refresh</code>
            </div>
          </section>

          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Excecoes</p>
                <h2>Fila operacional</h2>
              </div>
              <button
                className="ghost-button"
                type="button"
                disabled={config.mode === 'live'}
                onClick={handleCreateTestException}
              >
                Criar teste
              </button>
            </div>

            <div className="exception-list">
              {openExceptions.length === 0 ? (
                <div className="empty-state">
                  <strong>Nenhuma excecao aberta</strong>
                  <p>O painel esta limpo. Use o modo mock para gerar eventos.</p>
                </div>
              ) : (
                openExceptions.map((item) => (
                  <article key={item.id} className="exception-card">
                    <div className="exception-meta">
                      <span className="pill pill-warning">{item.kind}</span>
                      <span>{formatDate(item.detectedAt)}</span>
                    </div>
                    <strong>{item.phone}</strong>
                    <p>{item.summary}</p>
                    <div className="exception-actions">
                      <span>origem: {item.source}</span>
                      <button
                        type="button"
                        disabled={config.mode === 'live'}
                        onClick={() => handleResolveException(item.id)}
                      >
                        {config.mode === 'live' ? 'Somente leitura' : 'Resolver'}
                      </button>
                    </div>
                  </article>
                ))
              )}
            </div>
          </section>
        </aside>

        <main className="column-stack main-column">
          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">ClickUp</p>
                <h2>Oportunidades para Bradial</h2>
              </div>
              <span className={`pill ${pendingContacts.length ? 'pill-warning' : 'pill-success'}`}>
                {pendingContacts.length} pendentes
              </span>
            </div>

            <div className="opportunity-list">
              {config.mode !== 'live' ? (
                <div className="empty-state">
                  <strong>Disponivel apenas no modo live</strong>
                  <p>Essa fila mostra tasks reais do ClickUp que ainda precisam virar contato no Bradial.</p>
                </div>
              ) : pendingContacts.length === 0 ? (
                <div className="empty-state">
                  <strong>Nenhuma oportunidade pendente</strong>
                  <p>Todas as tasks elegiveis ja possuem contato Bradial com a label OPORTUNIDADE.</p>
                </div>
              ) : (
                pendingContacts.map((item) => (
                  <article key={item.id} className="opportunity-card">
                    <div className="exception-meta">
                      <span className={`pill ${item.syncAllowed ? 'pill-warning' : 'pill-risk'}`}>
                        {item.syncState}
                      </span>
                      <span>{formatDate(item.dateUpdated)}</span>
                    </div>
                    <strong>{item.taskName}</strong>
                    <p>{item.summary}</p>
                    <div className="opportunity-meta">
                      <span>{item.phone}</span>
                      <span>{item.status}</span>
                      <span>{item.owner || 'sem owner'}</span>
                      <span>{item.listName}</span>
                    </div>
                    <div className="opportunity-actions">
                      <span>
                        {item.bradialContactId
                          ? `Contato Bradial ${item.bradialContactId}`
                          : 'Contato ainda nao existe na Bradial'}
                      </span>
                      <div className="detail-actions">
                        {item.url ? (
                          <a className="inline-link" href={item.url} target="_blank" rel="noreferrer">
                            Abrir task
                          </a>
                        ) : null}
                        <button
                          type="button"
                          disabled={!item.syncAllowed || syncingTaskId === item.taskId}
                          onClick={() => void handleSyncPendingTask(item)}
                        >
                          {syncingTaskId === item.taskId
                            ? 'Processando...'
                            : item.syncState === 'missing_contact'
                              ? 'Criar contato'
                              : 'Atualizar contato'}
                        </button>
                      </div>
                    </div>
                  </article>
                ))
              )}
            </div>
          </section>

          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Leads</p>
                <h2>Explorador de vinculos</h2>
              </div>
              <input
                className="search-input"
                value={leadSearch}
                onChange={(event) => setLeadSearch(event.target.value)}
                placeholder="Buscar por nome, telefone, stage ou status"
              />
            </div>

            <div className="lead-grid">
              <div className="lead-list">
                {filteredLeads.map((lead) => (
                  <button
                    key={lead.id}
                    className={`lead-list-item ${selectedLead?.id === lead.id ? 'selected' : ''}`}
                    type="button"
                    onClick={() => handleSelectLead(lead.id)}
                  >
                    <div className="lead-line">
                      <strong>{lead.name}</strong>
                      <span className={`pill pill-${lead.health}`}>{lead.health}</span>
                    </div>
                    <div className="lead-line">
                      <span>{lead.phone}</span>
                      <span>{lead.clickupStage}</span>
                    </div>
                    <small>{lead.lastAction}</small>
                  </button>
                ))}
              </div>

              {selectedLead ? (
                <article className="lead-detail">
                  <div className="lead-detail-header">
                    <div>
                      <p className="eyebrow">Lead selecionado</p>
                      <h3>{selectedLead.name}</h3>
                    </div>
                    <div className="detail-actions">
                      <button
                        type="button"
                        disabled={config.mode === 'live'}
                        onClick={() => handleReprocessLead(selectedLead.id)}
                      >
                        Reprocessar
                      </button>
                      <button
                        className="ghost-button"
                        type="button"
                        disabled={config.mode === 'live'}
                        onClick={() => handleToggleSync(selectedLead.id)}
                      >
                        {selectedLead.syncEnabled ? 'Pausar sync' : 'Retomar sync'}
                      </button>
                    </div>
                  </div>

                  <div className="detail-grid">
                    <div>
                      <span className="detail-label">Telefone</span>
                      <strong>{selectedLead.phone}</strong>
                    </div>
                    <div>
                      <span className="detail-label">ClickUp</span>
                      <strong>{selectedLead.clickupTaskId ?? 'nao identificado'}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Bradial contact</span>
                      <strong>{selectedLead.chatContactId}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Conversa ativa</span>
                      <strong>{selectedLead.chatConversationId ?? selectedLead.conversationId ?? 'nao identificado'}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Etapa comercial</span>
                      <strong>{selectedLead.clickupStage ?? 'nao identificado'}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Status da conversa</span>
                      <strong>{selectedLead.chatStatus}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Owner</span>
                      <strong>{selectedLead.owner ?? 'nao atribuido'}</strong>
                    </div>
                    <div>
                      <span className="detail-label">Ultimo sync</span>
                      <strong>{formatDate(selectedLead.lastSyncAt)}</strong>
                    </div>
                  </div>

                  <div className="tag-row">
                    {(selectedLead.tags ?? []).map((tag) => (
                      <span key={tag} className="tag-chip">
                        {tag}
                      </span>
                    ))}
                  </div>

                  <div className="lead-notes">
                    <p>{selectedLead.summary}</p>
                    <small>Ultima acao: {selectedLead.lastAction}</small>
                  </div>

                  <div className="mini-log-list">
                    <div className="section-heading compact">
                      <h4>Ultimos eventos deste lead</h4>
                    </div>
                    {selectedLeadLogs.length === 0 ? (
                      <p className="empty-inline">Nenhum evento local para este lead ainda.</p>
                    ) : (
                      selectedLeadLogs.map((entry) => (
                        <div key={entry.id} className="mini-log-item">
                          <span className={`log-dot tone-${entry.level}`} />
                          <div>
                            <strong>{entry.title}</strong>
                            <p>{entry.message}</p>
                          </div>
                          <small>{formatDate(entry.createdAt)}</small>
                        </div>
                      ))
                    )}
                  </div>
                </article>
              ) : null}
            </div>
          </section>

          <section className="panel section-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Telemetria</p>
                <h2>Stream de logs</h2>
              </div>
              <div className="detail-actions">
                {config.mode === 'mock' ? (
                  <button type="button" onClick={runMockTick}>
                    Gerar evento
                  </button>
                ) : (
                  <button type="button" onClick={() => void refreshBackendSnapshot()}>
                    Atualizar snapshot
                  </button>
                )}
                <button className="ghost-button" type="button" onClick={clearLogs}>
                  Limpar logs
                </button>
              </div>
            </div>

            <div className="logs-panel">
              {logs.length === 0 ? (
                <div className="empty-state">
                  <strong>Nenhum log carregado</strong>
                  <p>Gere um evento mock ou conecte um backend real para popular a lista.</p>
                </div>
              ) : (
                logs.map((entry) => (
                  <article key={entry.id} className="log-row">
                    <div className={`log-level tone-${entry.level}`}>{entry.level}</div>
                    <div className="log-content">
                      <strong>{entry.title}</strong>
                      <p>{entry.message}</p>
                    </div>
                    <div className="log-meta">
                      <span>{entry.leadId || 'global'}</span>
                      <small>{formatDate(entry.createdAt)}</small>
                    </div>
                  </article>
                ))
              )}
            </div>
          </section>
        </main>
      </section>
    </div>
  )
}

export default App
