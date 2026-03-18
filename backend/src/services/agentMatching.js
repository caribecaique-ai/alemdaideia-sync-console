import { normalizeText } from '../utils/normalizers.js'

function parseAliasMap(input) {
  if (!input) return {}
  if (typeof input === 'object' && !Array.isArray(input)) return input

  const raw = String(input || '').trim()
  if (!raw) return {}

  try {
    const parsed = JSON.parse(raw)
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed
    }
  } catch {}

  return {}
}

function unique(values = []) {
  return [...new Set(values.map((item) => String(item || '').trim()).filter(Boolean))]
}

function normalizeAgent(agent = {}) {
  const id = agent?.id == null ? null : String(agent.id)
  const names = unique([agent.name, agent.available_name, agent.display_name, agent.username])
  const emails = unique([agent.email, agent.mail, agent?.user?.email])

  return {
    id,
    raw: agent,
    names,
    emails,
    normalizedNames: names.map((item) => normalizeText(item)).filter(Boolean),
    normalizedEmails: emails.map((item) => normalizeText(item)).filter(Boolean),
  }
}

function normalizeActor(actor = {}) {
  const names = unique([actor.name, actor.username])
  const emails = unique([actor.email])

  return {
    id: actor?.id == null ? null : String(actor.id),
    names,
    emails,
    normalizedNames: names.map((item) => normalizeText(item)).filter(Boolean),
    normalizedEmails: emails.map((item) => normalizeText(item)).filter(Boolean),
  }
}

function matchByTarget(agents, target) {
  const normalizedTarget = normalizeText(target)
  if (!normalizedTarget) return []

  return agents.filter((agent) => {
    if (String(agent.id || '') === String(target || '')) return true
    if (agent.normalizedEmails.includes(normalizedTarget)) return true
    if (agent.normalizedNames.includes(normalizedTarget)) return true
    return false
  })
}

export function matchChatAgent(chatAgents = [], actor = null, aliasMapInput = null) {
  if (!actor) {
    return {
      matched: false,
      reason: 'missing_actor',
      agent: null,
      candidates: [],
    }
  }

  const agents = (chatAgents || []).map((agent) => normalizeAgent(agent)).filter((agent) => agent.id)
  if (!agents.length) {
    return {
      matched: false,
      reason: 'missing_agents',
      agent: null,
      candidates: [],
    }
  }

  const normalizedActor = normalizeActor(actor)
  const aliasMap = parseAliasMap(aliasMapInput)
  const actorKeys = [
    ...(normalizedActor.id ? [normalizedActor.id] : []),
    ...normalizedActor.normalizedEmails,
    ...normalizedActor.normalizedNames,
  ]

  for (const actorKey of actorKeys) {
    const aliasTarget = aliasMap[actorKey]
    if (!aliasTarget) continue
    const aliased = matchByTarget(agents, aliasTarget)

    if (aliased.length === 1) {
      return {
        matched: true,
        reason: 'alias',
        agent: aliased[0].raw,
        candidates: aliased.map((item) => item.raw),
      }
    }

    if (aliased.length > 1) {
      return {
        matched: false,
        ambiguous: true,
        reason: 'alias_ambiguous',
        agent: null,
        candidates: aliased.map((item) => item.raw),
      }
    }
  }

  const emailMatches = agents.filter((agent) =>
    normalizedActor.normalizedEmails.some((email) => agent.normalizedEmails.includes(email)),
  )

  if (emailMatches.length === 1) {
    return {
      matched: true,
      reason: 'email',
      agent: emailMatches[0].raw,
      candidates: emailMatches.map((item) => item.raw),
    }
  }

  if (emailMatches.length > 1) {
    return {
      matched: false,
      ambiguous: true,
      reason: 'email_ambiguous',
      agent: null,
      candidates: emailMatches.map((item) => item.raw),
    }
  }

  const nameMatches = agents.filter((agent) =>
    normalizedActor.normalizedNames.some((name) => agent.normalizedNames.includes(name)),
  )

  if (nameMatches.length === 1) {
    return {
      matched: true,
      reason: 'name',
      agent: nameMatches[0].raw,
      candidates: nameMatches.map((item) => item.raw),
    }
  }

  if (nameMatches.length > 1) {
    return {
      matched: false,
      ambiguous: true,
      reason: 'name_ambiguous',
      agent: null,
      candidates: nameMatches.map((item) => item.raw),
    }
  }

  return {
    matched: false,
    reason: 'no_match',
    agent: null,
    candidates: [],
  }
}
