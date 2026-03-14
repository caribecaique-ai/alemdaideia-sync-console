export function normalizeText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase()
}

export function digitsOnly(value) {
  return String(value || '').replace(/\D/g, '')
}

export function normalizePhone(value) {
  const digits = digitsOnly(value)
  if (!digits) return null
  if (String(value).trim().startsWith('+')) return `+${digits}`
  if (digits.startsWith('55')) return `+${digits}`
  return `+55${digits}`
}

export function buildPhoneVariants(value) {
  const normalizedPhone = normalizePhone(value)
  const digits = digitsOnly(normalizedPhone)
  if (!digits) return new Set()

  const variants = new Set([digits])

  if (/^55\d{2}9\d{8}$/.test(digits)) {
    variants.add(`${digits.slice(0, 4)}${digits.slice(5)}`)
  }

  if (/^55\d{2}\d{8}$/.test(digits)) {
    variants.add(`${digits.slice(0, 4)}9${digits.slice(4)}`)
  }

  return variants
}

export function normalizePhoneLooseKey(value) {
  const variants = [...buildPhoneVariants(value)]
  if (!variants.length) return null

  return [...variants].sort((left, right) => left.length - right.length || left.localeCompare(right))[0]
}

export function phonesMatchLoose(left, right) {
  const leftVariants = buildPhoneVariants(left)
  const rightVariants = buildPhoneVariants(right)

  if (!leftVariants.size || !rightVariants.size) return false

  for (const item of leftVariants) {
    if (rightVariants.has(item)) return true
  }

  return false
}

export function phoneFieldLooksRelevant(fieldName) {
  const normalized = normalizeText(fieldName)
  return ['whatsapp', 'telefone', 'phone', 'celular', 'fone'].some((token) =>
    normalized.includes(token),
  )
}
