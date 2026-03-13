export function normalizeText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase()
}

export function normalizePhone(value) {
  const digits = String(value || '').replace(/\D/g, '')
  if (!digits) return null
  if (String(value).trim().startsWith('+')) return `+${digits}`
  if (digits.startsWith('55')) return `+${digits}`
  return `+55${digits}`
}

export function phoneFieldLooksRelevant(fieldName) {
  const normalized = normalizeText(fieldName)
  return ['whatsapp', 'telefone', 'phone', 'celular', 'fone'].some((token) =>
    normalized.includes(token),
  )
}
