import dotenv from 'dotenv'
import { createBradialAdapter } from '../src/adapters/bradial.js'
import { createClickupAdapter } from '../src/adapters/clickup.js'
import {
  buildStageLabelMap,
  listControlledStageLabels,
  normalizeLabelKey,
  resolveClickupStatusFromLabel,
} from '../src/services/clickupStageLabels.js'
import {
  mapChatPriorityToClickupPriorityValue,
  mapUrgencyToChatPriority,
} from '../src/services/clickupLeadContext.js'

dotenv.config()

const logs = []

function pushLog(level, title, message, context = {}) {
  logs.push({
    at: new Date().toISOString(),
    level,
    title,
    message,
    context,
  })
}

const clickup = createClickupAdapter(
  {
    apiKey: process.env.CLICKUP_API_KEY,
    workspaceId: process.env.CLICKUP_WORKSPACE_ID,
    workspaceName: process.env.CLICKUP_WORKSPACE_NAME,
    commercialSpaceName: process.env.CLICKUP_COMMERCIAL_SPACE_NAME,
    commercialFolderName: process.env.CLICKUP_COMMERCIAL_FOLDER_NAME,
    commercialListName: process.env.CLICKUP_COMMERCIAL_LIST_NAME,
    maxListPages: process.env.CLICKUP_MAX_LIST_PAGES,
    backupClientName: process.env.CLICKUP_BACKUP_CLIENT_NAME,
    clientsBackupPath: process.env.CLICKUP_CLIENTS_BACKUP_PATH,
    urgencyFieldNames: process.env.CLICKUP_URGENCY_FIELD_NAMES,
  },
  pushLog,
)

const bradial = createBradialAdapter(
  {
    baseUrl: process.env.BRADIAL_BASE_URL,
    accountId: process.env.BRADIAL_ACCOUNT_ID,
    inboxId: process.env.BRADIAL_INBOX_ID,
    apiToken: process.env.BRADIAL_API_TOKEN,
    opportunityLabel: process.env.BRADIAL_OPPORTUNITY_LABEL,
    syncConversationLabels: process.env.BRADIAL_SYNC_CONVERSATION_LABELS,
    syncContactLabels: process.env.BRADIAL_SYNC_CONTACT_LABELS,
    chatBaseUrl: process.env.BRADIAL_CHAT_BASE_URL,
    chatAccountId: process.env.BRADIAL_CHAT_ACCOUNT_ID,
    chatApiToken: process.env.BRADIAL_CHAT_API_TOKEN,
    chatInboxId: process.env.BRADIAL_CHAT_INBOX_ID,
    maxPages: process.env.BRADIAL_MAX_PAGES,
    requestMaxAttempts: process.env.BRADIAL_REQUEST_MAX_ATTEMPTS,
    requestRetryBaseMs: process.env.BRADIAL_REQUEST_RETRY_BASE_MS,
    conversationSearchPages: process.env.BRADIAL_CONVERSATION_SEARCH_PAGES,
    labelVerifyAttempts: process.env.BRADIAL_LABEL_VERIFY_ATTEMPTS,
    labelVerifyDelayMs: process.env.BRADIAL_LABEL_VERIFY_DELAY_MS,
    stageLabelMap: process.env.CLICKUP_STAGE_LABEL_MAP,
    syncConversationPriority: process.env.BRADIAL_SYNC_CONVERSATION_PRIORITY,
    syncClosedByAssignment: process.env.BRADIAL_SYNC_CLOSED_BY_ASSIGNMENT,
    syncClosedByAttributes: process.env.BRADIAL_SYNC_CLOSED_BY_ATTRIBUTES,
    agentAliasMap: process.env.BRADIAL_AGENT_ALIAS_MAP,
    closedStageLabels: process.env.CLICKUP_CLOSED_STAGE_LABELS,
  },
  pushLog,
)

function buildPriorityMatrix() {
  return [
    'Urgente',
    'Alta',
    'Normal',
    'Baixa',
    'Nenhuma',
  ].map((label) => {
    const chatPriority = mapUrgencyToChatPriority(label)
    return {
      clickup: label,
      chat: chatPriority,
      clickupApiValue: mapChatPriorityToClickupPriorityValue(chatPriority),
    }
  })
}

async function main() {
  const stageLabelMap = buildStageLabelMap(process.env.CLICKUP_STAGE_LABEL_MAP)
  const [clickupSnapshot, bradialSnapshot, chatLabels] = await Promise.all([
    clickup.fetchSnapshot('audit-sync-script'),
    bradial.fetchSnapshot('audit-sync-script'),
    bradial.listChatAccountLabels().catch(() => []),
  ])

  const controlledStageLabels = listControlledStageLabels(clickupSnapshot.tasks || [], stageLabelMap)
  const normalizedChatLabelKeys = new Set(
    (chatLabels || []).map((label) => normalizeLabelKey(label)).filter(Boolean),
  )
  const missingBradialLabels = controlledStageLabels.filter(
    (label) => !normalizedChatLabelKeys.has(normalizeLabelKey(label)),
  )

  const reverseStageMap = controlledStageLabels.map((label) => ({
    label,
    clickupStatus: resolveClickupStatusFromLabel(label, stageLabelMap),
  }))
  const unmappedReverseLabels = reverseStageMap.filter((item) => !item.clickupStatus)
  const priorityMatrix = buildPriorityMatrix()
  const invalidPriorityMappings = priorityMatrix.filter(
    (item) => item.clickup !== 'Nenhuma' && (!item.chat || item.clickupApiValue == null),
  )

  const result = {
    ok:
      missingBradialLabels.length === 0 &&
      unmappedReverseLabels.length === 0 &&
      invalidPriorityMappings.length === 0,
    checkedAt: new Date().toISOString(),
    clickup: {
      workspace: clickupSnapshot.workspace,
      listCount: clickupSnapshot.navigation?.lists?.length || 0,
      taskCount: clickupSnapshot.tasks?.length || 0,
      controlledStageLabels,
    },
    bradial: {
      chatEnabled: bradial.chatEnabled,
      leadCount: bradialSnapshot.leads?.length || 0,
      chatAgentCount: bradialSnapshot.chatAgents?.length || 0,
      chatLabels,
    },
    sync: {
      refreshMs: Number(process.env.BRADIAL_REFRESH_MS || 0),
      conversationLabelSync: !['0', 'false', 'no', 'off'].includes(
        String(process.env.BRADIAL_SYNC_CONVERSATION_LABELS || 'true').trim().toLowerCase(),
      ),
      contactLabelSync: !['0', 'false', 'no', 'off'].includes(
        String(process.env.BRADIAL_SYNC_CONTACT_LABELS || 'false').trim().toLowerCase(),
      ),
      conversationPrioritySync: !['0', 'false', 'no', 'off'].includes(
        String(process.env.BRADIAL_SYNC_CONVERSATION_PRIORITY || 'true').trim().toLowerCase(),
      ),
      closedByAssignmentSync: !['0', 'false', 'no', 'off'].includes(
        String(process.env.BRADIAL_SYNC_CLOSED_BY_ASSIGNMENT || 'true').trim().toLowerCase(),
      ),
      closedByAttributesSync: !['0', 'false', 'no', 'off'].includes(
        String(process.env.BRADIAL_SYNC_CLOSED_BY_ATTRIBUTES || 'true').trim().toLowerCase(),
      ),
    },
    coverage: {
      reverseStageMap,
      missingBradialLabels,
      unmappedReverseLabels,
      priorityMatrix,
      invalidPriorityMappings,
    },
    recentLogs: logs.slice(-20),
  }

  console.log(JSON.stringify(result, null, 2))
  process.exit(result.ok ? 0 : 1)
}

main().catch((error) => {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: error.message,
        recentLogs: logs.slice(-20),
      },
      null,
      2,
    ),
  )
  process.exit(1)
})
