"""Notion chat-history endpoint names."""

from app.notion_hosts import notion_web_api_base

API_BASE = notion_web_api_base()

GET_TRANSCRIPTS = "/getInferenceTranscriptsForUser"
SYNC_RECORD_VALUES_SPACE_INITIAL = "/syncRecordValuesSpaceInitial"
GET_UNREAD_COUNT = "/getInferenceTranscriptsUnreadCount"
MARK_SEEN = "/markInferenceTranscriptSeen"
SAVE_TRANSACTIONS = "/saveTransactions"
SAVE_TRANSACTIONS_FANOUT = "/saveTransactionsFanout"

CHAT_ENDPOINTS = {
    GET_TRANSCRIPTS: "chat_list",
    SYNC_RECORD_VALUES_SPACE_INITIAL: "record_hydration",
    GET_UNREAD_COUNT: "unread_count",
    MARK_SEEN: "mark_seen",
    SAVE_TRANSACTIONS: "mutation",
    SAVE_TRANSACTIONS_FANOUT: "mutation",
}
