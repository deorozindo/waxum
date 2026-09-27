# Durable human-paced sends

RZDO production has **no business-hour window**, by captain order on 2026-09-27. Both services configure WAXUM_SEND_OPEN_HOUR=0, WAXUM_SEND_CLOSE_HOUR=24, WAXUM_SEND_WEEKDAYS=7. All chats/groups can dispatch 24/7, still behind cooldowns and hourly ceilings. WAXUM_SEND_CAPTAIN_CHATS identifies the private captain PN/LID only for its separate 60/hour chat ceiling; other chats stay at 12/hour.

All POST message send routes (including polls, media, reactions, edits, pins and revokes) and status reactions now return **202 Accepted**, for every session. No request option bypasses admission. Existing future `send_at` still works.

Response: `{"status":"pending","schedule_id":"<uuid>","send_at":"<UTC time>"}`. This confirms durable enqueue, not WhatsApp delivery. Poll `GET /api/v1/sessions/{sid}/scheduled` and match `id` to `schedule_id`. Final status is `sent` with `message_id`, or `failed` with `error`. Webhooks and NATS publish `scheduled_sent` / `scheduled_failed`. Do not retry a send just because a 202 response has no `message_id`.

Successful PN→LID lookups are persisted in the shared ledger. Historical PN attempts migrate to the canonical LID, so later phone fallbacks keep the same counters and cooldown. Known captain PN/LID aliases are also aggregated even before a lookup succeeds.

The existing `scheduled_messages` database stores requests. A SQLite WAL ledger stores attempts, rolling hourly counters and sampled next-send times. Reservations use `BEGIN IMMEDIATE`: independent gateway processes must share `WAXUM_SEND_LEDGER_PATH` on the **same local Docker volume** to enforce per-chat limits across them. Keep both databases on persistent volumes. Never put the ledger on NFS. A standalone instance defaults to `<WHATSAPP_STORAGE_PATH>/send-ledger.sqlite`.

Configuration is read at startup; invalid values fail startup. Change environment variables and recreate only the waxum service through the deployment manager. Defaults:

| Variable | Default |
|---|---|
| WAXUM_SEND_SESSION_MIN_SECONDS | 8 |
| WAXUM_SEND_SESSION_MAX_SECONDS | 20 |
| WAXUM_SEND_CHAT_MIN_SECONDS | 8 |
| WAXUM_SEND_CHAT_MAX_SECONDS | 20 |
| WAXUM_SEND_SESSION_PER_HOUR | 60 |
| WAXUM_SEND_CHAT_PER_HOUR | 12 |
| WAXUM_SEND_CAPTAIN_PER_HOUR | 60 |
| WAXUM_SEND_OPEN_HOUR | 8 |
| WAXUM_SEND_CLOSE_HOUR | 19 (exclusive) |
| WAXUM_SEND_WEEKDAYS | 6 (Monday through Saturday) |
| WAXUM_SEND_TIMEZONE | America/Sao_Paulo |
| WAXUM_SEND_CAPTAIN_CHATS | empty, comma-separated canonical captain PN/LID JIDs |
| WAXUM_SEND_LEDGER_PATH | persistent session directory/send-ledger.sqlite |

WAXUM_SEND_CAPTAIN_CHATS identifies only the private captain chat by canonical PN and resolved LID. Its chat ceiling is WAXUM_SEND_CAPTAIN_PER_HOUR (default 60/hour), independently of other chats’ 12/hour ceiling. Both cooldowns and the session ceiling still apply. Default is no captain alias; configure only the captain’s own aliases, never a group. The aliases also bypass a restricted window if an operator configures one, but RZDO production disables that window for everyone through 0/24/7. Defaults below the production policy are library fallbacks; production explicitly overrides them.

Each admission samples independent inclusive session/chat delays and persists them. Scheduler granularity can add delay; timestamps round conservatively by one second to avoid allowing a send less than eight seconds later. Hourly limits use a rolling hour, count attempts conservatively (including failures), and persist across restarts. Media preparation happens before admission; the protocol send itself is gated. Blasts and NATS use the same final gate. JetStream receives progress acknowledgements while a command waits, preventing timeout-driven redelivery during closed hours.

Pending requests resume after restart. At startup their original requested send_at (or creation time when absent) is restored for a fresh admission check, so policy changes release prior business-window deferrals without overriding explicit future schedules. Rows interrupted in `sending` become `failed` with delivery-unknown error: they are retained and never automatically replayed, because a crash after WhatsApp accepts the packet cannot safely prove delivery or justify a duplicate. Reconciliation/retry is an operator choice. Preserve volumes when updating; do not export, logout, or DELETE sessions.

## Received mentions

`wa.events.<session>.message` includes `data.mentioned_jids`, an array of JID strings (empty when absent), from any supported protobuf content with `contextInfo`. Ephemeral/view-once wrappers are unwrapped. If the session's own PN or LID is explicitly mentioned, both known aliases are included. Consumers can compare either alias to enforce their group reply rule; the gateway does not generate automatic group replies.

Example using synthetic identifiers: `{"mentioned_jids":["5511000000000@s.whatsapp.net","100000000000000@lid"]}`. This is additive; existing quoted-message fields remain.
