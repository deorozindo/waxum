//! RZDO fork: on-demand history fetch (ingest-only use). Asks the primary phone for older messages of one
//! chat through whatsapp-rust's `fetch_message_history` (PDO HISTORY_SYNC_ON_DEMAND). The answer arrives
//! later as an `Event::HistorySync`, which `sessions.rs` publishes as `history_message` events.
//! This module never sends a chat message: the only outbound frame is the peer-data request to the
//! account's own primary device.
use axum::{
    extract::{Path, State},
    Json,
};

use crate::error::ApiError;
use crate::state::AppState;

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct HistoryFetchRequest {
    /// Chat JID (group `...@g.us` or user)
    pub chat: String,
    /// Oldest message we already hold in that chat: the phone returns messages older than this one
    pub oldest_msg_id: String,
    #[serde(default)]
    pub oldest_msg_from_me: bool,
    /// Timestamp of that message, in milliseconds
    pub oldest_msg_timestamp_ms: i64,
    /// How many older messages to ask for (the phone may send fewer)
    #[serde(default = "default_count")]
    pub count: i32,
}

fn default_count() -> i32 {
    50
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct HistoryFetchResponse {
    pub requested: bool,
    pub request_id: String,
}

pub async fn fetch_history(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(req): Json<HistoryFetchRequest>,
) -> Result<Json<HistoryFetchResponse>, ApiError> {
    let runtime = state
        .get_session(&session_id)
        .ok_or_else(|| ApiError::SessionNotFound(session_id.clone()))?;
    let client = runtime.get_client().ok_or(ApiError::NotConnected)?;
    let chat = crate::handlers::messages::parse_jid(&req.chat)?;
    let id = client
        .fetch_message_history(
            &chat,
            &req.oldest_msg_id,
            req.oldest_msg_from_me,
            req.oldest_msg_timestamp_ms,
            req.count.clamp(1, 500),
        )
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(HistoryFetchResponse { requested: true, request_id: id }))
}
