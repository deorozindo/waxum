use axum::{
    extract::{Path, State},
    Json,
};
use wacore_binary::jid::Jid;
use waproto::buffa::MessageField;
use waproto::whatsapp as wa;

use crate::error::ApiError;
use crate::models::messages::MessageResponse;
use crate::models::schedule::SendResponse;
use crate::models::status::StatusReactionRequest;
use crate::state::AppState;

#[utoipa::path(
    post,
    security(("bearer_auth" = [])),
    path = "/api/v1/sessions/{session_id}/status/react",
    tag = "status",
    params(
        ("session_id" = String, Path, description = "Session ID")
    ),
    request_body = StatusReactionRequest,
    responses(
        (status = 202, description = "Status reaction queued", body = SendResponse),
        (status = 400, description = "Invalid JID"),
        (status = 404, description = "Session not found"),
        (status = 503, description = "Not connected")
    )
)]
pub async fn send_status_reaction(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(request): Json<StatusReactionRequest>,
) -> Result<(axum::http::StatusCode, Json<SendResponse>), ApiError> {
    let response = crate::handlers::schedule::maybe_schedule(
        &state,
        &session_id,
        "status-react",
        &request,
        None,
    )
    .await?
    .unwrap();
    Ok((axum::http::StatusCode::ACCEPTED, Json(response)))
}

pub async fn execute_status_reaction(
    state: &AppState,
    session_id: &str,
    request: StatusReactionRequest,
) -> Result<MessageResponse, ApiError> {
    let client = get_client(state, session_id)?;
    let owner: Jid = request
        .status_owner
        .parse()
        .map_err(|_| ApiError::InvalidJid(request.status_owner.clone()))?;

    let status_broadcast = Jid::status_broadcast();
    let now_ms = chrono::Utc::now().timestamp_millis();

    let message = wa::Message {
        reaction_message: MessageField::some(wa::message::ReactionMessage {
            key: Some(wa::MessageKey {
                remote_jid: Some(status_broadcast.to_string()),
                from_me: Some(false),
                id: Some(request.message_id.clone()),
                participant: Some(owner.to_string()),
            })
            .into(),
            text: Some(request.reaction.clone()),
            grouping_key: None,
            sender_timestamp_ms: Some(now_ms),
        }),
        ..Default::default()
    };

    crate::send_limiter::acquire(session_id, &status_broadcast.to_string())
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let sent = client
        .send_message(status_broadcast.clone(), message)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(MessageResponse {
        message_id: sent.message_id,
        timestamp: chrono::Utc::now().timestamp(),
        to: status_broadcast.to_string(),
    })
}

fn get_client(
    state: &AppState,
    session_id: &str,
) -> Result<std::sync::Arc<whatsapp_rust::Client>, ApiError> {
    let runtime = state
        .get_session(session_id)
        .ok_or(ApiError::NotConnected)?;

    runtime.get_live_client().ok_or(ApiError::NotConnected)
}
