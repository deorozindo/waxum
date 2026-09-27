//! Durable send queue, backed by the existing scheduled_messages table.
//! All send handlers enqueue; the scheduler checks business hours and rate
//! eligibility before dispatch. Results appear in the session scheduled
//! endpoint, webhooks and NATS scheduled_sent / scheduled_failed events.

use std::time::Duration;

use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::{DateTime, Utc};

use crate::db::scheduled::{self, ScheduledRow};
use crate::error::ApiError;
use crate::models::messages::MessageResponse;
use crate::models::schedule::{
    ScheduledFleetQuery, ScheduledListResponse, ScheduledMessage, ScheduledSessionQuery,
    ScheduledStatus, SendResponse,
};
use crate::models::webhooks::WebhookEvent;
use crate::state::AppState;

/// Max rows examined per scheduler tick. Deferred rows leave the due set.
const DUE_BATCH_LIMIT: i64 = 50;

/// Park every send durably. The optional timestamp delays admission further.
pub async fn maybe_schedule<T: serde::Serialize>(
    state: &AppState,
    session_id: &str,
    endpoint: &str,
    request: &T,
    send_at: Option<DateTime<Utc>>,
) -> Result<Option<SendResponse>, ApiError> {
    let send_at = send_at.unwrap_or_else(Utc::now).max(Utc::now());
    if state
        .session_manager()
        .get_session(session_id)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .is_none()
    {
        return Err(ApiError::SessionNotFound(session_id.to_string()));
    }
    let value = serde_json::to_value(request).map_err(|e| ApiError::Internal(e.to_string()))?;
    let chat = value
        .get("to")
        .or_else(|| value.get("chat"))
        .or_else(|| value.get("status_owner"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::BadRequest("missing recipient".into()))?;
    crate::handlers::messages::parse_jid(chat)?;
    let id = uuid::Uuid::new_v4().to_string();
    let body = serde_json::to_string(request)
        .map_err(|e| ApiError::Internal(format!("failed to serialize scheduled body: {e}")))?;
    scheduled::insert(
        state.session_manager().pool(),
        &id,
        session_id,
        endpoint,
        &body,
        send_at,
    )
    .await
    .map_err(|e| ApiError::Internal(format!("failed to store scheduled message: {e}")))?;
    Ok(Some(SendResponse::scheduled(id, send_at)))
}

#[utoipa::path(
    get,
    security(("bearer_auth" = [])),
    path = "/api/v1/sessions/{session_id}/scheduled",
    tag = "scheduler",
    params(
        ("session_id" = String, Path, description = "Session ID"),
        ScheduledSessionQuery,
    ),
    responses(
        (status = 200, description = "Scheduled messages for the session", body = ScheduledListResponse)
    )
)]
pub async fn list_session_scheduled(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(q): Query<ScheduledSessionQuery>,
) -> Result<Json<ScheduledListResponse>, ApiError> {
    let rows = scheduled::list(
        state.session_manager().pool(),
        Some(&session_id),
        q.status.as_deref(),
    )
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(rows_to_response(rows)))
}

#[utoipa::path(
    delete,
    security(("bearer_auth" = [])),
    path = "/api/v1/sessions/{session_id}/scheduled/{id}",
    tag = "scheduler",
    params(
        ("session_id" = String, Path, description = "Session ID"),
        ("id" = String, Path, description = "Scheduled message ID")
    ),
    responses(
        (status = 200, description = "Scheduled message cancelled", body = ScheduledMessage),
        (status = 400, description = "Scheduled message is not pending"),
        (status = 404, description = "Scheduled message not found")
    )
)]
pub async fn cancel_scheduled(
    State(state): State<AppState>,
    Path((session_id, id)): Path<(String, String)>,
) -> Result<Json<ScheduledMessage>, ApiError> {
    let pool = state.session_manager().pool();
    let row = scheduled::get(pool, &session_id, &id)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::MessageNotFound(format!("scheduled message {id}")))?;
    if row.status != ScheduledStatus::Pending.as_str() {
        return Err(ApiError::BadRequest(format!(
            "scheduled message {id} is {}, only pending messages can be cancelled",
            row.status
        )));
    }
    if !scheduled::cancel_pending(pool, &session_id, &id)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
    {
        return Err(ApiError::BadRequest(format!(
            "scheduled message {id} was claimed by the scheduler; try again"
        )));
    }
    let row = scheduled::get(pool, &session_id, &id)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::MessageNotFound(format!("scheduled message {id}")))?;
    Ok(Json(row_to_model(&row)))
}

#[utoipa::path(
    get,
    security(("bearer_auth" = [])),
    path = "/api/v1/scheduled",
    tag = "scheduler",
    params(
        ScheduledFleetQuery,
    ),
    responses(
        (status = 200, description = "Scheduled messages across all sessions", body = ScheduledListResponse)
    )
)]
pub async fn list_all_scheduled(
    State(state): State<AppState>,
    Query(q): Query<ScheduledFleetQuery>,
) -> Result<Json<ScheduledListResponse>, ApiError> {
    let rows = scheduled::list(
        state.session_manager().pool(),
        q.session.as_deref(),
        q.status.as_deref(),
    )
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(rows_to_response(rows)))
}

fn rows_to_response(rows: Vec<ScheduledRow>) -> ScheduledListResponse {
    let messages: Vec<ScheduledMessage> = rows.iter().map(row_to_model).collect();
    ScheduledListResponse {
        count: messages.len(),
        messages,
    }
}

fn row_to_model(row: &ScheduledRow) -> ScheduledMessage {
    ScheduledMessage {
        id: row.id.clone(),
        session_id: row.session_id.clone(),
        endpoint: row.endpoint.clone(),
        send_at: parse_stored_ts(&row.send_at),
        status: ScheduledStatus::from_str(&row.status),
        error: row.error.clone(),
        message_id: row.message_id.clone(),
        created_at: parse_stored_ts(&row.created_at),
        updated_at: parse_stored_ts(&row.updated_at),
    }
}

/// Parse a timestamp coming out of the DB layer: the canonical
/// `%Y-%m-%d %H:%M:%S` UTC text, its sub-second variant, or RFC 3339
/// (Postgres drivers occasionally hand back their own rendering on
/// hand-migrated rows). Unparseable values degrade to the Unix epoch
/// rather than failing the whole listing.
fn parse_stored_ts(s: &str) -> DateTime<Utc> {
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return dt.and_utc();
        }
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.with_timezone(&Utc);
    }
    DateTime::UNIX_EPOCH
}

/// Restore original requested times so a new policy can recheck deferred rows.
pub async fn recheck_pending(state: &AppState) -> anyhow::Result<()> {
    let pool = state.session_manager().pool();
    for row in scheduled::list(pool, None, Some("pending")).await? {
        let body: serde_json::Value = serde_json::from_str(&row.body)?;
        let requested = match body.get("send_at") {
            Some(serde_json::Value::String(value)) => {
                DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc)
            }
            None | Some(serde_json::Value::Null) => parse_stored_ts(&row.created_at),
            _ => continue,
        };
        scheduled::defer(pool, &row.id, requested).await?;
    }
    Ok(())
}

pub async fn run_scheduler(state: AppState) {
    if let Err(e) = recheck_pending(&state).await {
        tracing::error!("cannot recheck pending queue: {e}");
        return;
    }
    match scheduled::list(state.session_manager().pool(), None, Some("sending")).await {
        Ok(rows) => {
            for row in rows {
                if let Err(e) = scheduled::mark_failed(
                    state.session_manager().pool(),
                    &row.id,
                    "delivery unknown after restart; not replayed to avoid duplicate",
                )
                .await
                {
                    tracing::error!("cannot settle interrupted send: {e}");
                    return;
                }
            }
        }
        Err(e) => {
            tracing::error!("cannot recover send queue: {e}");
            return;
        }
    }
    let poll_ms: u64 = std::env::var("SCHEDULER_POLL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let mut ticker = tokio::time::interval(Duration::from_millis(poll_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tracing::info!(poll_ms, "scheduled-send dispatcher started");
    loop {
        ticker.tick().await;
        if let Err(e) = process_due(&state).await {
            tracing::warn!("scheduled-send tick failed: {e}");
        }
    }
}

/// One scheduler tick: fetch due pending rows, claim each (so a
/// concurrent cancel or a second poller loses the race), dispatch, and
/// settle the row plus its webhook event.
async fn process_due(state: &AppState) -> anyhow::Result<()> {
    let pool = state.session_manager().pool();
    let due = scheduled::due_pending(pool, DUE_BATCH_LIMIT).await?;
    for row in due {
        let body: serde_json::Value = serde_json::from_str(&row.body)?;
        let chat = body
            .get("to")
            .or_else(|| body.get("chat"))
            .or_else(|| body.get("status_owner"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("queued send missing chat"))?;
        let client = match crate::handlers::messages::get_client(state, &row.session_id) {
            Ok(client) => client,
            Err(_) => {
                scheduled::defer(pool, &row.id, Utc::now() + chrono::Duration::seconds(30)).await?;
                continue;
            }
        };
        let chat = if row.endpoint == "status-react" {
            "status@broadcast"
        } else {
            chat
        };
        let jid = crate::handlers::messages::parse_jid(chat).map_err(|e| anyhow::anyhow!("{e}"))?;
        let jid = crate::handlers::messages::resolve_recipient_jid(client, jid).await?;
        let (next, _) =
            crate::send_limiter::due(&row.session_id, &jid.to_non_ad_string(), false).await?;
        if next > Utc::now().timestamp() {
            scheduled::defer(pool, &row.id, DateTime::from_timestamp(next, 0).unwrap()).await?;
            continue;
        }
        if !scheduled::claim(pool, &row.id).await? {
            continue;
        }
        match dispatch(state, &row.session_id, &row.endpoint, &row.body).await {
            Ok(resp) => {
                if let Err(e) = scheduled::mark_sent(pool, &row.id, &resp.message_id).await {
                    tracing::warn!(
                        "scheduled {} sent (message {}) but mark_sent failed: {}",
                        row.id,
                        resp.message_id,
                        e
                    );
                }
                let payload = serde_json::json!({
                    "schedule_id": row.id,
                    "endpoint": row.endpoint,
                    "send_at": row.send_at,
                    "message_id": resp.message_id,
                    "to": resp.to,
                });
                state
                    .broadcast_to_webhooks(
                        &row.session_id,
                        WebhookEvent::ScheduledSent.as_str(),
                        &payload.to_string(),
                    )
                    .await;
                state
                    .publish_to_nats(&row.session_id, "scheduled_sent", &payload.to_string())
                    .await;
            }
            Err(err) => {
                if let Err(e) = scheduled::mark_failed(pool, &row.id, &err).await {
                    tracing::warn!("scheduled {} failed and mark_failed failed: {}", row.id, e);
                }
                let payload = serde_json::json!({
                    "schedule_id": row.id,
                    "endpoint": row.endpoint,
                    "send_at": row.send_at,
                    "error": err,
                });
                state
                    .broadcast_to_webhooks(
                        &row.session_id,
                        WebhookEvent::ScheduledFailed.as_str(),
                        &payload.to_string(),
                    )
                    .await;
                state
                    .publish_to_nats(&row.session_id, "scheduled_failed", &payload.to_string())
                    .await;
            }
        }
    }
    Ok(())
}

/// Replay a parked body: deserialize it into the request struct
/// matching `endpoint` and run its `execute_*` function. Shared by the
/// scheduler (via [`process_due`]) and the blast worker, which rewrites
/// `to` per recipient before calling in.
///
/// The `arm!` macro exists because a generic helper taking an async fn
/// item cannot be general enough over the borrowed `&AppState` / `&str`
/// lifetimes — expanding the two steps inline per arm sidesteps that.
pub async fn dispatch(
    state: &AppState,
    session_id: &str,
    endpoint: &str,
    body: &str,
) -> Result<MessageResponse, String> {
    use crate::handlers::messages as m;
    use crate::models::messages as mm;
    macro_rules! arm {
        ($ty:ty, $f:expr) => {{
            let request: $ty = serde_json::from_str(body)
                .map_err(|e| format!("stored body no longer deserializes: {e}"))?;
            $f(state, session_id, request)
                .await
                .map_err(|e| e.to_string())
        }};
    }
    match endpoint {
        "status-react" => arm!(
            crate::models::status::StatusReactionRequest,
            crate::handlers::status::execute_status_reaction
        ),
        "revoke" => arm!(mm::RevokeMessageRequest, m::execute_revoke),
        "react" => arm!(mm::SendReactionRequest, m::execute_react),
        "edit" => arm!(mm::EditMessageRequest, m::execute_edit),
        "pin" => arm!(mm::SendPinMessageRequest, m::execute_pin),
        "text" => arm!(mm::SendTextRequest, m::execute_text),
        "image" => arm!(mm::SendImageRequest, m::execute_image),
        "video" => arm!(mm::SendVideoRequest, m::execute_video),
        "audio" => arm!(mm::SendAudioRequest, m::execute_audio),
        "document" => arm!(mm::SendDocumentRequest, m::execute_document),
        "sticker" => arm!(mm::SendStickerRequest, m::execute_sticker),
        "location" => arm!(mm::SendLocationRequest, m::execute_location),
        "contact" => arm!(mm::SendContactRequest, m::execute_contact),
        "poll" => arm!(mm::SendPollRequest, m::execute_poll),
        "buttons" => arm!(mm::SendButtonsRequest, m::execute_buttons),
        "list" => arm!(mm::SendListRequest, m::execute_list),
        "interactive" => arm!(mm::SendInteractiveRequest, m::execute_interactive),
        "cta-url" => arm!(mm::SendCtaUrlRequest, m::execute_cta_url),
        "quick-reply" => arm!(mm::SendQuickReplyRequest, m::execute_quick_reply),
        "newsletter-admin-invite" => arm!(
            mm::SendNewsletterAdminInviteRequest,
            m::execute_newsletter_admin_invite
        ),
        "newsletter-follower-invite" => arm!(
            mm::SendNewsletterFollowerInviteRequest,
            m::execute_newsletter_follower_invite
        ),
        "order" => arm!(mm::SendOrderRequest, m::execute_order),
        "invoice" => arm!(mm::SendInvoiceRequest, m::execute_invoice),
        "payment-invite" => arm!(mm::SendPaymentInviteRequest, m::execute_payment_invite),
        "forward" => arm!(mm::ForwardMessageRequest, m::execute_forward_message),
        "poll-update" => arm!(mm::SendPollUpdateRequest, m::execute_poll_update),
        "buttons-response" => arm!(mm::SendButtonsResponseRequest, m::execute_buttons_response),
        "list-response" => arm!(mm::SendListResponseRequest, m::execute_list_response),
        "interactive-response" => arm!(
            mm::SendInteractiveResponseRequest,
            m::execute_interactive_response
        ),
        "highly-structured" => arm!(
            mm::SendHighlyStructuredRequest,
            m::execute_highly_structured
        ),
        "template-button-reply" => arm!(
            mm::SendTemplateButtonReplyRequest,
            m::execute_template_button_reply
        ),
        "comment" => arm!(mm::SendCommentRequest, m::execute_comment),
        "scheduled-call" => arm!(mm::SendScheduledCallRequest, m::execute_scheduled_call),
        "scheduled-call-edit" => arm!(
            mm::SendScheduledCallEditRequest,
            m::execute_scheduled_call_edit
        ),
        "send-payment" => arm!(mm::SendPaymentRequest, m::execute_payment),
        "request-payment" => arm!(mm::RequestPaymentRequest, m::execute_request_payment),
        "cancel-payment" => arm!(
            mm::CancelPaymentRequestRequest,
            m::execute_cancel_payment_request
        ),
        "decline-payment" => arm!(
            mm::DeclinePaymentRequestRequest,
            m::execute_decline_payment_request
        ),
        "newsletter-forward" => arm!(
            mm::SendNewsletterForwardRequest,
            m::execute_newsletter_forward
        ),
        other => Err(format!("unknown scheduled endpoint: {other}")),
    }
}

/// Validate an endpoint key and body WITHOUT executing anything: the
/// key must be one of [`dispatch`]'s arms and the body must deserialize
/// into that arm's request struct. Used by the blast create endpoint to
/// reject bad jobs up front (400) instead of letting them die in the
/// worker.
///
/// The arm list intentionally mirrors [`dispatch`]; keep the two in
/// sync when adding an endpoint.
pub fn validate_body(endpoint: &str, body: &serde_json::Value) -> Result<(), String> {
    use crate::models::messages as mm;

    fn check<T: serde::de::DeserializeOwned>(
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<(), String> {
        serde_json::from_value::<T>(body.clone())
            .map(|_| ())
            .map_err(|e| format!("body does not fit the {endpoint} request shape: {e}"))
    }

    match endpoint {
        "text" => check::<mm::SendTextRequest>(endpoint, body),
        "image" => check::<mm::SendImageRequest>(endpoint, body),
        "video" => check::<mm::SendVideoRequest>(endpoint, body),
        "audio" => check::<mm::SendAudioRequest>(endpoint, body),
        "document" => check::<mm::SendDocumentRequest>(endpoint, body),
        "sticker" => check::<mm::SendStickerRequest>(endpoint, body),
        "location" => check::<mm::SendLocationRequest>(endpoint, body),
        "contact" => check::<mm::SendContactRequest>(endpoint, body),
        "poll" => check::<mm::SendPollRequest>(endpoint, body),
        "buttons" => check::<mm::SendButtonsRequest>(endpoint, body),
        "list" => check::<mm::SendListRequest>(endpoint, body),
        "interactive" => check::<mm::SendInteractiveRequest>(endpoint, body),
        "cta-url" => check::<mm::SendCtaUrlRequest>(endpoint, body),
        "quick-reply" => check::<mm::SendQuickReplyRequest>(endpoint, body),
        "newsletter-admin-invite" => check::<mm::SendNewsletterAdminInviteRequest>(endpoint, body),
        "newsletter-follower-invite" => {
            check::<mm::SendNewsletterFollowerInviteRequest>(endpoint, body)
        }
        "order" => check::<mm::SendOrderRequest>(endpoint, body),
        "invoice" => check::<mm::SendInvoiceRequest>(endpoint, body),
        "payment-invite" => check::<mm::SendPaymentInviteRequest>(endpoint, body),
        "forward" => check::<mm::ForwardMessageRequest>(endpoint, body),
        "poll-update" => check::<mm::SendPollUpdateRequest>(endpoint, body),
        "buttons-response" => check::<mm::SendButtonsResponseRequest>(endpoint, body),
        "list-response" => check::<mm::SendListResponseRequest>(endpoint, body),
        "interactive-response" => check::<mm::SendInteractiveResponseRequest>(endpoint, body),
        "highly-structured" => check::<mm::SendHighlyStructuredRequest>(endpoint, body),
        "template-button-reply" => check::<mm::SendTemplateButtonReplyRequest>(endpoint, body),
        "comment" => check::<mm::SendCommentRequest>(endpoint, body),
        "scheduled-call" => check::<mm::SendScheduledCallRequest>(endpoint, body),
        "scheduled-call-edit" => check::<mm::SendScheduledCallEditRequest>(endpoint, body),
        "send-payment" => check::<mm::SendPaymentRequest>(endpoint, body),
        "request-payment" => check::<mm::RequestPaymentRequest>(endpoint, body),
        "cancel-payment" => check::<mm::CancelPaymentRequestRequest>(endpoint, body),
        "decline-payment" => check::<mm::DeclinePaymentRequestRequest>(endpoint, body),
        "newsletter-forward" => check::<mm::SendNewsletterForwardRequest>(endpoint, body),
        other => Err(format!("unknown endpoint: {other}")),
    }
}
