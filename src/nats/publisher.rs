use async_nats::jetstream;
use bytes::Bytes;

/// Publish a WhatsApp event to NATS JetStream.
/// Subject format: `wa.events.{session_id}.{event_type}`
pub async fn publish_event(
    jetstream: &jetstream::Context,
    session_id: &str,
    event_type: &str,
    payload: &str,
) {
    if let Err(e) = publish_event_confirmed(jetstream, session_id, event_type, payload).await {
        tracing::warn!("NATS event publish failed: {e}");
    }
}

/// Return success only after JetStream persists the event.
pub async fn publish_event_confirmed(
    jetstream: &jetstream::Context,
    session_id: &str,
    event_type: &str,
    payload: &str,
) -> anyhow::Result<()> {
    let subject = format!("wa.events.{}.{}", session_id, event_type);

    let mut headers = async_nats::HeaderMap::new();
    if matches!(event_type, "scheduled_sent" | "scheduled_failed") {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(payload) {
            if let Some(id) = value.get("schedule_id").and_then(|id| id.as_str()) {
                headers.insert(
                    async_nats::header::NATS_MESSAGE_ID,
                    format!("{subject}.{id}"),
                );
            }
        }
    }
    jetstream
        .publish_with_headers(subject, headers, Bytes::from(payload.to_string()))
        .await?
        .await?;
    Ok(())
}
