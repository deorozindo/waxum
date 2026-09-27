//! Tests for the scheduled-send feature.
//!
//! Unit-level: `send_at` serde parsing, the immediate-vs-scheduled
//! decision, status/event string mappings, and the send-response JSON
//! shape.
//!
//! Integration-level (through the full HTTP pipeline on a temp SQLite
//! DB, no live WhatsApp client): parking a future send, listing per
//! session and fleet-wide, cancelling, and the past-`send_at` fallthrough
//! to the immediate path. The dispatcher loop is not started by the
//! harness, so parked rows deterministically stay `pending`.
mod common;

use axum::http::{Method, StatusCode};
use chrono::{Duration, Timelike, Utc};
use common::{call, req_delete, req_get, req_json, Harness, TEST_TOKEN};
use serde_json::json;

use waxum::models::messages::SendTextRequest;
use waxum::models::schedule::{ScheduledStatus, SendResponse};
use waxum::models::webhooks::WebhookEvent;

async fn seed_session(h: &Harness, id: &str) {
    let (status, _) = call(
        &h.app,
        req_json(
            Method::POST,
            "/api/v1/sessions",
            Some(TEST_TOKEN),
            json!({"id": id, "name": id}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[test]
fn send_at_parses_iso8601_and_defaults_to_none() {
    let with: SendTextRequest = serde_json::from_value(json!({
        "to": "559999999999@s.whatsapp.net",
        "text": "hi",
        "send_at": "2026-01-01T12:00:00Z"
    }))
    .expect("deserialize with send_at");
    assert_eq!(
        with.send_at.map(|d| d.to_rfc3339()),
        Some("2026-01-01T12:00:00+00:00".to_string())
    );

    let without: SendTextRequest = serde_json::from_value(json!({
        "to": "559999999999@s.whatsapp.net",
        "text": "hi"
    }))
    .expect("deserialize without send_at");
    assert!(without.send_at.is_none());
}

#[test]
fn scheduled_status_roundtrips_strings() {
    for (s, expect) in [
        ("pending", ScheduledStatus::Pending),
        ("sending", ScheduledStatus::Sending),
        ("sent", ScheduledStatus::Sent),
        ("failed", ScheduledStatus::Failed),
        ("cancelled", ScheduledStatus::Cancelled),
        ("garbage", ScheduledStatus::Pending),
    ] {
        assert_eq!(ScheduledStatus::from_str(s), expect);
        if s != "garbage" {
            assert_eq!(expect.as_str(), s);
        }
    }
    assert_eq!(
        serde_json::to_string(&ScheduledStatus::Pending).unwrap(),
        "\"pending\""
    );
}

#[test]
fn webhook_event_strings_cover_scheduled_events() {
    assert_eq!(WebhookEvent::ScheduledSent.as_str(), "scheduled_sent");
    assert_eq!(WebhookEvent::ScheduledFailed.as_str(), "scheduled_failed");
    assert_eq!(
        WebhookEvent::from_str("scheduled_sent"),
        Some(WebhookEvent::ScheduledSent)
    );
    assert_eq!(
        WebhookEvent::from_str("scheduled_failed"),
        Some(WebhookEvent::ScheduledFailed)
    );
    assert!(WebhookEvent::ScheduledSent.matches("scheduled_sent"));
    assert!(!WebhookEvent::ScheduledSent.matches("scheduled_failed"));
    assert!(WebhookEvent::All.matches("scheduled_sent"));
}

#[test]
fn send_response_shapes_for_sent_and_pending() {
    let sent = SendResponse::sent(waxum::models::messages::MessageResponse {
        message_id: "MID".to_string(),
        timestamp: 123,
        to: "559999999999@s.whatsapp.net".to_string(),
    });
    let v = serde_json::to_value(&sent).unwrap();
    assert_eq!(v["status"], "sent");
    assert_eq!(v["message_id"], "MID");
    assert!(v.get("schedule_id").is_none());

    let at = Utc::now();
    let pending = SendResponse::scheduled("sched-1".to_string(), at);
    let v = serde_json::to_value(&pending).unwrap();
    assert_eq!(v["status"], "pending");
    assert_eq!(v["schedule_id"], "sched-1");
    assert_eq!(v["send_at"], serde_json::to_value(at).unwrap());
    assert!(v.get("message_id").is_none());
}

#[tokio::test]
async fn future_send_at_parks_message_and_lists_it() {
    let h = Harness::new().await;
    seed_session(&h, "sched-s-01").await;

    let send_at = (Utc::now() + Duration::hours(1)).to_rfc3339();
    let (status, body) = call(
        &h.app,
        req_json(
            Method::POST,
            "/api/v1/sessions/sched-s-01/messages/text",
            Some(TEST_TOKEN),
            json!({
                "to": "559999999999@s.whatsapp.net",
                "text": "later",
                "send_at": send_at
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["status"], "pending");
    let schedule_id = body["schedule_id"].as_str().expect("schedule_id");
    assert!(!schedule_id.is_empty());
    assert!(body.get("message_id").is_none());

    let (status, body) = call(
        &h.app,
        req_get("/api/v1/sessions/sched-s-01/scheduled", Some(TEST_TOKEN)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    let row = &body["messages"][0];
    assert_eq!(row["id"], schedule_id);
    assert_eq!(row["session_id"], "sched-s-01");
    assert_eq!(row["endpoint"], "text");
    assert_eq!(row["status"], "pending");

    let (status, body) = call(
        &h.app,
        req_get(
            "/api/v1/sessions/sched-s-01/scheduled?status=sent",
            Some(TEST_TOKEN),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 0);

    let (status, body) = call(
        &h.app,
        req_get("/api/v1/scheduled?session=sched-s-01", Some(TEST_TOKEN)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);

    let (status, body) = call(
        &h.app,
        req_get(
            "/api/v1/scheduled?session=sched-s-01&status=pending",
            Some(TEST_TOKEN),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
}

#[tokio::test]
async fn past_send_at_is_still_durably_queued() {
    let h = Harness::new().await;
    seed_session(&h, "sched-s-02").await;

    let send_at = (Utc::now() - Duration::seconds(5)).to_rfc3339();
    let (status, _) = call(
        &h.app,
        req_json(
            Method::POST,
            "/api/v1/sessions/sched-s-02/messages/text",
            Some(TEST_TOKEN),
            json!({
                "to": "559999999999@s.whatsapp.net",
                "text": "now",
                "send_at": send_at
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (_, body) = call(
        &h.app,
        req_get("/api/v1/sessions/sched-s-02/scheduled", Some(TEST_TOKEN)),
    )
    .await;
    assert_eq!(body["count"], 1);
}

#[tokio::test]
async fn cancel_pending_then_conflict_then_not_found() {
    let h = Harness::new().await;
    seed_session(&h, "sched-s-03").await;

    let send_at = (Utc::now() + Duration::hours(1)).to_rfc3339();
    let (_, body) = call(
        &h.app,
        req_json(
            Method::POST,
            "/api/v1/sessions/sched-s-03/messages/text",
            Some(TEST_TOKEN),
            json!({
                "to": "559999999999@s.whatsapp.net",
                "text": "later",
                "send_at": send_at
            }),
        ),
    )
    .await;
    let schedule_id = body["schedule_id"].as_str().expect("schedule_id");
    let path = format!("/api/v1/sessions/sched-s-03/scheduled/{schedule_id}");

    let (status, body) = call(&h.app, req_delete(&path, Some(TEST_TOKEN))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "cancelled");

    let (status, _) = call(&h.app, req_delete(&path, Some(TEST_TOKEN))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = call(
        &h.app,
        req_delete(
            "/api/v1/sessions/sched-s-03/scheduled/does-not-exist",
            Some(TEST_TOKEN),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (_, body) = call(
        &h.app,
        req_get(
            "/api/v1/sessions/sched-s-03/scheduled?status=cancelled",
            Some(TEST_TOKEN),
        ),
    )
    .await;
    assert_eq!(body["count"], 1);
}

#[tokio::test]
async fn scheduling_unknown_session_returns_404() {
    let h = Harness::new().await;
    let send_at = (Utc::now() + Duration::hours(1)).to_rfc3339();
    let (status, _) = call(
        &h.app,
        req_json(
            Method::POST,
            "/api/v1/sessions/sched-missing/messages/text",
            Some(TEST_TOKEN),
            json!({
                "to": "559999999999@s.whatsapp.net",
                "text": "later",
                "send_at": send_at
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn all_send_kinds_queue_offline_and_survive_database_reopen() {
    let h = Harness::new().await;
    h.state
        .session_manager()
        .create_session("queued", Some("fixture"), "unused-fixture-storage")
        .await
        .unwrap();
    for (endpoint, body) in [
        ("text", json!({"to":"5511000000000", "text":"fixture"})),
        (
            "poll",
            json!({"to":"5511000000000", "name":"fixture", "options":["A","B"], "selectable_count":1}),
        ),
        (
            "react",
            json!({"to":"5511000000000", "message_id":"fixture", "emoji":"ok"}),
        ),
    ] {
        let (status, body) = call(
            &h.app,
            req_json(
                Method::POST,
                &format!("/api/v1/sessions/queued/messages/{endpoint}"),
                Some(TEST_TOKEN),
                body,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["status"], "pending");
        assert!(body["schedule_id"].as_str().is_some());
    }
    let path = h._tmp.path().join("waxum.db");
    let reopened = waxum::db::session::DbPool::SQLite(
        waxum::db::sqlite_raw::open(path.to_str().unwrap()).unwrap(),
    );
    let rows = waxum::db::scheduled::list(&reopened, Some("queued"), Some("pending"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn restart_rechecks_window_deferral_but_preserves_explicit_future() {
    use waxum::db::scheduled;
    let h = Harness::new().await;
    let future = Utc::now() + Duration::days(3);
    let future = future.with_nanosecond(0).unwrap();
    for (id, body) in [
        (
            "window-deferred",
            json!({"to":"5511000000000@s.whatsapp.net", "text":"a", "send_at":null}),
        ),
        (
            "explicit-future",
            json!({"to":"5511000000000@s.whatsapp.net", "text":"b", "send_at":future.to_rfc3339()}),
        ),
    ] {
        scheduled::insert(&h.pool, id, "s", "text", &body.to_string(), future)
            .await
            .unwrap();
    }
    waxum::handlers::schedule::recheck_pending(&h.state)
        .await
        .unwrap();
    let due = scheduled::due_pending(&h.pool, 50).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, "window-deferred");
    let preserved = scheduled::get(&h.pool, "s", "explicit-future")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        preserved.send_at,
        future.format("%Y-%m-%d %H:%M:%S").to_string()
    );
}

#[tokio::test]
async fn deadline_scheduler_sleeps_and_wakes_on_insert_and_cancel() {
    use waxum::db::scheduled;
    let h = Harness::new().await;
    seed_session(&h, "timer").await;
    let task = tokio::spawn(waxum::handlers::schedule::run_scheduler(h.state.clone()));
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let initial_queries = h.state.scheduler_count(0);
    assert!((1..=2).contains(&initial_queries));
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(h.state.scheduler_count(0), initial_queries);
    assert_eq!(h.state.scheduler_count(1), 0);
    let (status, body) = call(
        &h.app,
        req_json(Method::POST, "/api/v1/sessions/timer/messages/text", Some(TEST_TOKEN),
            json!({"to":"5511000000000@s.whatsapp.net","text":"timer","send_at":(Utc::now()+Duration::hours(1)).to_rfc3339()})),
    ).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let id = body["schedule_id"].as_str().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(h.state.scheduler_count(0), initial_queries + 1);
    assert_eq!(h.state.scheduler_count(1), 0);
    assert_eq!(
        scheduled::next_pending(&h.pool).await.unwrap().unwrap().id,
        id
    );
    let (status, _) = call(
        &h.app,
        req_delete(
            &format!("/api/v1/sessions/timer/scheduled/{id}"),
            Some(TEST_TOKEN),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(h.state.scheduler_count(0), initial_queries + 2);
    assert!(scheduled::next_pending(&h.pool).await.unwrap().is_none());
    let at = Utc::now() + Duration::seconds(2);
    scheduled::insert(
        &h.pool,
        "due",
        "absent",
        "text",
        &json!({"to":"5511000000000@s.whatsapp.net","text":"a","send_at":at.to_rfc3339()})
            .to_string(),
        at,
    )
    .await
    .unwrap();
    h.state.scheduler_wake().notify_one();
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert_eq!(h.state.scheduler_count(1), 1);
    let row = scheduled::get(&h.pool, "absent", "due")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, "pending");
    assert!(scheduled::due_pending(&h.pool, 50)
        .await
        .unwrap()
        .is_empty());
    task.abort();
}

#[tokio::test]
async fn idle_before_after_window_counts_real_queries() {
    use waxum::db::scheduled;
    let h = Harness::new().await;
    let task = tokio::spawn(waxum::handlers::schedule::run_scheduler(h.state.clone()));
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let started = Utc::now();
    let before = h.state.scheduler_count(0);
    let mut legacy = tokio::time::interval(std::time::Duration::from_secs(1));
    let end = tokio::time::sleep(std::time::Duration::from_millis(10100));
    tokio::pin!(end);
    let mut legacy_queries = 0;
    loop {
        tokio::select! {
            _ = &mut end => break,
            _ = legacy.tick() => {
                assert!(scheduled::due_pending(&h.pool, 50).await.unwrap().is_empty());
                legacy_queries += 1;
            }
        }
    }
    let after = h.state.scheduler_count(0);
    println!(
        "IDLE_WINDOW start={} end={} legacy_due_queries={} deadline_queries={} deadline_rounds={}",
        started.to_rfc3339(),
        Utc::now().to_rfc3339(),
        legacy_queries,
        after - before,
        h.state.scheduler_count(1)
    );
    assert!(legacy_queries >= 10);
    assert_eq!(after, before);
    assert_eq!(h.state.scheduler_count(1), 0);
    task.abort();
}

#[tokio::test]
async fn interrupted_claim_is_failed_once_and_never_replayed() {
    use waxum::db::scheduled;
    let h = Harness::new().await;
    scheduled::insert(
        &h.pool,
        "interrupted",
        "absent",
        "text",
        &json!({"to":"5511000000000@s.whatsapp.net","text":"a"}).to_string(),
        Utc::now(),
    )
    .await
    .unwrap();
    assert!(scheduled::claim(&h.pool, "interrupted").await.unwrap());
    assert!(!scheduled::claim(&h.pool, "interrupted").await.unwrap());
    let task = tokio::spawn(waxum::handlers::schedule::run_scheduler(h.state.clone()));
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let row = scheduled::get(&h.pool, "absent", "interrupted")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, "failed");
    assert!(row.error.unwrap().contains("not replayed"));
    assert!(!scheduled::claim(&h.pool, "interrupted").await.unwrap());
    assert_eq!(h.state.scheduler_count(1), 0);
    task.abort();
}

#[tokio::test]
async fn terminal_receipt_ledger_survives_restart_and_marks_after_ack_only() {
    use waxum::db::{scheduled, schema};
    let h = Harness::new().await;
    if let waxum::db::session::DbPool::SQLite(handle) = &h.pool {
        waxum::db::session::sqlite_blocking(handle, |conn| {
            waxum::db::sqlite_raw::exec_batch(
                conn,
                "ALTER TABLE scheduled_messages DROP COLUMN receipt_published",
            )
        })
        .await
        .unwrap();
    }
    schema::init_schema(&h.pool).await.unwrap();
    for id in ["sent", "failed", "pending"] {
        scheduled::insert(&h.pool, id, "s", "text", "{}", Utc::now())
            .await
            .unwrap();
    }
    scheduled::mark_receipt_published(&h.pool, "pending")
        .await
        .unwrap();
    assert!(scheduled::claim(&h.pool, "sent").await.unwrap());
    scheduled::mark_sent(&h.pool, "sent", "wa-1").await.unwrap();
    assert!(scheduled::claim(&h.pool, "failed").await.unwrap());
    scheduled::mark_failed(&h.pool, "failed", "delivery unknown")
        .await
        .unwrap();
    let reopened = waxum::db::session::DbPool::SQLite(
        waxum::db::sqlite_raw::open(h._tmp.path().join("waxum.db").to_str().unwrap()).unwrap(),
    );
    schema::init_schema(&reopened).await.unwrap();
    assert_eq!(
        scheduled::unpublished_receipts(&reopened)
            .await
            .unwrap()
            .len(),
        2
    );
    for id in ["sent", "failed"] {
        scheduled::mark_receipt_published(&reopened, id)
            .await
            .unwrap();
        scheduled::mark_receipt_published(&reopened, id)
            .await
            .unwrap();
    }
    assert!(scheduled::unpublished_receipts(&reopened)
        .await
        .unwrap()
        .is_empty());
    assert!(!scheduled::claim(&reopened, "sent").await.unwrap());
    assert_eq!(
        scheduled::get(&reopened, "s", "sent")
            .await
            .unwrap()
            .unwrap()
            .message_id
            .as_deref(),
        Some("wa-1")
    );
}

/// Opt-in proof against the existing WA_EVENTS stream; never sends WhatsApp.
#[tokio::test]
#[ignore]
async fn live_wa_events_failed_receipt_and_duplicate_publish() {
    use waxum::db::scheduled;
    use waxum::nats::{config::NatsConfig, NatsManager};
    let h = Harness::new().await;
    let nats = NatsManager::connect(NatsConfig {
        url: std::env::var("F3_NATS_URL").expect("explicit live-proof URL required"),
        events_stream: "WA_EVENTS".into(),
        send_stream: "WA_SEND".into(),
        events_max_age_days: 7,
        send_max_age_days: 1,
        creds_file: None,
        token: None,
    })
    .await
    .unwrap();
    let state = waxum::state::AppState::new(
        h.pool.clone(),
        Some(nats),
        waxum::storage::RecordingStore::local(h._tmp.path().to_str().unwrap()),
    )
    .await;
    let id = format!("lacos-f3-proof-{}", uuid::Uuid::new_v4());
    scheduled::insert(&h.pool, &id, "lacos-f3-proof", "poll", "{}", Utc::now())
        .await
        .unwrap();
    assert!(scheduled::claim(&h.pool, &id).await.unwrap());
    scheduled::mark_failed(&h.pool, &id, "synthetic recovery proof; no WhatsApp send")
        .await
        .unwrap();
    waxum::handlers::schedule::publish_receipts(&state)
        .await
        .unwrap();
    assert!(scheduled::unpublished_receipts(&h.pool)
        .await
        .unwrap()
        .is_empty());
    let stream = state
        .nats()
        .unwrap()
        .jetstream()
        .get_stream("WA_EVENTS")
        .await
        .unwrap();
    let subject = "wa.events.lacos-f3-proof.scheduled_failed";
    let event = stream
        .get_last_raw_message_by_subject(subject)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
    assert_eq!(body["schedule_id"], id);
    waxum::nats::publisher::publish_event_confirmed(
        state.nats().unwrap().jetstream(),
        "lacos-f3-proof",
        "scheduled_failed",
        std::str::from_utf8(&event.payload).unwrap(),
    )
    .await
    .unwrap();
    let repeated = stream
        .get_last_raw_message_by_subject(subject)
        .await
        .unwrap();
    assert_eq!(repeated.sequence, event.sequence);
    waxum::handlers::schedule::publish_receipts(&state)
        .await
        .unwrap();
    assert_eq!(
        state
            .recent_events(10)
            .iter()
            .filter(|e| e.event_type == "scheduled_failed")
            .count(),
        1
    );
    let sent_id = format!("lacos-f3-sent-{}", uuid::Uuid::new_v4());
    let poll_id = format!("lacos-f3-poll-{}", uuid::Uuid::new_v4());
    scheduled::insert(
        &h.pool,
        &sent_id,
        "lacos-f3-proof",
        "poll",
        "{}",
        Utc::now(),
    )
    .await
    .unwrap();
    let vote = serde_json::json!({"session_id":"lacos-f3-proof","event":"message","data":{"type":"poll_vote","message_id":"synthetic-vote","poll_id":poll_id,"selected_options":["proof"]}});
    let vote_ack = state
        .nats()
        .unwrap()
        .jetstream()
        .publish("wa.events.lacos-f3-proof.message", vote.to_string().into())
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(scheduled::claim(&h.pool, &sent_id).await.unwrap());
    scheduled::mark_sent(&h.pool, &sent_id, &poll_id)
        .await
        .unwrap();
    waxum::handlers::schedule::publish_receipts(&state)
        .await
        .unwrap();
    let sent = stream
        .get_last_raw_message_by_subject("wa.events.lacos-f3-proof.scheduled_sent")
        .await
        .unwrap();
    let sent_body: serde_json::Value = serde_json::from_slice(&sent.payload).unwrap();
    assert_eq!(sent_body["message_id"], poll_id);
    assert!(vote_ack.sequence < sent.sequence);
    let retained_vote = stream.get_raw_message(vote_ack.sequence).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&retained_vote.payload).unwrap()["data"]
            ["poll_id"],
        poll_id
    );
    println!("EARLY_VOTE vote_sequence={} receipt_sequence={} poll_id={} recoverable=true synthetic=true",vote_ack.sequence,sent.sequence,poll_id);
    println!("LIVE_RECEIPT subject={subject} schedule_id={id} sequence={} duplicate_sequence={} callbacks=1",event.sequence,repeated.sequence);
}
