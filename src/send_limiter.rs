//! Durable send admission shared by HTTP, scheduler, blasts and NATS.

use crate::db::sqlite_raw::{self, SqliteHandle, Value as V};
use chrono::{Datelike, TimeZone, Timelike, Utc};
use rand::Rng;
use std::{path::PathBuf, sync::OnceLock, time::Duration};

#[derive(Clone)]
pub struct Config {
    pub session_min: i64,
    pub session_max: i64,
    pub chat_min: i64,
    pub chat_max: i64,
    pub session_hour: usize,
    pub chat_hour: usize,
    pub captain_hour: usize,
    pub open: u32,
    pub close: u32,
    pub days: u32,
    pub timezone: chrono_tz::Tz,
    pub captain_chats: Vec<String>,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        fn number(name: &str, default: i64) -> anyhow::Result<i64> {
            Ok(match std::env::var(name) {
                Ok(v) => v.parse()?,
                Err(_) => default,
            })
        }
        let c = Self {
            session_min: number("WAXUM_SEND_SESSION_MIN_SECONDS", 8)?,
            session_max: number("WAXUM_SEND_SESSION_MAX_SECONDS", 20)?,
            chat_min: number("WAXUM_SEND_CHAT_MIN_SECONDS", 8)?,
            chat_max: number("WAXUM_SEND_CHAT_MAX_SECONDS", 20)?,
            session_hour: number("WAXUM_SEND_SESSION_PER_HOUR", 60)?.try_into()?,
            chat_hour: number("WAXUM_SEND_CHAT_PER_HOUR", 12)?.try_into()?,
            captain_hour: number("WAXUM_SEND_CAPTAIN_PER_HOUR", 60)?.try_into()?,
            open: number("WAXUM_SEND_OPEN_HOUR", 8)?.try_into()?,
            close: number("WAXUM_SEND_CLOSE_HOUR", 19)?.try_into()?,
            days: number("WAXUM_SEND_WEEKDAYS", 6)?.try_into()?,
            timezone: std::env::var("WAXUM_SEND_TIMEZONE")
                .unwrap_or_else(|_| "America/Sao_Paulo".into())
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid send timezone: {e}"))?,
            captain_chats: std::env::var("WAXUM_SEND_CAPTAIN_CHATS")
                .unwrap_or_default()
                .split(',')
                .filter(|v| !v.trim().is_empty())
                .map(|v| v.trim().to_string())
                .collect(),
        };
        anyhow::ensure!(
            c.session_min > 0
                && c.chat_min > 0
                && c.session_max >= c.session_min
                && c.chat_max >= c.chat_min,
            "invalid send intervals"
        );
        anyhow::ensure!(
            c.session_hour > 0
                && c.chat_hour > 0
                && c.captain_hour > 0
                && c.open < c.close
                && c.close <= 24
                && (1..=7).contains(&c.days),
            "invalid send ceilings/window"
        );
        anyhow::ensure!(
            c.captain_chats
                .iter()
                .all(|jid| jid.ends_with("@s.whatsapp.net") || jid.ends_with("@lid")),
            "captain window exception must contain only private PN/LID JIDs"
        );
        Ok(c)
    }

    fn chat_ceiling(&self, chat: &str) -> usize {
        if self.captain_chats.iter().any(|s| s == chat) {
            self.captain_hour
        } else {
            self.chat_hour
        }
    }

    pub fn window(&self, now: i64, chat: &str) -> i64 {
        if self.captain_chats.iter().any(|s| s == chat) {
            return now;
        }
        let zone = self.timezone;
        let local = zone.timestamp_opt(now, 0).single().unwrap();
        if local.weekday().number_from_monday() <= self.days
            && local.hour() >= self.open
            && local.hour() < self.close
        {
            return now;
        }
        let mut day = local.date_naive();
        if local.hour() >= self.close {
            day = day.succ_opt().unwrap();
        }
        while day.weekday().number_from_monday() > self.days {
            day = day.succ_opt().unwrap();
        }
        zone.from_local_datetime(&day.and_hms_opt(self.open, 0, 0).unwrap())
            .single()
            .unwrap()
            .timestamp()
            .max(now)
    }
}

pub struct Limiter {
    db: SqliteHandle,
    config: Config,
}
static LIMITER: OnceLock<Limiter> = OnceLock::new();

pub fn initialize() -> anyhow::Result<()> {
    let storage =
        std::env::var("WHATSAPP_STORAGE_PATH").unwrap_or_else(|_| "./whatsapp_sessions".into());
    let path = std::env::var("WAXUM_SEND_LEDGER_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(storage).join("send-ledger.sqlite"));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let db = sqlite_raw::open(
        path.to_str()
            .ok_or_else(|| anyhow::anyhow!("invalid ledger path"))?,
    )?;
    sqlite_raw::exec_batch(&db.lock(), "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=10000; CREATE TABLE IF NOT EXISTS send_attempts (session TEXT NOT NULL, chat TEXT NOT NULL, at INTEGER NOT NULL, session_next INTEGER NOT NULL, chat_next INTEGER NOT NULL); CREATE INDEX IF NOT EXISTS send_attempts_session ON send_attempts(session, at); CREATE INDEX IF NOT EXISTS send_attempts_chat ON send_attempts(chat, at);")?;
    let config = Config::from_env()?;
    LIMITER
        .set(Limiter { db, config })
        .map_err(|_| anyhow::anyhow!("send limiter already initialized"))
}

fn deadline(now: i64, entries: &[(i64, i64)], cap: usize) -> i64 {
    let active: Vec<_> = entries.iter().filter(|(at, _)| *at > now - 3600).collect();
    let cooldown = entries.iter().map(|(_, next)| *next).max().unwrap_or(now);
    let ceiling = if active.len() >= cap {
        active[active.len() - cap].0 + 3600
    } else {
        now
    };
    now.max(cooldown).max(ceiling)
}

impl Limiter {
    fn check(&self, session: &str, chat: &str, reserve: bool) -> anyhow::Result<(i64, bool)> {
        self.check_at(session, chat, reserve, Utc::now().timestamp())
    }

    fn check_at(
        &self,
        session: &str,
        chat: &str,
        reserve: bool,
        now: i64,
    ) -> anyhow::Result<(i64, bool)> {
        let conn = self.db.lock();
        sqlite_raw::exec_batch(&conn, "BEGIN IMMEDIATE")?;
        let result = (|| {
            let query = |column: &str, value: &str, next: &str| {
                sqlite_raw::query(&conn, &format!("SELECT at, {next} FROM send_attempts WHERE {column} = ? AND (at > ? OR {next} > ?) ORDER BY at"), &[V::Text(value.into()), V::Int(now - 3600), V::Int(now)], |r| (r.get_int(0), r.get_int(1)))
            };
            let session_entries = query("session", session, "session_next")?;
            let chat_entries = query("chat", chat, "chat_next")?;
            let due = self.config.window(
                deadline(now, &session_entries, self.config.session_hour).max(deadline(
                    now,
                    &chat_entries,
                    self.config.chat_ceiling(chat),
                )),
                chat,
            );
            if reserve && due <= now {
                let mut rng = rand::thread_rng();
                let recorded = now + 1;
                let session_next =
                    recorded + rng.gen_range(self.config.session_min..=self.config.session_max);
                let chat_next =
                    recorded + rng.gen_range(self.config.chat_min..=self.config.chat_max);
                sqlite_raw::execute(
                    &conn,
                    "INSERT INTO send_attempts VALUES (?, ?, ?, ?, ?)",
                    &[
                        V::Text(session.into()),
                        V::Text(chat.into()),
                        V::Int(recorded),
                        V::Int(session_next),
                        V::Int(chat_next),
                    ],
                )?;
                sqlite_raw::execute(&conn, "DELETE FROM send_attempts WHERE at <= ? AND session_next <= ? AND chat_next <= ?", &[V::Int(now - 3600), V::Int(now), V::Int(now)])?;
            }
            Ok((due, reserve && due <= now))
        })();
        match result {
            Ok(due) => {
                sqlite_raw::exec_batch(&conn, "COMMIT")?;
                Ok(due)
            }
            Err(e) => {
                let _ = sqlite_raw::exec_batch(&conn, "ROLLBACK");
                Err(e)
            }
        }
    }
}

pub async fn due(session: &str, chat: &str, reserve: bool) -> anyhow::Result<(i64, bool)> {
    let session = session.to_string();
    let chat = chat.to_string();
    tokio::task::spawn_blocking(move || {
        LIMITER
            .get()
            .ok_or_else(|| anyhow::anyhow!("send limiter not initialized"))?
            .check(&session, &chat, reserve)
    })
    .await?
}

pub async fn acquire(session: &str, chat: &str) -> anyhow::Result<()> {
    loop {
        let (next, admitted) = due(session, chat, true).await?;
        let now = Utc::now().timestamp();
        if admitted {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs((next - now).max(1) as u64)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> Config {
        Config {
            session_min: 8,
            session_max: 20,
            chat_min: 8,
            chat_max: 20,
            session_hour: 60,
            chat_hour: 12,
            captain_hour: 60,
            open: 8,
            close: 19,
            days: 6,
            timezone: chrono_tz::America::Sao_Paulo,
            captain_chats: vec!["a".into(), "b".into()],
        }
    }
    #[test]
    fn durable_admission_survives_reopen_and_shared_chat() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ledger.sqlite");
        let db = sqlite_raw::open(path.to_str().unwrap()).unwrap();
        sqlite_raw::exec_batch(&db.lock(), "CREATE TABLE send_attempts (session TEXT, chat TEXT, at INTEGER, session_next INTEGER, chat_next INTEGER)").unwrap();
        let limiter = Limiter {
            db,
            config: config(),
        };
        let now = chrono::DateTime::parse_from_rfc3339("2026-09-27T12:00:00Z")
            .unwrap()
            .timestamp();
        assert!(limiter.check_at("s1", "a", true, now).unwrap().1);
        assert!(!limiter.check_at("s1", "b", true, now + 1).unwrap().1);
        assert!(!limiter.check_at("s2", "a", true, now + 1).unwrap().1);
        let due = limiter.check_at("s1", "a", false, now + 1).unwrap().0;
        assert!((now + 9..=now + 21).contains(&due));
        drop(limiter);
        let limiter = Limiter {
            db: sqlite_raw::open(path.to_str().unwrap()).unwrap(),
            config: config(),
        };
        assert_eq!(limiter.check_at("s1", "a", false, now + 1).unwrap().0, due);
        assert!(limiter.check_at("s1", "a", true, due).unwrap().1);
        let mut capped = config();
        capped.captain_hour = 1;
        let limiter = Limiter {
            db: limiter.db,
            config: capped,
        };
        assert!(limiter.check_at("s2", "a", false, due + 50).unwrap().0 >= due + 3601);
    }
    #[test]
    fn simulated_clock_cooldowns_and_rolling_ceiling() {
        assert_eq!(deadline(100, &[], 12), 100);
        assert_eq!(deadline(101, &[(100, 120)], 12), 120);
        assert_eq!(deadline(120, &[(100, 120)], 1), 3700);
        assert_eq!(deadline(3700, &[(100, 120)], 1), 3700);
        let entries = vec![(10, 18), (20, 28), (30, 38)];
        assert_eq!(deadline(40, &entries, 2), 3620);
    }
    #[test]
    fn simulated_clock_commercial_window_and_captain_24x7() {
        let mut c = Config {
            session_min: 8,
            session_max: 20,
            chat_min: 8,
            chat_max: 20,
            session_hour: 60,
            chat_hour: 12,
            captain_hour: 60,
            open: 8,
            close: 19,
            days: 6,
            timezone: chrono_tz::America::Sao_Paulo,
            captain_chats: Vec::new(),
        };
        c.captain_chats = vec![
            "5511000000000@s.whatsapp.net".into(),
            "100000000000000@lid".into(),
        ];
        let utc = |s: &str| chrono::DateTime::parse_from_rfc3339(s).unwrap().timestamp();
        let sunday = utc("2026-09-27T12:00:00Z");
        assert_eq!(c.window(sunday, "third-party"), utc("2026-09-28T11:00:00Z"));
        assert_eq!(
            c.window(utc("2026-09-28T21:59:59Z"), "third-party"),
            utc("2026-09-28T21:59:59Z")
        );
        assert_eq!(c.chat_ceiling("third-party"), 12);
        assert_eq!(c.chat_ceiling("100000000000000@g.us"), 12);
        for captain in &c.captain_chats {
            assert_eq!(c.chat_ceiling(captain), 60);
            assert_eq!(c.window(sunday, captain), sunday);
            assert_eq!(
                c.window(utc("2026-09-28T22:00:00Z"), captain),
                utc("2026-09-28T22:00:00Z")
            );
        }
        assert_eq!(
            c.window(sunday, "100000000000000@g.us"),
            utc("2026-09-28T11:00:00Z")
        );
        let monday_open = utc("2026-09-28T11:00:00Z");
        assert_eq!(c.window(monday_open, "third-party"), monday_open);
        assert_eq!(
            c.window(utc("2026-09-28T22:00:00Z"), "third-party"),
            utc("2026-09-29T11:00:00Z")
        );
    }
}
