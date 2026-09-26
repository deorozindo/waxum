//! Session-local poll secrets and vote decoding.

use std::path::{Path, PathBuf};

use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use wacore::poll::{self, PollVoteCiphertext};
use wacore_binary::Jid;
use waproto::whatsapp::Message;

#[derive(Serialize, Deserialize)]
pub(crate) struct SentPoll {
    pub name: String,
    pub options: Vec<String>,
    pub chat: String,
    pub creator: String,
    pub message_secret: String,
}

fn poll_path(storage: &str, id: &str) -> PathBuf {
    let key = Sha256::digest(id.as_bytes());
    Path::new(storage)
        .join("polls")
        .join(format!("{}.json", hex::encode(key)))
}

pub(crate) async fn save(storage: &str, id: &str, poll: &SentPoll) -> std::io::Result<()> {
    let path = poll_path(storage, id);
    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    let bytes = serde_json::to_vec(poll).map_err(std::io::Error::other)?;
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .await?;
    use tokio::io::AsyncWriteExt;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    if let Err(error) = tokio::fs::rename(&temporary, &path).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error);
    }
    Ok(())
}

fn selected_options(poll: &SentPoll, hashes: &[Vec<u8>]) -> Result<Vec<String>, String> {
    hashes
        .iter()
        .map(|hash| {
            poll.options
                .iter()
                .find(|option| poll::compute_option_hash(option).as_slice() == hash)
                .cloned()
                .ok_or_else(|| "hash_desconhecido".to_string())
        })
        .collect()
}

fn decrypt_options(
    poll: &SentPoll,
    ciphertext: PollVoteCiphertext<'_>,
    secret: &[u8],
    id: &str,
    voter: &str,
) -> Result<Vec<String>, String> {
    let hashes = poll::decrypt_poll_vote_with_secret(ciphertext, secret, id, &poll.creator, voter)
        .map_err(|error| error.to_string())?;
    selected_options(poll, &hashes)
}

pub(crate) async fn enrich(
    storage: &str,
    client: &whatsapp_rust::Client,
    msg: &Message,
    voter: &Jid,
    data: &mut serde_json::Value,
) {
    let Some(update) = msg.poll_update_message.as_option() else {
        return;
    };
    let Some(id) = update
        .poll_creation_message_key
        .as_option()
        .and_then(|key| key.id.as_deref())
    else {
        return;
    };
    data["poll_id"] = id.into();
    let path = poll_path(storage, id);
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            data["poll_decrypt"] = "secret_desconhecido".into();
            return;
        }
        Err(error) => {
            tracing::warn!(poll_id = id, %error, "could not load poll secret");
            data["poll_decrypt"] = "falha".into();
            return;
        }
    };
    let Ok(poll) = serde_json::from_slice::<SentPoll>(&bytes) else {
        data["poll_decrypt"] = "falha".into();
        return;
    };
    data["poll_name"] = poll.name.clone().into();
    let Some(vote) = update.vote.as_option() else {
        data["poll_decrypt"] = "falha".into();
        return;
    };
    let (Some(payload), Some(iv)) = (vote.enc_payload.as_deref(), vote.enc_iv.as_deref()) else {
        data["poll_decrypt"] = "falha".into();
        return;
    };
    let (Ok(secret), Ok(creator)) = (
        base64::engine::general_purpose::STANDARD.decode(&poll.message_secret),
        poll.creator.parse::<Jid>(),
    ) else {
        data["poll_decrypt"] = "falha".into();
        return;
    };
    let ciphertext = PollVoteCiphertext {
        enc_payload: payload,
        enc_iv: iv,
    };
    let options = match decrypt_options(&poll, ciphertext, &secret, id, &voter.to_non_ad_string()) {
        Ok(options) => Ok(options),
        Err(_) => client
            .polls()
            .decrypt_vote(ciphertext, &secret, id, &creator, voter)
            .await
            .map_err(|e| e.to_string())
            .and_then(|hashes| selected_options(&poll, &hashes)),
    };
    match options {
        Ok(options) => {
            data["text"] = options.join(", ").into();
            data["selected_options"] = serde_json::json!(options);
        }
        Err(error) => {
            tracing::warn!(poll_id = id, %error, "could not decrypt poll vote");
            data["poll_decrypt"] = "falha".into();
        }
    }
}

/// Atomically claim a vote event. A later vote has a different message ID.
pub(crate) async fn claim_vote(storage: &str, id: &str, voter: &Jid) -> std::io::Result<bool> {
    let key = Sha256::digest(format!("{}\0{id}", voter.to_non_ad_string()).as_bytes());
    let dir = Path::new(storage).join("poll_votes");
    tokio::fs::create_dir_all(&dir).await?;
    match tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join(hex::encode(key)))
        .await
    {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stores_poll_and_claims_each_vote_once() {
        let dir = tempfile::tempdir().unwrap();
        let storage = dir.path().to_str().unwrap();
        let poll = SentPoll {
            name: "Confirma?".into(),
            options: vec!["Sim".into(), "Não".into()],
            chat: "123@s.whatsapp.net".into(),
            creator: "456@s.whatsapp.net".into(),
            message_secret: base64::engine::general_purpose::STANDARD.encode([7u8; 32]),
        };
        save(storage, "poll-1", &poll).await.unwrap();
        let loaded: SentPoll =
            serde_json::from_slice(&tokio::fs::read(poll_path(storage, "poll-1")).await.unwrap())
                .unwrap();
        assert_eq!(loaded.options, poll.options);
        assert_eq!(loaded.message_secret, poll.message_secret);

        let voter: Jid = "789@s.whatsapp.net".parse().unwrap();
        assert!(claim_vote(storage, "vote-1", &voter).await.unwrap());
        assert!(!claim_vote(storage, "vote-1", &voter).await.unwrap());
        assert!(claim_vote(storage, "vote-2", &voter).await.unwrap());
    }

    #[test]
    fn decrypts_selected_option_text() {
        let poll = SentPoll {
            name: "Confirma?".into(),
            options: vec!["Sim".into(), "Não".into()],
            chat: "123@s.whatsapp.net".into(),
            creator: "456@s.whatsapp.net".into(),
            message_secret: String::new(),
        };
        let secret = [7u8; 32];
        let hash = poll::compute_option_hash("Sim").to_vec();
        let (payload, iv) = poll::encrypt_poll_vote_with_secret(
            &[hash],
            &secret,
            "poll-1",
            &poll.creator,
            "789@s.whatsapp.net",
        )
        .unwrap();
        let options = decrypt_options(
            &poll,
            PollVoteCiphertext {
                enc_payload: &payload,
                enc_iv: &iv,
            },
            &secret,
            "poll-1",
            "789@s.whatsapp.net",
        )
        .unwrap();
        assert_eq!(options, vec!["Sim"]);
    }
}
