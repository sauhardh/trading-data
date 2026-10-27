use base64::{Engine, engine::general_purpose::STANDARD};
use std::time::{SystemTime, UNIX_EPOCH};

pub const WEBSOCKET_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";

pub fn timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub fn sign(private_key_pem: &str, message: &[u8]) -> anyhow::Result<String> {
    use openssl::{pkey::PKey, sign::Signer};

    let private_key = PKey::private_key_from_pem(private_key_pem.as_bytes())?;

    anyhow::ensure!(
        private_key.id() == openssl::pkey::Id::ED25519,
        "Expected an Ed25519 private key"
    );

    let mut signer = Signer::new_without_digest(&private_key)?;
    let signature = signer.sign_oneshot_to_vec(message)?;

    Ok(STANDARD.encode(signature))
}
