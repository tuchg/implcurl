use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use impcurl_ws::{Message, WsConnection};
use serde_json::{Value, json};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_PUBLIC_ECHO_URL: &str = "wss://ws.postman-echo.com/raw";
const CLIENT_IDS: [&str; 3] = ["conn-a", "conn-b", "conn-c"];
const MIN_ROUND_TRIPS_PER_CLIENT: usize = 3;
const DEFAULT_ROUND_TRIPS_PER_CLIENT: usize = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_public_websocket_connections_do_not_mix_messages() -> Result<()> {
    if std::env::var("IMPCURL_RUN_PUBLIC_WS_TESTS").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping public websocket integration test; set IMPCURL_RUN_PUBLIC_WS_TESTS=1 to enable"
        );
        return Ok(());
    }

    let ws_url = std::env::var("IMPCURL_PUBLIC_ECHO_URL")
        .unwrap_or_else(|_| DEFAULT_PUBLIC_ECHO_URL.to_owned());
    let round_trips_per_client = parse_round_trips_per_client()?;
    let _ = impcurl_sys::resolve_impersonate_lib_path(&[]).with_context(|| {
        "failed to resolve libcurl-impersonate; set CURL_IMPERSONATE_LIB to a valid runtime library"
    })?;
    let run_id = format!("impcurl-ws-{}-{}", std::process::id(), now_millis());

    let mut tasks = Vec::with_capacity(CLIENT_IDS.len());
    for &client_id in &CLIENT_IDS {
        let ws_url = ws_url.clone();
        let run_id = run_id.clone();
        let round_trips_per_client = round_trips_per_client;
        tasks.push(tokio::spawn(async move {
            run_client_round_trips(client_id, ws_url, run_id, round_trips_per_client).await
        }));
    }

    for task in tasks {
        task.await.context("client task panicked")??;
    }

    Ok(())
}

fn parse_round_trips_per_client() -> Result<usize> {
    let configured = std::env::var("IMPCURL_PUBLIC_WS_ROUNDS")
        .ok()
        .map(|raw| raw.parse::<usize>())
        .transpose()
        .context("IMPCURL_PUBLIC_WS_ROUNDS must be a positive integer")?
        .unwrap_or(DEFAULT_ROUND_TRIPS_PER_CLIENT);

    if configured < MIN_ROUND_TRIPS_PER_CLIENT {
        bail!("IMPCURL_PUBLIC_WS_ROUNDS must be >= {MIN_ROUND_TRIPS_PER_CLIENT}, got {configured}");
    }
    Ok(configured)
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

async fn run_client_round_trips(
    client_id: &'static str,
    ws_url: String,
    run_id: String,
    round_trips_per_client: usize,
) -> Result<()> {
    let mut client = WsConnection::builder(ws_url.clone())
        .connect_timeout(Duration::from_secs(15))
        .connect()
        .await
        .with_context(|| format!("failed to connect websocket client {client_id} to {ws_url}"))?;

    for seq in 0..round_trips_per_client {
        let payload = json!({
            "test": "impcurl-ws-three-connections",
            "run_id": run_id,
            "client_id": client_id,
            "seq": seq,
            "message": format!("payload-{client_id}-{seq}"),
        })
        .to_string();

        client
            .send(Message::Text(payload.clone()))
            .await
            .with_context(|| format!("failed sending payload for client {client_id} seq {seq}"))?;

        let echoed = recv_expected_echo(&mut client, client_id, &run_id, seq).await?;
        let echoed_client_id = echoed
            .get("client_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("echo payload missing client_id for client {client_id}"))?;
        if echoed_client_id != client_id {
            bail!(
                "echo message mixed up: expected {client_id}, got {echoed_client_id}; payload={echoed}"
            );
        }

        for &other_client in &CLIENT_IDS {
            if other_client == client_id {
                continue;
            }
            if echoed_client_id == other_client {
                bail!(
                    "echo message routed to wrong client: expected {client_id}, got {other_client}; payload={echoed}"
                );
            }
        }
    }
    Ok(())
}

async fn recv_expected_echo(
    client: &mut WsConnection,
    expected_client_id: &str,
    run_id: &str,
    seq: usize,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let now = Instant::now();
        if now >= deadline {
            bail!("timed out waiting for echoed payload for client {expected_client_id} seq {seq}");
        }
        let wait = (deadline - now).min(Duration::from_secs(5));
        let Some(message) = tokio::time::timeout(wait, client.next())
            .await
            .with_context(|| {
                format!("failed receiving websocket echo for {expected_client_id} seq {seq}")
            })?
        else {
            continue;
        };

        let message = message.with_context(|| {
            format!("websocket stream returned error for {expected_client_id} seq {seq}")
        })?;

        let data = match message {
            Message::Text(text) => text.into_bytes(),
            Message::Binary(bytes) => bytes.to_vec(),
            Message::Ping(_) | Message::Pong(_) | Message::Close(_) => continue,
        };

        let Ok(value) = serde_json::from_slice::<Value>(&data) else {
            continue;
        };
        if value.get("run_id").and_then(Value::as_str) != Some(run_id) {
            continue;
        }
        if value.get("seq").and_then(Value::as_u64) != Some(seq as u64) {
            continue;
        }
        return Ok(value);
    }
}
