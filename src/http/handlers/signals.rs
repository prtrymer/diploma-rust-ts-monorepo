use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;

use crate::message_broker::ports::{self, MessageHandler};

use super::AppState;

// ---------------------------------------------------------------------------
// Adapter: Kafka → broadcast channel bridge
// ---------------------------------------------------------------------------
/// Receives trading-signal messages from Kafka and fans them out to all
/// connected WebSocket / SSE clients via a `broadcast::Sender`.
///
/// This struct lives in the HTTP handler layer because it is the inbound
/// adapter that bridges the message-broker hexagon with the HTTP hexagon.
pub struct HttpSignalHandler {
    pub tx: broadcast::Sender<String>,
    pub recent_signals: Arc<RwLock<std::collections::VecDeque<String>>>,
    pub topic: String,
}

const MAX_RECENT_SIGNALS: usize = 50;

#[async_trait]
impl MessageHandler for HttpSignalHandler {
    fn topics(&self) -> Vec<String> {
        vec![self.topic.clone()]
    }

    async fn handle(&self, message: &ports::Message) -> Result<()> {
        let payload = message.payload.clone();

        // 1. Update recent signals buffer
        {
            let mut buffer: tokio::sync::RwLockWriteGuard<std::collections::VecDeque<String>> = 
                self.recent_signals.write().await;
            if buffer.len() >= MAX_RECENT_SIGNALS {
                buffer.pop_back(); // remove oldest (at the back)
            }
            buffer.push_front(payload.clone()); // newest at the front
        }

        // 2. Broadcast to live listeners
        let _ = self.tx.send(payload);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Query parameters for JWT token and optional symbol filter
// ---------------------------------------------------------------------------
#[derive(Debug, Deserialize)]
pub struct WsAuthQuery {
    /// JWT token passed as `?token=<jwt>` because the browser WebSocket API
    /// does not allow setting Authorization headers.
    #[serde(default)]
    pub token: Option<String>,
}

// ---------------------------------------------------------------------------
// Handler: dual-mode — WebSocket upgrade OR SSE fallback
// ---------------------------------------------------------------------------
/// Stream live trading signals produced by the trading engine.
///
/// **Protocol negotiation:**
/// - If the client sends a proper WebSocket upgrade request, the connection
///   is upgraded to a WebSocket and signals are pushed as text frames.
/// - Otherwise the endpoint falls back to **Server-Sent Events (SSE)**, which
///   works with curl, Swagger UI, and any plain HTTP client.
///
/// **Authentication**: Pass your JWT token as a query parameter:
///   `ws://host/api/signals/stream?token=<jwt>` (WebSocket)
///   `GET /api/signals/stream?token=<jwt>` (SSE)
#[utoipa::path(
    get,
    path = "/api/signals/stream",
    tag = "Signals",
    params(
        ("token" = Option<String>, Query, description = "JWT bearer token"),
    ),
    responses(
        (status = 200, description = "SSE stream of trading signals (fallback for plain HTTP)"),
        (status = 101, description = "WebSocket upgrade – trading signals stream"),
        (status = 401, description = "Unauthorized – missing or invalid token"),
    ),
    security(
        ("BearerAuth" = [])
    )
)]
pub async fn stream_signals(
    ws: Option<WebSocketUpgrade>,
    State(state): State<AppState>,
    Query(query): Query<WsAuthQuery>,
) -> Response {
    // --- Authenticate via query-param JWT ---
    if let Err(resp) = authenticate(&query) {
        return resp;
    }

    println!("📡 New signal stream request: mode={}", 
        if ws.is_some() { "WebSocket" } else { "SSE" });

    match ws {
        // ---- WebSocket path ----
        Some(upgrade) => {
            let rx = state.signals_tx.subscribe();
            let history = state.recent_signals.clone();
            upgrade
                .on_upgrade(move |socket| handle_ws(socket, rx, history))
                .into_response()
        }
        // ---- SSE fallback (curl / Swagger / plain HTTP) ----
        None => {
            let rx = state.signals_tx.subscribe();
            let stream = BroadcastStream::new(rx).filter_map(move |result| {
                async move {
                    match result {
                        Ok(payload) => {
                            let event: std::result::Result<Event, std::convert::Infallible> =
                                Ok(Event::default().event("signal").data(payload));
                            Some(event)
                        }
                        Err(_) => None, // lagged — skip
                    }
                }
            });

            Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// JWT validation helper
// ---------------------------------------------------------------------------
fn authenticate(query: &WsAuthQuery) -> std::result::Result<(), Response> {
    let secret = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "super-secret-key-change-me".to_string());

    if let Some(ref token) = query.token {
        let validation = jsonwebtoken::Validation::default();
        if jsonwebtoken::decode::<crate::http::middlewares::auth::Claims>(
            token,
            &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
            &validation,
        )
        .is_err()
        {
            return Err(axum::response::Response::builder()
                .status(axum::http::StatusCode::UNAUTHORIZED)
                .body(axum::body::Body::from("Invalid token"))
                .unwrap()
                .into_response());
        }
    }
    // NOTE: if you want to *require* auth, return Err(401) when token is None.
    // Currently kept permissive for development convenience.
    Ok(())
}

// ---------------------------------------------------------------------------
// Test endpoint: inject a mock signal for development/debugging
// ---------------------------------------------------------------------------
/// Publish a mock `SignalEvent` into the broadcast channel so connected
/// WebSocket / SSE clients can verify data flow without the full Kafka
/// trading pipeline running.
#[utoipa::path(
    post,
    path = "/api/signals/test",
    tag = "Signals",
    responses(
        (status = 200, description = "Mock signal published", body = MessageResponse),
    ),
    security(
        ("BearerAuth" = [])
    )
)]
pub async fn test_signal(
    State(state): State<AppState>,
) -> impl IntoResponse {
    use crate::trading::domain::events::{SignalDirection, SignalEvent};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    let directions = [SignalDirection::Long, SignalDirection::Short, SignalDirection::Exit];
    let symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"];

    let signal = SignalEvent {
        id: Uuid::new_v4(),
        timestamp: Utc::now(),
        symbol: symbols[rand_index(symbols.len())].to_string(),
        direction: directions[rand_index(directions.len())],
        strength: dec!(0.42),
        strategy_name: "MomentumStrategy".to_string(),
        metadata: Some(r#"{"rsi":62.3,"ema_cross":"bullish"}"#.to_string()),
    };

    let json = serde_json::to_string(&signal).unwrap();
    let receivers = state.signals_tx.send(json.clone()).unwrap_or(0);

    let resp = serde_json::json!({
        "message": format!("Mock signal published to {} receiver(s)", receivers),
        "signal": signal,
    });
    (axum::http::StatusCode::OK, axum::Json(resp))
}

/// Simple pseudo-random index (no extra deps needed).
fn rand_index(len: usize) -> usize {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as usize;
    nanos % len
}

// ---------------------------------------------------------------------------
// WebSocket session loop
// ---------------------------------------------------------------------------
async fn handle_ws(
    socket: WebSocket,
    mut rx: broadcast::Receiver<String>,
    recent_signals: Arc<RwLock<std::collections::VecDeque<String>>>,
) {
    let (mut sender, mut receiver) = socket.split();
    println!("🔌 WebSocket connection established (replaying history...)");

    // 1. Replay recent signals
    {
        let history: tokio::sync::RwLockReadGuard<std::collections::VecDeque<String>> = 
            recent_signals.read().await;
        // Send from back to front (oldest to newest)
        for signal_json in history.iter().rev() {
            if sender.send(Message::Text(signal_json.clone().into())).await.is_err() {
                return;
            }
        }
    }

    // 2. Spawn a task that forwards live broadcast signals → WebSocket text frames.
    let mut send_task = tokio::spawn(async move {
        while let Ok(signal_json) = rx.recv().await {
            if sender.send(Message::Text(signal_json.into())).await.is_err() {
                break;
            }
        }
        println!("🔌 WebSocket send task terminated");
    });

    // Spawn a task that reads client messages (pings / close).
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Close(_) => {
                    println!("🔌 WebSocket closed by client");
                    break;
                }
                Message::Ping(payload) => {
                    let _ = payload;
                }
                _ => {} // Ignore other client messages.
            }
        }
        println!("🔌 WebSocket receive task terminated");
    });

    // When either task finishes, abort the other.
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
}
