use axum::{
    routing::{delete, get, post},
    Router,
};
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use super::handlers::{auth, backtest, signals, symbols, AppState};
use super::openapi::ApiDoc;

pub fn create_router(state: AppState) -> Router {
    // Serve the built Vite SPA from static/dist (production).
    // In dev, the Vite dev server (:5173) proxies /api calls to :3000.
    let spa_fallback = ServeDir::new("static/dist")
        .not_found_service(ServeFile::new("static/dist/index.html"));

    Router::new()
        // Auth routes
        .route("/api/auth/register", post(auth::register))
        .route("/api/auth/login", post(auth::login))
        // Symbols routes (protected via Claims extractor in handlers)
        .route("/api/symbols", get(symbols::get_symbols))
        .route("/api/symbols", post(symbols::add_symbol))
        .route("/api/symbols", delete(symbols::remove_symbol))
        .route("/api/symbols/search", get(symbols::search_symbols))
        .route("/api/symbols/chart", get(symbols::get_chart_data))
        // Legacy symbols routes
        .route("/http/symbols", get(symbols::get_symbols))
        .route("/http/symbols", post(symbols::add_symbol))
        .route("/http/symbols", delete(symbols::remove_symbol))
        // Backtest route (protected via Claims extractor in handler)
        .route("/api/backtest", post(backtest::run_backtest))
        // Signals stream + test routes (WS supports ?symbol=AAPL filter)
        .route("/api/signals/stream", get(signals::stream_signals))
        .route("/api/signals/test", post(signals::test_signal))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-doc/openapi.json", ApiDoc::openapi()))
        .layer(CorsLayer::permissive())
        .with_state(state)
        // SPA catch-all: serve static/dist for everything that isn't an API route.
        // In development this is a no-op (files won't exist); Vite handles it.
        .fallback_service(spa_fallback)
}

