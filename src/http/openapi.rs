use utoipa::OpenApi;
use crate::http::models::*;

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::http::handlers::auth::register,
        crate::http::handlers::auth::login,
        crate::http::handlers::symbols::get_symbols,
        crate::http::handlers::symbols::add_symbol,
        crate::http::handlers::symbols::remove_symbol,
        crate::http::handlers::symbols::search_symbols,
        crate::http::handlers::symbols::get_chart_data,
        crate::http::handlers::backtest::run_backtest,
        crate::http::handlers::signals::stream_signals,
        crate::http::handlers::signals::test_signal,
    ),
    components(
        schemas(
            AddSymbolRequest,
            RemoveSymbolRequest,
            SymbolsResponse,
            MessageResponse,
            BacktestRequest,
            RegisterRequest,
            LoginRequest,
            AuthResponse,
            SymbolSearchResponse,
            SymbolSearchItem,
            ChartCandle,
            ChartResponse
        )
    ),
    modifiers(&SecurityAddon),
    tags(
        (name = "Auth", description = "Authentication endpoints"),
        (name = "Symbols", description = "Symbol management endpoints"),
        (name = "Backtest", description = "Backtesting engine endpoints"),
        (name = "Signals", description = "Real-time trading signals"),
    )
)]
pub struct ApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "BearerAuth",
                utoipa::openapi::security::SecurityScheme::Http(
                    utoipa::openapi::security::HttpBuilder::new()
                        .scheme(utoipa::openapi::security::HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            )
        }
    }
}
