//! HTX-нативний селектор carry-кошика.
//!
//! Ранжує торгований юніверс HTX (перп у торгах + спот-пара online, тобто
//! обидві ноги дельта-нейтральної позиції) за трейлінг-середнім останніх
//! settled funding-ставок — РІВНО той самий скоринг, що використовує
//! xs_carry-бектест на кожному ребалансі (trailing 21 інтервал = 7 днів).
//! Едж цього ранжування підтверджено бектестом на datasets/funding_htx
//! (OOS Sharpe 4.4 тейкер / 6.3 мейкер, 2026-07-12, config 0a440a7b).
//!
//! Ставки беруться живцем з API (settled, сторінка новіших 100) — жодної
//! залежності від свіжості локальних CSV. Id вибірки детермінований від
//! часу останнього розрахунку funding: повторний запуск у тому самому
//! 8г-вікні дає ті самі client-order-id → ідемпотентність журналу працює.

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use std::collections::HashSet;

use super::client::HtxClient;
use super::executor::LedgerBasketEntry;
use super::funding_history::csv_symbol;
use crate::shared::run_config::LiveConfig;

/// Кошик як «basket» у LiveConfig, що вмикає селектор замість shadow-журналу.
pub const HTX_TRAILING_BASKET: &str = "htx_trailing";

pub struct TrailingSelection {
    pub entry: LedgerBasketEntry,
    /// Повне ранжування (символ, трейлінг-середнє за 8г) — для друку/журналу.
    pub ranked: Vec<(String, Decimal)>,
    pub universe_size: usize,
    /// Скільки контрактів відсіяно через закоротку історію funding.
    pub short_history: usize,
}

/// Чисте ранжування: `series` = (символ, settled-ставки НОВІШІ ПЕРШИМИ).
/// Береться середнє останніх `trailing`; символи з коротшою історією та
/// невід'ємним... точніше НЕдодатним середнім відсіюються (шорт без
/// додатного карі нам не потрібен). Тай-брейк — за символом, детерміновано.
pub fn rank_by_trailing(
    series: &[(String, Vec<Decimal>)],
    trailing: usize,
) -> (Vec<(String, Decimal)>, usize) {
    let mut short_history = 0usize;
    let mut ranked: Vec<(String, Decimal)> = Vec::new();
    for (sym, rates) in series {
        if rates.len() < trailing {
            short_history += 1;
            continue;
        }
        let mean = rates[..trailing].iter().copied().sum::<Decimal>()
            / Decimal::from(trailing as u64);
        if mean > Decimal::ZERO {
            ranked.push((sym.clone(), mean));
        }
    }
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    (ranked, short_history)
}

/// Живий вибір кошика з API HTX.
pub async fn select_htx_trailing(
    client: &HtxClient,
    cfg: &LiveConfig,
) -> Result<TrailingSelection> {
    let trailing = cfg.selector_trailing_intervals;
    anyhow::ensure!(
        (3..=100).contains(&trailing),
        "selector_trailing_intervals має бути 3..=100 (одна сторінка історії)"
    );

    let spot_pairs: HashSet<String> = client
        .spot_symbols()
        .await?
        .into_iter()
        .filter(|m| m.state.as_deref() == Some("online"))
        .map(|m| m.sc.to_uppercase())
        .collect();
    let mut codes: Vec<String> = client
        .swap_contracts()
        .await?
        .into_iter()
        .filter(|c| c.is_trading() && c.supports_cross())
        .map(|c| c.contract_code)
        .filter(|code| spot_pairs.contains(&csv_symbol(code)))
        .collect();
    codes.sort();
    anyhow::ensure!(!codes.is_empty(), "порожній торгований юніверс HTX");

    let mut series: Vec<(String, Vec<Decimal>)> = Vec::new();
    let mut latest_ms: i64 = 0;
    for code in &codes {
        let page = client.swap_historical_funding(code, 1, 100).await?;
        let mut rates: Vec<Decimal> = Vec::new();
        for row in &page.data {
            if let (Some(ts), Some(rate)) = (row.time_ms(), row.settled_rate()) {
                latest_ms = latest_ms.max(ts);
                rates.push(rate);
            }
        }
        series.push((csv_symbol(code), rates));
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    }

    let anchor = Utc
        .timestamp_millis_opt(latest_ms)
        .single()
        .context("не визначився час останнього funding-розрахунку")?;
    // Свіжість: останній розрахунок має бути в межах ~9 годин (8г інтервал
    // + запас), інакше API віддає щось не те.
    anyhow::ensure!(
        Utc::now() - anchor < chrono::Duration::hours(9),
        "останній settled funding застарий ({anchor}) — щось не так із даними"
    );

    let (ranked, short_history) = rank_by_trailing(&series, trailing);
    anyhow::ensure!(
        ranked.len() >= cfg.top_k,
        "лише {} символів з додатним трейлінг-фандингом — менше за top_k={}",
        ranked.len(),
        cfg.top_k
    );

    // Перші 8 символів id — YYMMDDHH останнього розрахунку: детерміновані
    // client-order-id у межах одного funding-вікна (див. spot_client_id).
    let entry = LedgerBasketEntry {
        id: format!("{}-htxsel-t{}", anchor.format("%y%m%d%H"), trailing),
        anchor_ts: anchor.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        retro: false,
        basket_name: HTX_TRAILING_BASKET.to_string(),
        symbols: ranked
            .iter()
            .take(cfg.top_k)
            .map(|(s, _)| s.clone())
            .collect(),
    };
    Ok(TrailingSelection {
        entry,
        ranked,
        universe_size: codes.len(),
        short_history,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn s(sym: &str, rates: &[Decimal]) -> (String, Vec<Decimal>) {
        (sym.to_string(), rates.to_vec())
    }

    #[test]
    fn ranks_by_mean_of_newest_trailing_window() {
        let series = vec![
            // Середнє останніх 2: (0.4+0.2)/2 = 0.3; старий хвіст ігнорується.
            s("AAAUSDT", &[dec!(0.4), dec!(0.2), dec!(-99)]),
            s("BBBUSDT", &[dec!(0.1), dec!(0.1)]),
        ];
        let (ranked, short) = rank_by_trailing(&series, 2);
        assert_eq!(short, 0);
        assert_eq!(ranked[0], ("AAAUSDT".to_string(), dec!(0.3)));
        assert_eq!(ranked[1], ("BBBUSDT".to_string(), dec!(0.1)));
    }

    #[test]
    fn drops_short_history_and_non_positive_means() {
        let series = vec![
            s("SHORTUSDT", &[dec!(0.5)]),                  // 1 точка < trailing=2
            s("NEGUSDT", &[dec!(-0.1), dec!(0.05)]),       // середнє < 0
            s("ZEROUSDT", &[dec!(0.1), dec!(-0.1)]),       // середнє == 0 — теж геть
            s("OKUSDT", &[dec!(0.02), dec!(0.02)]),
        ];
        let (ranked, short) = rank_by_trailing(&series, 2);
        assert_eq!(short, 1);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].0, "OKUSDT");
    }

    #[test]
    fn ties_break_by_symbol_deterministically() {
        let series = vec![
            s("BBBUSDT", &[dec!(0.1), dec!(0.1)]),
            s("AAAUSDT", &[dec!(0.1), dec!(0.1)]),
        ];
        let (ranked, _) = rank_by_trailing(&series, 2);
        assert_eq!(ranked[0].0, "AAAUSDT");
        assert_eq!(ranked[1].0, "BBBUSDT");
    }
}
