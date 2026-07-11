//! Якість даних (M0.6): point-in-time доступ, явні корпоративні дії,
//! юніверс без survivorship bias.
//!
//! Тихий вбивця бектестів — ретроспективно змінені дані. Тут це виключено
//! контрактом типів:
//!   1. `PointInTimeStore` віддає записи ТІЛЬКИ через `as_of(t)` — API
//!      фізично не має методу «дай усе» чи «дай майбутнє».
//!   2. Корекції на спліти/дивіденди — явна функція над raw-серією;
//!      зберігаються ОБИДВІ серії (raw і adjusted).
//!   3. `HistoricalUniverse` включає делістингнуті тікери, якщо вони
//!      існували в запитаний період.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::database::domain::models::HistoricalCandle;

// ── 1. Point-in-time доступ ──────────────────────────────────────────────────

/// Запис із двома часами: коли ПОДІЯ сталась і коли стала ВІДОМА системі.
/// Ретроспективні корекції отримують пізніший `known_at` і не видні бектесту
/// на момент події.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PitRecord<T> {
    pub effective_at: DateTime<Utc>,
    pub known_at: DateTime<Utc>,
    pub value: T,
}

/// Сховище point-in-time записів. Єдиний метод читання — `as_of`.
#[derive(Debug, Clone, Default)]
pub struct PointInTimeStore<T: Clone> {
    records: Vec<PitRecord<T>>,
}

impl<T: Clone> PointInTimeStore<T> {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    /// Додає запис. Якщо `known_at` невідомий (стандартний фід у реальному
    /// часі) — використовуйте `effective_at`.
    pub fn insert(&mut self, record: PitRecord<T>) {
        self.records.push(record);
    }

    /// ЄДИНИЙ метод читання: усе, що було ВІДОМЕ на момент `t`
    /// (known_at ≤ t), відсортоване за effective_at.
    /// Попросити «майбутнє» неможливо — параметра для цього не існує.
    pub fn as_of(&self, t: DateTime<Utc>) -> Vec<&PitRecord<T>> {
        let mut out: Vec<&PitRecord<T>> = self
            .records
            .iter()
            .filter(|r| r.known_at <= t)
            .collect();
        out.sort_by_key(|r| r.effective_at);
        out
    }
}

// ── 2. Корпоративні дії: спліти й дивіденди ──────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CorporateAction {
    /// Спліт `ratio`:1 (2.0 = кожна акція стала двома).
    Split { ratio: Decimal },
    /// Грошовий дивіденд на акцію.
    Dividend { amount: Decimal },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorporateActionEvent {
    pub symbol: String,
    /// Ex-date: перший день, коли ціна торгується без права на дію.
    pub ex_date: DateTime<Utc>,
    pub action: CorporateAction,
}

/// Явна корекція raw-цін на спліти/дивіденди (backward adjustment):
/// усі бари ДО ex-date множаться на кумулятивний фактор, щоб серія була
/// неперервною в термінах total return. Повертає НОВУ серію; raw лишається
/// недоторканим (М0.6: зберігати і raw, і adjusted).
pub fn adjust_for_corporate_actions(
    raw: &[HistoricalCandle],
    actions: &[CorporateActionEvent],
) -> Vec<HistoricalCandle> {
    let mut adjusted: Vec<HistoricalCandle> = raw.to_vec();
    // Обробка від найранішої дії до найпізнішої; фактори накопичуються на
    // сегменті [0, ex_date).
    let mut sorted: Vec<&CorporateActionEvent> = actions.iter().collect();
    sorted.sort_by_key(|a| a.ex_date);

    for event in sorted {
        // Ціна закриття останнього бару перед ex-date — база дивідендного фактора.
        let last_before = adjusted
            .iter().rfind(|c| c.timestamp < event.ex_date);
        let factor = match &event.action {
            CorporateAction::Split { ratio } => {
                if *ratio <= Decimal::ZERO {
                    continue;
                }
                Decimal::ONE / *ratio
            }
            CorporateAction::Dividend { amount } => {
                let Some(base) = last_before else { continue };
                if base.close <= Decimal::ZERO {
                    continue;
                }
                (base.close - *amount) / base.close
            }
        };
        for candle in adjusted
            .iter_mut()
            .filter(|c| c.timestamp < event.ex_date)
        {
            candle.open *= factor;
            candle.high *= factor;
            candle.low *= factor;
            candle.close *= factor;
            candle.adj_close = Some(candle.close);
            if let CorporateAction::Split { ratio } = &event.action {
                // Обсяг масштабується обернено до ціни при спліті.
                let scaled = Decimal::from(candle.volume) * *ratio;
                candle.volume = scaled.round().mantissa() as i64 / 10i64.pow(scaled.scale());
            }
        }
    }
    for candle in adjusted.iter_mut() {
        if candle.adj_close.is_none() {
            candle.adj_close = Some(candle.close);
        }
    }
    adjusted
}

// ── 3. Юніверс без survivorship bias ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniverseEntry {
    pub symbol: String,
    pub listed_at: DateTime<Utc>,
    /// None = досі торгується. «Мертві» тікери зберігаються в юніверсі.
    pub delisted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct HistoricalUniverse {
    entries: Vec<UniverseEntry>,
}

impl HistoricalUniverse {
    pub fn new(entries: Vec<UniverseEntry>) -> Self {
        Self { entries }
    }

    /// Символи, що ІСНУВАЛИ на момент `t` — включно з тими, що пізніше
    /// делістингнулись. Бектест по цьому списку не має survivorship bias.
    pub fn members_at(&self, t: DateTime<Utc>) -> Vec<&UniverseEntry> {
        self.entries
            .iter()
            .filter(|e| e.listed_at <= t && e.delisted_at.map(|d| d > t).unwrap_or(true))
            .collect()
    }

    /// Усі символи, що існували БУДЬ-КОЛИ в періоді [start, end] —
    /// правильний історичний юніверс для бектесту періоду.
    pub fn members_during(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<&UniverseEntry> {
        self.entries
            .iter()
            .filter(|e| {
                e.listed_at <= end && e.delisted_at.map(|d| d >= start).unwrap_or(true)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn ts(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap()
    }

    fn candle(day: u32, close: Decimal) -> HistoricalCandle {
        HistoricalCandle {
            symbol: "TEST".into(),
            timeframe: "1d".into(),
            timestamp: ts(2024, 6, day),
            open: close,
            high: close,
            low: close,
            close,
            adj_close: None,
            volume: 1000,
            source: "test".into(),
        }
    }

    // M0.6: API неможливо попросити «майбутнє» — as_of повертає тільки
    // записи, відомі на момент t; ретроспективна корекція невидима.
    #[test]
    fn as_of_hides_future_and_late_corrections() {
        let mut store: PointInTimeStore<Decimal> = PointInTimeStore::new();
        store.insert(PitRecord {
            effective_at: ts(2024, 6, 3),
            known_at: ts(2024, 6, 3),
            value: dec!(100),
        });
        // Корекція заднім числом: значення за 3 червня, опубліковане 10-го.
        store.insert(PitRecord {
            effective_at: ts(2024, 6, 3),
            known_at: ts(2024, 6, 10),
            value: dec!(99),
        });
        store.insert(PitRecord {
            effective_at: ts(2024, 6, 5),
            known_at: ts(2024, 6, 5),
            value: dec!(101),
        });

        let visible_4th = store.as_of(ts(2024, 6, 4));
        assert_eq!(visible_4th.len(), 1);
        assert_eq!(visible_4th[0].value, dec!(100), "корекція з 10-го невидима 4-го");

        let visible_11th = store.as_of(ts(2024, 6, 11));
        assert_eq!(visible_11th.len(), 3, "після публікації корекція видима");
    }

    // M0.6: коректність adjusted-цін навколо відомого спліту 2:1.
    #[test]
    fn split_adjustment_halves_prices_before_ex_date() {
        let raw = vec![
            candle(1, dec!(100)),
            candle(2, dec!(102)),
            candle(3, dec!(51)), // після спліту 2:1 ринкова ціна ~51
            candle(4, dec!(52)),
        ];
        let actions = vec![CorporateActionEvent {
            symbol: "TEST".into(),
            ex_date: ts(2024, 6, 3),
            action: CorporateAction::Split { ratio: dec!(2) },
        }];
        let adjusted = adjust_for_corporate_actions(&raw, &actions);

        assert_eq!(adjusted[0].close, dec!(50));
        assert_eq!(adjusted[1].close, dec!(51));
        assert_eq!(adjusted[2].close, dec!(51), "бар після ex-date не змінюється");
        assert_eq!(adjusted[3].close, dec!(52));
        // Adjusted-серія неперервна: 51 → 51 без фіктивного −50% гепа.
        // Raw недоторканий:
        assert_eq!(raw[0].close, dec!(100));
        // Обсяг масштабований у 2 рази:
        assert_eq!(adjusted[0].volume, 2000);
    }

    #[test]
    fn dividend_adjustment_scales_prior_bars() {
        let raw = vec![candle(1, dec!(100)), candle(2, dec!(98))];
        let actions = vec![CorporateActionEvent {
            symbol: "TEST".into(),
            ex_date: ts(2024, 6, 2),
            action: CorporateAction::Dividend { amount: dec!(2) },
        }];
        let adjusted = adjust_for_corporate_actions(&raw, &actions);
        // Фактор = (100 − 2)/100 = 0.98 → бар до ex-date: 98.
        assert_eq!(adjusted[0].close, dec!(98));
        assert_eq!(adjusted[1].close, dec!(98));
    }

    // M0.6: історичний юніверс містить делістингнуті інструменти за період.
    #[test]
    fn universe_includes_delisted_tickers_for_period() {
        let universe = HistoricalUniverse::new(vec![
            UniverseEntry {
                symbol: "ALIVE".into(),
                listed_at: ts(2010, 1, 1),
                delisted_at: None,
            },
            UniverseEntry {
                symbol: "DEAD2022".into(),
                listed_at: ts(2015, 1, 1),
                delisted_at: Some(ts(2022, 6, 1)),
            },
            UniverseEntry {
                symbol: "LATE_IPO".into(),
                listed_at: ts(2023, 5, 1),
                delisted_at: None,
            },
        ]);

        // Бектест 2021 року: DEAD2022 ще живий і ЗОБОВ'ЯЗАНИЙ бути в юніверсі.
        let members = universe.members_during(ts(2021, 1, 1), ts(2021, 12, 31));
        let symbols: Vec<&str> = members.iter().map(|e| e.symbol.as_str()).collect();
        assert!(symbols.contains(&"ALIVE"));
        assert!(symbols.contains(&"DEAD2022"), "survivorship bias!");
        assert!(!symbols.contains(&"LATE_IPO"), "IPO 2023 не існував у 2021");

        // На конкретну дату після делістингу — DEAD2022 вже відсутній.
        let at_2023: Vec<&str> = universe
            .members_at(ts(2023, 6, 1))
            .iter()
            .map(|e| e.symbol.as_str())
            .collect();
        assert!(!at_2023.contains(&"DEAD2022"));
        assert!(at_2023.contains(&"LATE_IPO"));
    }
}
