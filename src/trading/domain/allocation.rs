//! Портфельні стратегії рівня барів: цільові ваги на кожен момент часу.
//!
//! `AllocationStrategy` — це трейт `Strategy` з роадмапу (M0.2): бенчмарки
//! (BuyAndHold, EqualWeight, 60/40), TSMOM (M2.1) і cross-sectional momentum
//! (M3.2) — його імплементації, що проходять ОДИН і той самий движок
//! виконання з тією самою моделлю витрат (інваріант 1).
//!
//! Інваріант 3 (нема look-ahead) забезпечений типом `UniverseView`: стратегія
//! фізично не може прочитати бар пізніше за поточний індекс — API просто не
//! надає такого методу.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Вирівняні мульти-символьні серії барів (спільна вісь часу).
/// Зберігає і raw, і adjusted ціни (M0.6: корекції явні, обидві серії доступні).
#[derive(Debug, Clone)]
pub struct AlignedMarketData {
    timestamps: Vec<DateTime<Utc>>,
    /// symbol → close-серія (adjusted, для сигналів і PnL безрозмірних величин).
    closes: BTreeMap<String, Vec<Decimal>>,
    /// symbol → raw close-серія (як торгувалась; для виконання).
    raw_closes: BTreeMap<String, Vec<Decimal>>,
    /// symbol → обсяги.
    volumes: BTreeMap<String, Vec<Decimal>>,
}

impl AlignedMarketData {
    /// Створює вирівняний датасет. Кожна серія має збігатися за довжиною з
    /// віссю часу; неповні серії — помилка (вирівнювання робить адаптер даних).
    pub fn new(
        timestamps: Vec<DateTime<Utc>>,
        closes: BTreeMap<String, Vec<Decimal>>,
        raw_closes: BTreeMap<String, Vec<Decimal>>,
        volumes: BTreeMap<String, Vec<Decimal>>,
    ) -> anyhow::Result<Self> {
        let n = timestamps.len();
        for (sym, series) in closes.iter().chain(raw_closes.iter()).chain(volumes.iter()) {
            anyhow::ensure!(
                series.len() == n,
                "series length mismatch for {sym}: {} != {n}",
                series.len()
            );
        }
        anyhow::ensure!(
            timestamps.windows(2).all(|w| w[0] < w[1]),
            "timestamps must be strictly increasing"
        );
        Ok(Self {
            timestamps,
            closes,
            raw_closes,
            volumes,
        })
    }

    /// Спрощений конструктор, коли adjusted == raw і обсяги невідомі.
    pub fn from_closes(
        timestamps: Vec<DateTime<Utc>>,
        closes: BTreeMap<String, Vec<Decimal>>,
    ) -> anyhow::Result<Self> {
        let n = timestamps.len();
        let volumes: BTreeMap<String, Vec<Decimal>> = closes
            .keys()
            .map(|s| (s.clone(), vec![Decimal::ZERO; n]))
            .collect();
        Self::new(timestamps, closes.clone(), closes, volumes)
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn symbols(&self) -> impl Iterator<Item = &String> {
        self.closes.keys()
    }

    pub fn timestamp(&self, idx: usize) -> Option<DateTime<Utc>> {
        self.timestamps.get(idx).copied()
    }

    fn close_at(&self, symbol: &str, idx: usize) -> Option<Decimal> {
        self.closes.get(symbol).and_then(|s| s.get(idx)).copied()
    }

    fn raw_close_at(&self, symbol: &str, idx: usize) -> Option<Decimal> {
        self.raw_closes.get(symbol).and_then(|s| s.get(idx)).copied()
    }

    fn volume_at(&self, symbol: &str, idx: usize) -> Option<Decimal> {
        self.volumes.get(symbol).and_then(|s| s.get(idx)).copied()
    }
}

/// Point-in-time вікно даних: видно тільки бари з індексом ≤ `upto`.
/// Єдиний спосіб для стратегії читати ринок — через цей тип.
#[derive(Clone)]
pub struct UniverseView {
    data: Arc<AlignedMarketData>,
    upto: usize,
}

impl UniverseView {
    /// `upto` — індекс поточного бару (включно). Панікує поза межами даних —
    /// це помилка движка, не стратегії.
    pub fn new(data: Arc<AlignedMarketData>, upto: usize) -> Self {
        assert!(upto < data.len(), "UniverseView index out of range");
        Self { data, upto }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.data.timestamps[self.upto]
    }

    /// Кількість доступних барів (усі ≤ поточного часу).
    pub fn bars_available(&self) -> usize {
        self.upto + 1
    }

    pub fn symbols(&self) -> impl Iterator<Item = &String> {
        self.data.symbols()
    }

    /// Поточна ціна (adjusted close бару t).
    pub fn price(&self, symbol: &str) -> Option<Decimal> {
        self.data.close_at(symbol, self.upto)
    }

    /// Raw ціна поточного бару (для виконання ордерів).
    pub fn raw_price(&self, symbol: &str) -> Option<Decimal> {
        self.data.raw_close_at(symbol, self.upto)
    }

    /// Ціна `n` барів тому (n=0 → поточна). None, якщо історії не вистачає.
    pub fn price_n_bars_ago(&self, symbol: &str, n: usize) -> Option<Decimal> {
        let idx = self.upto.checked_sub(n)?;
        self.data.close_at(symbol, idx)
    }

    /// Прості дохідності останніх `n` барів (закінчуються на поточному барі).
    pub fn returns_window(&self, symbol: &str, n: usize) -> Vec<Decimal> {
        let mut out = Vec::with_capacity(n);
        let start = self.upto.saturating_sub(n);
        for i in start..self.upto {
            if let (Some(prev), Some(curr)) = (
                self.data.close_at(symbol, i),
                self.data.close_at(symbol, i + 1),
            ) {
                if prev > Decimal::ZERO {
                    out.push((curr - prev) / prev);
                }
            }
        }
        out
    }

    /// Середній обсяг за останні `n` барів (включно з поточним).
    pub fn avg_volume(&self, symbol: &str, n: usize) -> Option<Decimal> {
        if n == 0 {
            return None;
        }
        let start = self.upto.saturating_sub(n.saturating_sub(1));
        let mut sum = Decimal::ZERO;
        let mut count = 0u32;
        for i in start..=self.upto {
            if let Some(v) = self.data.volume_at(symbol, i) {
                sum += v;
                count += 1;
            }
        }
        if count == 0 {
            None
        } else {
            Some(sum / Decimal::from(count))
        }
    }
}

/// Стратегія портфельних ваг. Ваги — частки капіталу; від'ємні = шорт;
/// незгадані символи = 0. Сума |ваг| може бути < 1 (решта в кеші).
pub trait AllocationStrategy: Send {
    fn name(&self) -> &str;

    /// Цільові ваги на момент `view.timestamp()`. Викликається на кожному
    /// ребаланс-барі; повертає None — тримати поточні позиції без змін.
    fn target_weights(&mut self, view: &UniverseView) -> Option<BTreeMap<String, Decimal>>;

    /// Скільки барів історії потрібно до першого рішення.
    fn warmup_bars(&self) -> usize {
        0
    }

    /// Інтервал ребалансу в барах (1 = кожен бар).
    fn rebalance_every(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn ts(day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 1, day, 0, 0, 0).unwrap()
    }

    fn sample() -> Arc<AlignedMarketData> {
        let timestamps = vec![ts(1), ts(2), ts(3), ts(4)];
        let mut closes = BTreeMap::new();
        closes.insert("A".to_string(), vec![dec!(10), dec!(11), dec!(12), dec!(13)]);
        Arc::new(AlignedMarketData::from_closes(timestamps, closes).unwrap())
    }

    // Інваріант 3: API не дає прочитати майбутнє — з view на t=1
    // доступні тільки бари 0..=1.
    #[test]
    fn view_cannot_see_future() {
        let data = sample();
        let view = UniverseView::new(data, 1);
        assert_eq!(view.price("A"), Some(dec!(11)));
        assert_eq!(view.price_n_bars_ago("A", 1), Some(dec!(10)));
        // Історії глибше за початок немає:
        assert_eq!(view.price_n_bars_ago("A", 2), None);
        // Методу "ціна через n барів у майбутньому" не існує за конструкцією;
        // returns_window теж закінчується поточним баром:
        let w = view.returns_window("A", 10);
        assert_eq!(w.len(), 1); // тільки перехід 0→1
    }

    #[test]
    fn misaligned_series_rejected() {
        let timestamps = vec![ts(1), ts(2)];
        let mut closes = BTreeMap::new();
        closes.insert("A".to_string(), vec![dec!(10)]);
        assert!(AlignedMarketData::from_closes(timestamps, closes).is_err());
    }

    #[test]
    fn non_monotonic_timestamps_rejected() {
        let timestamps = vec![ts(2), ts(1)];
        let mut closes = BTreeMap::new();
        closes.insert("A".to_string(), vec![dec!(10), dec!(11)]);
        assert!(AlignedMarketData::from_closes(timestamps, closes).is_err());
    }
}
