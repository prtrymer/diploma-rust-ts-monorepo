use anyhow::Result;
use rust_decimal::Decimal;

use super::models::FeatureValue;
use crate::database::domain::models::Candle;

pub trait Feature: Send + Sync {
    fn name(&self) -> &str;
    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue>;
}

// --- SMA ---

pub struct SmaFeature {
    pub period: usize,
    pub label: String,
}

impl SmaFeature {
    pub fn new(period: usize) -> Self {
        Self {
            label: format!("sma_{}", period),
            period,
        }
    }
}

impl Feature for SmaFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if data.len() < self.period {
            return Ok(FeatureValue::Missing);
        }
        let sum: Decimal = data[data.len() - self.period..]
            .iter()
            .map(|c| c.close)
            .sum();
        let avg = sum / Decimal::from(self.period as u64);
        Ok(FeatureValue::Scalar(avg))
    }
}

// --- EMA ---

pub struct EmaFeature {
    pub period: usize,
    pub label: String,
}

impl EmaFeature {
    pub fn new(period: usize) -> Self {
        Self {
            label: format!("ema_{}", period),
            period,
        }
    }
}

impl Feature for EmaFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if data.len() < self.period {
            return Ok(FeatureValue::Missing);
        }
        let multiplier = Decimal::TWO / Decimal::from((self.period + 1) as u64);
        let seed: Decimal = data[..self.period].iter().map(|c| c.close).sum::<Decimal>()
            / Decimal::from(self.period as u64);
        let ema = data[self.period..].iter().fold(seed, |prev_ema, candle| {
            (candle.close - prev_ema) * multiplier + prev_ema
        });
        Ok(FeatureValue::Scalar(ema))
    }
}

// --- RSI ---

pub struct RsiFeature {
    pub period: usize,
    pub label: String,
}

impl RsiFeature {
    pub fn new(period: usize) -> Self {
        Self {
            label: format!("rsi_{}", period),
            period,
        }
    }
}

impl Feature for RsiFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if data.len() < self.period + 1 {
            return Ok(FeatureValue::Missing);
        }
        let mut gains = Decimal::ZERO;
        let mut losses = Decimal::ZERO;
        let start = data.len() - self.period - 1;
        for i in (start + 1)..data.len() {
            let change = data[i].close - data[i - 1].close;
            if change > Decimal::ZERO {
                gains += change;
            } else {
                losses += change.abs();
            }
        }
        let period_dec = Decimal::from(self.period as u64);
        let avg_gain = gains / period_dec;
        let avg_loss = losses / period_dec;
        if avg_loss == Decimal::ZERO {
            return Ok(FeatureValue::Scalar(Decimal::from(100)));
        }
        let rs = avg_gain / avg_loss;
        let rsi = Decimal::from(100) - (Decimal::from(100) / (Decimal::ONE + rs));
        Ok(FeatureValue::Scalar(rsi))
    }
}

// --- MACD ---

pub struct MacdFeature {
    pub label: String,
}

impl Default for MacdFeature {
    fn default() -> Self {
        Self::new()
    }
}

impl MacdFeature {
    pub fn new() -> Self {
        Self {
            label: "macd".to_string(),
        }
    }
}

impl Feature for MacdFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        let ema_12 = EmaFeature::new(12);
        let ema_26 = EmaFeature::new(26);
        let short = ema_12.calculate(data)?;
        let long = ema_26.calculate(data)?;
        match (short, long) {
            (FeatureValue::Scalar(s), FeatureValue::Scalar(l)) => Ok(FeatureValue::Scalar(s - l)),
            _ => Ok(FeatureValue::Missing),
        }
    }
}

// --- Bollinger Bands ---

pub struct BollingerBandsFeature {
    pub period: usize,
    pub std_dev_multiplier: Decimal,
    pub label: String,
}

impl BollingerBandsFeature {
    pub fn new(period: usize, std_dev_multiplier: Decimal) -> Self {
        Self {
            label: format!("bbands_{}", period),
            period,
            std_dev_multiplier,
        }
    }
}

impl Feature for BollingerBandsFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if data.len() < self.period {
            return Ok(FeatureValue::Missing);
        }
        let slice = &data[data.len() - self.period..];
        let sum: Decimal = slice.iter().map(|c| c.close).sum();
        let mean = sum / Decimal::from(self.period as u64);
        let variance: Decimal = slice
            .iter()
            .map(|c| {
                let diff = c.close - mean;
                diff * diff
            })
            .sum::<Decimal>()
            / Decimal::from(self.period as u64);

        // Approximate sqrt via Newton's method (5 iterations)
        let mut std_dev = variance / Decimal::TWO;
        if std_dev > Decimal::ZERO {
            for _ in 0..10 {
                std_dev = (std_dev + variance / std_dev) / Decimal::TWO;
            }
        }

        let upper = mean + self.std_dev_multiplier * std_dev;
        let lower = mean - self.std_dev_multiplier * std_dev;
        // Return as vector: [lower, middle, upper]
        Ok(FeatureValue::Vector(vec![lower, mean, upper]))
    }
}

// --- Volume SMA ---

pub struct VolumeSmaFeature {
    pub period: usize,
    pub label: String,
}

impl VolumeSmaFeature {
    pub fn new(period: usize) -> Self {
        Self {
            label: format!("volume_sma_{}", period),
            period,
        }
    }
}

impl Feature for VolumeSmaFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if data.len() < self.period {
            return Ok(FeatureValue::Missing);
        }
        let sum: i64 = data[data.len() - self.period..]
            .iter()
            .map(|c| c.volume)
            .sum();
        let avg = Decimal::from(sum) / Decimal::from(self.period as u64);
        Ok(FeatureValue::Scalar(avg))
    }
}

// --- Momentum ---

pub struct MomentumFeature {
    pub period: usize,
    pub label: String,
}

impl MomentumFeature {
    pub fn new(period: usize) -> Self {
        Self {
            label: format!("momentum_{}", period),
            period,
        }
    }
}

impl Feature for MomentumFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if data.len() <= self.period {
            return Ok(FeatureValue::Missing);
        }
        let now = data[data.len() - 1].close;
        let then = data[data.len() - 1 - self.period].close;
        if then == Decimal::ZERO {
            return Ok(FeatureValue::Missing);
        }
        Ok(FeatureValue::Scalar((now - then) / then))
    }
}

// --- Mean Reversion (z-score against SMA/stddev window) ---

pub struct MeanReversionFeature {
    pub period: usize,
    pub label: String,
}

impl MeanReversionFeature {
    pub fn new(period: usize) -> Self {
        Self {
            label: format!("mean_reversion_{}", period),
            period,
        }
    }
}

impl Feature for MeanReversionFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if data.len() < self.period {
            return Ok(FeatureValue::Missing);
        }

        let slice = &data[data.len() - self.period..];
        let mean: Decimal =
            slice.iter().map(|c| c.close).sum::<Decimal>() / Decimal::from(self.period as u64);
        let variance: Decimal = slice
            .iter()
            .map(|c| {
                let d = c.close - mean;
                d * d
            })
            .sum::<Decimal>()
            / Decimal::from(self.period as u64);
        let std_dev = decimal_sqrt(variance);
        if std_dev <= Decimal::ZERO {
            return Ok(FeatureValue::Scalar(Decimal::ZERO));
        }
        let Some(last) = slice.last() else {
            return Ok(FeatureValue::Missing);
        };
        let z = (last.close - mean) / std_dev;
        Ok(FeatureValue::Scalar(z))
    }
}

// --- Volatility (rolling stddev of simple returns) ---

pub struct VolatilityFeature {
    pub period: usize,
    pub label: String,
}

impl VolatilityFeature {
    pub fn new(period: usize) -> Self {
        Self {
            label: format!("volatility_{}", period),
            period,
        }
    }
}

impl Feature for VolatilityFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if data.len() <= self.period {
            return Ok(FeatureValue::Missing);
        }
        let start = data.len() - self.period - 1;
        let mut returns = Vec::with_capacity(self.period);
        for i in (start + 1)..data.len() {
            let prev = data[i - 1].close;
            if prev == Decimal::ZERO {
                return Ok(FeatureValue::Missing);
            }
            returns.push((data[i].close - prev) / prev);
        }

        let n = Decimal::from(returns.len() as u64);
        if n == Decimal::ZERO {
            return Ok(FeatureValue::Missing);
        }
        let mean = returns.iter().copied().sum::<Decimal>() / n;
        let var = returns
            .iter()
            .map(|r| {
                let d = *r - mean;
                d * d
            })
            .sum::<Decimal>()
            / n;
        Ok(FeatureValue::Scalar(decimal_sqrt(var)))
    }
}

// --- Volatility Clustering (short/long vol ratio) ---

pub struct VolatilityClusteringFeature {
    pub short_period: usize,
    pub long_period: usize,
    pub label: String,
}

impl VolatilityClusteringFeature {
    pub fn new(short_period: usize, long_period: usize) -> Self {
        Self {
            short_period,
            long_period,
            label: format!("vol_cluster_{}_{}", short_period, long_period),
        }
    }
}

impl Feature for VolatilityClusteringFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if self.short_period >= self.long_period || data.len() <= self.long_period {
            return Ok(FeatureValue::Missing);
        }
        let short = VolatilityFeature::new(self.short_period).calculate(data)?;
        let long = VolatilityFeature::new(self.long_period).calculate(data)?;
        match (short, long) {
            (FeatureValue::Scalar(s), FeatureValue::Scalar(l)) if l > Decimal::ZERO => {
                Ok(FeatureValue::Scalar(s / l))
            }
            _ => Ok(FeatureValue::Missing),
        }
    }
}

// --- Market Microstructure proxies ---

// Approximation of bid-ask spread from candle micro-range.
pub struct BidAskSpreadProxyFeature {
    pub label: String,
}

impl Default for BidAskSpreadProxyFeature {
    fn default() -> Self {
        Self::new()
    }
}

impl BidAskSpreadProxyFeature {
    pub fn new() -> Self {
        Self {
            label: "bid_ask_spread_proxy".to_string(),
        }
    }
}

impl Feature for BidAskSpreadProxyFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if let Some(last) = data.last() {
            if last.close > Decimal::ZERO {
                return Ok(FeatureValue::Scalar((last.high - last.low) / last.close));
            }
        }
        Ok(FeatureValue::Missing)
    }
}

// Approximates liquidity imbalance from volume shock versus rolling volume mean.
pub struct LiquidityImbalanceProxyFeature {
    pub period: usize,
    pub label: String,
}

impl LiquidityImbalanceProxyFeature {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            label: format!("liquidity_imbalance_proxy_{}", period),
        }
    }
}

impl Feature for LiquidityImbalanceProxyFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if data.len() < self.period {
            return Ok(FeatureValue::Missing);
        }
        let slice = &data[data.len() - self.period..];
        let avg_volume = Decimal::from(slice.iter().map(|c| c.volume).sum::<i64>())
            / Decimal::from(self.period as u64);
        let Some(last) = slice.last() else {
            return Ok(FeatureValue::Missing);
        };
        if avg_volume <= Decimal::ZERO {
            return Ok(FeatureValue::Missing);
        }
        Ok(FeatureValue::Scalar(
            Decimal::from(last.volume) / avg_volume - Decimal::ONE,
        ))
    }
}

// Order-flow proxy: signed pressure by candle body * normalized volume.
pub struct OrderFlowProxyFeature {
    pub volume_period: usize,
    pub label: String,
}

impl OrderFlowProxyFeature {
    pub fn new(volume_period: usize) -> Self {
        Self {
            volume_period,
            label: format!("order_flow_proxy_{}", volume_period),
        }
    }
}

impl Feature for OrderFlowProxyFeature {
    fn name(&self) -> &str {
        &self.label
    }

    fn calculate(&self, data: &[Candle]) -> Result<FeatureValue> {
        if data.len() < self.volume_period {
            return Ok(FeatureValue::Missing);
        }
        let Some(last) = data.last() else {
            return Ok(FeatureValue::Missing);
        };
        if last.open == Decimal::ZERO {
            return Ok(FeatureValue::Missing);
        }
        let body_ret = (last.close - last.open) / last.open;
        let avg_volume = Decimal::from(
            data[data.len() - self.volume_period..]
                .iter()
                .map(|c| c.volume)
                .sum::<i64>(),
        ) / Decimal::from(self.volume_period as u64);
        if avg_volume <= Decimal::ZERO {
            return Ok(FeatureValue::Missing);
        }
        let vol_ratio = Decimal::from(last.volume) / avg_volume;
        Ok(FeatureValue::Scalar(body_ret * vol_ratio))
    }
}

fn decimal_sqrt(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let mut x = value / Decimal::TWO;
    if x <= Decimal::ZERO {
        x = Decimal::ONE;
    }
    for _ in 0..10 {
        x = (x + value / x) / Decimal::TWO;
    }
    x
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::domain::models::{Candle, Timeframe};
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn candle(open: Decimal, high: Decimal, low: Decimal, close: Decimal, volume: i64) -> Candle {
        Candle {
            symbol: "TEST".to_string(),
            timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap(),
            timeframe: Timeframe::OneMin,
            open,
            high,
            low,
            close,
            volume,
            trades_count: None,
            vwap: None,
        }
    }

    fn closes(values: &[i64]) -> Vec<Candle> {
        values
            .iter()
            .map(|v| {
                let d = Decimal::from(*v);
                candle(d, d, d, d, 1000)
            })
            .collect()
    }

    fn scalar(value: FeatureValue) -> Decimal {
        match value {
            FeatureValue::Scalar(v) => v,
            other => panic!("expected scalar, got {:?}", other),
        }
    }

    #[test]
    fn sma_returns_mean_of_window() {
        let data = closes(&[1, 2, 3, 4, 5]);
        let v = scalar(SmaFeature::new(5).calculate(&data).unwrap());
        assert_eq!(v, dec!(3));
    }

    #[test]
    fn sma_uses_only_last_period_values() {
        let data = closes(&[100, 100, 2, 4, 6]);
        let v = scalar(SmaFeature::new(3).calculate(&data).unwrap());
        assert_eq!(v, dec!(4));
    }

    #[test]
    fn sma_missing_when_not_enough_data() {
        let data = closes(&[1, 2]);
        assert!(matches!(
            SmaFeature::new(3).calculate(&data).unwrap(),
            FeatureValue::Missing
        ));
    }

    #[test]
    fn ema_equals_price_for_constant_series() {
        let data = closes(&[10; 30]);
        let v = scalar(EmaFeature::new(12).calculate(&data).unwrap());
        assert_eq!(v, dec!(10));
    }

    #[test]
    fn ema_reacts_faster_than_sma_to_recent_jump() {
        let mut series = vec![100i64; 19];
        series.push(200);
        let data = closes(&series);
        let ema = scalar(EmaFeature::new(10).calculate(&data).unwrap());
        let sma = scalar(SmaFeature::new(10).calculate(&data).unwrap());
        assert!(ema > sma);
    }

    #[test]
    fn rsi_is_100_when_only_gains() {
        let data = closes(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        let v = scalar(RsiFeature::new(14).calculate(&data).unwrap());
        assert_eq!(v, dec!(100));
    }

    #[test]
    fn rsi_is_50_for_alternating_equal_moves() {
        let mut series = Vec::with_capacity(15);
        let mut price = 100i64;
        for i in 0..15 {
            series.push(price);
            price += if i % 2 == 0 { 1 } else { -1 };
        }
        let data = closes(&series);
        let v = scalar(RsiFeature::new(14).calculate(&data).unwrap());
        assert_eq!(v, dec!(50));
    }

    #[test]
    fn macd_zero_on_constant_series() {
        let data = closes(&[50; 30]);
        let v = scalar(MacdFeature::new().calculate(&data).unwrap());
        assert_eq!(v, Decimal::ZERO);
    }

    #[test]
    fn bollinger_bands_ordered_centered_and_symmetric() {
        let data = closes(&[
            10, 12, 14, 16, 18, 20, 18, 16, 14, 12, 10, 12, 14, 16, 18, 20, 18, 16, 14, 12,
        ]);
        let bands = match BollingerBandsFeature::new(20, dec!(2)).calculate(&data).unwrap() {
            FeatureValue::Vector(v) => v,
            other => panic!("expected vector, got {:?}", other),
        };
        assert_eq!(bands.len(), 3);
        assert!(bands[0] < bands[1] && bands[1] < bands[2]);
        let sma = scalar(SmaFeature::new(20).calculate(&data).unwrap());
        assert_eq!(bands[1], sma);
        assert_eq!(bands[1] - bands[0], bands[2] - bands[1]);
    }

    #[test]
    fn momentum_is_relative_price_change() {
        let data = closes(&[100, 100, 100, 100, 100, 110]);
        let v = scalar(MomentumFeature::new(5).calculate(&data).unwrap());
        assert_eq!(v, dec!(0.1));
    }

    #[test]
    fn zscore_zero_for_constant_series() {
        let data = closes(&[42; 20]);
        let v = scalar(MeanReversionFeature::new(20).calculate(&data).unwrap());
        assert_eq!(v, Decimal::ZERO);
    }

    #[test]
    fn zscore_positive_when_price_above_mean() {
        let mut series = vec![100i64; 19];
        series.push(120);
        let data = closes(&series);
        let v = scalar(MeanReversionFeature::new(20).calculate(&data).unwrap());
        assert!(v > Decimal::ZERO);
    }

    #[test]
    fn volatility_zero_for_constant_prices() {
        let data = closes(&[75; 16]);
        let v = scalar(VolatilityFeature::new(14).calculate(&data).unwrap());
        assert_eq!(v, Decimal::ZERO);
    }

    #[test]
    fn vol_cluster_missing_when_short_window_not_less_than_long() {
        let data = closes(&[10; 30]);
        assert!(matches!(
            VolatilityClusteringFeature::new(20, 20)
                .calculate(&data)
                .unwrap(),
            FeatureValue::Missing
        ));
    }

    #[test]
    fn vol_cluster_positive_for_noisy_series() {
        let mut series = Vec::with_capacity(30);
        for i in 0..30 {
            series.push(if i % 2 == 0 { 100 } else { 102 });
        }
        let data = closes(&series);
        let v = scalar(
            VolatilityClusteringFeature::new(5, 20)
                .calculate(&data)
                .unwrap(),
        );
        assert!(v > Decimal::ZERO);
    }

    #[test]
    fn bid_ask_proxy_is_range_over_close() {
        let data = vec![candle(dec!(100), dec!(102), dec!(98), dec!(100), 1000)];
        let v = scalar(BidAskSpreadProxyFeature::new().calculate(&data).unwrap());
        assert_eq!(v, dec!(0.04));
    }

    #[test]
    fn liquidity_imbalance_zero_for_constant_volume() {
        let data = closes(&[10; 20]);
        let v = scalar(
            LiquidityImbalanceProxyFeature::new(20)
                .calculate(&data)
                .unwrap(),
        );
        assert_eq!(v, Decimal::ZERO);
    }

    #[test]
    fn order_flow_proxy_sign_follows_candle_body() {
        let mut data = closes(&[100; 19]);
        data.push(candle(dec!(100), dec!(106), dec!(99), dec!(105), 1000));
        let v = scalar(OrderFlowProxyFeature::new(20).calculate(&data).unwrap());
        assert!(v > Decimal::ZERO);
    }
}
