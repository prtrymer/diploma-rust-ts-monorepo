# Архітектура квант-системи

Огляд гексагональної архітектури, мапа domain/ports/adapters і залізні
правила системи. Оновлювати при додаванні нових портів.

**Стек:** Rust/Tokio · Kafka · ScyllaDB · PostgreSQL · React · ensemble ML ·
hexagonal architecture · Kafka-replay (єдиний пайплайн бектест + live).

---

## Інваріанти (не порушувати ніколи)

Дослівно з роадмапу (CLAUDE.md). Кожен інваріант enforced кодом/тестом,
а не домовленістю — посилання в дужках.

1. **Єдиний шлях коду.** Бектест і live використовують ту саму логіку
   сигналів/виконання. Форк логіки під бектест заборонено — тільки різні
   adapter'и на портах. Це головна перевага системи, вона гарантується тестом
   (M0.5), а не обіцянкою. *(→ `tests/determinism_replay.rs`: прямий цикл ==
   replay через event-bus побайтово; навмисний розсинхрон ловиться.)*
2. **Витрати завжди увімкнені.** Жоден прогін не рахує PnL без транзакційних
   витрат. «Gross PnL» дозволений лише як окрема діагностична метрика поряд
   із net, ніколи замість. *(→ `CostModel` у кожному брокер-адаптері;
   `BacktestReport.total_return` — net; gross — окреме поле; тест
   `smoke_zero_costs_net_equals_gross`.)*
3. **Нема look-ahead.** Фіча/сигнал у момент `t` використовує тільки дані з
   часом `≤ t`. Має бути неможливо порушити на рівні типів/API, а не за
   домовленістю. *(→ `UniverseView` не має методу читання майбутнього;
   `PointInTimeStore.as_of` — єдиний метод читання.)*
4. **Детермінізм.** Той самий вхід + той самий config hash → побайтово той
   самий результат. Стосується і ML-ансамблю (фіксовані сіди).
   *(→ id подій — UUIDv5 від (символ, час, сторона); час філів — ринковий;
   RandomForest — `with_seed`; property-тест M0.5.)*
5. **Provenance.** Кожен прогін логується з хешем повного конфіга. Немає
   «магічних» параметрів у коді — усе з конфіга. *(→ `RunConfig` +
   SHA-256; `RunLogger` порт: PostgreSQL `runs` + файловий JSONL.)*
6. **Decimal для грошей.** PnL, ціни, розміри позицій — `Decimal`,
   ніколи `f64`. *(f64 дозволений лише для безрозмірних коефіцієнтів:
   Sharpe, DSR, нормалізовані фічі.)*

---

## Гексагональна мапа: domain / ports / adapters

Кожен bounded context — свій модуль зі структурою
`domain/` (чиста логіка, без I/O) · `ports/` (трейти-контракти) ·
`adapters/` (імплементації портів: Kafka, Scylla, HTTP, in-memory, CSV).

### `trading` — торгове ядро

| Шар | Що | Файл |
|---|---|---|
| domain | Події `SignalEvent/OrderEvent/FillEvent` (+`MarketContext`) | `src/trading/domain/events.rs` |
| domain | **CostModel** (M0.1): `SimpleCommissionSpread`, `AlmgrenChrissImpact`, `CompositeCost`, `ZeroCost` | `src/trading/domain/costs.rs` |
| domain | **PositionSizer** — єдиний компонент сайзингу (SL/TP/Kelly-кап) | `src/trading/domain/sizing.rs` |
| domain | **AllocationStrategy** (трейт `Strategy` з роадмапу) + `UniverseView` (PIT-доступ) | `src/trading/domain/allocation.rs` |
| ports | `StrategyPort`, `ExecutionHandlerPort`, `BrokerSimulatorPort`, `PortfolioPort` | `src/trading/ports/` |
| adapter | `MomentumStrategy` (тіковий ML-momentum) | `src/trading/adapters/momentum_strategy.rs` |
| adapter | `SimpleExecutionHandler` (сигнал → ордер через PositionSizer) | `src/trading/adapters/execution_handler.rs` |
| adapter | `SimpleBrokerSimulator` (виконання + CostModel) | `src/trading/adapters/broker_simulator.rs` |
| adapter | `PortfolioManager` (лонг-only за замовч.; `new_allowing_short` для M3.2) | `src/trading/adapters/portfolio_manager.rs` |
| adapter | Бенчмарки (M0.2): `BuyAndHold`, `EqualWeight`, `SixtyForty` | `src/trading/adapters/benchmark_strategies.rs` |
| adapter | `TsmomStrategy` (M2.1, gate-валідація движка) | `src/trading/adapters/tsmom_strategy.rs` |
| adapter | `CrossSectionalMomentum` (M3.2, dollar-neutral) | `src/trading/adapters/cross_sectional_momentum.rs` |
| adapter | `FundingCarryBacktest` (M3.1) | `src/trading/adapters/funding_carry.rs` |
| adapter | Kafka-обгортки портів: `StrategyHandler`, `ExecutionKafkaHandler`, `BrokerKafkaHandler` | `src/trading/adapters/*_handler*.rs` |
| adapter | **HTX live** — підпис v2 (HMAC-SHA256), REST спот + USDT-M своп, `HtxCarryExecutor` (кошик зі shadow-журналу → дельта-нейтральні ноги: лонг спот + шорт перп). Dry-run за замовчуванням; відправка лише за `HTX_TRADING_ENABLED=true`; ідемпотентність через `live/orders.jsonl` + детерміновані client-order-id | `src/trading/adapters/htx/` |

### `backtest` — вимірювання

| Шар | Що | Файл |
|---|---|---|
| domain | **metrics** (M0.3): median Sharpe, MDD, Calmar, turnover, hit rate, PF + **deflated Sharpe** (M1.3) | `src/backtest/domain/metrics.rs` |
| domain | `BacktestReport` — net-of-cost головний, gross діагностика | `src/backtest/domain/report.rs` |
| domain | `PortfolioBacktester` — мультиактивний движок для AllocationStrategy | `src/backtest/domain/portfolio_engine.rs` |
| domain | `ComparativeReport` — стратегія + бенчмарки поруч (M0.2) | `src/backtest/domain/multi_report.rs` |
| domain | **Purged CV** з ембарго (M1.2) | `src/backtest/domain/purged_cv.rs` |
| domain | Kafka-replay движок (легасі один-символьний) | `src/backtest/domain/engine.rs` |
| application | **Walk-forward** (M1.1): OOS-фолди, агрегація, DSR | `src/backtest/application/walk_forward.rs` |
| application | `BenchmarkRunner` — прогін стратегії + всіх бенчмарків одним движком | `src/backtest/application/benchmark_runner.rs` |

### `data_ingestion` — дані

| Шар | Що | Файл |
|---|---|---|
| domain | **point_in_time** (M0.6): `PointInTimeStore` (двочасові записи), корпоративні дії (спліти/дивіденди, raw+adjusted), `HistoricalUniverse` (делістинги) | `src/data_ingestion/domain/point_in_time.rs` |
| domain | `FundingRatePoint` (+basis) | `src/data_ingestion/domain/funding.rs` |
| ports | `DataSourcePort`, `MessageProducerPort`, **`FundingDataPort`** (M3.1) | `src/data_ingestion/ports/` |
| adapter | Yahoo, simulated, Kafka producer | `src/data_ingestion/adapters/` |
| adapter | `BinanceFundingAdapter` (REST, пагінація), `CsvFundingAdapter` (офлайн) | `src/data_ingestion/adapters/funding_*.rs` |

### `database` — сховища

| Шар | Що | Файл |
|---|---|---|
| ports | `Repository` (Scylla), **`RunLogger`** (M0.4) | `src/database/ports/` |
| adapter | `ScyllaRepository`, `PostgresRunLogger` (таблиця `runs`), `FileRunLogger` (JSONL fallback) | `src/database/adapters/` |

### `message_broker` — шина подій

| Шар | Що | Файл |
|---|---|---|
| ports | `Consumer`, `MessageHandler` | `src/message_broker/ports/` |
| adapter | Kafka consumer/admin | `src/message_broker/adapters/kafka_*.rs` |
| adapter | **`InMemoryBus`** (M0.5) — той самий пайплайн хендлерів без Kafka | `src/message_broker/adapters/in_memory_bus.rs` |

### `shared` — конфігурація

- **`RunConfig`** (M0.4) — єдине джерело правди для ВСІХ торгових параметрів
  (`strategy` / `sizing` / `costs` / `ensemble` / `walk_forward`). Дефолти
  визначені тільки тут. `config_hash()` — SHA-256 канонічного JSON.
  `src/shared/run_config.rs`.
- **`LiveConfig`** — параметри live-виконання на HTX (кошик, капітал, капи,
  стиль ордерів). Окрема структура зі своїм `config_hash()`, щоб research-хеші
  в shadow-журналі лишалися стабільними. Секрети (ключі API) — ТІЛЬКИ в env,
  ніколи в конфізі/хеші. `src/shared/run_config.rs`.
  `sizing_mode="auto"` — драбина розподілу капіталу (`capital_allocation` в
  htx/executor.rs): deploy_pct = частка всіх грошей у роботі, K символів
  виводиться з min_leg_usdt (стеля top_k), нога капиться max_order_usdt;
  capital_usdt=0 бере капітал з реальних балансів біржі.

---

## Потік даних (live == replay)

```
market ticks ──▶ StrategyHandler ──▶ signals ──▶ ExecutionKafkaHandler ──▶ orders
                (StrategyPort)                    (ExecutionHandlerPort
                                                   + PositionSizer)
orders ──▶ BrokerKafkaHandler ──▶ fills ──▶ FillCollector ──▶ PortfolioPort
           (BrokerSimulatorPort                                + BacktestReport
            + CostModel)
```

Транспорт (Kafka / InMemoryBus) — деталь адаптера. Доменна логіка всіх
чотирьох стадій спільна для live і бектесту; еквівалентність доведена
property-тестом (M0.5).

## Бінарники

- `db-con` — live-сервіс (Kafka + Scylla + HTTP).
- `backtest` — тіковий ML-бектест одного символу (Scylla + Kafka replay або
  direct). Друкує config hash, логує прогін у `runs`.
  Прапори провенансу: `--config`, `--print-config-hash`, `--diff-configs A B`,
  `--save-config`.
- `quant-backtest` — мультиактивний портфельний бектест з CSV
  (`--data-dir`): стратегія + бенчмарки + walk-forward + deflated Sharpe +
  purged CV; funding carry через `--funding-csv`.
- `htx-exec` — live-виконання carry-кошика на HTX:
  `check` (зв'язок/ключі/годинник) · `plan` (dry-run, нічого не шле) ·
  `positions` · `execute` (потребує `HTX_TRADING_ENABLED=true`) · `config` ·
  `fetch-funding` (історія funding HTX → `datasets/funding_htx/`, формат
  `CsvFundingAdapter`, інкрементально).
  Кошик за замовчуванням — `htx_trailing`: живий селектор за трейлінг-середнім
  settled funding HTX (той самий скоринг, що в xs_carry-бектесті, який
  валідував едж на цих даних). Кошики shadow-журналу (`baseline_est`|`rf`,
  Binance-ранжування) — через `--basket`; вони на HTX переносяться погано.
  Журнал ордерів і ex-ante вибірок: `live/orders.jsonl`.
- `latency-bench`, `dataset-sync` — допоміжні.

## Тести як enforcement

| Інваріант/мілстоун | Тест |
|---|---|
| M0.1 монотонність impact, zero-cost → net==gross | `trading::domain::costs::tests` |
| M0.2 BuyAndHold ≈ Δціни − one-off costs; бенчмарки поруч | `backtest::application::benchmark_runner::tests` |
| M0.3 медіана vs викид; MDD на спадній кривій | `backtest::domain::metrics::tests` |
| M0.4 hash стабільний/чутливий; лог прогонів | `shared::run_config::tests`, `run_logger_file::tests` |
| M0.5 direct == replay побайтово; ловить розсинхрон | `tests/determinism_replay.rs` |
| M0.6 PIT ховає майбутнє; спліт 2:1; делістинги в юніверсі | `data_ingestion::domain::point_in_time::tests` |
| M1.1 вікна не перетинаються, ембарго | `backtest::application::walk_forward::tests` |
| M1.2 нуль інформаційного перекриття train/test | `backtest::domain::purged_cv::tests` |
| M1.3 N trials ↑ → DSR ↓ | `metrics::tests::deflated_sharpe_decreases_with_trials` |
| M3.1 нема подвійного обліку funding/costs | `trading::adapters::funding_carry::tests` |
| M3.2 сума ваг = 0; turnover-банда | `trading::adapters::cross_sectional_momentum::tests` |
| CI-smoke повного стеку на фікстурі | `tests/smoke_backtest.rs` |

## Конвенції

- Новий зовнішній сервіс = новий порт (трейт) у `ports/` + адаптер в
  `adapters/`; domain не імпортує адаптери.
- Усі нові торгові параметри додаються в `RunConfig` (ніколи хардкодом);
  зміна структури конфіга змінює hash — це очікувано.
- Грошові величини — `Decimal`. Час подій — час ринку, не `Utc::now()`.
- Id подій — детерміновані (UUIDv5 від змістовного сіда).
- Нові стратегії портфельного рівня імплементують `AllocationStrategy` і
  автоматично отримують движок, витрати, бенчмарки, walk-forward і DSR.
