# Market Data & Trading Engine — Повний огляд проекту

---

## Суть проекту

`db-con` — це **монорепозиторій** повноцінної алготрейдингової платформи, яка складається з трьох рівноправних частин: Rust-бекенду (`src/`), React-дашборду (`dashboard/`) та інфраструктурного шару (`docker-compose.yml`, `Dockerfile`, `migrations/`, `scripts/`). Система вирішує повний цикл задачі: отримати потік ринкових котирувань, обробити їх через event-driven pipeline на Apache Kafka, обчислити технічні індикатори, згенерувати торгові сигнали за допомогою ансамблю ML-моделей, симулювати виконання ордерів, зберегти все у двох базах даних — і відобразити результат у реальному часі у браузері через WebSocket.

Проект є повноцінним, самодостатнім застосунком: один `docker compose up` піднімає всі сервіси, включно зі стрімінгом сигналів у браузер.

---

## Мета проекту

- Збирати та зберігати ринкові дані з різних джерел (Yahoo Finance або власний GBM-симулятор) з high-throughput та низькою затримкою.
- Агрегувати сирі тіки в OHLCV-свічки різних таймфреймів (1m, 5m, 15m, 1h, 1d) у реальному часі.
- Обчислювати технічні та мікроструктурні ознаки на кожному тіку.
- Генерувати торгові сигнали (Long / Short / Exit) через ансамбль ML-моделей із онлайн-навчанням.
- Симулювати виконання ордерів з урахуванням комісій і slippage.
- Підтримувати backtesting — відтворювати历існі дані через той самий pipeline і генерувати звіт.
- Надавати REST API, WebSocket-стрім і Swagger UI для зовнішніх клієнтів та фронтенду.
- Відображати live-сигнали, свічковий графік і портфель у веб-інтерфейсі.

---

## Структура монорепозиторію

```
db-con/                          ← корінь монорепо
├── src/                         ← Rust-бекенд (lib + binary crate)
│   ├── main.rs                  ← точка входу, делегує bootstrap::run()
│   ├── lib.rs                   ← публічний crate-root
│   ├── bootstrap/               ← wire-up усіх підсистем при старті
│   ├── shared/                  ← config.rs (.env → Config struct)
│   ├── database/                ← адаптери ScyllaDB і PostgreSQL
│   ├── data_ingestion/          ← Yahoo Finance, GBM-симулятор, Kafka Producer
│   ├── aggregation/             ← агрегація тіків у OHLCV-свічки
│   ├── features/                ← реєстр технічних індикаторів
│   ├── model/                   ← ML-моделі: лінійна, дерево, ансамбль
│   ├── trading/                 ← стратегія, ордери, брокер, портфель
│   ├── message_broker/          ← Kafka consumers і message handlers
│   ├── http/                    ← Axum routes, handlers, middleware, OpenAPI
│   ├── backtest/                ← backtest engine і звіт
│   └── bin/
│       ├── backtest.rs          ← окремий бінарник для офлайн-backtesting
│       └── dataset_sync.rs      ← синхронізація CSV-датасетів (polars, feature flag)
│
├── dashboard/                   ← React SPA (TypeScript + Vite)
│   ├── src/
│   │   ├── App.tsx              ← routing (Login / Dashboard), guard по токену
│   │   ├── pages/
│   │   │   ├── LoginPage.tsx    ← форма реєстрації / входу
│   │   │   └── DashboardPage.tsx← головна сторінка з усіма компонентами
│   │   ├── components/
│   │   │   ├── ChartWidget.tsx  ← свічковий графік (lightweight-charts) + signal markers
│   │   │   ├── SignalsGrid.tsx  ← virtualized grid сигналів (glide-data-grid)
│   │   │   ├── SymbolSelector.tsx← sidebar з пошуком і портфелем символів
│   │   │   ├── SignalsBadge.tsx ← лічильник нових сигналів
│   │   │   └── ConnectionStatus.tsx← індикатор стану WS
│   │   ├── store/
│   │   │   └── useStore.ts      ← Zustand store (auth, portfolio, signals, wsStatus)
│   │   ├── ws/
│   │   │   └── useSignalWs.ts   ← WebSocket hook з exponential backoff reconnect
│   │   └── types.ts             ← TypeScript-типи (дзеркало Rust-структур)
│   └── vite.config.ts           ← dev proxy /api → :3000, build → ../static/dist
│
├── static/dist/                 ← збудована SPA (build-артефакт, git-ignored)
│
├── migrations/
│   ├── postgres/                ← SQL-міграції (users, portfolio, active_symbols)
│   └── scylla/                  ← CQL-міграції (keyspace, tables, trading tables)
│
├── files/
│   ├── model_ckpt/              ← персистентні ваги ансамблю (ensemble.json, model_N.json)
│   └── *.csv                    ← CSV-датасети для офлайн-навчання
│
├── scripts/                     ← shell-утиліти (migrate, init_scylla, check_data, …)
├── docker-compose.yml           ← 7 сервісів: app, postgres, scylla, zookeeper, kafka, kafka-init
├── Dockerfile                   ← multi-stage build (node → rust → debian-slim)
├── Cargo.toml                   ← Rust workspace / dependencies / feature flags
└── .env.example                 ← шаблон змінних оточення
```

---

## Стек проекту

### Бекенд (Rust)

| Шар | Технологія |
|---|---|
| Мова | **Rust** (edition 2021) |
| Async runtime | **Tokio** (full features) |
| HTTP-сервер | **Axum 0.7** + Tower + Tower-HTTP (CORS, ServeDir) |
| API-документація | **utoipa 4** + Swagger UI (`/swagger-ui`) |
| Message broker | **Apache Kafka** — rdkafka 0.36 (cmake-build, tokio feature) |
| Time-series DB | **ScyllaDB** — scylla 0.13 (full-serialization) |
| Relational DB | **PostgreSQL** — sqlx 0.8 (runtime-tokio-rustls, chrono, uuid) |
| Ринкові дані | **Yahoo Finance API** 2.2 + власний GBM-симулятор |
| ML / Features | **smartcore 0.4** + власний ансамблевий движок |
| Опційний ML | **candle-core 0.8** (feature flag `candle-ml`) |
| Опційна обробка | **polars 0.46** (feature flag `polars-df`) |
| Серіалізація | **serde / serde_json** |
| Фінансова арифметика | **rust_decimal** + rust_decimal_macros — жодних `f64` у грошах |
| Аутентифікація | **jsonwebtoken 10** (JWT) + **argon2 0.5** (хешування паролів) |
| UUID | **uuid 1.0** (v4) |
| HTTP-клієнт | **reqwest 0.12** (json feature) |
| Async traits | **async-trait 0.1** |

### Фронтенд (dashboard/)

| Шар | Технологія |
|---|---|
| Мова | **TypeScript 6** |
| Фреймворк | **React 18** |
| Bundler | **Vite 8** з `@vitejs/plugin-react` |
| Роутинг | **React Router DOM 7** |
| Стан | **Zustand 5** з persist middleware |
| Свічковий графік | **lightweight-charts 5** (TradingView) |
| Таблиця сигналів | **@glideapps/glide-data-grid 6** (virtualized canvas grid) |
| Транспорт | нативний **WebSocket** API |
| Лінтинг | ESLint 10 + typescript-eslint + eslint-plugin-react-hooks |

### Інфраструктура

| Сервіс | Технологія |
|---|---|
| Контейнеризація | **Docker** + **Docker Compose** (7 сервісів) |
| Build | Multi-stage **Dockerfile** (node → rust → debian-slim) |
| Kafka | **Confluent cp-kafka 7.5** + **cp-zookeeper 7.5** |
| ScyllaDB | **scylladb/scylla:5.4** |
| PostgreSQL | **postgres:16-alpine** |
| Kafka-топіки | Ініціалізація через `kafka-init` one-shot контейнер |
| Міграції | Shell-скрипти + `sqlx::migrate!` на старті |
| Конфігурація | `.env` файл + `dotenv` crate |

---

## Архітектура проекту

### Принципи

Проект побудований на **Hexagonal Architecture (Ports & Adapters)** та **Event-Driven Architecture**. Кожен модуль ізольований через trait-порти — бізнес-логіка не знає про конкретні бази даних чи транспорти. Kafka є центральною шиною повідомлень: усі підсистеми комунікують виключно через топіки, а не прямими викликами.

### Event-driven pipeline (повна схема)

```
                    ┌─────────────────────────────────┐
                    │     Yahoo Finance / GBM Simulator│
                    │  (fetch every 60s / 10s in sim)  │
                    └──────────────┬──────────────────┘
                                   │ StockQuote[]
                                   ▼
                         [IngestionService]
                         (KafkaProducer::send)
                                   │
                    Kafka topic: market-data-raw (3 partitions)
                                   │
                    ┌──────────────▼──────────────────┐
                    │       MarketDataHandler          │
                    │ • insert_stock_tick → ScyllaDB   │
                    │ • CandleAggregator::process_tick │
                    │   └─ 1m/5m window → ScyllaDB    │
                    │ • publish → market-data-processed│
                    └──────────────┬──────────────────┘
                                   │
                    Kafka topic: market-data-processed
                                   │
                    ┌──────────────▼──────────────────┐
                    │        StrategyHandler           │
                    │ • feature_registry.calculate_all │
                    │ • model.learn(prev_f, ret)       │
                    │ • model.predict(features)        │
                    │ • MomentumStrategy filters       │
                    │ • publish SignalEvent            │
                    └──────────────┬──────────────────┘
                                   │
                    Kafka topic: trading-signals
                    │                         │
                    │                WebSocket broadcast
                    │                (→ React Dashboard)
                    ▼
          [ExecutionHandler]
          Signal → OrderEvent
                    │
          Kafka topic: trading-orders
                    │
                    ▼
          [BrokerHandler / BrokerSimulator]
          simulate fill (slippage, commission)
          OrderEvent → FillEvent
                    │
          Kafka topic: trading-fills
                    │
                    ▼
          [PortfolioHandler]
          update positions, cash, PnL
          → PortfolioManager (in-memory)
```

### Kafka-топіки

| Топік | Партиції | Призначення |
|---|---|---|
| `market-data-raw` | 3 | Сирі тіки від джерела даних |
| `market-data-processed` | 3 | Тіки після збереження в ScyllaDB |
| `market-data-1min` | 3 | Готові 1m-свічки |
| `market-data-5min` | 3 | Готові 5m-свічки |
| `market-data-daily` | 3 | Денні свічки |
| `trading-signals` | 3 | SignalEvent від стратегії |
| `trading-orders` | 3 | OrderEvent від execution handler |
| `trading-fills` | 3 | FillEvent від брокера |

### Бази даних

**ScyllaDB** — time-series сховище з TTL та TimeWindowCompactionStrategy:

| Таблиця | TTL | Призначення |
|---|---|---|
| `stock_ticks` | 30 днів | Сирі тіки, partition key: (symbol, bucket-день) |
| `stock_1min` | 90 днів | 1m OHLCV-свічки |
| `stock_5min` | 180 днів | 5m OHLCV-свічки |
| `stock_15min` | ∞ | 15m свічки |
| `stock_hourly` | ∞ | Годинні свічки |
| `stock_daily` | ∞ | Денні свічки |
| `stock_latest_prices` | — | Остання ціна по символу (швидкий доступ) |
| `stock_metadata` | — | Метадані компаній |
| `kafka_messages` | 7 днів | Idempotency tracking (topic, partition, offset) |
| `trading_signals` | 90 днів | Збережені сигнали |
| `trading_orders` | 90 днів | Ордери |
| `trading_fills` | 90 днів | Виконані угоди |
| `portfolio_snapshots` | ∞ | Знімки стану портфеля |

**PostgreSQL** — реляційний стан:

| Таблиця | Призначення |
|---|---|
| `users` | Акаунти (UUID, username, argon2 password_hash) |
| `active_symbols` | Символи, що моніторяться (прив'язані до users) |
| `portfolios` | Placeholder для майбутньої реляційної частини портфеля |

### Інфраструктура (Docker Compose)

Сім сервісів із health checks і explicit залежностями:

```
zookeeper (healthcheck: nc -z 2181)
    └─▶ kafka (healthcheck: kafka-broker-api-versions)
              └─▶ kafka-init (one-shot: create 8 topics)
                        │
postgres (healthcheck: pg_isready)   scylla (healthcheck: cqlsh describe cluster)
         │                                   │
         └──────────────┬───────────────────┘
                        ▼
                  app (db-con binary)
                  ports: 3000:3000
                  depends_on: postgres, scylla, kafka, kafka-init
```

ScyllaDB запускається з `--smp 2 --memory 2G --overprovisioned 1`. Kafka має два listener-и: `INTERNAL://kafka:9093` (між контейнерами) і `EXTERNAL://localhost:9092` (для dev з хосту).

### Multi-stage Dockerfile

Збірка розбита на три незалежні стадії:

**Stage 1 — `dashboard-builder` (node:22-bookworm-slim)**: `npm ci` → `npm run build` → виводить у `../static/dist/`.

**Stage 2 — `backend-builder` (rust:1-bookworm)**: встановлює системні залежності (clang, cmake, libssl, libsasl2 для rdkafka), потім `cargo build --release --bin db-con`.

**Stage 3 — `runtime` (debian:bookworm-slim)**: копіює лише бінарник і статику, не тягне Rust toolchain у production image. Кінцевий образ мінімальний.

---

## Вимоги до коду і підходу

### Traits-first дизайн
Кожна підсистема взаємодіє через trait-об'єкти (`Arc<dyn Trait>`), а не конкретні типи. Наприклад, `Repository`, `StrategyPort`, `PredictionModel`, `DataSourcePort`, `PortfolioPort` — усі є trait-ами. Це забезпечує loose coupling і можливість підміни реалізацій (prod vs. simulation) без зміни бізнес-логіки.

### Decimal-арифметика у фінансах
Жодного `f64` у грошових значеннях. Все через `rust_decimal::Decimal` — точна десяткова арифметика без floating-point похибок. `rust_decimal_macros` дозволяє писати літерали: `dec!(0.32)`. Конвертація з `f64` (Yahoo Finance API) відбувається лише на межі входу через `Decimal::from_f64_retain`.

### Async-first, явна конкурентність
Весь I/O — асинхронний через Tokio. `async-trait` для async методів у trait-об'єктах. Конкурентність побудована на `tokio::spawn` для незалежних задач і `Arc<RwLock<_>>` для shared mutable state (наприклад, `CandleWindow`, адаптивні ваги ансамблю).

### Feature flags для важких залежностей
`polars-df` і `candle-ml` — опційні залежності, вмикаються лише для конкретних бінарників (`dataset-sync`, `backtest`). Основний `db-con` бінарник не тягне їх у production.

### Error handling
`anyhow::Result` скрізь — зручний propagation через `?` з контекстом через `.context("...")`. Немає `unwrap()` у production-шляхах, помилки логуються через `eprintln!` у spawn-задачах.

### Simulation mode
Змінна `USE_SIMULATION=true` перемикає весь pipeline на GBM-симулятор без зміни коду. Інтервал публікації також зменшується (10s vs 60s). Це дозволяє розробляти і тестувати без реального ринкового доступу.

### Модельні чекпоінти
`WeightedEnsembleModel` підтримує `save_checkpoint` / `load_checkpoint` у `files/model_ckpt/`. Ваги ансамблю (`ensemble.json`) і кожної моделі (`model_N.json`) персистуються між запусками.

### SPA в одному сервері
Vite build виводить у `static/dist/`, який в production Axum роздає як статику через `ServeDir`. У dev Vite dev-сервер (`:5173`) проксує всі `/api` запити на бекенд (`:3000`), включно з WebSocket апгрейдами (`ws: true` у vite.config.ts).

---

## Ключові features проекту

### 1. Dual-mode data ingestion
Два режими без зміни коду: **real** (Yahoo Finance, HTTP fetch кожні 60s) і **simulated** (Geometric Brownian Motion, кожні 10s). GBM-симулятор генерує реалістичні OHLCV-свічки з дрейфом 8%/рік, гарантуючи коректні OHLC-відносини (`low ≤ open, close ≤ high`). Базова ціна задається per-symbol (AAPL=190, NVDA=875, тощо).

### 2. Real-time candle aggregation
`CandleAggregator` обробляє кожен тік і накопичує його у time-window буфери per-symbol. Коли поточне вікно (1m або 5m) закривається (прийшов тік з наступного вікна), готова свічка зберігається асинхронно у ScyllaDB. Паралельно працюють два незалежні агрегатори — 1m і 5m.

### 3. Feature engineering registry
`FeatureRegistry` — реєстр технічних індикаторів, кожен реалізує trait `Feature`. Зареєстровані 12 ознак: RSI(14), EMA(12), EMA(26), MACD, Momentum(5), Momentum(20), MeanReversion(20), Volatility(20), VolatilityClustering(5,20), BidAskSpreadProxy, LiquidityImbalanceProxy(20), OrderFlowProxy(20).

### 4. Ансамблева ML-система з онлайн-навчанням
`WeightedEnsembleModel` агрегує передбачення кількох моделей з адаптивними вагами. Після кожного тіку: обчислюється `realized_return`, через `model.learn()` оновлюються внутрішні параметри моделей, а ваги ансамблю коригуються — моделі з меншою помилкою отримують більшу вагу. Підтримує збереження та завантаження стану.

### 5. Signal filtering у стратегії
`MomentumStrategy` фільтрує сигнали за: мінімальним confidence, дедублікацією напрямку (не емітує той самий Long/Short двічі поспіль), мінімальним інтервалом між сигналами (`min_signal_gap_secs`), та `long_only` режимом (Short → Exit).

### 6. Startup warmup
При старті система асинхронно завантажує 1000 останніх 1m-свічок (≈5 торгових днів) для кожного символу, відтворює їх через Kafka-pipeline для прогрівання ML-моделей, і надсилає один фінальний тік з `Utc::now()` для анкорування timestamp сигналів.

### 7. Backtesting engine
HTTP endpoint `POST /api/backtest` і окремий бінарник запускають backtest: завантажують历існі дані, відтворюють через той самий Kafka-pipeline (той самий код стратегії!), збирають FillEvent-и і генерують `BacktestReport` з метриками.

### 8. JWT-аутентифікація
`/api/auth/register` і `/api/auth/login` видають JWT-токени. Паролі хешуються через Argon2. Захищені endpoints використовують Axum-extractor `Claims` прямо в сигнатурі handler-функції. Фронтенд зберігає токен у Zustand persist store.

### 9. WebSocket signals stream
`/api/signals/stream?token=<jwt>` — WebSocket endpoint, який стрімить `SignalEvent`-и в реальному часі через `tokio::sync::broadcast` channel. Фронтенд автоматично перепідключається з exponential backoff (до 8 спроб, максимум 30s між спробами).

### 10. Інтерактивний дашборд
React SPA з трьома ключовими UI-блоками: sidebar з портфелем символів (пошук з debounce 250ms, keyboard navigation, `/api/symbols/search`), свічковий графік (lightweight-charts) з нанесенням сигналів як markers поверх свічок, і virtualized таблиця сигналів (glide-data-grid canvas-рендеринг, темна тема, до 500 записів у пам'яті).

### 11. Swagger UI
Повна OpenAPI 3.0 документація через utoipa, доступна на `/swagger-ui`.

---

## Логіка роботи проекту

### Старт системи (`bootstrap::run`)

```
1. dotenv::dotenv()            — завантаження .env
2. database::init_database()   — PostgreSQL + ScyllaDB підключення,
                                  завантаження активних символів з postgres
3. ingestion::init_ingestion() — KafkaProducer + DataSource (real/sim)
4. features::init_features()   — реєстрація 12 індикаторів
5. models::init_models()       — побудова WeightedEnsembleModel
6. trading::init_trading_engine() — warmup стратегії на历існих даних,
                                     ініціалізація Portfolio, BrokerSimulator
7. message_broker::wire()      — створення Kafka consumers + handlers
8. tokio::spawn × 6            — незалежні задачі:
                                    • data ingestion loop
                                    • market data consumer
                                    • strategy consumer
                                    • execution consumer
                                    • broker consumer
                                    • portfolio consumer
9. tokio::spawn (warmup)       — асинхронний прогрів ML (через 3s після старту)
10. http::start()              — Axum HTTP server на :3000
```

### Як обробляється один тік (детально)

```
Yahoo Finance fetch (every 60s)
  └─▶ Vec<StockQuote> → StockTick { symbol, price, volume, timestamp }
        └─▶ KafkaProducer::send → "market-data-raw"
              └─▶ KafkaConsumer::poll → MarketDataHandler::on_message()
                    ├─▶ ScyllaDB: INSERT INTO stock_ticks
                    ├─▶ CandleAggregator::process_tick(tick)
                    │     ├─▶ 1m window: накопичення open/high/low/close/vol
                    │     │   └─▶ якщо вікно закрите → INSERT INTO stock_1min
                    │     └─▶ 5m window: аналогічно → INSERT INTO stock_5min
                    └─▶ KafkaProducer::send → "market-data-processed"
                          └─▶ StrategyHandler::on_message()
                                └─▶ MomentumStrategy::on_market_event(tick)
                                      ├─▶ add_candle() → VecDeque<Candle> lookback
                                      ├─▶ якщо buffer.len() < lookback_size → return None
                                      ├─▶ feature_registry.calculate_all(symbol, &data)
                                      ├─▶ model.learn(prev_features, realized_return) [online]
                                      ├─▶ model.predict(features) → Prediction { direction, confidence }
                                      ├─▶ фільтр confidence < min_confidence → None
                                      ├─▶ фільтр дублікат направлення → None
                                      ├─▶ фільтр min_signal_gap → None
                                      └─▶ SignalEvent { id, symbol, direction, strength, ... }
                                            ├─▶ Kafka: "trading-signals"
                                            │     └─▶ ExecutionHandler
                                            │           └─▶ OrderEvent → Kafka: "trading-orders"
                                            │                 └─▶ BrokerSimulator
                                            │                       └─▶ FillEvent → Kafka: "trading-fills"
                                            │                             └─▶ PortfolioHandler::update()
                                            └─▶ broadcast::Sender<SignalEvent>
                                                  └─▶ WebSocket → React Dashboard
```

### Як працює ансамбль ML

`EnsembleFeatureModel::predict()` обчислює зважений score з чотирьох компонент:

```
trend        = EMA-crossover (EMA12 vs EMA26) × 6 + MACD × 0.25
mean_rev     = RSI-deviation × 0.7 + z-score × 0.3
momentum     = Momentum5 × 15 + Momentum20 × 6
microstr     = (OrderFlow × 0.65 + LiquidityImbalance × 0.35) × (1 - spread × 10)
vol_penalty  = (Volatility20 × 12).clamp(0, 0.4) + 0.1 if VolCluster > 1.5

score = (0.32×trend + 0.23×mean_rev + 0.25×momentum + 0.20×microstr) × (1 - vol_penalty)

│score│ < 0.01  → Exit
score > 0        → Long
score < 0        → Short
```

`WeightedEnsembleModel` агрегує кілька таких моделей. При `learn()` для кожної моделі обчислюється помилка `|target - pred_score|`, і вага оновлюється: `w += lr × (1 - err)`, потім нормалізується. Ваги зберігаються у `files/model_ckpt/ensemble.json`.

### Як працює дашборд

При відкритті браузера React SPA перевіряє Zustand persist store: якщо є JWT-токен — редирект на `/dashboard`, інакше — `/login`. На dashboard `useSignalWs()` hook підключається до `ws://host/api/signals/stream?token=<jwt>` і при кожному повідомленні додає `SignalEvent` у Zustand store (prepend + обрізка до 500 записів). `ChartWidget` при зміні activeSymbol завантажує свічки через `GET /api/symbols/chart?symbol=X&timeframe=1m` і рендерить через lightweight-charts. Коли у store з'являються нові сигнали — `setMarkers()` наносить їх поверх свічок. `SignalsGrid` рендерить сигнали через glide-data-grid (canvas), що дозволяє відображати сотні рядків без просідання performance. `SymbolSelector` шукає символи через `GET /api/symbols/search?q=...` з debounce 250ms і дозволяє додавати/видаляти з портфеля через `POST/DELETE /api/symbols`.

---

## Міграції та скрипти

`scripts/migrate.sh` застосовує SpyllaDB CQL-файли через `cqlsh` і PostgreSQL SQL-файли через `docker exec psql`. PostgreSQL-міграції також автоматично застосовуються через `sqlx::migrate!` при старті застосунку.

Порядок ScyllaDB-міграцій: `01_create_keyspace.cql` → `02_create_tables.cql` (тіки, свічки) → `03_unified_candles.cql` → `04_trading_tables.cql` (signals, orders, fills, portfolio_snapshots) → `05_historical_dataset.cql`.

Додаткові скрипти: `check_data.sh` (перевірка наявності даних), `drop_tables.sh`, `reinit_scylla.sh` (повне перестворення схеми).

---

*Документ згенеровано на основі повного аналізу вихідного коду монорепозиторію `db-con`.*
