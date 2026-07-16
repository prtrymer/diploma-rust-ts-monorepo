# db-con — квант-платформа: трейдинг-движок + дослідницький стек

[![CI](https://github.com/prtrymer/db-con/actions/workflows/ci.yml/badge.svg)](https://github.com/prtrymer/db-con/actions/workflows/ci.yml)
[![research](https://github.com/prtrymer/db-con/actions/workflows/research.yml/badge.svg)](https://github.com/prtrymer/db-con/actions/workflows/research.yml)

Event-driven алготрейдингова платформа на Rust (гексагональна архітектура) з
React-дашбордом і дослідницьким контуром: бектести через той самий пайплайн,
що й live, модель транзакційних витрат як first-class концепт, анти-оверфіт
інструментарій (walk-forward, purged CV, deflated Sharpe) і shadow-трейдинг
crypto funding-carry з live-виконанням на HTX.

**Стек:** Rust/Tokio · Kafka · ScyllaDB · PostgreSQL · Axum · React (Vite) · ensemble ML

## Що вміє

- **Live-пайплайн:** тіки (Yahoo Finance або GBM-симулятор) → Kafka →
  агрегація у свічки → фічі → ансамбль ML → сигнали → симуляція виконання з
  комісіями і slippage → портфель → WebSocket/SSE у дашборд.
- **Бектест = replay того самого коду.** Жодного форку логіки під бектест:
  історія проганяється через ті самі домени, ідентичність доведена
  property-тестом ([tests/determinism_replay.rs](tests/determinism_replay.rs)).
- **Дослідницький CLI** (`quant-backtest`): TSMOM-валідація движка,
  funding-rate carry, cross-sectional carry — усе net-of-cost, з бенчмарками
  і робастними метриками.
- **Live-виконання на HTX** (`htx-exec`): dry-run за замовчуванням, реальна
  відправка тільки з `HTX_TRADING_ENABLED=true`, ідемпотентний журнал ордерів.
- **Provenance:** кожен прогін логується з SHA-256-хешем повного конфіга.

## Інваріанти

Залізні правила, enforced тестами (деталі: [ARCHITECTURE.md](ARCHITECTURE.md)):

1. Єдиний шлях коду для бектесту і live.
2. Витрати завжди увімкнені — PnL тільки net-of-cost.
3. Нема look-ahead — point-in-time доступ на рівні API.
4. Детермінізм — той самий вхід → побайтово той самий результат.
5. Provenance — кожен прогін із хешем конфіга.
6. `Decimal` для грошей, ніколи `f64`.

## Швидкий старт

### Docker (усе разом)

```bash
docker compose up --build
# дашборд: http://localhost:3000, Swagger: http://localhost:3000/swagger-ui/
```

### Локально

```bash
cp .env.example .env   # або створи .env вручну
# ОБОВ'ЯЗКОВО: JWT_SECRET (сервер не стартує без нього)
#   openssl rand -hex 32
cargo run              # бекенд на :3000 (потрібні Kafka/Scylla/Postgres з compose)

cd dashboard && npm install && npm run dev   # фронтенд у dev-режимі
```

Логи керуються `RUST_LOG` (default `info` для сервера, `warn` для CLI):
`RUST_LOG=db_con=debug cargo run`.

### Дослідницькі прогони

```bash
# TSMOM — валідація движка (Фаза 2 роадмапу)
cargo run --bin quant-backtest -- --data-dir datasets/daily --strategy tsmom --walk-forward

# Funding carry на одному символі
cargo run --bin quant-backtest -- --funding-csv datasets/funding.csv --strategy carry --symbol BTCUSDT

# Cross-sectional carry на кошику HTX-перпів
cargo run --bin quant-backtest -- --strategy xs_carry --funding-dir datasets/funding_htx --top-k 10

# Live-контур HTX: перевірка ключів → план (нічого не шле) → виконання
cargo run --bin htx-exec -- check
cargo run --bin htx-exec -- plan
HTX_TRADING_ENABLED=true cargo run --bin htx-exec -- execute
```

## Структура

```
src/            Rust: domain/ports/adapters по підсистемах
  bootstrap/    wire-up сервера (Kafka-консюмери, HTTP, warmup)
  trading/      стратегії, витрати, sizing, брокер, HTX-адаптери
  backtest/     движок, метрики, walk-forward, purged CV
  bin/          CLI: quant-backtest, htx-exec, backtest, latency-bench
dashboard/      React SPA (сигнали live, свічки, портфель)
datasets/       історія funding (HTX/Binance), денні свічки
shadow/         git-нотаризований журнал shadow-трейдингу + критерії рішень
tests/          determinism replay, smoke-бектест
```

## Методологія і журнали

- [CLAUDE.md](CLAUDE.md) — роадмап з фазами і kill-критеріями (вимірювання →
  анти-оверфіт → валідація движка → пошук альфи).
- [ARCHITECTURE.md](ARCHITECTURE.md) — мапа domain/ports/adapters, інваріанти.
- [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) — детальний огляд усіх підсистем.
- [RESEARCH_LOG.md](RESEARCH_LOG.md) — автогенерований щотижневий журнал
  досліджень (GitHub Actions).
- [docs/xs-carry-sharpe-audit.md](docs/xs-carry-sharpe-audit.md) — аудит
  «xs_carry Sharpe 4.86»: що число міряє насправді і чому рішення
  приймаються не за ним.
- [shadow/DECISION_CRITERIA.md](shadow/DECISION_CRITERIA.md) — ex-ante
  критерії рішень по live-виконанню.

## Тести

```bash
cargo test          # юніт + інтеграційні (детермінізм, smoke)
cargo clippy        # CI ганяє з -D warnings
```
