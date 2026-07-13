//! CLI live-виконання carry-кошика на HTX.
//!
//! ```text
//! htx-exec check                 — зв'язок, ключі, годинник, комісії
//! htx-exec plan                  — план ребалансу, НІЧОГО не шле (dry-run)
//! htx-exec positions             — позиції свопів + спот-баланси
//! htx-exec execute               — реально шле (потрібен HTX_TRADING_ENABLED=true)
//! htx-exec config                — чинний LiveConfig + його hash
//! htx-exec fetch-funding         — історія funding HTX → CSV для research
//!
//! Кошик за замовчуванням — htx_trailing: живий HTX-селектор (трейлінг-середнє
//! settled funding, скоринг з xs_carry-бектесту). Кошики shadow-журналу
//! (baseline_est|rf, Binance-ранжування) доступні через --basket.
//!
//! Прапорці: --config <live_config.json>  --ledger <shadow/ledger.jsonl>
//!           --basket <htx_trailing|baseline_est|rf>  --entry <id>  --allow-retro
//! fetch-funding: --out <dir=datasets/funding_htx>  --all (і без спот-пари)
//!                --contracts BTC-USDT,ETH-USDT (обмежити список)
//! ```

use anyhow::{Context, Result};
use rust_decimal_macros::dec;

use db_con::shared::run_config::LiveConfig;
use db_con::trading::adapters::htx::executor::{
    build_plan, last_real_entry, parse_ledger_entry, HtxCarryExecutor, LedgerBasketEntry,
};
use db_con::trading::adapters::htx::selector::{select_htx_trailing, HTX_TRAILING_BASKET};
use db_con::trading::adapters::htx::HtxClient;

struct Args {
    command: String,
    config_path: String,
    ledger_path: String,
    basket_override: Option<String>,
    entry_id: Option<String>,
    allow_retro: bool,
    out_dir: String,
    all_contracts: bool,
    contracts_filter: Option<Vec<String>>,
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        command: String::new(),
        config_path: "live_config.json".to_string(),
        ledger_path: "shadow/ledger.jsonl".to_string(),
        basket_override: None,
        entry_id: None,
        allow_retro: false,
        out_dir: "datasets/funding_htx".to_string(),
        all_contracts: false,
        contracts_filter: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "check" | "plan" | "positions" | "execute" | "config" | "fetch-funding" => {
                args.command = a
            }
            "--config" => args.config_path = it.next().context("--config без значення")?,
            "--ledger" => args.ledger_path = it.next().context("--ledger без значення")?,
            "--basket" => args.basket_override = Some(it.next().context("--basket без значення")?),
            "--entry" => args.entry_id = Some(it.next().context("--entry без значення")?),
            "--allow-retro" => args.allow_retro = true,
            "--out" => args.out_dir = it.next().context("--out без значення")?,
            "--all" => args.all_contracts = true,
            "--contracts" => {
                args.contracts_filter = Some(
                    it.next()
                        .context("--contracts без значення")?
                        .split(',')
                        .map(|s| s.trim().to_uppercase())
                        .collect(),
                )
            }
            other => anyhow::bail!(
                "невідомий аргумент: {other} (команди: check|plan|positions|execute|config|fetch-funding)"
            ),
        }
    }
    anyhow::ensure!(
        !args.command.is_empty(),
        "вкажи команду: check | plan | positions | execute | config | fetch-funding"
    );
    Ok(args)
}

/// LiveConfig: з файлу, якщо він є, інакше дефолт (і кажемо про це чесно).
fn load_config(args: &Args) -> Result<LiveConfig> {
    let mut cfg = match std::fs::read_to_string(&args.config_path) {
        Ok(s) => LiveConfig::from_json(&s)
            .with_context(|| format!("битий {}", args.config_path))?,
        Err(_) => {
            println!("(конфіг {} не знайдено — дефолтний LiveConfig)", args.config_path);
            LiveConfig::default()
        }
    };
    if let Some(b) = &args.basket_override {
        cfg.basket = b.clone();
    }
    Ok(cfg)
}

fn load_entry(args: &Args, cfg: &LiveConfig) -> Result<LedgerBasketEntry> {
    let ledger = std::fs::read_to_string(&args.ledger_path)
        .with_context(|| format!("нема журналу {} — спершу прожени shadow_carry", args.ledger_path))?;
    if let Some(id) = &args.entry_id {
        let entry = ledger
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter(|v| v["id"].as_str() == Some(id.as_str()))
            .find_map(|v| parse_ledger_entry(&v, &cfg.basket))
            .with_context(|| format!("запису {id} з кошиком {} немає в журналі", cfg.basket))?;
        anyhow::ensure!(
            !entry.retro || args.allow_retro,
            "запис {id} — retro (тестовий, не ex-ante); якщо точно треба — додай --allow-retro"
        );
        return Ok(entry);
    }
    last_real_entry(&ledger, &cfg.basket)
        .context("у журналі немає жодного справжнього (не-retro) запису з таким кошиком")
}

async fn cmd_check(client: &HtxClient) -> Result<()> {
    // Годинник: підпис живе 5 хв, розсинхрон — найчастіша причина
    // «api-signature-not-valid».
    let t0 = chrono::Utc::now().timestamp_millis();
    let server_ms = client.spot_server_time().await?;
    let skew_ms = (chrono::Utc::now().timestamp_millis() + t0) / 2 - server_ms;
    println!("Годинник: розсинхрон із біржею ≈ {skew_ms} мс {}", if skew_ms.abs() < 30_000 { "— ок" } else { "— НАЛАШТУЙ NTP!" });

    let contracts = client.swap_contracts().await?;
    let trading = contracts.iter().filter(|c| c.is_trading()).count();
    println!("USDT-M свопів у торгах: {trading}");
    let funding = client.swap_funding_rates().await?;
    println!("Funding-ставок віддано: {}", funding.len());
    let symbols = client.spot_symbols().await?;
    println!("Спот-пар у довіднику: {}", symbols.len());

    if !client.has_creds() {
        println!("\nКлючів немає в env — приватну частину не перевіряю.");
        println!("Додай HTX_ACCESS_KEY / HTX_SECRET_KEY у .env і повтори.");
        return Ok(());
    }
    let account_id = client.spot_account_id().await?;
    println!("\nСпот-акаунт: {account_id}");
    let balances = client.spot_balances(account_id).await?;
    let mut top: Vec<_> = balances.iter().collect();
    top.sort_by_key(|entry| std::cmp::Reverse(*entry.1));
    for (cur, amt) in top.iter().take(8) {
        println!("  {cur}: {amt}");
    }
    for acc in client.swap_cross_account().await? {
        println!(
            "Крос-маржа {}: баланс {} (вільно {})",
            acc.margin_account.as_deref().unwrap_or("USDT"),
            acc.margin_balance.unwrap_or_default(),
            acc.withdraw_available.unwrap_or_default()
        );
    }
    match client.spot_fee_rates(&["btcusdt".into()]).await {
        Ok(fees) => {
            for f in fees {
                println!(
                    "Комісії {} (спот): maker {} / taker {}",
                    f.symbol,
                    f.actual_maker_rate.unwrap_or_default(),
                    f.actual_taker_rate.unwrap_or_default()
                );
            }
        }
        Err(e) => println!("Комісії не прочитались: {e:#}"),
    }

    // Стан самого ключа: права (щоб зловити випадковий withdraw), IP-прив'язка
    // і лічильник деактивації за неактивності (90 днів для непривʼязаних).
    if let Some(access_key) = client.access_key() {
        let report = async {
            let uid = client.user_uid().await?;
            client.api_key_info(uid, access_key).await
        };
        match report.await {
            Ok(keys) => {
                for k in &keys {
                    println!(
                        "\nAPI-ключ «{}»: права [{}], статус {}",
                        k.note.as_deref().unwrap_or("без назви"),
                        k.permission.as_deref().unwrap_or("?"),
                        k.status.as_deref().unwrap_or("?")
                    );
                    if k.has_withdraw() {
                        println!("  ⚠⚠ У КЛЮЧА Є ПРАВО ВИВОДУ КОШТІВ — видали його і створи новий без виводу!");
                    }
                    if k.is_ip_bound() {
                        println!("  IP-прив'язка: {}", k.ip_addresses.as_deref().unwrap_or(""));
                    } else {
                        println!("  Без IP-прив'язки: деактивація після 90 днів БЕЗ використання (запуски скидають лічильник)");
                    }
                    match k.valid_days {
                        Some(-1) | None => {}
                        Some(d) if d <= 14 => println!("  ⚠ Лишилось {d} дн. валідності — запусти будь-яку приватну команду або перевір прив'язку"),
                        Some(d) => println!("  Валідність: ще {d} дн."),
                    }
                }
            }
            Err(e) => println!("\nСтан ключа не прочитався (не критично): {e:#}"),
        }
    }
    println!("\nГОТОВО: ключі працюють. Нагадування: у ключа НЕ має бути права на вивід.");
    Ok(())
}

async fn cmd_positions(client: &HtxClient) -> Result<()> {
    anyhow::ensure!(client.has_creds(), "потрібні ключі HTX у env");
    let positions = client.swap_cross_positions().await?;
    if positions.is_empty() {
        println!("Відкритих своп-позицій немає.");
    } else {
        println!("Своп-позиції (крос):");
        for p in &positions {
            println!(
                "  {} {} {} контрактів, PnL {}",
                p.contract_code,
                if p.direction == "sell" { "SHORT" } else { "LONG" },
                p.volume,
                p.profit_unreal.unwrap_or_default()
            );
        }
    }
    let account_id = client.spot_account_id().await?;
    let balances = client.spot_balances(account_id).await?;
    let mut list: Vec<_> = balances.into_iter().collect();
    list.sort_by_key(|entry| std::cmp::Reverse(entry.1));
    println!("Спот-баланси (>0):");
    for (cur, amt) in list {
        println!("  {cur}: {amt}");
    }
    Ok(())
}

/// Історія funding усіх (або відфільтрованих) перпів HTX → CSV research-формату.
async fn cmd_fetch_funding(client: &HtxClient, args: &Args) -> Result<()> {
    use db_con::trading::adapters::htx::funding_history::{csv_symbol, FundingHistoryCollector};

    let contracts = client.swap_contracts().await?;
    let spot_pairs: std::collections::HashSet<String> = client
        .spot_symbols()
        .await?
        .into_iter()
        .filter(|m| m.state.as_deref() == Some("online"))
        .map(|m| m.sc.to_uppercase())
        .collect();

    let mut selected: Vec<String> = contracts
        .iter()
        .filter(|c| c.is_trading())
        .map(|c| c.contract_code.clone())
        .filter(|code| {
            args.contracts_filter
                .as_ref()
                .map(|f| f.contains(&code.to_uppercase()))
                .unwrap_or(true)
        })
        .filter(|code| args.all_contracts || spot_pairs.contains(&csv_symbol(code)))
        .collect();
    selected.sort();

    let hedgeable = contracts
        .iter()
        .filter(|c| c.is_trading() && spot_pairs.contains(&csv_symbol(&c.contract_code)))
        .count();
    println!(
        "Перпів у торгах: {} | зі спот-парою на HTX (торгований юніверс): {} | збираю: {}{}",
        contracts.iter().filter(|c| c.is_trading()).count(),
        hedgeable,
        selected.len(),
        if args.all_contracts { " (--all: і без спот-ноги)" } else { "" }
    );
    anyhow::ensure!(!selected.is_empty(), "порожній список контрактів");

    let out_dir = std::path::PathBuf::from(&args.out_dir);
    let collector = FundingHistoryCollector::new(client);
    let (mut total_added, mut total_no_price, mut failed) = (0usize, 0usize, 0usize);
    for (i, code) in selected.iter().enumerate() {
        match collector.collect_contract(code, &out_dir).await {
            Ok(r) => {
                total_added += r.added;
                total_no_price += r.no_price;
                if r.added > 0 || r.no_price > 0 {
                    println!(
                        "[{}/{}] {code}: +{} рядків{}",
                        i + 1,
                        selected.len(),
                        r.added,
                        if r.no_price > 0 {
                            format!(" ({} без ціни — пропущено)", r.no_price)
                        } else {
                            String::new()
                        }
                    );
                } else {
                    println!("[{}/{}] {code}: актуально", i + 1, selected.len());
                }
            }
            Err(e) => {
                failed += 1;
                println!("[{}/{}] {code}: ПОМИЛКА {e:#}", i + 1, selected.len());
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
    println!(
        "\nГотово: +{total_added} рядків у {} | без ціни: {total_no_price} | помилок контрактів: {failed}",
        args.out_dir
    );
    println!(
        "Бектест: cargo run --bin quant-backtest -- --strategy xs_carry --funding-dir {} --top-k 10",
        args.out_dir
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = parse_args()?;
    let client = HtxClient::from_env()?;

    // fetch-funding/check/positions не залежать від LiveConfig — не шумимо хешем.
    if args.command == "fetch-funding" {
        return cmd_fetch_funding(&client, &args).await;
    }

    let cfg = load_config(&args)?;
    sanity(&cfg)?;
    println!("LiveConfig hash: {}", cfg.config_hash());

    match args.command.as_str() {
        "config" => println!("{}", cfg.to_pretty_json()),
        "check" => cmd_check(&client).await?,
        "positions" => cmd_positions(&client).await?,
        "plan" | "execute" => {
            let mut selection_scores: Option<Vec<(String, String)>> = None;
            let entry = if cfg.basket == HTX_TRAILING_BASKET {
                println!(
                    "Кошик: живий HTX-селектор (трейлінг {} інтервалів ≈ {} днів, top-{})",
                    cfg.selector_trailing_intervals,
                    cfg.selector_trailing_intervals / 3,
                    cfg.top_k
                );
                let sel = select_htx_trailing(&client, &cfg).await?;
                println!(
                    "Юніверс: {} контрактів зі спот-парою | додатний трейлінг: {} | закоротка історія: {}",
                    sel.universe_size,
                    sel.ranked.len(),
                    sel.short_history
                );
                println!(
                    "Ранжування (▸ = кандидати; скільки реально торгуємо — вирішує розподіл капіталу нижче), anchor {}:",
                    sel.entry.anchor_ts
                );
                for (i, (sym, score)) in sel.ranked.iter().take(cfg.top_k + 5).enumerate() {
                    println!(
                        "  {} {:>2}. {sym:<16} {:.4}%/8г ≈ {:.1}% річних",
                        if i < cfg.top_k { "▸" } else { " " },
                        i + 1,
                        score * dec!(100),
                        score * dec!(1095) * dec!(100)
                    );
                }
                selection_scores = Some(
                    sel.ranked
                        .iter()
                        .take(cfg.top_k)
                        .map(|(s, r)| (s.clone(), r.to_string()))
                        .collect(),
                );
                sel.entry
            } else {
                let entry = load_entry(&args, &cfg)?;
                println!(
                    "Запис shadow-журналу: {} (anchor {}, retro={}, {} символів)",
                    entry.id,
                    entry.anchor_ts,
                    entry.retro,
                    entry.symbols.len()
                );
                println!(
                    "УВАГА: кошик ранжований за Binance-даними — на HTX сигнал переноситься погано \
                     (перевір скіпи в плані); HTX-нативний шлях: --basket htx_trailing"
                );
                entry
            };
            let executor = HtxCarryExecutor::new(client, cfg.clone());
            let (inputs, warnings) = executor.gather_inputs(&entry).await?;
            let mut plan = build_plan(&entry, &inputs, &cfg);
            plan.warnings.extend(warnings);
            println!("\n{plan}");

            if args.command == "execute" {
                if plan.is_empty() {
                    println!("План порожній — нічого відправляти.");
                    return Ok(());
                }
                // Провенанс вибірки: ex-ante фіксуємо кошик і скори селектора
                // в журналі ДО першого ордера.
                if let Some(scores) = &selection_scores {
                    executor.journal.append(&serde_json::json!({
                        "phase": "selection",
                        "ts": chrono::Utc::now(),
                        "entry_id": plan.entry_id,
                        "basket": plan.basket_name,
                        "anchor_ts": plan.anchor_ts,
                        "trailing_intervals": cfg.selector_trailing_intervals,
                        "scores_per_8h": scores.iter().cloned()
                            .collect::<std::collections::BTreeMap<_, _>>(),
                    }))?;
                }
                // Запобіжник перевіряється всередині execute_plan.
                let outcome = executor.execute_plan(&plan).await?;
                println!("\n── ВИКОНАННЯ ──");
                for line in &outcome.lines {
                    println!("{line}");
                }
                println!(
                    "\nВиставлено: {} | Помилок: {} | Журнал: live/orders.jsonl",
                    outcome.placed, outcome.failed
                );
                if outcome.failed > 0 {
                    println!("Помилкові ноги НЕ ретраяться автоматично — подивись журнал і перезапусти execute: ідемпотентність не дасть задублювати вже відправлене.");
                }
                println!("Post-only ордери можуть лишитися невиконаними — перевір пізніше `htx-exec positions` або в UI біржі.");
            } else {
                println!("Це план (dry-run) — нічого не відправлено.");
                println!("Відправка: HTX_TRADING_ENABLED=true і `htx-exec execute`.");
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

/// Страховка від сплутаних одиниць: deploy_pct — частка, не відсотки.
/// Краще впасти на старті, ніж відкрити 5000% капіталу.
fn sanity(cfg: &LiveConfig) -> Result<()> {
    anyhow::ensure!(
        cfg.deploy_pct > dec!(0) && cfg.deploy_pct <= dec!(1),
        "deploy_pct — частка (0..1], не відсотки"
    );
    anyhow::ensure!(
        cfg.lever_rate >= 1 && cfg.lever_rate <= 3,
        "lever_rate поза розумними межами для carry (1..3)"
    );
    anyhow::ensure!(
        cfg.order_style == "post_only" || cfg.order_style == "taker",
        "order_style: post_only | taker"
    );
    anyhow::ensure!(
        matches!(cfg.basket.as_str(), "htx_trailing" | "baseline_est" | "rf"),
        "basket: htx_trailing | baseline_est | rf"
    );
    anyhow::ensure!(
        (1..=20).contains(&cfg.top_k),
        "top_k поза розумними межами (1..20)"
    );
    anyhow::ensure!(
        cfg.sizing_mode == "fixed" || cfg.sizing_mode == "auto",
        "sizing_mode: fixed | auto"
    );
    anyhow::ensure!(
        cfg.capital_usdt > rust_decimal::Decimal::ZERO || cfg.sizing_mode == "auto",
        "capital_usdt=0 дозволено лише з sizing_mode=auto (капітал з балансів)"
    );
    Ok(())
}
