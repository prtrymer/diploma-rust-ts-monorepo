use anyhow::Result;
use chrono::{NaiveDate, TimeZone, Utc};
use std::env;
use std::sync::Arc;

use db_con::data_ingestion::adapters::yahoo_finance::YahooFinanceAdapter;
use db_con::data_ingestion::domain::dataset::HistoricalDatasetService;
use db_con::data_ingestion::ports::DataSourcePort;
use db_con::database::adapters::scylladb::ScyllaRepository;
use db_con::database::ports::repository::Repository;

#[derive(Debug, Clone)]
struct Args {
    symbol: String,
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
    interval: String,
    export_dataset: Option<String>,
    sync_scylla: bool,
    tensor_columns: Option<Vec<String>>,
}

fn parse_date(s: &str) -> Result<chrono::DateTime<Utc>> {
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
    Ok(Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap()))
}

fn parse_args() -> Result<Args> {
    let args: Vec<String> = env::args().collect();
    let mut symbol = "AAPL".to_string();
    let mut start = parse_date("2026-01-01")?;
    let mut end = Utc::now();
    let mut interval = "1m".to_string();
    let mut export_dataset = None;
    let mut sync_scylla = true;
    let mut tensor_columns: Option<Vec<String>> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--symbol" => {
                symbol = args.get(i + 1).cloned().unwrap_or(symbol);
                i += 2;
            }
            "--start" => {
                if let Some(v) = args.get(i + 1) {
                    start = parse_date(v)?;
                }
                i += 2;
            }
            "--end" => {
                if let Some(v) = args.get(i + 1) {
                    end = parse_date(v)?;
                }
                i += 2;
            }
            "--interval" => {
                interval = args.get(i + 1).cloned().unwrap_or(interval);
                i += 2;
            }
            "--export-dataset" => {
                export_dataset = args.get(i + 1).cloned();
                i += 2;
            }
            "--no-sync-scylla" => {
                sync_scylla = false;
                i += 1;
            }
            "--tensor-cols" => {
                tensor_columns = args.get(i + 1).map(|v| {
                    v.split(',')
                        .map(|x| x.trim().to_string())
                        .filter(|x| !x.is_empty())
                        .collect::<Vec<_>>()
                });
                i += 2;
            }
            _ => i += 1,
        }
    }

    Ok(Args {
        symbol,
        start,
        end,
        interval,
        export_dataset,
        sync_scylla,
        tensor_columns,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = parse_args()?;

    println!("Dataset sync:");
    println!(
        "symbol={}, range={}..{}, interval={}",
        args.symbol, args.start, args.end, args.interval
    );

    let repository =
        Arc::new(ScyllaRepository::new(vec!["127.0.0.1:9042".to_string()], "market_data").await?)
            as Arc<dyn Repository>;
    let data_source = Arc::new(YahooFinanceAdapter::new()) as Arc<dyn DataSourcePort>;
    let svc = HistoricalDatasetService::new(data_source, repository);

    let mut frame_for_tensor = None;
    if args.sync_scylla || args.tensor_columns.is_some() {
        let df = svc
            .load_symbol_frame(&args.symbol, args.start, args.end, &args.interval)
            .await?;
        if args.sync_scylla {
            let rows = svc.sync_frame(&df).await?;
            println!("Synced {} rows to Scylla", rows);
        }
        frame_for_tensor = Some(df);
    }

    if let Some(path) = &args.export_dataset {
        let rows = svc
            .export_symbol_dataset(path, &args.symbol, args.start, args.end, &args.interval)
            .await?;
        println!("Exported {} rows to {}", rows, path);
    }

    #[cfg(feature = "candle-ml")]
    if let (Some(cols), Some(df)) = (&args.tensor_columns, frame_for_tensor.as_ref()) {
        let col_refs: Vec<&str> = cols.iter().map(String::as_str).collect();
        let tensor = HistoricalDatasetService::frame_to_tensor(df, &col_refs)?;
        println!("Built Candle tensor shape: {:?}", tensor.shape());
    }

    #[cfg(not(feature = "candle-ml"))]
    if args.tensor_columns.is_some() {
        println!("Tensor build requested, but binary was built without feature `candle-ml`.");
    }

    Ok(())
}
