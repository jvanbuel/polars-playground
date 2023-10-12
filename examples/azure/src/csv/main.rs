use log::info;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::ObjectStore;
use polars::io::csv::CsvReader;
use polars::prelude::*;
use simplelog::SimpleLogger;
use simplelog::{Config, LevelFilter};

const STORAGE_ACCOUNT: &str = "holapolars";
const CONTAINER_NAME: &str = "data";

#[tokio::main]
async fn main() -> Result<(), PolarsError> {
    let _ = SimpleLogger::init(LevelFilter::Info, Config::default());

    info!("Connecting to Object Store...");
    let object_store = MicrosoftAzureBuilder::new()
        .with_account(STORAGE_ACCOUNT)
        .with_container_name(CONTAINER_NAME)
        .with_use_azure_cli(true)
        .build()?;

    let data = object_store.get(&Path::from("input.csv")).await?;

    let df = CsvReader::new(std::io::Cursor::new(data.bytes().await?))
        .infer_schema(Some(100))
        .has_header(true)
        .finish()?;

    println!("{:?}", df);

    info!("Done!");
    Ok(())
}
