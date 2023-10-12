use log::info;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::ObjectStore;
use polars::io::parquet::ParquetWriter;
use polars::prelude::*;
use simplelog::SimpleLogger;
use simplelog::{Config, LevelFilter};

const OUTPUT_LOCATION: &str = "output.parquet";

#[tokio::main]
async fn main() -> Result<(), PolarsError> {
    let _ = SimpleLogger::init(LevelFilter::Info, Config::default());

    let mut s = DataFrame::new(vec![
        Series::new("names", &["a", "b", "c"]),
        Series::new("values", &[1, 2, 3]),
    ])?;

    info!("Connecting to Object Store...");
    let object_store = MicrosoftAzureBuilder::new()
        .with_account("holapolars")
        .with_container_name("data")
        .with_use_azure_cli(true)
        .build()?;

    info!("Writing to cloud storage");
    let mut buffer = vec![];

    ParquetWriter::new(&mut buffer).finish(&mut s)?;
    object_store
        .put(&Path::from(OUTPUT_LOCATION), buffer.into())
        .await?;

    info!("Done!");
    Ok(())
}
