use log::info;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use polars::io::parquet::ParquetWriter;
use polars::{io::cloud::CloudWriter, prelude::*};
use simplelog::SimpleLogger;
use simplelog::{Config, LevelFilter};

fn main() -> Result<(), PolarsError> {
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

    let cloud_writer =
        CloudWriter::new_with_object_store(Arc::from(object_store), Path::from("example.parquet"))?;

    info!("Writing to cloud storage");
    ParquetWriter::new(cloud_writer).finish(&mut s)?;

    info!("Done!");
    Ok(())
}
