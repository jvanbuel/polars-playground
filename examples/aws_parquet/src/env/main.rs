use log::info;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use polars::io::parquet::ParquetWriter;
use polars::{io::cloud::CloudWriter, prelude::*};
use simplelog::SimpleLogger;
use simplelog::{Config, LevelFilter};

fn main() -> Result<(), PolarsError> {
    let _ = SimpleLogger::init(LevelFilter::Info, Config::default());

    info!("Connecting to Object Store...");
    let object_store = AmazonS3Builder::from_env()
        .with_bucket_name("holapolars")
        .build()?;

    info!("Writing to cloud storage");
    let mut s = DataFrame::new(vec![
        Series::new("names", &["a", "b", "c"]),
        Series::new("values", &[1, 2, 3]),
    ])?;

    let cloud_writer =
        CloudWriter::new_with_object_store(Arc::from(object_store), Path::from("example.parquet"))
            .unwrap();
    ParquetWriter::new(cloud_writer).finish(&mut s).unwrap();

    info!("Done!");
    Ok(())
}
