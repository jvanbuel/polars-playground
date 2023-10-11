use arrow_odbc::arrow::datatypes::SchemaBuilder;
use arrow_odbc::OdbcWriter;
use log::info;
use polars::prelude::*;
use simplelog::SimpleLogger;
use simplelog::{Config, LevelFilter};

fn main() -> Result<(), PolarsError> {
    let _ = SimpleLogger::init(LevelFilter::Info, Config::default());

    let mut s = DataFrame::new(vec![
        Series::new("names", &["a", "b", "c"]),
        Series::new("values", &[1, 2, 3]),
    ])?;

    info!("Connecting to SQL Server...");
    let schema = s.schema().to_arrow();
    // let mssql =  OdbcWriter::with_connection(connection, schema, table_name, row_capacity)

    // let arrow_table = s.into
    info!("Done!");
    Ok(())
}
