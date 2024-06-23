use polars::prelude::*;


fn main() -> PolarsResult<()> {
    let df = df!("parameters" => ["{\"parameters\":{\"firefox\":{\"name\":\"Firefox\",\"pref_url\":\"about:config\",\"releases\":{\"1\":{\"release_date\":\"2004-11-09\",\"status\":\"retired\",\"engine\":\"Gecko\",\"engine_version\":\"1.7\"}}}}}"])?;

    let parsed = df.lazy().with_column(col("parameters").str().json_decode());
    println!("{:?}", parsed.collect()?);
    Ok(())
}