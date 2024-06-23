use polars::{lazy::dsl::StrptimeOptions, prelude::*};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let df = df!(
        "createdAt" => &["2023-06-23 00:52:32"],
        "updatedAt" => &["2023-06-23 13:12:00"]
    )?;

    let out = df
        .lazy()
        .with_columns([
            col("createdAt").str().strptime(
                DataType::Datetime(TimeUnit::Milliseconds, None),
                StrptimeOptions::default(),
                lit("raise"),
            ),
            col("updatedAt").str().strptime(
                DataType::Datetime(TimeUnit::Milliseconds, None),
                StrptimeOptions::default(),
                lit("raise"), // raise an error if the parsing fails
            ),
        ])
        .with_columns([(col("updatedAt") - col("createdAt"))
            .alias("duration")
            .dt()
            .total_seconds()])
        .collect()?;

    println!("{out}");
    // shape: (1, 3)
    // ┌─────────────────────┬─────────────────────┬──────────┐
    // │ createdAt           ┆ updatedAt           ┆ duration │
    // │ ---                 ┆ ---                 ┆ ---      │
    // │ datetime[ms]        ┆ datetime[ms]        ┆ i64      │
    // ╞═════════════════════╪═════════════════════╪══════════╡
    // │ 2023-06-23 00:52:32 ┆ 2023-06-23 13:12:00 ┆ 44368    │
    // └─────────────────────┴─────────────────────┴──────────┘
    Ok(())
}
