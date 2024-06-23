use polars::{lazy::dsl::is_not_null, prelude::*};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let df = df!(
        "names" => &["None", "0", "1", "15", "1|2", "5 ??", "293 ", "XX"])?
        .lazy()
        .select([col("names")
            .str()
            .to_integer(lit("10"), false)
            .alias("parsed_int")])
        .filter(is_not_null(col("parsed_int")).and(col("parsed_int").gt(lit(0))))
        .collect()?;

    println!("{df}");
    //  shape: (2, 1)
    // ┌────────────┐
    // │ parsed_int │
    // │ ---        │
    // │ i64        │
    // ╞════════════╡
    // │ 1          │
    // │ 15         │
    // └────────────┘
    Ok(())
}
