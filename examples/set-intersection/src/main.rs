use polars::df;
use polars::prelude::*;

fn main() -> PolarsResult<()> {
    let df = df![
        "bar" => ["a", "b", "c", "a", "b", "c", "a", "c"],
        "ham" => ["foo", "foo", "foo", "bar", "bar", "bar", "bing", "bang"]
    ]
    .unwrap();

    let df_grp = df
        .lazy()
        .group_by(["bar"])
        .agg([col("ham").alias("aggregated")])
        .with_column(col("aggregated"))
        .drop_columns(["bar"])
        .collect()?;

    println!("{:?}", df_grp);
    // ┌────────────────────────┐
    // │ aggregated             │
    // │ ---                    │
    // │ list[str]              │
    // ╞════════════════════════╡
    // │ ["foo", "bar"]         │
    // │ ["foo", "bar", "bang"] │
    // │ ["foo", "bar", "bing"] │
    // └────────────────────────┘

    let df_w_all_hams = df_grp.transpose(None, None)?;

    println!("{:?}", df_w_all_hams);
    // ┌────────────────┬────────────────────────┬────────────────────────┐
    // │ column_0       ┆ column_1               ┆ column_2               │
    // │ ---            ┆ ---                    ┆ ---                    │
    // │ list[str]      ┆ list[str]              ┆ list[str]              │
    // ╞════════════════╪════════════════════════╪════════════════════════╡
    // │ ["foo", "bar"] ┆ ["foo", "bar", "bang"] ┆ ["foo", "bar", "bing"] │
    // └────────────────┴────────────────────────┴────────────────────────┘

    let common_vals = df_w_all_hams
        .lazy()
        .select([col("*").list().set_intersection("*").alias("common_vals")])
        .collect()?;

    println!("{:?}", common_vals);
    // ┌────────────────┐
    // │ common_vals    │
    // │ ---            │
    // │ list[str]      │
    // ╞════════════════╡
    // │ ["foo", "bar"] │
    // └────────────────┘

    Ok(())
}
