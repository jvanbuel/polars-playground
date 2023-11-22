use polars::prelude::*;

fn main() -> PolarsResult<()> {
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), Some(4), Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "groups" => &["A", "A", "B", "C", "B"],
    )?;

    println!("{:?}", df);

    //     shape: (5, 3)
    // ┌─────┬───────┬────────┐
    // │ nrs ┆ names ┆ groups │
    // │ --- ┆ ---   ┆ ---    │
    // │ i32 ┆ str   ┆ str    │
    // ╞═════╪═══════╪════════╡
    // │ 1   ┆ foo   ┆ A      │
    // │ 2   ┆ ham   ┆ A      │
    // │ 3   ┆ spam  ┆ B      │
    // │ 4   ┆ eggs  ┆ C      │
    // │ 5   ┆ null  ┆ B      │
    // └─────┴───────┴────────┘

    let df2 = df.clone().lazy().filter(col("nrs").lt(lit(4)));

    println!("{:?}", df2.clone().collect()?);

    //     shape: (3, 3)
    // ┌─────┬───────┬────────┐
    // │ nrs ┆ names ┆ groups │
    // │ --- ┆ ---   ┆ ---    │
    // │ i32 ┆ str   ┆ str    │
    // ╞═════╪═══════╪════════╡
    // │ 1   ┆ foo   ┆ A      │
    // │ 2   ┆ ham   ┆ A      │
    // │ 3   ┆ spam  ┆ B      │
    // └─────┴───────┴────────┘

    let disjoint_df = df.lazy().join(
        df2,
        [col("nrs"), col("names"), col("groups")],
        [col("nrs"), col("names"), col("groups")],
        JoinArgs::new(JoinType::Anti),
    );

    println!("{:?}", disjoint_df.collect()?);

    //     shape: (2, 3)
    // ┌─────┬───────┬────────┐
    // │ nrs ┆ names ┆ groups │
    // │ --- ┆ ---   ┆ ---    │
    // │ i32 ┆ str   ┆ str    │
    // ╞═════╪═══════╪════════╡
    // │ 4   ┆ eggs  ┆ C      │
    // │ 5   ┆ null  ┆ B      │
    // └─────┴───────┴────────┘
    Ok(())
}
