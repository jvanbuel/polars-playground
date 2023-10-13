use polars::{lazy::dsl::all_horizontal, prelude::*};

fn main() -> Result<(), PolarsError> {
    let df = df!(
        "foo" => &[Some(1.0), Some(2.3), None, Some(f64::NAN)],
        "bar" => [None, Some(34.5), Some(4.5), Some(3.0)],
        "baz" => [f64::NAN, 0.0, 5.0, 4.0]
    )?;

    println!("{:?}", df);
    // ┌──────┬──────┬─────┐
    // │ foo  ┆ bar  ┆ baz │
    // │ ---  ┆ ---  ┆ --- │
    // │ f64  ┆ f64  ┆ f64 │
    // ╞══════╪══════╪═════╡
    // │ 1.0  ┆ null ┆ NaN │
    // │ 2.3  ┆ 34.5 ┆ 0.0 │
    // │ null ┆ 4.5  ┆ 5.0 │
    // │ NaN  ┆ 3.0  ┆ 4.0 │
    // └──────┴──────┴─────┘

    let without_nulls = df
        .lazy()
        .drop_nulls(None)
        .filter(all_horizontal([all().is_not_nan()]))
        .collect()?;

    println!("{:?}", without_nulls);
    // ┌─────┬──────┬─────┐
    // │ foo ┆ bar  ┆ baz │
    // │ --- ┆ ---  ┆ --- │
    // │ f64 ┆ f64  ┆ f64 │
    // ╞═════╪══════╪═════╡
    // │ 2.3 ┆ 34.5 ┆ 0.0 │
    // └─────┴──────┴─────┘

    Ok(())
}
