use polars::prelude::*;

fn main() -> PolarsResult<()> {
    let mut df = df! (
        "col_1" => &[1, 2, 3, 4, 5],
        "col_2" => &[2, 4, 6, 8, 5],
        "col_3" => &[10, 8, 6, 4, 5],
    )?;

    let n = df.get_column_names().len() as i32;

    let col_mean = df.mean_horizontal(polars::frame::NullStrategy::Ignore)?;

    let df_w_mean: &mut DataFrame;
    if let Some(mean) = col_mean {
        df_w_mean = df.with_column(mean.with_name("col_mean"))?;
    } else {
        return Err(PolarsError::ComputeError("No mean can be calculated".into()));
    }
    
    let sse = df_w_mean
        .clone()
        .lazy()
        .with_column((col("*") - col("col_mean")).pow(2))
        .collect()?
        .sum_horizontal(polars::frame::NullStrategy::Ignore)?;
    
    let df_w_sse: &mut DataFrame;
    if let Some(sse) = sse {
        df_w_sse = df_w_mean.with_column(sse.with_name("col_std"))?;
    } else {
        return Err(PolarsError::ComputeError("No sse can be caculated".into()));
    }

    let result = df_w_sse.clone().lazy().with_column((col("col_std") / lit(n - 1)).sqrt()).collect()?;
    println!("{:?}", result);

    Ok(())
}
