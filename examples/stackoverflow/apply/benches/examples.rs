use itertools::izip;
use polars::{
    lazy::dsl::{as_struct, GetOutput},
    prelude::*,
};
use rand::{distributions::Uniform, Rng};

const NUMBER_OF_RECORES: usize = 1000;

pub fn unlevered_beta_f(
    levered_beta: f32,
    de_ratio: f32,
    marginal_tax_rate: Option<f32>,
    effective_tax_rate: f32,
    cash_firm_value: f32,
) -> Option<f32> {
    // Do you want to use marginal or effective tax trates in unlevering betas?
    // if marginal tax rate, enter the marginal tax rate to use
    let tax_rate = tax_rate_f(marginal_tax_rate, effective_tax_rate);
    let mut unlevered_beta = levered_beta / (1.0 + (1.0 - tax_rate) * de_ratio);
    unlevered_beta /= 1.0 - cash_firm_value;
    Some(unlevered_beta)
}

pub fn tax_rate_f(marginal_tax_rate: Option<f32>, effective_tax_rate: f32) -> f32 {
    match marginal_tax_rate {
        Some(marginal_tax_rate) => marginal_tax_rate,
        None => effective_tax_rate,
    }
}

fn main() {
    divan::main();
}

fn get_df() -> Result<DataFrame, PolarsError> {
    let rng = rand::thread_rng();
    DataFrame::new(vec![
        Series::new(
            "Average of Beta",
            rng.clone()
                .sample_iter(&Uniform::new(0.0, 2.0))
                .take(NUMBER_OF_RECORES)
                .collect::<Vec<f32>>(),
        ),
        Series::new(
            "Sum of Total Debt incl leases (in US $)",
            rng.clone()
                .sample_iter(&Uniform::new(1e6, 10e6))
                .take(NUMBER_OF_RECORES)
                .collect::<Vec<f32>>(),
        ),
        Series::new(
            "Sum of Market Cap (in US $)",
            rng.clone()
                .sample_iter(&Uniform::new(0.0, 100e6))
                .take(NUMBER_OF_RECORES)
                .collect::<Vec<f32>>(),
        ),
        Series::new(
            "Average of Effective Tax Rate",
            rng.clone()
                .sample_iter(&Uniform::new(0.05, 0.35))
                .take(NUMBER_OF_RECORES)
                .collect::<Vec<f32>>(),
        ),
        Series::new(
            "Sum of Cash",
            rng.clone()
                .sample_iter(&Uniform::new(1e6, 10e6))
                .take(NUMBER_OF_RECORES)
                .collect::<Vec<f32>>(),
        ),
        Series::new(
            "Sum of Firm Value (in US $)",
            rng.clone()
                .sample_iter(&Uniform::new(10e6, 20e6))
                .take(NUMBER_OF_RECORES)
                .collect::<Vec<f32>>(),
        ),
    ])
}

#[divan::bench]
fn levered_beta_f() {
    fn add_col_1(df: DataFrame) -> DataFrame {
        df.lazy()
            .with_column(lit(NULL).alias("Marginal Tax Rate"))
            .with_column(
                as_struct(vec![
                    col("Average of Beta"),
                    col("Sum of Total Debt incl leases (in US $)"),
                    col("Sum of Market Cap (in US $)"),
                    col("Average of Effective Tax Rate"),
                    col("Sum of Cash"),
                    col("Sum of Firm Value (in US $)"),
                ])
                .map(
                    |s| {
                        let cols = s.struct_()?;
                        let avg_beta = cols.field_by_name("Average of Beta")?;
                        let avg_beta = avg_beta.f32()?;
                        let sum_debt =
                            cols.field_by_name("Sum of Total Debt incl leases (in US $)")?;
                        let sum_debt = sum_debt.f32()?;
                        let sum_mkt_cap = cols.field_by_name("Sum of Market Cap (in US $)")?;
                        let sum_mkt_cap = sum_mkt_cap.f32()?;
                        let avg_tax_rate = cols.field_by_name("Average of Effective Tax Rate")?;
                        let avg_tax_rate = avg_tax_rate.f32()?;
                        let sum_cash = cols.field_by_name("Sum of Cash")?;
                        let sum_cash = sum_cash.f32()?;
                        let sum_firm_value = cols.field_by_name("Sum of Firm Value (in US $)")?;
                        let sum_firm_value = sum_firm_value.f32()?;

                        let zipped_iterables = izip!(
                            avg_beta,
                            sum_debt,
                            sum_mkt_cap,
                            avg_tax_rate,
                            sum_cash,
                            sum_firm_value
                        );

                        let x: ChunkedArray<Float32Type> = zipped_iterables
                            .map(
                                |(
                                    avg_beta,
                                    sum_debt,
                                    sum_mkt_cap,
                                    avg_tax_rate,
                                    sum_cash,
                                    sum_firm_value,
                                )| {
                                    if let (
                                        Some(avg_beta),
                                        Some(sum_debt),
                                        Some(sum_mkt_cap),
                                        Some(avg_tax_rate),
                                        Some(sum_cash),
                                        Some(sum_firm_value),
                                    ) = (
                                        avg_beta,
                                        sum_debt,
                                        sum_mkt_cap,
                                        avg_tax_rate,
                                        sum_cash,
                                        sum_firm_value,
                                    ) {
                                        unlevered_beta_f(
                                            avg_beta,
                                            sum_debt / sum_mkt_cap,
                                            None,
                                            avg_tax_rate,
                                            sum_cash / sum_firm_value,
                                        )
                                    } else {
                                        None
                                    }
                                },
                            )
                            .collect();

                        Ok(Some(x.into_series()))
                    },
                    GetOutput::from_type(DataType::Float32),
                )
                .alias("Average Unlevered Beta"),
            )
            .collect()
            .unwrap()
    }

    let df = get_df().unwrap();

    add_col_1(divan::black_box(df));
}

#[divan::bench]
fn levered_beta_f_expr() {
    fn add_col_2(df: DataFrame) -> DataFrame {
        df.lazy()
            .with_column(lit(NULL).alias("Marginal Tax Rate"))
            .with_column(
                col("Average of Beta").alias("Average Unlevered Beta")
                    / (lit(1.0)
                        + (lit(1.0)
                            - when(col("Marginal Tax Rate").is_not_null())
                                .then("Marginal Tax Rate")
                                .otherwise("Average of Effective Tax Rate"))
                            * (col("Sum of Total Debt incl leases (in US $)")
                                / col("Sum of Market Cap (in US $)")))
                    / (lit(1.0) - (col("Sum of Cash") / col("Sum of Firm Value (in US $)"))),
            )
            .collect()
            .unwrap()
    }

    let df = get_df().unwrap();

    add_col_2(divan::black_box(df));
}
