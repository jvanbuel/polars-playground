use polars::datatypes;
use polars::prelude::*;

#[derive(Debug)]
struct Record {
    field1: String,
    field2: i32,
}

macro_rules! struct_to_dataframe {
    ($input:expr, [$($field:ident),+]) => {
        {
            // Extract the field values into separate vectors
            $(let mut $field = Vec::new();)*

            for e in $input.into_iter() {
                $($field.push(e.$field);)*
            }
            df!(
                $(stringify!($field) => $field,)*
            )
        }
    };
}

fn main() -> PolarsResult<()> {
    let df: DataFrame = struct_to_dataframe!(
        vec![
            Record {
                field1: "val1".into(),
                field2: 1
            },
            Record {
                field1: "val2".into(),
                field2: 2
            }
        ],
        [field1, field2]
    )?;
    println!("{:?}", df);
    // shape: (2, 2)
    // ┌────────┬────────┐
    // │ field1 ┆ field2 │
    // │ ---    ┆ ---    │
    // │ str    ┆ i32    │
    // ╞════════╪════════╡
    // │ val1   ┆ 1      │
    // │ val2   ┆ 2      │
    // └────────┴────────┘

    let records: Vec<Record> = df.into_struct("test").into_series().iter().collect();
    println!("{:?}", records);
    Ok(())
}
