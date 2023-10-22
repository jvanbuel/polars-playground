import polars as pl
from polars_casing import Casing  # noqa: F401

df = pl.DataFrame(
    {
        "convert": ["my variable", "polars is cool", "expressions are tha bomb", "i love rust"],
    }
)


out = df.with_columns(
    camel_case=pl.col("convert").casing.camel_case(),
    snake_case=pl.col("convert").casing.snake_case(),
)

print(out)

# ┌──────────────────────────┬───────────────────────┬──────────────────────────┐
# │ convert                  ┆ camel_case            ┆ snake_case               │
# │ ---                      ┆ ---                   ┆ ---                      │
# │ str                      ┆ str                   ┆ str                      │
# ╞══════════════════════════╪═══════════════════════╪══════════════════════════╡
# │ my variable              ┆ MyVariable            ┆ my_variable              │
# │ polars is cool           ┆ PolarsIsCool          ┆ polars_is_cool           │
# │ expressions are tha bomb ┆ ExpressionsAreThaBomb ┆ expressions_are_tha_bomb │
# │ i love rust              ┆ ILoveRust             ┆ i_love_rust              │
# └──────────────────────────┴───────────────────────┴──────────────────────────┘