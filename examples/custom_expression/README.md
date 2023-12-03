# Polars Custom Expression

This crate contains an example of how to create a custom expression for Polars.

It exposes two functions in an Expression namespace `Casing`:

- `to_pascal_case`
- `to_snake_case`

which convert a string to PascalCase and snake_case respectively.

To build the custom expression and package it with `maturin` run:

```bash
make develop
```

To run the benchmarks, which compare the custom expression with a naive python implementation, run:

```bash
make bench
```

To plot histograms of the benchmarks run:

```bash
make hist
```

This will populate histograms for each parametrized benchmark in the `bench` folder.
