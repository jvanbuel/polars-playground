use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use regex::Regex;

fn pascal_case_str(value: &str, output: &mut String) {
    // write a function that converts a string to pascal case
    let pascal_str = Regex::new(r"(\b\w)")
        .unwrap()
        .replace_all(value, |caps: &regex::Captures| caps[0].to_ascii_uppercase());
    let pascal_str = Regex::new(r"(\s+)").unwrap().replace_all(&pascal_str, "");
    output.push_str(&pascal_str);
}

fn snake_case_str(value: &str, output: &mut String) {
    let pascal_str = Regex::new(r"(\b\w)")
        .unwrap()
        .replace_all(value, |caps: &regex::Captures| caps[0].to_ascii_lowercase());
    let pascal_str = Regex::new(r"(\s+)").unwrap().replace_all(&pascal_str, "_");
    output.push_str(&pascal_str);
}

#[polars_expr(output_type=Utf8)]
fn pascal_case(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].utf8()?;
    let out: Utf8Chunked = ca.apply_to_buffer(pascal_case_str);
    Ok(out.into_series())
}

#[polars_expr(output_type=Utf8)]
fn snake_case(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].utf8()?;
    let out: Utf8Chunked = ca.apply_to_buffer(snake_case_str);
    Ok(out.into_series())
}
