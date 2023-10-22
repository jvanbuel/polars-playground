use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use regex::Regex;

fn camel_case_str(value: &str, output: &mut String) {
    let re = Regex::new(r"(?:^\w|[A-Z]|\b\w|\s+)").unwrap();
    let result = re.replace_all(value, |caps: &regex::Captures| {
        let mut s = String::new();
        for cap in caps.iter().flatten() {
            match cap.as_str() {
                " " => (),
                _ => s.push_str(cap.as_str().to_ascii_uppercase().as_str()),
            }
        }
        s
    });
    output.push_str(&result);
}

fn snake_case_str(value: &str, output: &mut String) {
    let re = Regex::new(r"(?:^\w|[A-Z]|\b\w|\s+)").unwrap();
    let result = re.replace_all(value, |caps: &regex::Captures| {
        let mut s = String::new();
        for cap in caps.iter().flatten() {
            match cap.as_str() {
                " " => s.push('_'),
                _ => s.push_str(cap.as_str().to_ascii_lowercase().as_str()),
            }
        }
        s
    });
    output.push_str(&result);
}

#[polars_expr(output_type=Utf8)]
fn camel_case(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].utf8()?;
    let out: Utf8Chunked = ca.apply_to_buffer(camel_case_str);
    Ok(out.into_series())
}

#[polars_expr(output_type=Utf8)]
fn snake_case(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].utf8()?;
    let out: Utf8Chunked = ca.apply_to_buffer(snake_case_str);
    Ok(out.into_series())
}
