use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

fn pascal_case_str(value: &str, output: &mut String) {
    // write a function that converts a string to pascal case
    let mut capitalize = true;
    for c in value.chars() {
        if c == ' ' {
            capitalize = true;
            continue;
        }
        if c.is_alphanumeric() {
            if capitalize {
                output.push(c.to_ascii_uppercase());
                capitalize = false;
            } else {
                output.push(c.to_ascii_lowercase());
            }
        }
    }
}

fn snake_case_str(value: &str, output: &mut String) {
    for c in value.chars() {
        if c == ' ' {
            
            output.push('_');
            continue;
        }
        if c.is_alphanumeric() {
            output.push(c.to_ascii_lowercase());
        }
    }
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
