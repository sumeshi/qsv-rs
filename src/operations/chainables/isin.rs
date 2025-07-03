use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn isin(df: &LazyFrame, colname: &str, values: &[String]) -> LazyFrame {
    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema for isin operation: {e}");
            std::process::exit(1);
        }
    };

    if !schema.iter_names().any(|s| s == colname) {
        eprintln!("Error: Column '{colname}' not found in DataFrame for isin operation");
        std::process::exit(1);
    }

    LogController::debug(&format!(
        "Applying isin: column={colname} values={values:?}"
    ));

    if values.is_empty() {
        LogController::debug("Empty values list for isin, returning empty result");
        return df.clone().filter(lit(false));
    }

    // Get the column data type
    let col_dtype = schema.get(colname).unwrap();

    // Build filter expression efficiently using fold instead of manual iteration
    let filter_expr = if matches!(
        col_dtype,
        DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Float32
    ) {
        // For numeric columns, convert to string and compare
        values
            .iter()
            .map(|val_str| {
                col(colname)
                    .cast(DataType::String)
                    .eq(lit(val_str.as_str()))
            })
            .reduce(|acc, expr| acc.or(expr))
            .unwrap_or_else(|| lit(false))
    } else {
        // For string and other types, use direct comparison
        values
            .iter()
            .map(|val_str| col(colname).eq(lit(val_str.as_str())))
            .reduce(|acc, expr| acc.or(expr))
            .unwrap_or_else(|| lit(false))
    };

    df.clone().filter(filter_expr)
}
