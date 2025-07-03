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

    // Get the column data type
    let col_dtype = schema.get(colname).unwrap();

    // For numeric columns, convert to string and do string comparison to avoid type issues
    let filter_expr = if matches!(
        col_dtype,
        DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Float32
    ) {
        // Convert column to string and compare
        let mut string_filter = lit(false);
        for val_str in values {
            string_filter =
                string_filter.or(col(colname).cast(DataType::String).eq(lit(val_str.clone())));
        }
        string_filter
    } else {
        // For string and other types, use direct comparison
        let mut filter_expr = lit(false);
        for val_str in values {
            filter_expr = filter_expr.or(col(colname).eq(lit(val_str.clone())));
        }
        filter_expr
    };

    df.clone().filter(filter_expr)
}
