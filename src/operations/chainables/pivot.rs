use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn pivot(
    df: &LazyFrame,
    rows: &[String],
    columns: &[String],
    values: &str,
    agg_func: &str,
) -> LazyFrame {
    LogController::debug(&format!(
        "Creating pivot table with rows: {rows:?}, columns: {columns:?}, values: {values}, aggregation: {agg_func}"
    ));

    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema for pivot operation: {e}");
            std::process::exit(1);
        }
    };

    // Validate columns exist
    for col in rows
        .iter()
        .chain(columns.iter())
        .chain(std::iter::once(&values.to_string()))
    {
        if !schema.iter_names().any(|s| s == col) {
            eprintln!("Error: Column '{col}' not found in DataFrame for pivot operation");
            std::process::exit(1);
        }
    }

    // For now, implement a simple pivot using group_by and aggregation
    // This is a simplified version - full pivot table functionality would be more complex
    if rows.is_empty() && columns.is_empty() {
        eprintln!("Error: At least one of --rows or --cols must be specified for pivot");
        std::process::exit(1);
    }

    // Create group by columns
    let mut group_cols = Vec::new();
    group_cols.extend(rows.iter().map(col));
    group_cols.extend(columns.iter().map(col));

    // Create aggregation expression
    let value_col = col(values);
    let agg_expr = match agg_func {
        "sum" => value_col.sum(),
        "mean" => value_col.mean(),
        "count" => value_col.count(),
        "min" => value_col.min(),
        "max" => value_col.max(),
        "median" => value_col.median(),
        "std" => value_col.std(1),
        _ => {
            LogController::warn(&format!(
                "Unknown aggregation function '{agg_func}', using sum"
            ));
            value_col.sum()
        }
    };

    // Group by and aggregate
    let result = df
        .clone()
        .group_by(group_cols)
        .agg([agg_expr.alias(format!("{values}_{agg_func}"))]);

    LogController::debug(&format!(
        "Pivot operation completed: {} rows, {} columns, {} values, {} aggregation",
        rows.len(),
        columns.len(),
        values,
        agg_func
    ));

    result
}
