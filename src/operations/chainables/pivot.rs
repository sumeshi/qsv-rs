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
        "Creating pivot table with rows: {:?}, columns: {:?}, values: {}, aggregation: {}",
        rows, columns, values, agg_func
    ));

    // Collect the DataFrame to work with the data
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for pivot: {}", e);
            std::process::exit(1);
        }
    };

    // Validate columns exist
    let schema = collected_df.schema();
    for col in rows
        .iter()
        .chain(columns.iter())
        .chain(std::iter::once(&values.to_string()))
    {
        if !schema.iter_names().any(|s| s == col) {
            eprintln!(
                "Error: Column '{}' not found in DataFrame for pivot operation",
                col
            );
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
                "Unknown aggregation function '{}', using sum",
                agg_func
            ));
            value_col.sum()
        }
    };

    // Group by and aggregate
    let result = df
        .clone()
        .group_by(group_cols)
        .agg([agg_expr.alias(format!("{}_{}", values, agg_func))]);

    LogController::debug(&format!(
        "Pivot operation completed: {} rows, {} columns, {} values, {} aggregation",
        rows.len(),
        columns.len(),
        values,
        agg_func
    ));

    result
}
