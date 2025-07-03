use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn grep(df: &LazyFrame, pattern: &str, ignorecase: bool, is_inverted: bool) -> LazyFrame {
    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema for grep operation: {e}");
            std::process::exit(1);
        }
    };

    let all_column_names: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();

    LogController::debug(&format!(
        "Applying grep: pattern='{pattern}', ignorecase={ignorecase}, invert={is_inverted}"
    ));

    let final_pattern = if ignorecase {
        format!("(?i){pattern}")
    } else {
        pattern.to_string()
    };

    // Create a single filter expression that checks all string columns
    // Use reference to avoid cloning the pattern for each column
    let pattern_lit = lit(final_pattern);
    let filter_expr = all_column_names
        .iter()
        .map(|col_name| {
            col(col_name)
                .cast(DataType::String)
                .str()
                .contains(pattern_lit.clone(), false) // literal=false for regex
                .fill_null(lit(false))
        })
        .reduce(|acc, expr| acc.or(expr))
        .unwrap_or_else(|| lit(false));

    if is_inverted {
        df.clone().filter(filter_expr.not())
    } else {
        df.clone().filter(filter_expr)
    }
}
