use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn sed(df: &LazyFrame, colname: &str, pattern: &str, replacement: &str, ignorecase: bool) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for schema check in sed: {}", e);
            std::process::exit(1);
        }
    };
    let schema = collected_df.schema();

    if !schema.iter_names().any(|s| s == colname) {
        eprintln!("Error: Column '{}' not found in DataFrame for sed operation", colname);
        std::process::exit(1);
    }
    
    LogController::debug(&format!("Replacing values in '{}' column using regex pattern '{}' -> '{}' (case-insensitive: {})", 
        colname, pattern, replacement, ignorecase
    ));
    
    let final_pattern = if ignorecase {
        format!("(?i){}", pattern) // Prepend (?i) flag for case-insensitivity
    } else {
        pattern.to_string()
    };

    // Assuming Polars version has been updated to a recent one (e.g., 0.38+)
    // where replace_all(pattern_expr, replacement_expr, literal: bool) is available.
    // For regex, literal must be false.
    let replace_expr = col(colname)
        .cast(DataType::String) // Ensure the column is String
        .str()
        .replace_all(lit(final_pattern), lit(replacement.to_string()), false) // literal: false for regex
        .alias(colname);
    
    df.clone().with_column(replace_expr)
}