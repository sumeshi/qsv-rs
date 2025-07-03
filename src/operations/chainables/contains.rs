use crate::controllers::log::LogController;
use polars::prelude::*;
use regex;

pub fn contains(df: &LazyFrame, colname: &str, pattern: &str, ignorecase: bool) -> LazyFrame {
    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema for contains operation: {e}");
            std::process::exit(1);
        }
    };

    if !schema.iter_names().any(|s| s == colname) {
        eprintln!("Error: Column '{colname}' not found in DataFrame for contains operation");
        std::process::exit(1);
    }

    LogController::debug(&format!(
        "Applying contains: column={colname} pattern='{pattern}' ignorecase={ignorecase}"
    ));

    // Use Polars' native string operations for better performance
    let expr = if ignorecase {
        // For case-insensitive search, use regex with (?i) flag
        let pattern_regex = format!("(?i){}", regex::escape(pattern));
        col(colname)
            .cast(DataType::String)
            .str()
            .contains(lit(pattern_regex), false) // literal=false for regex
    } else {
        // For case-sensitive search, use literal contains
        col(colname)
            .cast(DataType::String)
            .str()
            .contains(lit(pattern), true) // literal=true for exact string match
    };

    df.clone().filter(expr)
}
