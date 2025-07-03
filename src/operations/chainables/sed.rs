use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn sed(
    df: &LazyFrame,
    colname: Option<&str>,
    pattern: &str,
    replacement: &str,
    ignorecase: bool,
) -> LazyFrame {
    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema for sed operation: {e}");
            std::process::exit(1);
        }
    };

    let final_pattern = if ignorecase {
        format!("(?i){pattern}") // Prepend (?i) flag for case-insensitivity
    } else {
        pattern.to_string()
    };

    match colname {
        Some(col) => {
            // Apply sed to specific column
            if !schema.iter_names().any(|s| s == col) {
                eprintln!("Error: Column '{col}' not found in DataFrame for sed operation");
                std::process::exit(1);
            }
            LogController::debug(&format!(
                "Replacing values in '{col}' column using regex pattern '{pattern}' -> '{replacement}' (case-insensitive: {ignorecase})"
            ));
            let replace_expr = polars::prelude::col(col)
                .cast(DataType::String) // Ensure the column is String
                .str()
                .replace_all(lit(final_pattern), lit(replacement.to_string()), false) // literal: false for regex
                .alias(col);
            df.clone().with_column(replace_expr)
        }
        None => {
            // Apply sed to all columns
            LogController::debug(&format!(
                "Replacing values in all columns using regex pattern '{pattern}' -> '{replacement}' (case-insensitive: {ignorecase})"
            ));
            let mut result_df = df.clone();
            // Apply replacement to all columns
            for (column_name, _) in schema.iter() {
                let col_str = column_name.as_str();
                let replace_expr = polars::prelude::col(col_str)
                    .cast(DataType::String) // Ensure the column is String
                    .str()
                    .replace_all(
                        lit(final_pattern.clone()),
                        lit(replacement.to_string()),
                        false,
                    ) // literal: false for regex
                    .alias(col_str);
                result_df = result_df.with_column(replace_expr);
            }
            result_df
        }
    }
}
