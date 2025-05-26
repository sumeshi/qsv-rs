use polars::prelude::*;
use regex::Regex;
use crate::controllers::log::LogController;

pub fn grep(df: &LazyFrame, pattern: &str, ignorecase: bool, is_inverted: bool) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for grep: {}", e);
            return df.clone(); // Return original LazyFrame on error
        }
    };
    let all_column_names: Vec<String> = collected_df.schema().iter_names().map(|s| s.to_string()).collect();
    
    LogController::debug(&format!(
        "Filtering rows where any column {} pattern '{}' (case-insensitive: {})",
        if is_inverted { "does not match" } else { "matches" },
        pattern,
        ignorecase
    ));

    let re_pattern = if ignorecase {
        format!("(?i){}", pattern)
    } else {
        pattern.to_string()
    };

    let re = match Regex::new(&re_pattern) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: Invalid regex pattern '{}': {}", pattern, e);
            std::process::exit(1);
        }
    };

    let mut expr_list: Vec<Expr> = Vec::new();

    for colname in all_column_names.iter() {
        let re_clone = re.clone();
        let expr = col(colname)
            .cast(DataType::String) // Cast to String first
            .map(move |s_col: Column| { // s_col is polars_plan::dsl::Series (alias for polars_core::series::Series)
                let ca = s_col.str()?; // Column itself should have .str() if it's a Series alias
                let series_bool: Series = ca.into_iter().map(|opt_s| {
                    opt_s.map_or(false, |text| re_clone.is_match(text))
                }).collect::<ChunkedArray<BooleanType>>().into_series();
                Ok(Some(series_bool.into())) // Added .into() to convert Series to Column
            }, GetOutput::from_type(DataType::Boolean))
            .alias(&format!("{}_matches_pattern", colname));
        expr_list.push(expr);
    }

    if expr_list.is_empty() {
        return df.clone(); // No columns to filter on, return original
    }

    // Combine filter expressions using OR logic
    // any_horizontal is replaced by folding with OR
    let combined_filter_expr = expr_list.into_iter().reduce(|acc, expr| acc.or(expr)).unwrap();

    if is_inverted {
        df.clone().filter(combined_filter_expr.not())
    } else {
        df.clone().filter(combined_filter_expr)
    }
}