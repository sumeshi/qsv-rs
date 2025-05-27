use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn select(df: &LazyFrame, colnames: &[String]) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for schema check in select: {}", e);
            std::process::exit(1);
        }
    };
    let schema = collected_df.schema();

    for colname in colnames {
        if !schema.iter_names().any(|s| s == colname) {
            eprintln!("Error: Column '{}' not found in DataFrame for select operation", colname);
            std::process::exit(1);
        }
    }
    
    let existing_cols: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
    let mut selected_cols: Vec<Expr> = Vec::new();

    for name in colnames {
        if existing_cols.contains(name) {
            selected_cols.push(col(name));
        } else {
            LogController::warn(&format!("Column '{}' not found in DataFrame.", name));
        }
    }

    if selected_cols.is_empty() {
        LogController::warn("No valid columns selected. Returning original DataFrame.");
        return df.clone();
    }
    
    df.clone().select(&selected_cols)
}