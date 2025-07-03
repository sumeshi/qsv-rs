use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn renamecol(df: &LazyFrame, old_colname: &str, new_colname: &str) -> LazyFrame {
    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema for renamecol operation: {e}");
            std::process::exit(1);
        }
    };

    if !schema.iter_names().any(|s| s == old_colname) {
        eprintln!("Error: Column '{old_colname}' not found in DataFrame for renamecol operation");
        std::process::exit(1);
    }

    LogController::debug(&format!(
        "Renaming column '{old_colname}' to '{new_colname}'"
    ));

    // Get all column names and replace the old one with the new one
    let all_columns: Vec<Expr> = schema
        .iter_names()
        .map(|name| {
            if name.as_str() == old_colname {
                col(old_colname).alias(new_colname)
            } else {
                col(name.as_str())
            }
        })
        .collect();

    df.clone().select(all_columns)
}
