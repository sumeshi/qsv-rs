use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn sort(df: &LazyFrame, colnames: &[String], desc: bool) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for schema check in sort: {}", e);
            std::process::exit(1);
        }
    };
    let schema = collected_df.schema();

    for colname in colnames {
        if !schema.iter_names().any(|s| s == colname) {
            eprintln!(
                "Error: Column '{}' not found in DataFrame for sort operation",
                colname
            );
            std::process::exit(1);
        }
    }

    LogController::debug(&format!(
        "Sorting by columns: {:?}, descending: {}",
        colnames, desc
    ));

    let sort_exprs: Vec<Expr> = colnames.iter().map(|name| col(name)).collect();

    if sort_exprs.is_empty() {
        LogController::warn("No columns specified for sorting.");
        return df.clone();
    }

    let options = SortMultipleOptions {
        descending: vec![desc; sort_exprs.len()],
        nulls_last: vec![false; sort_exprs.len()],
        multithreaded: true,
        maintain_order: false,
        limit: None,
    };

    df.clone().sort_by_exprs(sort_exprs, options)
}
