use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn sort(df: &LazyFrame, colnames: &[String], desc: bool) -> LazyFrame {
    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema for sort operation: {e}");
            std::process::exit(1);
        }
    };

    for colname in colnames {
        if !schema.iter_names().any(|s| s == colname) {
            eprintln!("Error: Column '{colname}' not found in DataFrame for sort operation");
            std::process::exit(1);
        }
    }

    LogController::debug(&format!(
        "Sorting by columns: {colnames:?}, descending: {desc}"
    ));

    let sort_exprs: Vec<Expr> = colnames.iter().map(col).collect();
    let sort_options = SortMultipleOptions::default().with_order_descending(desc);

    df.clone().sort_by_exprs(sort_exprs, sort_options)
}
