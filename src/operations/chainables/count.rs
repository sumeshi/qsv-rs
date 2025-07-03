use crate::controllers::log::LogController;
use polars::prelude::{col, len, Expr, LazyFrame, SortMultipleOptions};

pub fn count(df: &LazyFrame) -> LazyFrame {
    LogController::debug("Applying count");

    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            LogController::error(&format!(
                "Failed to get schema for count: {e}. Returning original LazyFrame."
            ));
            return df.clone();
        }
    };

    let all_colnames: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();

    df.clone()
        .group_by(all_colnames.iter().map(col).collect::<Vec<Expr>>())
        .agg([len().alias("count")])
        .sort(
            ["count"],
            SortMultipleOptions::default().with_order_descending(true),
        )
}
