use crate::controllers::log::LogController;
use polars::prelude::{col, len, Expr, LazyFrame, SortMultipleOptions};

pub fn count(df: &LazyFrame) -> LazyFrame {
    LogController::debug("Applying count");

    let collected_df_for_schema = match df.clone().collect() {
        Ok(d) => d,
        Err(e) => {
            LogController::error(&format!("Failed to collect DataFrame for schema in count: {}. Returning original LazyFrame.", e));
            return df.clone();
        }
    };

    let schema_ref = collected_df_for_schema.schema();
    let all_colnames: Vec<String> = schema_ref.iter_names().map(|s| s.to_string()).collect();

    df.clone()
        .group_by(all_colnames.iter().map(col).collect::<Vec<Expr>>())
        .agg([len().alias("count")])
        .sort(["count"], SortMultipleOptions::default().with_order_descending(true))
}
