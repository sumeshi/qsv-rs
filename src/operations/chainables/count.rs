use polars::prelude::{LazyFrame, Expr, col, len};
// use polars::lazy::dsl; // Removed unused import
use crate::controllers::log::LogController;
// No specific Schema import needed if using SchemaRef directly

pub fn count(df: &LazyFrame) -> LazyFrame {
    LogController::debug("Applying count");
    
    let collected_df_for_schema = match df.clone().collect() {
        Ok(d) => d,
        Err(e) => {
            LogController::error(&format!("Failed to collect DataFrame for schema in count: {}. Returning original LazyFrame.", e));
            return df.clone(); // Return original on error
        }
    };
    // Get SchemaRef (Arc<Schema>) directly
    let schema_ref = collected_df_for_schema.schema(); 

    // Iterate over names directly from SchemaRef
    let all_colnames: Vec<String> = schema_ref.iter_names().map(|s| s.to_string()).collect();

    // Group by all columns and count
    df.clone()
        .group_by(all_colnames.iter().map(|s| col(s)).collect::<Vec<Expr>>()) // Group by all columns
        .agg([
            // Use polars::prelude::len() to count rows and alias as "count"
            len().alias("count"),
        ])
}