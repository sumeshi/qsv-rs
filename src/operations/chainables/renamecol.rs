use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn renamecol(df: &LazyFrame, old_colname: &str, new_colname: &str) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for schema check in renamecol: {}", e);
            std::process::exit(1);
        }
    };
    let schema = collected_df.schema();

    if !schema.iter_names().any(|s| s == old_colname) {
        eprintln!("Error: Column '{}' not found in DataFrame for renamecol operation", old_colname);
        std::process::exit(1);
    }
    
    LogController::debug(&format!("Renaming column '{}' to '{}'", old_colname, new_colname));
    
    // Polars 0.48.1 rename signature: existing: impl IntoVec<PlSmallStr>, new: impl IntoVec<PlSmallStr>
    df.clone().rename([old_colname], [new_colname], true)
}