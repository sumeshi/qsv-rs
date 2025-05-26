use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn uniq(df: &LazyFrame, colnames_opt: Option<&[String]>) -> LazyFrame {
    LogController::debug(&format!("Applying uniq: colnames={:?}", colnames_opt));

    let subset: Option<Vec<String>> = match colnames_opt {
        Some(names) => {
            let collected_df = match df.clone().collect() {
                Ok(df) => df,
                Err(e) => {
                    eprintln!("Error collecting DataFrame for schema check in uniq: {}", e);
                    std::process::exit(1);
                }
            };
            let schema = collected_df.schema();
            for colname in names {
                if !schema.iter_names().any(|s| s == colname) {
                    eprintln!("Error: Column '{}' not found in DataFrame for uniq operation", colname);
                    std::process::exit(1);
                }
            }
            LogController::debug(&format!("Removing duplicates based on columns: [{}]", 
                names.join(", ")
            ));
            Some(names.to_vec())
        }
        None => {
            LogController::debug("Removing duplicates based on all columns.");
            None
        }
    };
    
    // If subset is None in Polars unique, all columns are targeted
    // Convert Vec<String> to Vec<PlSmallStr> for unique_stable
    let subset_plsmallstr: Option<Vec<PlSmallStr>> = subset.map(|s| s.into_iter().map(PlSmallStr::from_string).collect());
    df.clone().unique_stable(subset_plsmallstr, UniqueKeepStrategy::First)
}