use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn isin(df: &LazyFrame, colname: &str, values: &[String]) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for schema check in isin: {}", e);
            std::process::exit(1);
        }
    };
    let schema = collected_df.schema();

    if !schema.iter_names().any(|s| s == colname) {
        eprintln!("Error: Column '{}' not found in DataFrame for isin operation", colname);
        std::process::exit(1);
    }
    
    LogController::debug(&format!("Applying isin: column={} values={:?}", colname, values));
    
    let mut conditions: Vec<Expr> = Vec::new();

    for val_str in values {
        // Create a literal from the string value
        let lit_val = lit(val_str.clone());
        // Create an equality condition for the current value
        conditions.push(col(colname).eq(lit_val));
    }

    // Create a base condition (false literal) to OR with other conditions
    // Use a basic logical operation to create an "equals any of" condition
    let mut filter_expr = lit(false); 

    // Combine multiple equality conditions with OR operator
    for cond in conditions {
        filter_expr = filter_expr.or(cond);
    }

    // Create an initial false literal for the filter expression
    // Create an equality condition for each value and combine with OR
    df.clone().filter(filter_expr)
}