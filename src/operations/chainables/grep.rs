use polars::prelude::*;
use crate::controllers::log::LogController;
use regex::{RegexBuilder};

pub fn grep(df: &LazyFrame, pattern: &str, ignorecase: bool) -> LazyFrame {
    LogController::debug(&format!("Filtering rows where any column (treated as string) matches regex pattern '{}' (case-insensitive: {})",
        pattern, ignorecase
    ));

    let schema = match df.schema() {
        Ok(schema) => schema,
        Err(e) => {
            eprintln!("Error getting schema: {}", e);
            return df.clone();
        }
    };

    let re = match RegexBuilder::new(pattern)
        .case_insensitive(ignorecase)
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Invalid regex pattern: '{}'. Error: {}", pattern, e);
            return df.clone();
        }
    };

    let mut expr_list: Vec<Expr> = Vec::new();

    // Iterate over all columns, cast them to Utf8, then apply the regex UDF
    for (name, _dtype) in schema.iter() { // _dtype is not used in the condition anymore
        let re_clone = re.clone();
        let col_expr = col(name.as_ref())
            .cast(DataType::Utf8) // Cast to Utf8 first
            .map(
                move |s: Series| {
                    let ca = s.utf8()?; 
                    let re_inner = re_clone.clone();

                    let result: BooleanChunked = ca
                        .into_iter()
                        .map(|opt_s| {
                            opt_s.map_or(false, |s_val| {
                                if s_val.is_empty() {
                                    false
                                } else {
                                    re_inner.is_match(s_val)
                                }
                            })
                        })
                        .collect();
                    
                    Ok(Some(result.into_series()))
                },
                GetOutput::from_type(DataType::Boolean),
            );
        
        expr_list.push(col_expr);
    }
    
    if expr_list.is_empty() {
        // This case should ideally not be hit if there's at least one column in the CSV.
        // If schema.iter() is empty, df.clone() is fine.
        LogController::debug("No columns found in schema to grep.");
        df.clone()
    } else {
        let combined_filter_expr = any_horizontal(expr_list);
        df.clone().filter(combined_filter_expr)
    }
}