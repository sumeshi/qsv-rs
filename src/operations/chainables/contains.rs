use polars::prelude::*;
use crate::controllers::log::LogController;
use crate::controllers::dataframe::exists_colname;

pub fn contains(df: &LazyFrame, colname: &str, pattern: &str, ignorecase: bool) -> LazyFrame {
    if !exists_colname(df, &[colname.to_string()]) {
        eprintln!("Error: Column '{}' not found in DataFrame", colname);
        std::process::exit(1);
    }
    
    LogController::debug(&format!("Filtering rows where '{}' column matches pattern '{}' (case-insensitive: {})", 
        colname, pattern, ignorecase
    ));
    
    // カスタムUDFを使用してパターンマッチを実装
    let pattern_str = pattern.to_string();
    let contains_fn = move |s: &str| -> bool {
        if s.is_empty() {
            return false;
        }
        
        if ignorecase {
            s.to_lowercase().contains(&pattern_str.to_lowercase())
        } else {
            s.contains(&pattern_str)
        }
    };
    
    // UDFをExprに変換
    let expr = col(colname).cast(DataType::Utf8).map(move |s| {
        let ca = s.utf8()?;
        let result: Vec<bool> = ca.into_iter()
            .map(|opt_s| opt_s.map(|s| contains_fn(s)).unwrap_or(false))
            .collect();
        Ok(Some(Series::new(s.name(), result)))
    }, GetOutput::from_type(DataType::Boolean));
    
    df.clone().filter(expr)
}