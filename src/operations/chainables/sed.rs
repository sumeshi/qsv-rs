use polars::prelude::*;
use crate::controllers::log::LogController;
use crate::controllers::dataframe::exists_colname;

pub fn sed(df: &LazyFrame, colname: &str, pattern: &str, replacement: &str, ignorecase: bool) -> LazyFrame {
    if !exists_colname(df, &[colname.to_string()]) {
        eprintln!("Error: Column '{}' not found in DataFrame", colname);
        std::process::exit(1);
    }
    
    LogController::debug(&format!("Replacing values in '{}' column using pattern '{}' -> '{}' (case-insensitive: {})", 
        colname, pattern, replacement, ignorecase
    ));
    
    // カスタムUDFを使用して文字列置換を実装
    let pattern_str = pattern.to_string();
    let replacement_str = replacement.to_string();
    
    let replace_fn = move |s: &str| -> String {
        if s.is_empty() {
            return String::new();
        }
        
        if ignorecase {
            // 大文字小文字を区別しない置換の実装
            let s_lower = s.to_lowercase();
            let pattern_lower = pattern_str.to_lowercase();
            
            // 元の文字列内でのパターン位置を検出
            let mut result = s.to_string();
            let mut start_pos = 0;
            
            while let Some(pos) = s_lower[start_pos..].find(&pattern_lower) {
                let actual_pos = start_pos + pos;
                let matched_text = &s[actual_pos..actual_pos + pattern_str.len()];
                result = result.replacen(matched_text, &replacement_str, 1);
                start_pos = actual_pos + replacement_str.len();
                
                if start_pos >= s.len() {
                    break;
                }
            }
            
            result
        } else {
            // 標準の置換
            s.replace(&pattern_str, &replacement_str)
        }
    };
    
    // UDFをExprに変換
    let replace_expr = col(colname).cast(DataType::Utf8).map(move |s| {
        let ca = s.utf8()?;
        let result: Vec<String> = ca.into_iter()
            .map(|opt_s| opt_s.map(|s| replace_fn(s)).unwrap_or_default())
            .collect();
        Ok(Some(Series::new(s.name(), result)))
    }, GetOutput::from_type(DataType::Utf8));
    
    // 他の列はそのままで、指定された列のみ置換
    df.clone().with_column(replace_expr.alias(colname))
}