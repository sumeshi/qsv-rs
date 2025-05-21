use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn grep(df: &LazyFrame, pattern: &str, ignorecase: bool) -> LazyFrame {
    LogController::debug(&format!("Filtering rows where any column matches pattern '{}' (case-insensitive: {})", 
        pattern, ignorecase
    ));
    
    // スキーマからすべての列名を取得
    let schema = match df.schema() {
        Ok(schema) => schema,
        Err(e) => {
            eprintln!("Error getting schema: {}", e);
            return df.clone();
        }
    };
    
    // 各列に対して文字列パターン検索のフィルタを適用
    let mut expr_list = Vec::new();

    for (name, dtype) in schema.iter() {
        // 文字列型の列に対してのみ適用
        if matches!(dtype, DataType::Utf8) {
            // 各反復で新しい変数を作成して所有権の問題を回避
            let pattern_str = pattern.to_string();
            let ignore = ignorecase;
            
            // 列ごとにパターンを検索するUDFを適用
            let col_expr = col(name.as_ref()).cast(DataType::Utf8).map(move |s| {
                let ca = s.utf8()?;
                let pattern_inner = pattern_str.clone();
                
                let result: Vec<bool> = ca.into_iter()
                    .map(|opt_s| {
                        opt_s.map(|s| {
                            if s.is_empty() {
                                false
                            } else if ignore {
                                s.to_lowercase().contains(&pattern_inner.to_lowercase())
                            } else {
                                s.contains(&pattern_inner)
                            }
                        }).unwrap_or(false)
                    })
                    .collect();
                    
                Ok(Some(Series::new(s.name(), result)))
            }, GetOutput::from_type(DataType::Boolean));
            
            expr_list.push(col_expr);
        }
    }
    
    // すべての列のORフィルタを適用
    if expr_list.is_empty() {
        // 文字列列がない場合は元のデータフレームをそのまま返す
        df.clone()
    } else {
        // 最初の条件
        let mut combined_expr = expr_list.remove(0);
        
        // 残りの条件をORで結合
        for expr in expr_list {
            combined_expr = combined_expr.or(expr);
        }
        
        df.clone().filter(combined_expr)
    }
}