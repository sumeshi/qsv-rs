use polars::prelude::*;
use comfy_table::Table;
use comfy_table::presets::UTF8_FULL;
use crate::controllers::log::LogController;

pub fn stats(df: &LazyFrame) {
    LogController::debug("Calculating statistics for DataFrame");
    
    // LazyFrameを具体化（所有権問題を解決するためにclone()を追加）
    let df_collected = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame: {}", e);
            return;
        }
    };
    
    // 現在のPolarsのバージョンではdescribeメソッドが別の形式なので手動で統計情報を計算
    let shape = df_collected.shape();
    println!("shape: ({}, {})", shape.0, shape.1);
    
    // テーブルを作成して基本的な情報を表示
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    
    // ヘッダー行を設定
    let mut headers = vec!["Column", "Type", "Non-Null Count", "Null Count"];
    table.set_header(headers);
    
    // 各列の情報を追加
    for col_name in df_collected.get_column_names() {
        match df_collected.column(col_name) {
            Ok(series) => {
                let dtype = format!("{:?}", series.dtype());
                let non_null_count = series.len() - series.null_count();
                let null_count = series.null_count();
                
                table.add_row(vec![
                    col_name.to_string(),
                    dtype,
                    format!("{}", non_null_count),
                    format!("{}", null_count),
                ]);
            },
            Err(e) => {
                eprintln!("Error getting column {}: {}", col_name, e);
            }
        }
    }
    
    // 表示
    println!("{}", table);
}