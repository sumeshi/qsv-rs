use polars::prelude::*;
use comfy_table::Table;
use comfy_table::presets::UTF8_FULL;
use crate::controllers::log::LogController;

pub fn showtable(df: &LazyFrame) {
    // データフレームを具体化（clone()を使用して所有権問題を解決）
    let df_collected = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame: {}", e);
            return;
        }
    };
    
    // 行と列の情報を取得
    let shape = df_collected.shape();
    LogController::debug(&format!("Showing table with shape: ({}, {})", shape.0, shape.1));
    
    // テーブル作成
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    
    // ヘッダー行を追加
    let column_names = df_collected.get_column_names();
    table.set_header(&column_names);
    
    // 行を追加
    for row_idx in 0..std::cmp::min(shape.0, 20) {  // 最大20行まで表示
        let mut row_data = Vec::new();
        for col_idx in 0..shape.1 {
            // Series を取得してから値を取得
            let value = match df_collected.select_at_idx(col_idx) {
                Some(series) => {
                    match series.get(row_idx) {
                        Ok(val) => format!("{:?}", val), // Result<AnyValue, _>をデバッグ形式で表示
                        Err(_) => "N/A".to_string(),
                    }
                },
                None => "N/A".to_string(),
            };
            row_data.push(value);
        }
        table.add_row(row_data);
    }
    
    // 表示
    println!("shape: ({}, {})", shape.0, shape.1);
    println!("{}", table);
    
    // 20行を超える場合は省略メッセージを表示
    if shape.0 > 20 {
        println!("... {} more rows", shape.0 - 20);
    }
}