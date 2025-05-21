use polars::prelude::*;
use comfy_table::{Table, Cell};
use comfy_table::presets::UTF8_FULL;
use crate::controllers::log::LogController;

pub fn headers(df: &LazyFrame, plain: bool) {
    // スキーマを取得（clone()を使用して所有権問題を解決）
    let schema = match df.clone().schema() {
        Ok(schema) => schema,
        Err(e) => {
            eprintln!("Error getting schema: {}", e);
            return;
        }
    };
    
    // SmartStringをStringに変換
    let column_names: Vec<String> = schema.iter()
        .map(|(name, _)| name.to_string())
        .collect();
    
    LogController::debug(&format!("Showing headers: {} columns", column_names.len()));
    
    if plain {
        // プレーンテキスト形式での表示
        for (i, name) in column_names.iter().enumerate() {
            println!("{}: {}", i, name);
        }
    } else {
        // テーブル形式での表示
        let mut table = Table::new();
        table.load_preset(UTF8_FULL);
        table.set_header(vec!["#", "Column Name"]);
        
        for (i, name) in column_names.iter().enumerate() {
            table.add_row(vec![
                Cell::new(format!("{:02}", i)),
                Cell::new(name),
            ]);
        }
        
        println!("{}", table);
    }
}