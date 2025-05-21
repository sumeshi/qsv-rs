use polars::prelude::*;
use std::fs::File;
use std::time::SystemTime;
use crate::controllers::log::LogController;

pub fn dump(df: &LazyFrame, path: Option<&str>) -> () {
    // 出力用のパスを決定
    let output_path = match path {
        Some(p) => p.to_string(),
        None => {
            // デフォルトのファイル名を現在時刻で生成
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            format!("{}_output.csv", now)
        }
    };
    
    LogController::debug(&format!("Dumping results to {}", output_path));
    
    // LazyFrameを具体化（cloneを追加して所有権問題を解決）
    let mut df_collected = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame: {}", e);
            return;
        }
    };
    
    // CSVファイルに書き出し（可変参照を使用）
    match File::create(&output_path) {
        Ok(file) => {
            match CsvWriter::new(file)
                .has_header(true)
                .with_delimiter(b',')
                .finish(&mut df_collected) {
                Ok(_) => println!("Successfully wrote results to {}", output_path),
                Err(e) => eprintln!("Error writing CSV file: {}", e),
            }
        }
        Err(e) => eprintln!("Error creating file {}: {}", output_path, e),
    }
}