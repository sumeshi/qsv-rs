use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn tail(df: &LazyFrame, number: usize) -> LazyFrame {
    LogController::debug(&format!("Selecting last {} rows", number));
    
    // データフレームのサイズを確認（必要な場合のみ）
    let _df_height = match df.clone().collect() {
        Ok(collected_df) => collected_df.height(),
        Err(_) => {
            eprintln!("Warning: Could not collect DataFrame to determine size");
            // clone()を使用して所有権問題を解決
            return df.clone().tail(number as u32);
        }
    };
    
    // デフォルトのtail操作を実行（clone()を使用して所有権問題を解決）
    df.clone().tail(number as u32)
}