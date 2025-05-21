use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn head(df: &LazyFrame, number: usize) -> LazyFrame {
    LogController::debug(&format!("Showing the first {} rows", number));
    
    // dfを複製して所有権の問題を解決
    df.clone().limit(number as u32)
}