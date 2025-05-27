use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn tail(df: &LazyFrame, n: usize) -> LazyFrame {
    LogController::debug(&format!("Applying tail: n={}", n));
    
    df.clone().tail(n as u32)
}