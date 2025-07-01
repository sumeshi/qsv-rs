use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn tail(df: &LazyFrame, n: usize) -> LazyFrame {
    LogController::debug(&format!("Applying tail: n={n}"));

    df.clone().tail(n as u32)
}
