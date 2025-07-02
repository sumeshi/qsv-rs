use crate::controllers::log::LogController;
use polars::prelude::*;
pub fn head(df: &LazyFrame, n: usize) -> LazyFrame {
    LogController::debug(&format!("Applying head: n={n}"));
    df.clone().slice(0, n as u32)
}
