use crate::controllers::log::LogController;
use polars::prelude::*;
pub fn uniq(df: &LazyFrame) -> LazyFrame {
    LogController::debug("Applying uniq - removing duplicates based on all columns");
    df.clone().unique_stable(None, UniqueKeepStrategy::First)
}
