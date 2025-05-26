use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn tail(df: &LazyFrame, n: usize) -> LazyFrame {
    LogController::debug(&format!("Applying tail: n={}", n));
    
    // Check DataFrame size (only if necessary for some logic, Polars' tail handles large n gracefully)
    // For basic tail, direct Polars method is fine.
    df.clone().tail(n as u32)
}