use crate::controllers::log::LogController;
use polars::prelude::*;
pub fn show(df: &LazyFrame) {
    LogController::debug("Applying show (print as CSV)");
    let mut df_collected = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame: {e}");
            return;
        }
    };
    if let Err(e) = CsvWriter::new(std::io::stdout())
        .include_header(true)
        .with_separator(b',')
        .finish(&mut df_collected)
    {
        eprintln!("Error writing CSV to stdout: {e}");
    }
}
