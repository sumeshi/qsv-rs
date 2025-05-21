use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn show(df: &LazyFrame) {
    LogController::debug("Showing DataFrame as CSV");
    
    // LazyFrameを具体化
    let mut df_collected = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame: {}", e);
            return;
        }
    };
    
    // CSVとして表示（可変参照を使用）
    match CsvWriter::new(std::io::stdout())
        .has_header(true)
        .with_delimiter(b',')
        .finish(&mut df_collected) {
        Ok(_) => {},
        Err(e) => eprintln!("Error writing CSV to stdout: {}", e),
    }
}