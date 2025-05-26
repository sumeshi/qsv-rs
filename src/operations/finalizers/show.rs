use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn show(df: &LazyFrame) {
    LogController::debug("Applying show (print as CSV)");
    
    // Materialize LazyFrame
    let mut df_collected = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame: {}", e);
            return;
        }
    };
    
    // Display as CSV (using mutable reference)
    if let Err(e) = CsvWriter::new(std::io::stdout())
        .include_header(true)
        .with_separator(b',')
        .finish(&mut df_collected)
    {
        eprintln!("Error writing CSV to stdout: {}", e);
    }
}

#[allow(dead_code)]
fn write_stdout(mut df: DataFrame, no_header: bool, pretty: bool) -> PolarsResult<()> {
    if pretty {
        // For pretty print, use the DataFrame's fmt::Display capabilities
        println!("{}", df);
    } else {
        // For standard CSV output to stdout
        let mut writer = CsvWriter::new(std::io::stdout())
            .include_header(!no_header);
        writer.finish(&mut df)?;
    }
    Ok(())
}