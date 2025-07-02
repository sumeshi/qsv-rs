use crate::controllers::log::LogController;
use polars::prelude::*;
use std::fs::File;
use std::path::PathBuf;
pub fn dump(df: &LazyFrame, output_path_str: &str, separator: char) {
    LogController::debug(&format!("Dumping DataFrame to CSV: {output_path_str}"));
    let output_path = PathBuf::from(output_path_str);
    let mut df_collected = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame for dumping: {e}");
            return;
        }
    };
    let file = match File::create(&output_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "Error: Failed to create file '{}': {}",
                output_path.display(),
                e
            );
            return;
        }
    };
    match CsvWriter::new(file)
        .include_header(true)
        .with_separator(separator as u8)
        .finish(&mut df_collected)
    {
        Ok(_) => (),
        Err(e) => eprintln!(
            "Error writing CSV to file '{}': {}",
            output_path.display(),
            e
        ),
    }
}
