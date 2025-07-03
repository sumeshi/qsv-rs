use crate::controllers::log::LogController;
use chrono;
use polars::prelude::*;
use std::fs::File;
use std::path::PathBuf;

pub fn dumpcache(df: &LazyFrame, output_path_opt: Option<&str>) {
    LogController::debug("Applying dumpcache (save DataFrame as parquet cache)");

    let output_path = if let Some(path_str) = output_path_opt {
        PathBuf::from(path_str)
    } else {
        // Default: save in current directory with readable timestamp
        let now = chrono::Local::now();
        let timestamp = now.format("%Y%m%d_%H%M%S").to_string();
        PathBuf::from(format!("cache_{timestamp}.parquet"))
    };

    // Ensure the output path has .parquet extension
    let final_path = if output_path.extension().is_none()
        || output_path.extension().unwrap_or_default() != "parquet"
    {
        output_path.with_extension("parquet")
    } else {
        output_path
    };

    LogController::debug(&format!(
        "Saving DataFrame cache to: {}",
        final_path.display()
    ));

    let _file = match File::create(&final_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "Error: Failed to create cache file '{}': {}",
                final_path.display(),
                e
            );
            return;
        }
    };

    // Use streaming sink_parquet to write in batches
    match df.clone().sink_parquet(
        SinkTarget::Path(final_path.clone().into()),
        ParquetWriteOptions {
            compression: ParquetCompression::Snappy,
            statistics: StatisticsOptions::default(),
            row_group_size: None,
            data_page_size: None,
            key_value_metadata: None,
            field_overwrites: vec![],
        },
        None,               // CloudOptions
        Default::default(), // SinkOptions
    ) {
        Ok(_) => {
            LogController::info(&format!(
                "DataFrame cache saved successfully to: {}",
                final_path.display()
            ));
        }
        Err(e) => {
            eprintln!(
                "Error writing parquet cache to file '{}': {}",
                final_path.display(),
                e
            );
        }
    }
}
