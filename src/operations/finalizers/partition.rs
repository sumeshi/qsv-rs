use crate::controllers::log::LogController;
use polars::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
pub fn partition(df: &LazyFrame, colname: &str, output_dir: &str) {
    // Collect the DataFrame to access the data
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for partition: {e}");
            std::process::exit(1);
        }
    };
    let schema = collected_df.schema();
    if !schema.iter_names().any(|s| s == colname) {
        eprintln!("Error: Column '{colname}' not found in DataFrame for partition operation");
        std::process::exit(1);
    }
    LogController::debug(&format!(
        "Partitioning data by column '{colname}' into directory '{output_dir}'"
    ));
    // Create output directory if it doesn't exist
    let output_path = Path::new(output_dir);
    if let Err(e) = fs::create_dir_all(output_path) {
        eprintln!("Error creating output directory '{output_dir}': {e}");
        std::process::exit(1);
    }
    // Get unique values in the partition column
    let unique_values = match collected_df
        .clone()
        .lazy()
        .select([col(colname)])
        .unique(None, UniqueKeepStrategy::First)
        .collect()
    {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error getting unique values for partition: {e}");
            std::process::exit(1);
        }
    };
    let partition_column = match unique_values.column(colname) {
        Ok(col) => col,
        Err(e) => {
            eprintln!("Error accessing partition column: {e}");
            std::process::exit(1);
        }
    };
    // Convert column values to strings for file naming
    let mut partition_values = Vec::new();
    for i in 0..partition_column.len() {
        let value = match partition_column.get(i) {
            Ok(any_value) => {
                // Convert AnyValue to string for file naming
                match any_value {
                    AnyValue::String(s) => s.to_string(),
                    AnyValue::Int32(i) => i.to_string(),
                    AnyValue::Int64(i) => i.to_string(),
                    AnyValue::Float32(f) => f.to_string(),
                    AnyValue::Float64(f) => f.to_string(),
                    AnyValue::Boolean(b) => b.to_string(),
                    AnyValue::Date(d) => d.to_string(),
                    AnyValue::Datetime(dt, _, _) => dt.to_string(),
                    AnyValue::Null => "null".to_string(),
                    _ => format!("{any_value:?}"),
                }
            }
            Err(e) => {
                LogController::warn(&format!("Error getting value at index {i}: {e}"));
                continue;
            }
        };
        partition_values.push(value);
    }
    LogController::info(&format!(
        "Found {} unique values in column '{}': {:?}",
        partition_values.len(),
        colname,
        partition_values
    ));
    // Create a file for each unique value
    let mut files_created = 0;
    for value in partition_values {
        // Sanitize filename (remove/replace invalid characters)
        let safe_filename = sanitize_filename(&value);
        let output_file = output_path.join(format!("{safe_filename}.csv"));
        // Filter data for this partition value
        let filtered_df = match collected_df
            .clone()
            .lazy()
            .filter(col(colname).cast(DataType::String).eq(lit(value.clone())))
            .collect()
        {
            Ok(df) => df,
            Err(e) => {
                LogController::error(&format!(
                    "Error filtering data for partition value '{value}': {e}"
                ));
                continue;
            }
        };
        // Write to CSV file
        match write_csv_file(&filtered_df, &output_file) {
            Ok(_) => {
                files_created += 1;
                LogController::info(&format!(
                    "Created partition file: {} ({} rows)",
                    output_file.display(),
                    filtered_df.height()
                ));
            }
            Err(e) => {
                LogController::error(&format!(
                    "Error writing partition file '{}': {}",
                    output_file.display(),
                    e
                ));
            }
        }
    }
    LogController::info(&format!(
        "Partition complete: {files_created} files created in '{output_dir}'"
    ));
}
fn sanitize_filename(filename: &str) -> String {
    // Replace invalid filename characters with underscores
    filename
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect::<String>()
        .trim()
        .to_string()
}
fn write_csv_file(df: &DataFrame, output_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    use polars::prelude::CsvWriter;
    use polars::prelude::SerWriter;
    use std::fs::File;
    let file = File::create(output_path)?;
    let mut df_clone = df.clone();
    CsvWriter::new(file)
        .include_header(true)
        .finish(&mut df_clone)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    Ok(())
}
