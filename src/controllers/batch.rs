use crate::controllers::log::LogController;
use polars::prelude::*;

pub const MIN_BATCH_SIZE_ROWS: usize = 1000; // Minimum 1K rows per batch
pub const MAX_BATCH_SIZE_ROWS: usize = 1_000_000; // Maximum 1M rows per batch

/// Calculate optimal batch size based on memory target and data characteristics
pub fn calculate_batch_size(
    df: &LazyFrame,
    target_bytes: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    // Sample a small batch to estimate row size
    let sample_size = 100;
    let sample = df.clone().limit(sample_size).collect()?;

    if sample.height() == 0 {
        return Ok(MIN_BATCH_SIZE_ROWS);
    }

    // Estimate bytes per row
    let estimated_bytes_per_row = estimate_row_size(&sample)?;

    if estimated_bytes_per_row == 0 {
        return Ok(MAX_BATCH_SIZE_ROWS); // Avoid division by zero if rows are empty
    }

    // Calculate batch size to fit target memory
    let calculated_batch_size = target_bytes / estimated_bytes_per_row;

    // Clamp to reasonable bounds
    let batch_size = calculated_batch_size.clamp(MIN_BATCH_SIZE_ROWS, MAX_BATCH_SIZE_ROWS);

    LogController::debug(&format!(
        "Estimated {estimated_bytes_per_row} bytes per row, using batch size: {batch_size} rows"
    ));

    Ok(batch_size)
}

/// Estimate the size of a row in bytes
pub fn estimate_row_size(sample: &DataFrame) -> Result<usize, Box<dyn std::error::Error>> {
    let mut total_size = 0;
    let height = sample.height();

    if height == 0 {
        return Ok(0);
    }

    for column in sample.get_columns() {
        let column_size = match column.dtype() {
            DataType::String => {
                // For strings, estimate based on actual string lengths
                if let Ok(str_column) = column.str() {
                    str_column
                        .iter()
                        .map(|opt_str| opt_str.map_or(0, |s| s.len()))
                        .sum::<usize>()
                } else {
                    0 // If casting to string fails, assume 0 size for this column
                }
            }
            DataType::Int8 | DataType::UInt8 => height,
            DataType::Int16 | DataType::UInt16 => height * 2,
            DataType::Int32 | DataType::UInt32 | DataType::Float32 => height * 4,
            DataType::Int64 | DataType::UInt64 | DataType::Float64 => height * 8,
            DataType::Date => height * 4,
            DataType::Datetime(_, _) => height * 8,
            DataType::Time => height * 8,
            DataType::Boolean => height,
            _ => height * 8, // Default assumption for other types
        };
        total_size += column_size;
    }

    // Add some overhead for DataFrame structure
    total_size += height * 8; // Row overhead

    Ok(total_size / height) // Average bytes per row
}
