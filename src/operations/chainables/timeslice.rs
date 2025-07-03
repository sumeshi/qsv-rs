use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn timeslice(
    df: &LazyFrame,
    time_column: &str,
    start_time: Option<&str>,
    end_time: Option<&str>,
) -> LazyFrame {
    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema for timeslice operation: {e}");
            std::process::exit(1);
        }
    };

    if !schema.iter_names().any(|s| s == time_column) {
        eprintln!(
            "Error: Time column '{time_column}' not found in DataFrame for timeslice operation"
        );
        std::process::exit(1);
    }

    LogController::debug(&format!(
        "Creating timeslice: column={time_column}, start={start_time:?}, end={end_time:?}"
    ));

    // Start with the original dataframe
    let mut result_df = df.clone();

    // Convert the time column to datetime for efficient comparison
    // Try multiple formats automatically with Polars
    let time_col_expr = col(time_column)
        .str()
        .to_datetime(
            Some(TimeUnit::Milliseconds),
            None,
            StrptimeOptions::default(),
            lit("raise"),
        )
        .alias("_temp_datetime");

    // Add the converted datetime column temporarily
    result_df = result_df.with_columns([time_col_expr]);

    // Apply start time filter if provided
    if let Some(start) = start_time {
        LogController::debug(&format!("Applying start time filter: {start}"));

        // Parse start time to timestamp
        let start_datetime = match parse_datetime_string(start) {
            Some(dt) => dt,
            None => {
                eprintln!("Error: Could not parse start time '{start}'");
                std::process::exit(1);
            }
        };

        let start_filter = col("_temp_datetime").gt_eq(lit(start_datetime));
        result_df = result_df.filter(start_filter);
    }

    // Apply end time filter if provided
    if let Some(end) = end_time {
        LogController::debug(&format!("Applying end time filter: {end}"));

        // Parse end time to timestamp
        let end_datetime = match parse_datetime_string(end) {
            Some(dt) => dt,
            None => {
                eprintln!("Error: Could not parse end time '{end}'");
                std::process::exit(1);
            }
        };

        let end_filter = col("_temp_datetime").lt_eq(lit(end_datetime));
        result_df = result_df.filter(end_filter);
    }

    // Remove the temporary datetime column
    let original_columns: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
    result_df.select([cols(original_columns)])
}

fn parse_datetime_string(time_str: &str) -> Option<i64> {
    use chrono::NaiveDateTime;

    // Try multiple datetime formats
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d/%b/%Y:%H:%M:%S", // Apache log format
        "%Y-%m-%d",
        "%H:%M:%S",
    ];

    for format in &formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(time_str, format) {
            return Some(dt.and_utc().timestamp_millis());
        }
    }

    // Try parsing as timestamp
    if let Ok(timestamp) = time_str.parse::<i64>() {
        return Some(timestamp * 1000); // Convert to milliseconds
    }

    None
}
