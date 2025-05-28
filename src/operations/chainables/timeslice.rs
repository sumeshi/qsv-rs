use crate::controllers::log::LogController;
use chrono::{DateTime, NaiveDateTime};
use polars::prelude::*;

pub fn timeslice(
    df: &LazyFrame,
    time_column: &str,
    start_time: Option<&str>,
    end_time: Option<&str>,
) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for timeslice: {}", e);
            std::process::exit(1);
        }
    };

    let schema = collected_df.schema();
    if !schema.iter_names().any(|s| s == time_column) {
        eprintln!(
            "Error: Time column '{}' not found in DataFrame for timeslice operation",
            time_column
        );
        std::process::exit(1);
    }

    LogController::debug(&format!(
        "Creating timeslice: column={}, start={:?}, end={:?}",
        time_column, start_time, end_time
    ));

    let mut filter_expr: Option<Expr> = None;

    // Add start time filter
    if let Some(start) = start_time {
        let start_owned = start.to_string(); // Clone the string
        let start_filter = col(time_column).cast(DataType::String).map(
            move |s_col: Column| {
                let ca = s_col.str()?;
                let mut results: Vec<Option<bool>> = Vec::new();

                for opt_time_str in ca.into_iter() {
                    if let Some(time_str) = opt_time_str {
                        let is_after_start = is_time_after_or_equal(time_str, &start_owned);
                        results.push(Some(is_after_start));
                    } else {
                        results.push(Some(false));
                    }
                }

                Ok(Some(Series::new("start_filter".into(), results).into()))
            },
            GetOutput::from_type(DataType::Boolean),
        );

        filter_expr = Some(match filter_expr {
            Some(existing) => existing.and(start_filter),
            None => start_filter,
        });
    }

    // Add end time filter
    if let Some(end) = end_time {
        let end_owned = end.to_string(); // Clone the string
        let end_filter = col(time_column).cast(DataType::String).map(
            move |s_col: Column| {
                let ca = s_col.str()?;
                let mut results: Vec<Option<bool>> = Vec::new();

                for opt_time_str in ca.into_iter() {
                    if let Some(time_str) = opt_time_str {
                        let is_before_end = is_time_before_or_equal(time_str, &end_owned);
                        results.push(Some(is_before_end));
                    } else {
                        results.push(Some(false));
                    }
                }

                Ok(Some(Series::new("end_filter".into(), results).into()))
            },
            GetOutput::from_type(DataType::Boolean),
        );

        filter_expr = Some(match filter_expr {
            Some(existing) => existing.and(end_filter),
            None => end_filter,
        });
    }

    match filter_expr {
        Some(expr) => df.clone().filter(expr),
        None => {
            LogController::debug("No time filters specified, returning original DataFrame");
            df.clone()
        }
    }
}

fn parse_time_string(time_str: &str) -> Option<NaiveDateTime> {
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
            return Some(dt);
        }
    }

    // Try parsing as timestamp
    if let Ok(timestamp) = time_str.parse::<i64>() {
        return DateTime::from_timestamp(timestamp, 0).map(|dt| dt.naive_utc());
    }

    None
}

fn is_time_after_or_equal(time_str: &str, reference_str: &str) -> bool {
    match (
        parse_time_string(time_str),
        parse_time_string(reference_str),
    ) {
        (Some(time), Some(reference)) => time >= reference,
        _ => false,
    }
}

fn is_time_before_or_equal(time_str: &str, reference_str: &str) -> bool {
    match (
        parse_time_string(time_str),
        parse_time_string(reference_str),
    ) {
        (Some(time), Some(reference)) => time <= reference,
        _ => false,
    }
}
