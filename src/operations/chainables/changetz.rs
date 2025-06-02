use chrono::TimeZone as ChronoTimeZone;
use chrono::{Local, NaiveDateTime, Utc};
use chrono_tz::Tz;
use polars::prelude::*;

// Helper function to parse a string to NaiveDateTime with multiple formats
fn parse_datetime_multiple_formats(s: &str, format_str: &str, _colname: &str) -> NaiveDateTime {
    let formats = if format_str == "auto" {
        vec![
            "%Y-%m-%d %H:%M:%S%.f", // Nanoseconds, microseconds, or milliseconds
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.f", // ISO 8601 with T
            "%Y-%m-%dT%H:%M:%S",
            "%Y/%m/%d %H:%M:%S%.f",
            "%Y/%m/%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S%.f",
            "%m/%d/%Y %H:%M:%S",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%m/%d/%Y",
        ]
    } else {
        vec![format_str]
    };

    for fmt in formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return dt;
        }
    }
    // Log error with the original string value that failed to parse
    // LogController::error(&format!(
    //     "Failed to parse date '{}' in column '{}' with any format, using current time",
    //     s, colname
    // ));
    Utc::now().naive_utc()
}

// Main time conversion logic
fn time_conversion(
    s_val: &str, // Changed to &str
    from_tz_str: &str,
    to_tz_str: &str,
    colname: &str, // Added colname for logging
    input_format_str: &str,
    output_format_str: &str,
    ambiguous_time_str: &str,
) -> String {
    if s_val.is_empty() {
        return String::new();
    }

    let from_tz: Option<Tz> = if from_tz_str.to_lowercase() == "local" {
        None // Represent local time by None, to use Local.from_local_datetime
    } else {
        // Timezone is already validated in changetz function, so unwrap is safe
        Some(from_tz_str.parse::<Tz>().unwrap())
    };

    // Timezone is already validated in changetz function, so unwrap is safe
    let to_tz = to_tz_str.parse::<Tz>().unwrap();

    let naive_dt = parse_datetime_multiple_formats(s_val, input_format_str, colname);

    let localized_dt_utc: chrono::DateTime<Utc> = if let Some(tz) = from_tz {
        match ChronoTimeZone::from_local_datetime(&tz, &naive_dt) {
            chrono::LocalResult::Single(dt) => dt.with_timezone(&Utc),
            chrono::LocalResult::Ambiguous(dt1, dt2) => {
                // LogController::warn(&format!(
                //     "Ambiguous local time '{}' in column '{}'. Could be {} or {}. Using '{}' strategy.",
                //     naive_dt, colname, dt1, dt2, ambiguous_time_str
                // ));
                if ambiguous_time_str == "earliest" {
                    dt1.with_timezone(&Utc)
                } else {
                    dt2.with_timezone(&Utc)
                }
            }
            chrono::LocalResult::None => {
                // LogController::error(&format!(
                //     "Non-existent local time '{}' for timezone '{}' in column '{}'",
                //     naive_dt, tz, colname
                // ));
                return s_val.to_string();
            }
        }
    } else {
        match Local.from_local_datetime(&naive_dt) {
            chrono::LocalResult::Single(dt) => dt.with_timezone(&Utc),
            chrono::LocalResult::Ambiguous(dt1, dt2) => {
                // LogController::warn(&format!(
                //     "Ambiguous local time '{}' in column '{}'. Could be {} or {}. Using '{}' strategy.",
                //     naive_dt, colname, dt1, dt2, ambiguous_time_str
                // ));
                if ambiguous_time_str == "earliest" {
                    dt1.with_timezone(&Utc)
                } else {
                    dt2.with_timezone(&Utc)
                }
            }
            chrono::LocalResult::None => {
                // LogController::error(&format!(
                //     "Non-existent local time '{}' for local system in column '{}'",
                //     naive_dt, colname
                // ));
                return s_val.to_string();
            }
        }
    };

    let final_format = if output_format_str == "auto" {
        "%Y-%m-%dT%H:%M:%S%.6f%:z" // ISO8601 format with microsecond precision (Windows compatible)
    } else {
        output_format_str
    };

    localized_dt_utc
        .with_timezone(&to_tz)
        .format(final_format)
        .to_string()
}

pub fn changetz(
    df: &LazyFrame,
    colname: &str,
    from_tz: &str,
    to_tz: &str,
    input_format: &str,
    output_format: &str,
    ambiguous_time: &str,
) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!(
                "Error collecting DataFrame for schema check in changetz: {}",
                e
            );
            std::process::exit(1);
        }
    };
    let schema = collected_df.schema();

    if !schema.iter_names().any(|s| s == colname) {
        eprintln!(
            "Error: Column '{}' not found in DataFrame for changetz operation",
            colname
        );
        std::process::exit(1);
    }

    // Validate timezones early to fail fast
    if from_tz.to_lowercase() != "local" {
        if let Err(_) = from_tz.parse::<Tz>() {
            eprintln!(
                "Error: Invalid source timezone '{}' in changetz operation",
                from_tz
            );
            std::process::exit(1);
        }
    }
    
    if let Err(_) = to_tz.parse::<Tz>() {
        eprintln!(
            "Error: Invalid target timezone '{}' in changetz operation", 
            to_tz
        );
        std::process::exit(1);
    }

    // Debug information
    // LogController::info(&format!(
    //     "Attempting to change timezone for column {} from {} to {}",
    //     colname, from_tz, to_tz
    // ));

    // Parse source and target timezones
    let from_tz_clone = from_tz.to_string();
    let to_tz_clone = to_tz.to_string();
    let colname_clone = colname.to_string();
    let input_format_clone = input_format.to_string();
    let output_format_clone = output_format.to_string();
    let ambiguous_time_clone = ambiguous_time.to_string();

    // LogController::debug(&format!(
    //     "Changing timezone of '{}' column from '{}' to '{}', input_format: '{}', output_format: '{}', ambiguous: '{}'",
    //     colname, from_tz, to_tz, input_format, output_format, ambiguous_time
    // ));

    // Create UDF and apply to DataFrame
    let timezone_udf = move |s_col: Column| -> PolarsResult<Option<Column>> {
        let s = s_col;
        let s_str_values_result: PolarsResult<StringChunked> = match s.dtype() {
            DataType::String => Ok(s.str()?.clone()),
            DataType::Date => {
                // Convert Date type to NaiveDateTime then to string
                let s_datetime = s
                    .date()?
                    .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))?;
                s_datetime.datetime()?.strftime("%Y-%m-%dT%H:%M:%S%.7f")
            }
            DataType::Datetime(_, _) => s.datetime()?.strftime("%Y-%m-%dT%H:%M:%S%.7f"),
            _ => return Ok(Some(s.clone())), // s is Column, return as is
        };

        let s_str_values = s_str_values_result?;

        let mut new_values: Vec<Option<String>> = Vec::with_capacity(s_str_values.len());

        for opt_val in s_str_values.into_iter() {
            // opt_val is Option<&str>
            let result = opt_val.map_or_else(
                String::new, // Handle None case
                |val_str| {
                    time_conversion(
                        val_str,
                        &from_tz_clone,
                        &to_tz_clone,
                        &colname_clone,
                        &input_format_clone,
                        &output_format_clone,
                        &ambiguous_time_clone,
                    )
                },
            );
            new_values.push(Some(result));
        }

        // Log sample converted time
        if !new_values.is_empty() && new_values[0].is_some() {
            // LogController::debug(&format!(
            //     "Sample converted time: {}",
            //     new_values[0].as_ref().unwrap()
            // ));
        }

        Ok(Some(Series::new(s.name().clone(), new_values).into())) // Convert Series to Column with .into()
    };

    df.clone().with_column(
        col(colname)
            .map(timezone_udf, GetOutput::from_type(DataType::String))
            .alias(colname),
    )
}
