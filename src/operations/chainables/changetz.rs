use polars::prelude::*;
use chrono::{NaiveDateTime, Utc, Local};
use chrono::TimeZone as ChronoTimeZone;
use chrono_tz::Tz;

use crate::controllers::log::LogController;
// use crate::controllers::dataframe::exists_colname; // Removed

// Helper function to parse a string to NaiveDateTime with multiple formats
fn parse_datetime_multiple_formats(s: &str, format_str: &str, colname: &str) -> NaiveDateTime {
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
    LogController::error(&format!("Failed to parse date '{}' in column '{}' with any format, using current time", s, colname));
    Utc::now().naive_utc()
}

// Main time conversion logic
fn time_conversion(
    s_val: &str, // Changed to &str
    from_tz_str: &str,
    to_tz_str: &str,
    colname: &str, // Added colname for logging
    format_str: &str,
    ambiguous_time_str: &str,
) -> String {
    if s_val.is_empty() {
        return String::new();
    }

    let from_tz: Option<Tz> = if from_tz_str.to_lowercase() == "local" {
        None // Represent local time by None, to use Local.from_local_datetime
    } else {
        match from_tz_str.parse::<Tz>() {
            Ok(tz) => Some(tz),
            Err(_) => {
                LogController::error(&format!("Invalid 'from' timezone string: '{}' in column '{}'", from_tz_str, colname));
                return s_val.to_string(); // Return original if timezone is invalid
            }
        }
    };

    let to_tz = match to_tz_str.parse::<Tz>() {
        Ok(tz) => tz,
        Err(_) => {
            LogController::error(&format!("Invalid 'to' timezone string: '{}' in column '{}'", to_tz_str, colname));
            return s_val.to_string(); // Return original if timezone is invalid
        }
    };

    let naive_dt = parse_datetime_multiple_formats(s_val, format_str, colname);

    let localized_dt_utc: chrono::DateTime<Utc> = if let Some(tz) = from_tz {
        match ChronoTimeZone::from_local_datetime(&tz, &naive_dt) {
            chrono::LocalResult::Single(dt) => dt.with_timezone(&Utc),
            chrono::LocalResult::Ambiguous(dt1, dt2) => {
                LogController::warn(&format!(
                    "Ambiguous local time '{}' in column '{}'. Could be {} or {}. Using '{}' strategy.",
                    naive_dt, colname, dt1, dt2, ambiguous_time_str
                ));
                if ambiguous_time_str == "earliest" { dt1.with_timezone(&Utc) } else { dt2.with_timezone(&Utc) }
            }
            chrono::LocalResult::None => {
                LogController::error(&format!("Non-existent local time '{}' for timezone '{}' in column '{}'", naive_dt, tz, colname));
                return s_val.to_string();
            }
        }
    } else {
        match Local.from_local_datetime(&naive_dt) {
            chrono::LocalResult::Single(dt) => dt.with_timezone(&Utc),
            chrono::LocalResult::Ambiguous(dt1, dt2) => {
                 LogController::warn(&format!(
                    "Ambiguous local time '{}' in column '{}'. Could be {} or {}. Using '{}' strategy.",
                    naive_dt, colname, dt1, dt2, ambiguous_time_str
                ));
                if ambiguous_time_str == "earliest" { dt1.with_timezone(&Utc) } else { dt2.with_timezone(&Utc) }
            }
            chrono::LocalResult::None => {
                LogController::error(&format!("Non-existent local time '{}' for local system in column '{}'", naive_dt, colname));
                return s_val.to_string();
            }
        }
    };

    localized_dt_utc.with_timezone(&to_tz).format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

pub fn changetz(
    df: &LazyFrame,
    colname: &str,
    from_tz: &str,
    to_tz: &str,
    format: &str,
    ambiguous_time: &str,
) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for schema check in changetz: {}", e);
            std::process::exit(1);
        }
    };
    let schema = collected_df.schema();

    if !schema.iter_names().any(|s| s == colname) {
        eprintln!("Error: Column '{}' not found in DataFrame for changetz operation", colname);
        std::process::exit(1);
    }
    
    // Debug information
    LogController::info(&format!("Attempting to change timezone for column {} from {} to {}", colname, from_tz, to_tz));
    
    // Parse source and target timezones
    let from_tz_clone = from_tz.to_string();
    let to_tz_clone = to_tz.to_string();
    let colname_clone = colname.to_string();
    let format_clone = format.to_string();
    let ambiguous_time_clone = ambiguous_time.to_string();
    
    LogController::debug(&format!(
        "Changing timezone of '{}' column from '{}' to '{}', format: '{}', ambiguous: '{}'", 
        colname, from_tz, to_tz, format, ambiguous_time
    ));
    
    // Create UDF and apply to DataFrame
    let timezone_udf = move |s_col: Column| -> PolarsResult<Option<Column>> {
        let s = s_col;
        let s_str_values_result: PolarsResult<StringChunked> = match s.dtype() {
            DataType::String => Ok(s.str()?.clone()),
            DataType::Date => {
                // Convert Date type to NaiveDateTime then to string
                let s_datetime = s.date()?.cast(&DataType::Datetime(TimeUnit::Milliseconds, None))?;
                s_datetime.datetime()?.strftime("%Y-%m-%d %H:%M:%S%.3f")
            },
            DataType::Datetime(_, _) => {
                s.datetime()?.strftime("%Y-%m-%d %H:%M:%S%.3f")
            },
            _ => return Ok(Some(s.clone())), // s is Column, return as is
        };

        let s_str_values = match s_str_values_result {
            Ok(ca) => ca,
            Err(e) => return Err(e),
        };

        let mut new_values: Vec<Option<String>> = Vec::with_capacity(s_str_values.len());

        for opt_val in s_str_values.into_iter() { // opt_val is Option<&str>
            let result = opt_val.map_or_else(
                || String::new(), // Handle None case
                |val_str| {
                    time_conversion(
                        val_str,
                        &from_tz_clone,
                        &to_tz_clone,
                        &colname_clone,
                        &format_clone,
                        &ambiguous_time_clone,
                    )
                }
            );
            new_values.push(Some(result));
        }
        
        // Log sample converted time
        if !new_values.is_empty() && new_values[0].is_some() {
            LogController::debug(&format!("Sample converted time: {}", new_values[0].as_ref().unwrap()));
        }
        
        Ok(Some(Series::new(s.name().clone(), new_values).into())) // Convert Series to Column with .into()
    };
    
    // Clone df to resolve ownership issues
    df.clone().with_column(col(colname).map(timezone_udf, GetOutput::from_type(DataType::String)).alias(colname))
}