use polars::prelude::*;
use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use std::str::FromStr;

use crate::controllers::log::LogController;
use crate::controllers::dataframe::exists_colname;

pub fn changetz(df: &LazyFrame, colname: &str, tz_from: &str, tz_to: &str, dt_format: Option<&str>) -> LazyFrame {
    if !exists_colname(df, &[colname.to_string()]) {
        eprintln!("Error: Column '{}' not found in DataFrame", colname);
        std::process::exit(1);
    }
    
    // Debug information
    LogController::info(&format!("Attempting to change timezone for column {} from {} to {}", colname, tz_from, tz_to));
    
    // Parse source and target timezones
    let source_tz = match Tz::from_str(tz_from) {
        Ok(tz) => tz,
        Err(_) => {
            eprintln!("Error: Invalid source timezone '{}'", tz_from);
            std::process::exit(1);
        }
    };
    
    let target_tz = match Tz::from_str(tz_to) {
        Ok(tz) => tz,
        Err(_) => {
            eprintln!("Error: Invalid target timezone '{}'", tz_to);
            std::process::exit(1);
        }
    };
    
    LogController::debug(&format!(
        "Changing timezone of '{}' column from {} to {}", 
        colname, tz_from, tz_to
    ));
    
    // Clone dt_format to avoid 'static lifetime issues
    let dt_format_owned: Option<String> = dt_format.map(|s| s.to_string());
    
    // Create a function to process date strings
    let time_conversion = move |s: &str| -> String {
        if s.is_empty() {
            return String::new();
        }
        
        // Log input string
        LogController::debug(&format!("Processing date string: '{}'", s));
        
        // Parse date
        let naive_dt = if let Some(fmt) = &dt_format_owned {
            // If format is specified, use it to parse
            LogController::debug(&format!("Parsing with specified format: '{}'", fmt));
            
            match NaiveDateTime::parse_from_str(s, fmt) {
                Ok(dt) => dt,
                Err(e) => {
                    LogController::debug(&format!("Failed with specified format, trying default formats: {}", e));
                    // If parsing fails with specified format, try standard formats
                    parse_with_standard_formats(s)
                }
            }
        } else {
            // If no format is specified, try standard formats
            parse_with_standard_formats(s)
        };
        
        // Apply timezone and convert to target timezone
        let dt_utc = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);
        let dt_with_source_tz = dt_utc.with_timezone(&source_tz);
        let converted = dt_with_source_tz.with_timezone(&target_tz);
        
        // Format and return
        if let Some(fmt) = &dt_format_owned {
            // Use output format
            let result = converted.format(fmt).to_string();
            LogController::debug(&format!("Formatted result: '{}'", result));
            result
        } else {
            // Use default format
            let result = converted.format("%Y-%m-%d %H:%M:%S %Z").to_string();
            LogController::debug(&format!("Default formatted result: '{}'", result));
            result
        }
    };
    
    // Standard format parsing function
    fn parse_with_standard_formats(s: &str) -> NaiveDateTime {
        // Japanese format used in CSV (YYYY/MM/DD HH:MM:SS)
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S") {
            LogController::debug(&format!("Parsed with YYYY/MM/DD HH:MM:SS"));
            return dt;
        }
        
        // ISO 8601 format
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
            return dt;
        }
        
        // General date format
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return dt;
        }
        
        // American format
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%m/%d/%Y %H:%M:%S") {
            return dt;
        }
        
        // Short time format
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M") {
            return dt;
        }
        
        // Final resort
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%m/%d/%Y %H:%M") {
            return dt;
        }
        
        // If no format can parse the date, display error message and return current time
        LogController::error(&format!("Failed to parse date '{}' with any format, using current time", s));
        Utc::now().naive_utc()
    }
    
    // Create UDF and apply to DataFrame
    let timezone_udf = move |s: Series| -> PolarsResult<Option<Series>> {
        // Get UTF8 data with utf8 method
        let ca = s.utf8()?;
        let result: Vec<String> = ca.into_iter()
            .map(|opt_s| opt_s.map(|s| time_conversion(s)).unwrap_or_default())
            .collect();
        
        // Log sample converted time
        if !result.is_empty() {
            LogController::debug(&format!("Sample converted time: {}", result[0]));
        }
        
        Ok(Some(Series::new(s.name(), result)))
    };
    
    // Clone df to resolve ownership issues
    df.clone().with_column(col(colname).map(timezone_udf, GetOutput::from_type(DataType::Utf8)).alias(colname))
}