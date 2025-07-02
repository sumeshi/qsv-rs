use polars::prelude::*;
pub fn timeround(
    df: &LazyFrame,
    colname: &str,
    unit: &str,
    output_colname: Option<&str>,
) -> LazyFrame {
    // Convert unit shorthand to polars duration format and determine output format
    let (duration, format) = match unit {
        "y" | "year" => ("1y", "%Y"),
        "M" | "month" => ("1mo", "%Y-%m"),
        "d" | "day" => ("1d", "%Y-%m-%d"),
        "h" | "hour" => ("1h", "%Y-%m-%d %H"),
        "m" | "minute" => ("1m", "%Y-%m-%d %H:%M"),
        "s" | "second" => ("1s", "%Y-%m-%d %H:%M:%S"),
        _ => {
            eprintln!("Error: Invalid time unit '{unit}'. Use: y/year, M/month, d/day, h/hour, m/minute, s/second");
            std::process::exit(1);
        }
    };
    let output_col = output_colname.unwrap_or(colname);
    df.clone().with_columns([col(colname)
        .str()
        .to_datetime(
            Some(TimeUnit::Microseconds),
            None,
            StrptimeOptions {
                format: None, // Auto-detect format
                ..Default::default()
            },
            lit("raise"),
        )
        .dt()
        .truncate(lit(duration))
        .dt()
        .to_string(format)
        .alias(output_col)])
}
