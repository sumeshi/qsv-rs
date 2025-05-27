use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn contains(df: &LazyFrame, colname: &str, pattern: &str, ignorecase: bool) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!(
                "Error collecting DataFrame for schema check in contains: {}",
                e
            );
            std::process::exit(1);
        }
    };
    let schema = collected_df.schema();

    if !schema.iter_names().any(|s| s == colname) {
        eprintln!(
            "Error: Column '{}' not found in DataFrame for contains operation",
            colname
        );
        std::process::exit(1);
    }

    LogController::debug(&format!(
        "Applying contains: column={} pattern='{}' ignorecase={}",
        colname, pattern, ignorecase
    ));

    // Consider case-insensitive option
    let pattern_to_use = if ignorecase {
        pattern.to_lowercase()
    } else {
        pattern.to_string()
    };

    let expr = col(colname)
        .cast(DataType::String)
        .map(
            move |s_col: Column| {
                let ca = s_col.str()?;
                let result_ca: ChunkedArray<BooleanType> = ca
                    .into_iter()
                    .map(|opt_val: Option<&str>| {
                        opt_val.map_or(false, |val| {
                            if ignorecase {
                                val.to_lowercase().contains(&pattern_to_use)
                            } else {
                                val.contains(&pattern_to_use)
                            }
                        })
                    })
                    .collect();
                Ok(Some(result_ca.into_series().into()))
            },
            GetOutput::from_type(DataType::Boolean),
        )
        .alias("contains_result");

    df.clone().filter(expr)
}
