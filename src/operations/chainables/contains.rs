use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn contains(df: &LazyFrame, colname: &str, pattern: &str, ignorecase: bool) -> LazyFrame {
    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema for contains operation: {e}");
            std::process::exit(1);
        }
    };

    if !schema.iter_names().any(|s| s == colname) {
        eprintln!("Error: Column '{colname}' not found in DataFrame for contains operation");
        std::process::exit(1);
    }

    LogController::debug(&format!(
        "Applying contains: column={colname} pattern='{pattern}' ignorecase={ignorecase}"
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
                        opt_val.is_some_and(|val| {
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
