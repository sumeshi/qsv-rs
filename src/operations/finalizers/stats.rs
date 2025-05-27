use crate::controllers::log::LogController;
use comfy_table::{presets::UTF8_FULL, Cell, Color, Table};
use polars::prelude::*;

pub fn stats(df: &LazyFrame) {
    LogController::debug("Calculating statistics for DataFrame manually");

    let df_collected = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame: {}", e);
            return;
        }
    };

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);

    let column_names = df_collected.get_column_names();
    let mut header_cells = vec![Cell::new("Statistic").fg(Color::Green)];
    for name in column_names.iter() {
        header_cells.push(Cell::new(name).fg(Color::Green));
    }
    table.set_header(header_cells);

    let height = df_collected.height();
    let mut count_row = vec![Cell::new("count")];
    let mut null_count_row = vec![Cell::new("null_count")];
    let mut mean_row = vec![Cell::new("mean")];
    let mut std_row = vec![Cell::new("std")];
    let mut min_row = vec![Cell::new("min")];
    let mut p25_row = vec![Cell::new("25%")];
    let mut p50_row = vec![Cell::new("50% (median)")];
    let mut p75_row = vec![Cell::new("75%")];
    let mut max_row = vec![Cell::new("max")];
    let mut dtype_row = vec![Cell::new("datatype")];

    for col_name in column_names {
        let series = df_collected.column(col_name).unwrap();

        count_row.push(Cell::new(height));
        null_count_row.push(Cell::new(series.null_count()));
        dtype_row.push(Cell::new(series.dtype().to_string()));

        if matches!(
            series.dtype(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
        ) {
            // Polars upcasts all numeric types to Float64 for these operations or requires it
            // We'll try to cast to f64, if it fails for some numeric types (like i128), then skip numeric stats
            let s_f64 = series.cast(&DataType::Float64);

            if let Ok(s_f64) = s_f64 {
                if let Ok(ca) = s_f64.f64() {
                    mean_row.push(Cell::new(
                        ca.mean()
                            .map_or_else(|| "-".to_string(), |v| format!("{:.4}", v)),
                    ));
                    std_row.push(Cell::new(
                        ca.std(1)
                            .map_or_else(|| "-".to_string(), |v| format!("{:.4}", v)),
                    ));
                    min_row.push(Cell::new(
                        ca.min()
                            .map_or_else(|| "-".to_string(), |v| format!("{}", v)),
                    ));

                    let quantiles = [0.25, 0.50, 0.75];
                    let interpolated = polars::prelude::QuantileMethod::Linear;

                    // Call quantile_reduce for each percentile
                    if let Ok(scalar_25) = s_f64.quantile_reduce(quantiles[0], interpolated) {
                        p25_row.push(Cell::new(
                            scalar_25
                                .value()
                                .extract::<f64>()
                                .map_or_else(|| "-".to_string(), |v| format!("{:.4}", v)),
                        ));
                    } else {
                        p25_row.push(Cell::new("-"));
                    }
                    if let Ok(scalar_50) = s_f64.quantile_reduce(quantiles[1], interpolated) {
                        p50_row.push(Cell::new(
                            scalar_50
                                .value()
                                .extract::<f64>()
                                .map_or_else(|| "-".to_string(), |v| format!("{:.4}", v)),
                        ));
                    } else {
                        p50_row.push(Cell::new("-"));
                    }
                    if let Ok(scalar_75) = s_f64.quantile_reduce(quantiles[2], interpolated) {
                        p75_row.push(Cell::new(
                            scalar_75
                                .value()
                                .extract::<f64>()
                                .map_or_else(|| "-".to_string(), |v| format!("{:.4}", v)),
                        ));
                    } else {
                        p75_row.push(Cell::new("-"));
                    }

                    max_row.push(Cell::new(
                        ca.max()
                            .map_or_else(|| "-".to_string(), |v| format!("{}", v)),
                    ));
                } else {
                    mean_row.push(Cell::new("-"));
                    std_row.push(Cell::new("-"));
                    min_row.push(Cell::new("-"));
                    p25_row.push(Cell::new("-"));
                    p50_row.push(Cell::new("-"));
                    p75_row.push(Cell::new("-"));
                    max_row.push(Cell::new("-"));
                }
            } else {
                mean_row.push(Cell::new("-"));
                std_row.push(Cell::new("-"));
                min_row.push(Cell::new("-"));
                p25_row.push(Cell::new("-"));
                p50_row.push(Cell::new("-"));
                p75_row.push(Cell::new("-"));
                max_row.push(Cell::new("-"));
            }
        } else {
            // For non-numeric types
            mean_row.push(Cell::new("-"));
            std_row.push(Cell::new("-"));
            if series.dtype() == &DataType::String {
                if let Ok(ca_str) = series.str() {
                    min_row.push(Cell::new(
                        ca_str
                            .into_iter()
                            .min()
                            .flatten()
                            .map_or_else(|| "-".to_string(), |v| v.to_string()),
                    ));
                    max_row.push(Cell::new(
                        ca_str
                            .into_iter()
                            .max()
                            .flatten()
                            .map_or_else(|| "-".to_string(), |v| v.to_string()),
                    ));
                } else {
                    min_row.push(Cell::new("-"));
                    max_row.push(Cell::new("-"));
                }
            } else {
                min_row.push(Cell::new("-"));
                max_row.push(Cell::new("-"));
            }
            p25_row.push(Cell::new("-"));
            p50_row.push(Cell::new("-"));
            p75_row.push(Cell::new("-"));
        }
    }

    table.add_row(count_row);
    table.add_row(null_count_row);
    table.add_row(dtype_row);
    table.add_row(mean_row);
    table.add_row(std_row);
    table.add_row(min_row);
    table.add_row(p25_row);
    table.add_row(p50_row);
    table.add_row(p75_row);
    table.add_row(max_row);

    println!("{}", table);
}
