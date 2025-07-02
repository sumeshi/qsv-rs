use crate::controllers::log::LogController;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};
use polars::prelude::*;
pub fn showtable(df: &LazyFrame) {
    LogController::debug("Applying showtable (display DataFrame as a formatted table)");
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame: {e}");
            return;
        }
    };
    let shape = collected_df.shape();
    let colnames: Vec<String> = collected_df
        .get_column_names_owned()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    // Display table size information like Python polars
    println!("shape: ({}, {})", shape.0, shape.1);
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    let header_cells: Vec<Cell> = colnames.iter().map(Cell::new).collect();
    table.set_header(header_cells);
    let max_display_rows = 8;
    let total_rows = shape.0;
    let show_truncation = total_rows >= max_display_rows;
    let rows_to_show = if show_truncation {
        // Show first 3 and last 3 rows when truncating
        std::cmp::min(3, total_rows)
    } else {
        total_rows
    };
    // Add first rows
    for row_idx in 0..rows_to_show {
        let mut row_cells = Vec::new();
        for col_name in &colnames {
            let s = collected_df.column(col_name).unwrap();
            let val_result = s.get(row_idx);
            let cell_content = match val_result {
                Ok(val) => match val {
                    AnyValue::Null => "null".to_string(),
                    AnyValue::Boolean(b) => b.to_string(),
                    AnyValue::String(s) => s.to_string(),
                    AnyValue::Int8(i) => i.to_string(),
                    AnyValue::Int16(i) => i.to_string(),
                    AnyValue::Int32(i) => i.to_string(),
                    AnyValue::Int64(i) => i.to_string(),
                    AnyValue::UInt8(i) => i.to_string(),
                    AnyValue::UInt16(i) => i.to_string(),
                    AnyValue::UInt32(i) => i.to_string(),
                    AnyValue::UInt64(i) => i.to_string(),
                    AnyValue::Float32(f) => f.to_string(),
                    AnyValue::Float64(f) => f.to_string(),
                    AnyValue::Date(d) => d.to_string(),
                    AnyValue::Datetime(dt, _, _) => dt.to_string(),
                    AnyValue::Time(t) => t.to_string(),
                    AnyValue::Duration(d, _) => d.to_string(),
                    _ => format!("{val}"),
                },
                Err(_) => "Error".to_string(),
            };
            row_cells.push(Cell::new(cell_content));
        }
        table.add_row(row_cells);
    }
    // Add truncation indicator if needed
    if show_truncation {
        let mut truncation_row = Vec::new();
        for _ in &colnames {
            truncation_row.push(Cell::new("…"));
        }
        table.add_row(truncation_row);
        // Add last rows
        let last_rows_start = total_rows - std::cmp::min(3, total_rows - rows_to_show);
        for row_idx in last_rows_start..total_rows {
            let mut row_cells = Vec::new();
            for col_name in &colnames {
                let s = collected_df.column(col_name).unwrap();
                let val_result = s.get(row_idx);
                let cell_content = match val_result {
                    Ok(val) => match val {
                        AnyValue::Null => "null".to_string(),
                        AnyValue::Boolean(b) => b.to_string(),
                        AnyValue::String(s) => s.to_string(),
                        AnyValue::Int8(i) => i.to_string(),
                        AnyValue::Int16(i) => i.to_string(),
                        AnyValue::Int32(i) => i.to_string(),
                        AnyValue::Int64(i) => i.to_string(),
                        AnyValue::UInt8(i) => i.to_string(),
                        AnyValue::UInt16(i) => i.to_string(),
                        AnyValue::UInt32(i) => i.to_string(),
                        AnyValue::UInt64(i) => i.to_string(),
                        AnyValue::Float32(f) => f.to_string(),
                        AnyValue::Float64(f) => f.to_string(),
                        AnyValue::Date(d) => d.to_string(),
                        AnyValue::Datetime(dt, _, _) => dt.to_string(),
                        AnyValue::Time(t) => t.to_string(),
                        AnyValue::Duration(d, _) => d.to_string(),
                        _ => format!("{val}"),
                    },
                    Err(_) => "Error".to_string(),
                };
                row_cells.push(Cell::new(cell_content));
            }
            table.add_row(row_cells);
        }
    }
    println!("{table}");
}
