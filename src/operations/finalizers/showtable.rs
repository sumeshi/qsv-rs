use crate::controllers::log::LogController;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};
use polars::prelude::*;

const MAX_DISPLAY_ROWS: usize = 8;

pub fn showtable(df: &LazyFrame) {
    LogController::debug("Applying showtable (display DataFrame as a formatted table)");

    // Try to estimate the size using limit + head approach to avoid full collection
    let head_df = match df.clone().limit((MAX_DISPLAY_ROWS + 1) as u32).collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame for showtable: {e}");
            return;
        }
    };

    let is_truncated = head_df.height() > MAX_DISPLAY_ROWS;
    let display_df = if is_truncated {
        // If we have more rows than display limit, take only the first MAX_DISPLAY_ROWS
        head_df.slice(0, MAX_DISPLAY_ROWS)
    } else {
        head_df
    };

    let shape = display_df.shape();
    let colnames: Vec<String> = display_df
        .get_column_names_owned()
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    // Display table size information
    if is_truncated {
        println!(
            "shape: ({}+, {}) [showing first {} rows]",
            shape.0, shape.1, MAX_DISPLAY_ROWS
        );
    } else {
        println!("shape: ({}, {})", shape.0, shape.1);
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    let header_cells: Vec<Cell> = colnames.iter().map(Cell::new).collect();
    table.set_header(header_cells);

    // Add data rows
    for row_idx in 0..shape.0 {
        let mut row_cells = Vec::new();
        for col_name in &colnames {
            let s = display_df.column(col_name).unwrap();
            let val_result = s.get(row_idx);
            let cell_content = match val_result {
                Ok(val) => format_anyvalue(&val),
                Err(_) => "Error".to_string(),
            };
            row_cells.push(Cell::new(cell_content));
        }
        table.add_row(row_cells);
    }

    // Add truncation indicator if needed
    if is_truncated {
        let mut truncation_row = Vec::new();
        for _ in &colnames {
            truncation_row.push(Cell::new("⋮"));
        }
        table.add_row(truncation_row);
    }

    println!("{table}");
}

fn format_anyvalue(val: &AnyValue) -> String {
    match val {
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
    }
}
