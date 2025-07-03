use crate::controllers::log::LogController;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};
use polars::prelude::*;

const MAX_DISPLAY_ROWS: usize = 8;

pub fn showtable(df: &LazyFrame) {
    LogController::debug("Applying showtable (display DataFrame as a formatted table)");

    // Estimate total rows to avoid collecting a huge DataFrame unnecessarily
    let total_rows = match df.clone().select([len().alias("count")]).collect() {
        Ok(count_df) => count_df
            .column("count")
            .unwrap()
            .get(0)
            .unwrap()
            .try_extract()
            .unwrap_or(0),
        Err(_) => 0, // Fallback to assuming a small number of rows
    };

    let collected_df = if total_rows > MAX_DISPLAY_ROWS {
        // If the frame is large, collect only the head and tail
        let head_df = df.clone().limit(3).collect().unwrap();
        let _tail_df = df.clone().tail(3).collect().unwrap();
        // This is a simplified approach. For a true head/tail view, we'd need to handle
        // the display logic differently. For now, we'll just show the head to be safe.
        // A more robust solution would involve custom table drawing logic.
        head_df
    } else {
        // If the frame is small, collect the whole thing
        match df.clone().collect() {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error: Failed to collect DataFrame: {e}");
                return;
            }
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

    let show_truncation = total_rows > MAX_DISPLAY_ROWS;

    // Add first rows
    for row_idx in 0..shape.0 {
        let mut row_cells = Vec::new();
        for col_name in &colnames {
            let s = collected_df.column(col_name).unwrap();
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
    if show_truncation {
        let mut truncation_row = Vec::new();
        for _ in &colnames {
            truncation_row.push(Cell::new("..."));
        }
        table.add_row(truncation_row);
        // In a more advanced version, we would fetch and display the tail rows here.
        // For now, we've only collected the head.
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
