use polars::prelude::*;
use comfy_table::{Table, Row, Cell, ContentArrangement};
use comfy_table::presets::UTF8_FULL;
use crate::controllers::log::LogController;

pub fn showtable(df: &LazyFrame) {
    LogController::debug("Applying showtable (display DataFrame as a formatted table)");
    
    // Materialize DataFrame (use clone() to resolve ownership issues)
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame: {}", e);
            return;
        }
    };
    
    // Get row and column information
    let shape = collected_df.shape();
    let colnames: Vec<String> = collected_df.get_column_names_owned().into_iter().map(|s| s.to_string()).collect();
    
    // Create table
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    
    // Add header row
    let header_cells: Vec<Cell> = colnames.iter().map(|name| Cell::new(name)).collect();
    table.set_header(header_cells);
    
    // Add rows
    for row_idx in 0..std::cmp::min(shape.0, 20) {  // Display up to 20 rows
        let mut row_cells = Vec::new();
        for col_name in &colnames {
            // Get Series then get value
            let s = collected_df.column(col_name).unwrap();
            let val_result = s.get(row_idx);
            let cell_content = match val_result {
                Ok(val) => format!("{:?}", val), // Display Result<AnyValue, _> in debug format
                Err(_) => "Error".to_string(), // Should not happen if row_idx is valid
            };
            row_cells.push(Cell::new(cell_content));
        }
        table.add_row(row_cells);
    }
    
    // Display
    println!("{}", table);
    
    // If more than 20 rows, display a message indicating truncation
    if shape.0 > 20 {
        println!("... (showing first 20 of {} rows)", shape.0);
    }
}