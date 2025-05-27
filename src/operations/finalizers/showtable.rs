use polars::prelude::*;
use comfy_table::{Table, Cell, ContentArrangement};
use comfy_table::presets::UTF8_FULL;
use crate::controllers::log::LogController;

pub fn showtable(df: &LazyFrame) {
    LogController::debug("Applying showtable (display DataFrame as a formatted table)");
    
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error: Failed to collect DataFrame: {}", e);
            return;
        }
    };
    
    let shape = collected_df.shape();
    let colnames: Vec<String> = collected_df.get_column_names_owned().into_iter().map(|s| s.to_string()).collect();
    
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    
    let header_cells: Vec<Cell> = colnames.iter().map(|name| Cell::new(name)).collect();
    table.set_header(header_cells);
    
    for row_idx in 0..std::cmp::min(shape.0, 20) {
        let mut row_cells = Vec::new();
        for col_name in &colnames {
            let s = collected_df.column(col_name).unwrap();
            let val_result = s.get(row_idx);
            let cell_content = match val_result {
                Ok(val) => format!("{:?}", val),
                Err(_) => "Error".to_string(),
            };
            row_cells.push(Cell::new(cell_content));
        }
        table.add_row(row_cells);
    }
    
    println!("{}", table);
}