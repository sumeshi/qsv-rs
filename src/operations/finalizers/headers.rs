use polars::prelude::*;
use comfy_table::{Table, Cell};
use comfy_table::presets::UTF8_FULL;
use crate::controllers::log::LogController;

pub fn headers(df: &LazyFrame, plain: bool) {
    // Get schema (use clone() to solve ownership problem)
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame: {}", e);
            return;
        }
    };
    let schema = collected_df.schema();
    
    // Convert SmartString to String
    let column_names: Vec<String> = schema.iter()
        .map(|(name, _)| name.to_string())
        .collect();
    
    LogController::debug(&format!("Showing headers: {} columns", column_names.len()));
    
    if plain {
        // Display in plain text format
        for name in column_names.iter() {
            println!("{}", name);
        }
    } else {
        // Display in table format
        let mut table = Table::new();
        table.load_preset(UTF8_FULL);
        table.set_header(vec!["#", "Column Name"]);
        
        for (i, name) in column_names.iter().enumerate() {
            table.add_row(vec![
                Cell::new(format!("{:02}", i)),
                Cell::new(name),
            ]);
        }
        
        println!("{}", table);
    }
}