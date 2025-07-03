use crate::controllers::log::LogController;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, Table};
use polars::prelude::*;

pub fn headers(df: &LazyFrame, plain: bool) {
    // Get schema from LazyFrame without collecting
    let schema = match df.clone().collect_schema() {
        Ok(schema) => schema,
        Err(e) => {
            eprintln!("Error getting schema: {e}");
            return;
        }
    };

    let column_names: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
    LogController::debug(&format!("Showing headers: {} columns", column_names.len()));

    if plain {
        for name in column_names.iter() {
            println!("{name}");
        }
    } else {
        let mut table = Table::new();
        table.load_preset(UTF8_FULL);
        table.set_header(vec!["#", "Column Name"]);
        for (i, name) in column_names.iter().enumerate() {
            table.add_row(vec![Cell::new(format!("{i:02}")), Cell::new(name)]);
        }
        println!("{table}");
    }
}
