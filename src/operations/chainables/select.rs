use polars::prelude::*;
use crate::controllers::log::LogController;
use crate::controllers::dataframe::{exists_colname, parse_column_ranges};

pub fn select(df: &LazyFrame, colnames: &[String]) -> LazyFrame {
    if !exists_colname(df, colnames) {
        eprintln!("Error: One or more column names do not exist in the DataFrame");
        std::process::exit(1);
    }
    
    let selected_columns = parse_column_ranges(df, colnames);
    LogController::debug(&format!("{} columns are selected. [{}]", 
        selected_columns.len(), 
        selected_columns.join(", ")
    ));
    
    // dfを複製して所有権の問題を解決
    df.clone().select(selected_columns.iter().map(|s| col(s)).collect::<Vec<_>>())
}