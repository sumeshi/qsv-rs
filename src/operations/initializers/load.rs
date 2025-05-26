use polars::prelude::*;
use std::path::PathBuf;
use crate::controllers::csv::{CsvController, exists_path};
use crate::controllers::log::LogController;

pub fn load(paths: &[PathBuf], separator: &str, low_memory: bool) -> LazyFrame {
    if !exists_path(paths) {
        eprintln!("One or more files do not exist");
        std::process::exit(1);
    }

    LogController::debug(&format!("{} files are loaded. [{}]", 
        paths.len(), 
        paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>().join(", ")
    ));
    
    CsvController::new(paths).get_dataframe(separator, low_memory)
}