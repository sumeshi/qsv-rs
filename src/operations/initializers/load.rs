use crate::controllers::csv::{exists_path, CsvController};
use crate::controllers::log::LogController;
use polars::prelude::*;
use std::path::PathBuf;

pub fn load(paths: &[PathBuf], separator: &str, low_memory: bool, no_headers: bool) -> LazyFrame {
    if !exists_path(paths) {
        eprintln!("One or more files do not exist");
        std::process::exit(1);
    }

    LogController::debug(&format!(
        "{} files are loaded. [{}]",
        paths.len(),
        paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    ));

    CsvController::new(paths).get_dataframe(separator, low_memory, no_headers)
}
