use crate::controllers::log::LogController;
use glob::glob;
use polars::prelude::*;
use std::path::{Path, PathBuf};

// Utility function to check if file paths exist
pub fn exists_path(paths: &[impl AsRef<Path>]) -> bool {
    for path in paths {
        if !path.as_ref().exists() {
            eprintln!("Error: File not found: {}", path.as_ref().display());
            return false;
        }
    }
    true
}

pub struct CsvController {
    paths: Vec<PathBuf>,
}

impl CsvController {
    pub fn new(paths: &[PathBuf]) -> Self {
        Self {
            paths: paths.to_vec(),
        }
    }

    pub fn get_dataframe(&self, separator: &str, low_memory: bool) -> LazyFrame {
        if self.paths.len() == 1 {
            let path = &self.paths[0];
            let path_str = path.to_string_lossy();

            if path_str.contains('*') || path_str.contains('?') || path_str.contains('[') {
                self.handle_glob_pattern(path, separator, low_memory)
            } else {
                self.read_csv_file(path, separator, low_memory)
            }
        } else {
            self.concat_csv_files(separator, low_memory)
        }
    }

    fn read_csv_file(&self, path: &Path, separator: &str, low_memory: bool) -> LazyFrame {
        LogController::debug(&format!("Reading CSV file: {}", path.display()));

        let sep_byte = separator.as_bytes()[0];

        let reader = LazyCsvReader::new(path)
            .with_separator(sep_byte)
            .with_has_header(true)
            .with_low_memory(low_memory)
            .finish();

        match reader {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error with Polars CSV reader for file {}: {}. Please check the file format and separator.", path.display(), e);
                std::process::exit(1);
            }
        }
    }

    fn concat_csv_files(&self, separator: &str, low_memory: bool) -> LazyFrame {
        let mut dataframes = Vec::new();

        for path in &self.paths {
            dataframes.push(self.read_csv_file(path, separator, low_memory));
        }

        concat(
            dataframes,
            UnionArgs {
                parallel: true,
                rechunk: true,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| {
            eprintln!("Error concatenating CSV files: {}", e);
            std::process::exit(1);
        })
    }

    fn handle_glob_pattern(
        &self,
        pattern: &Path,
        separator: &str,
        low_memory: bool,
    ) -> LazyFrame {
        let pattern_str = pattern.to_string_lossy();
        let mut paths = Vec::new();

        match glob(&pattern_str) {
            Ok(entries) => {
                for entry in entries {
                    match entry {
                        Ok(path) => paths.push(path),
                        Err(e) => LogController::warn(&format!("Error with glob pattern: {}", e)),
                    }
                }
            }
            Err(e) => {
                eprintln!("Invalid glob pattern '{}': {}", pattern_str, e);
                std::process::exit(1);
            }
        }

        if paths.is_empty() {
            eprintln!("No files found matching pattern: {}", pattern_str);
            std::process::exit(1);
        }

        LogController::debug(&format!(
            "Found {} files matching pattern: {}",
            paths.len(),
            pattern_str
        ));

        let controller = CsvController::new(&paths);
        controller.get_dataframe(separator, low_memory)
    }
}
