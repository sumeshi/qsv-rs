use crate::controllers::csv::{exists_path, CsvController};
use crate::controllers::log::LogController;
use polars::prelude::*;
use std::path::PathBuf;
pub fn load(
    paths: &[PathBuf],
    separator: &str,
    low_memory: bool,
    no_headers: bool,
    chunk_size: Option<usize>,
) -> LazyFrame {
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
    // Check if any files are parquet
    let has_parquet = paths.iter().any(|path| {
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_lowercase() == "parquet")
            .unwrap_or(false)
    });
    let has_csv = paths.iter().any(|path| {
        let ext = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_lowercase());
        matches!(ext, Some(ref e) if e == "csv" || e == "tsv" || e == "gz" || e == "txt")
            || ext.is_none() // Files without extension are assumed to be CSV
    });
    // Cannot mix parquet and CSV files
    if has_parquet && has_csv {
        eprintln!("Error: Cannot mix parquet and CSV files in the same load command");
        std::process::exit(1);
    }
    if has_parquet {
        load_parquet_files(paths)
    } else {
        load_csv_files(paths, separator, low_memory, no_headers, chunk_size)
    }
}
fn load_parquet_files(paths: &[PathBuf]) -> LazyFrame {
    if paths.len() == 1 {
        LazyFrame::scan_parquet(&paths[0], ScanArgsParquet::default()).unwrap_or_else(|e| {
            eprintln!("Error reading parquet file {}: {}", paths[0].display(), e);
            std::process::exit(1);
        })
    } else {
        // Concatenate multiple parquet files
        let mut dataframes = Vec::new();
        for path in paths {
            let df =
                LazyFrame::scan_parquet(path, ScanArgsParquet::default()).unwrap_or_else(|e| {
                    eprintln!("Error reading parquet file {}: {}", path.display(), e);
                    std::process::exit(1);
                });
            dataframes.push(df);
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
            eprintln!("Error concatenating parquet files: {e}");
            std::process::exit(1);
        })
    }
}
fn load_csv_files(
    paths: &[PathBuf],
    separator: &str,
    low_memory: bool,
    no_headers: bool,
    chunk_size: Option<usize>,
) -> LazyFrame {
    CsvController::new(paths).get_dataframe(separator, low_memory, no_headers, chunk_size)
}
