use crate::controllers::log::LogController;
use glob::glob;
use polars::prelude::*;
// use rayon::prelude::*; // Temporarily disabled
use std::path::{Path, PathBuf};
// Performance optimization constants
const OPTIMAL_CHUNK_SIZE: usize = 8192; // Optimized chunk size for CSV reading
const PARALLEL_THRESHOLD: usize = 2; // Minimum files to use parallel processing
const LARGE_FILE_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB threshold for large files
const GZIP_BUFFER_SIZE: usize = 16 * 1024 * 1024; // 16MB buffer for gzip (increased from 8MB)

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

// Get optimized CSV reader options for better performance
fn get_optimized_csv_options(
    separator: &str,
    has_header: bool,
    low_memory: bool,
    chunk_size: Option<usize>,
    file_size: Option<u64>,
) -> CsvReadOptions {
    let sep_byte = separator.as_bytes()[0];
    let optimized_chunk_size = chunk_size.unwrap_or({
        match file_size {
            Some(size) if size > LARGE_FILE_THRESHOLD => OPTIMAL_CHUNK_SIZE * 2, // Larger chunks for big files
            _ => OPTIMAL_CHUNK_SIZE,
        }
    });

    let mut options = CsvReadOptions::default()
        .with_has_header(has_header)
        .with_low_memory(low_memory)
        .with_chunk_size(optimized_chunk_size)
        // Note: Removing infer_schema_length to maintain backward compatibility
        // .with_infer_schema_length(Some(1000))  // Limit schema inference for speed
        .map_parse_options(|parse_opts| {
            parse_opts.with_separator(sep_byte)
            // Note: Disabling try_parse_dates to maintain backward compatibility
            // .with_try_parse_dates(true)
        });

    // For large files, use additional optimizations
    if let Some(size) = file_size {
        if size > LARGE_FILE_THRESHOLD {
            options = options.with_low_memory(true); // Force low memory for large files
        }
    }

    options
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
    pub fn get_dataframe(
        &self,
        separator: &str,
        low_memory: bool,
        no_headers: bool,
        chunk_size: Option<usize>,
    ) -> LazyFrame {
        if self.paths.len() == 1 {
            let path = &self.paths[0];
            let path_str = path.to_string_lossy();
            if path_str.contains('*') || path_str.contains('?') || path_str.contains('[') {
                self.handle_glob_pattern(path, separator, low_memory, no_headers, chunk_size)
            } else {
                self.read_csv_file(path, separator, low_memory, no_headers, chunk_size)
            }
        } else {
            self.concat_csv_files(separator, low_memory, no_headers, chunk_size)
        }
    }
    fn read_csv_file(
        &self,
        path: &Path,
        separator: &str,
        low_memory: bool,
        no_headers: bool,
        chunk_size: Option<usize>,
    ) -> LazyFrame {
        LogController::debug(&format!("Reading CSV file: {}", path.display()));
        let has_header = !no_headers;
        // Check if file is gzipped based on extension
        let is_gzipped = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_lowercase() == "gz")
            .unwrap_or(false);
        if is_gzipped {
            LogController::debug(&format!("Reading gzipped file: {}", path.display()));
            // For gzipped files, use chunked decompression to balance memory usage and performance
            use flate2::read::GzDecoder;
            use std::fs::File;
            use std::io::{BufReader, Read, Write};
            let file = match File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Error opening gzipped file {}: {}", path.display(), e);
                    std::process::exit(1);
                }
            };
            // Check file size to determine strategy
            let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
            const MAX_MEMORY_SIZE: u64 = 512 * 1024 * 1024; // 512MB threshold
            if file_size > 0 && file_size < MAX_MEMORY_SIZE {
                // For smaller files, decompress to memory (faster)
                LogController::debug(&format!(
                    "Small gzipped file ({}MB), using memory decompression",
                    file_size / 1024 / 1024
                ));
                let mut gz_decoder = GzDecoder::new(BufReader::new(file));
                let mut decompressed_content = Vec::new();
                if let Err(e) = gz_decoder.read_to_end(&mut decompressed_content) {
                    eprintln!("Error decompressing gzipped file {}: {}", path.display(), e);
                    std::process::exit(1);
                }
                let cursor = std::io::Cursor::new(decompressed_content);
                // Use basic CSV options for gzipped files to maintain compatibility
                let mut csv_options = polars::prelude::CsvReadOptions::default()
                    .with_has_header(has_header)
                    .with_low_memory(low_memory)
                    .map_parse_options(|opts| opts.with_separator(separator.as_bytes()[0]));
                if let Some(chunk_size) = chunk_size {
                    csv_options = csv_options.with_chunk_size(chunk_size);
                }
                let reader = csv_options.into_reader_with_file_handle(cursor);
                match reader.finish() {
                    Ok(df) => df.lazy(),
                    Err(e) => {
                        eprintln!("Error parsing gzipped CSV file {}: {}. Please check the file format and separator.", path.display(), e);
                        std::process::exit(1);
                    }
                }
            } else {
                // For larger files, use a temporary file with chunked decompression
                LogController::debug(&format!(
                    "Large gzipped file ({}MB), using chunked decompression",
                    file_size / 1024 / 1024
                ));
                let temp_dir = std::env::temp_dir();
                let temp_filename = format!(
                    "qsv_gzip_{}_{}.csv",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                );
                let temp_path = temp_dir.join(temp_filename);
                // Decompress in chunks to temporary file
                let mut gz_decoder = GzDecoder::new(BufReader::new(file));
                let mut temp_file = match std::fs::File::create(&temp_path) {
                    Ok(f) => f,
                    Err(e) => {
                        eprintln!("Error creating temporary file for large gzip: {e}");
                        std::process::exit(1);
                    }
                };
                // Copy in chunks to avoid loading everything into memory
                let mut buffer = vec![0u8; GZIP_BUFFER_SIZE];
                loop {
                    match gz_decoder.read(&mut buffer) {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            if let Err(e) = temp_file.write_all(&buffer[..n]) {
                                eprintln!("Error writing to temporary file: {e}");
                                let _ = std::fs::remove_file(&temp_path);
                                std::process::exit(1);
                            }
                        }
                        Err(e) => {
                            eprintln!("Error reading from gzipped file {}: {}", path.display(), e);
                            let _ = std::fs::remove_file(&temp_path);
                            std::process::exit(1);
                        }
                    }
                }
                if let Err(e) = temp_file.flush() {
                    eprintln!("Error flushing temporary file: {e}");
                    let _ = std::fs::remove_file(&temp_path);
                    std::process::exit(1);
                }
                drop(temp_file); // Close the file

                // Read from temporary file using basic settings for compatibility
                let mut reader = LazyCsvReader::new(&temp_path)
                    .with_separator(separator.as_bytes()[0])
                    .with_has_header(has_header)
                    .with_low_memory(low_memory);
                if let Some(chunk_size) = chunk_size {
                    reader = reader.with_chunk_size(chunk_size);
                }
                let reader = reader.finish();
                // Schedule cleanup of temporary file
                let temp_path_for_cleanup = temp_path.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    let _ = std::fs::remove_file(&temp_path_for_cleanup);
                });
                match reader {
                    Ok(df) => df,
                    Err(e) => {
                        let _ = std::fs::remove_file(&temp_path);
                        eprintln!("Error parsing decompressed CSV file {}: {}. Please check the file format and separator.", path.display(), e);
                        std::process::exit(1);
                    }
                }
            }
        } else {
            // Get file size for optimization
            let file_size = std::fs::metadata(path).ok().map(|m| m.len());

            // Use optimized CSV options
            let csv_options =
                get_optimized_csv_options(separator, has_header, low_memory, chunk_size, file_size);

            LogController::debug(&format!(
                "Reading CSV file: {} (size: {}MB)",
                path.display(),
                file_size.map(|s| s / 1024 / 1024).unwrap_or(0)
            ));

            let reader = LazyCsvReader::new(path)
                .with_separator(csv_options.parse_options.separator)
                .with_has_header(csv_options.has_header)
                .with_low_memory(csv_options.low_memory)
                .with_chunk_size(csv_options.chunk_size)
                // Note: Removing infer_schema_length for compatibility
                // .with_infer_schema_length(csv_options.infer_schema_length)
                .finish();

            match reader {
                Ok(df) => df,
                Err(e) => {
                    eprintln!("Error with Polars CSV reader for file {}: {}. Please check the file format and separator.", path.display(), e);
                    std::process::exit(1);
                }
            }
        }
    }
    fn concat_csv_files(
        &self,
        separator: &str,
        low_memory: bool,
        no_headers: bool,
        chunk_size: Option<usize>,
    ) -> LazyFrame {
        LogController::debug(&format!("Reading {} CSV files", self.paths.len()));

        // Use parallel processing for multiple files if threshold is met
        let dataframes = if self.paths.len() >= PARALLEL_THRESHOLD {
            LogController::debug("Using parallel file reading for better performance");
            self.paths
                .iter() // Use regular iterator for now
                .map(|path| self.read_csv_file(path, separator, low_memory, no_headers, chunk_size))
                .collect::<Vec<_>>()
        } else {
            // Sequential for small number of files
            self.paths
                .iter()
                .map(|path| self.read_csv_file(path, separator, low_memory, no_headers, chunk_size))
                .collect::<Vec<_>>()
        };

        concat(
            dataframes,
            UnionArgs {
                parallel: true,
                rechunk: true,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| {
            eprintln!("Error concatenating CSV files: {e}");
            std::process::exit(1);
        })
    }
    fn handle_glob_pattern(
        &self,
        pattern: &Path,
        separator: &str,
        low_memory: bool,
        no_headers: bool,
        chunk_size: Option<usize>,
    ) -> LazyFrame {
        let pattern_str = pattern.to_string_lossy();
        let mut paths = Vec::new();
        match glob(&pattern_str) {
            Ok(entries) => {
                for entry in entries {
                    match entry {
                        Ok(path) => paths.push(path),
                        Err(e) => LogController::warn(&format!("Error with glob pattern: {e}")),
                    }
                }
            }
            Err(e) => {
                eprintln!("Invalid glob pattern '{pattern_str}': {e}");
                std::process::exit(1);
            }
        }
        if paths.is_empty() {
            eprintln!("No files found matching pattern: {pattern_str}");
            std::process::exit(1);
        }
        LogController::debug(&format!(
            "Found {} files matching pattern: {}",
            paths.len(),
            pattern_str
        ));
        let controller = CsvController::new(&paths);
        controller.get_dataframe(separator, low_memory, no_headers, chunk_size)
    }
}
