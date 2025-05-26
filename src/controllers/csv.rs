use polars::prelude::*;
use std::path::{PathBuf, Path};
use glob::glob;
use crate::controllers::log::LogController;

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
        // Handle the case where multiple files are specified
        if self.paths.len() > 1 {
            self.concat_csv_files(separator, low_memory)
        } else if self.paths.len() == 1 {
            // Case for a single file or a glob pattern
            let path = &self.paths[0];
            if path.to_string_lossy().contains('*') {
                // Glob pattern case
                self.handle_glob_pattern(path, separator, low_memory)
            } else {
                // Single explicit file
                self.read_csv_file(path, separator, low_memory)
            }
        } else {
            // No paths specified case
            eprintln!("Error: No CSV files specified");
            std::process::exit(1);
        }
    }
    
    fn read_csv_file(&self, path: &PathBuf, separator: &str, low_memory: bool) -> LazyFrame {
        LogController::debug(&format!("Reading CSV file: {}", path.display()));
        
        // First, inspect the file content to select the appropriate parser
        // let parser_mode = self.detect_best_parser_for_file(path);
        
        // if parser_mode == ParserMode::StandardCsv {
        //     eprintln!("Complex CSV detected, using robust CSV parser...");
        //     match self.read_csv_with_standard_library(path, separator) {
        //         Ok(df) => return df,
        //         Err(e) => {
        //             eprintln!("Error reading CSV file {}: {}", path.display(), e);
        //             std::process::exit(1);
        //         }
        //     }
        // }
        
        // Otherwise, use the standard Polars parser
        let sep_byte = separator.as_bytes()[0];
        
        // Attempt to read using the standard Polars method
        let csv_reader = LazyCsvReader::new(path.clone())
            .with_separator(sep_byte)
            .with_has_header(true)
            .with_ignore_errors(false)
            .with_quote_char(Some(b'"'))
            .with_infer_schema_length(Some(1000))
            .with_low_memory(low_memory);  // Removed truncate_ragged_lines
            
        match csv_reader.finish() {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Error with Polars CSV reader for file {}: {}. Please check the file format and separator.", path.display(), e);
                // Removed fallback to standard library
                // match self.read_csv_with_standard_library(path, separator) {
                //     Ok(df) => df,
                //     Err(e_fallback) => {
                //         eprintln!("Error reading CSV file {} with fallback: {}", path.display(), e_fallback);
                std::process::exit(1);
                //     }
                // }
            }
        }
    }
    
    // Function to detect the best parser
    // fn detect_best_parser_for_file(&self, path: &PathBuf) -> ParserMode {
    //     // Read the beginning of the file to assess complexity
    //     if let Ok(content) = std::fs::read(path) {
    //         // BOM check - Files with BOM might be complex
    //         let mut start_index = 0;
    //         if content.len() >= 3 && content[0] == 0xEF && content[1] == 0xBB && content[2] == 0xBF {
    //             start_index = 3;
    //             eprintln!("Removed BOM from file: {}", path.display());
    //         }
            
    //         // File size check - Very large files might be better handled by the standard parser
    //         if content.len() > 10 * 1024 * 1024 { // > 10MB
    //             return ParserMode::StandardCsv;
    //         }
            
    //         // Encoding check - Use a more robust parser for non-UTF-8
    //         if String::from_utf8(content[start_index..].to_vec()).is_err() {
    //             return ParserMode::StandardCsv;
    //         }
            
    //         // Check for many newlines within quotes
    //         let sample = String::from_utf8_lossy(&content[0..std::cmp::min(content.len(), 1024)]);
    //         let mut quote_count = 0;
    //         let mut newlines_in_quotes = 0;
            
    //         let mut in_quotes = false;
    //         for c in sample.chars() {
    //             if c == '"' {
    //                 in_quotes = !in_quotes;
    //                 quote_count += 1;
    //             } else if c == '\n' && in_quotes {
    //                 newlines_in_quotes += 1;
    //             }
    //         }
            
    //         // Complex CSV if there are many newlines within quotes or many quotes
    //         if newlines_in_quotes > 0 || quote_count > 20 {
    //             return ParserMode::StandardCsv;
    //         }
    //     }
        
    //     // Use Polars parser by default
    //     ParserMode::Polars
    // }
    
    // Read using the standard CSV library and convert to Polars DataFrame
    // fn read_csv_with_standard_library(&self, path: &PathBuf, separator: &str) -> Result<LazyFrame, String> {
    //     // Create a preprocessed temporary file
    //     let temp_path = match self.preprocess_csv_file(path, separator) {
    //         Ok(p) => p,
    //         Err(e) => return Err(format!("Failed to preprocess CSV: {}", e)),
    //     };
        
    //     // Open the file with the standard CSV reader
    //     let file = match File::open(&temp_path) {
    //         Ok(f) => f,
    //         Err(e) => return Err(format!("Failed to open preprocessed file: {}", e)),
    //     };
        
    //     let reader = BufReader::new(file);
    //     // let mut csv_reader = csv::ReaderBuilder::new()
    //     //     .delimiter(separator.as_bytes()[0])
    //     //     .has_headers(true)
    //     //     .flexible(true)
    //     //     .from_reader(reader);
        
    //     // // Read headers
    //     // let headers = match csv_reader.headers() {
    //     //     Ok(h) => h.iter().map(|s| s.to_string()).collect::<Vec<String>>(),
    //     //     Err(e) => return Err(format!("Failed to read CSV headers: {}", e)),
    //     // };
        
    //     // // Read data row by row
    //     // let mut rows: Vec<Vec<String>> = Vec::new();
    //     // for result in csv_reader.records() {
    //     //     match result {
    //     //         Ok(record) => {
    //     //             let row: Vec<String> = record.iter().map(|s| s.to_string()).collect();
    //     //             rows.push(row);
    //     //         },
    //     //         Err(e) => {
    //     //             eprintln!("Warning: Skipping malformed row: {}", e);
    //     //             continue;
    //     //         }
    //     //     }
    //     // }
        
    //     // // Convert to DataFrame
    //     // self.create_dataframe_from_rows(headers, rows)
    //     Err("Standard CSV library reading is currently disabled.".to_string())
    // }
    
    // Create a Polars DataFrame from row data
    // fn create_dataframe_from_rows(&self, headers: Vec<String>, rows: Vec<Vec<String>>) -> Result<LazyFrame, String> {
    //     if rows.is_empty() {
    //         return Err("No data rows found in CSV".to_string());
    //     }
        
    //     // Create Series for each column
    //     let mut series_vec: Vec<Series> = Vec::new(); // Changed to Vec<Series>
        
    //     for (col_idx, col_name) in headers.iter().enumerate() {
    //         // First, collect data as strings
    //         let mut col_data = Vec::new();
            
    //         for row in &rows {
    //             let value = if col_idx < row.len() {
    //                 row[col_idx].clone()
    //             } else {
    //                 // Empty string if column is missing
    //                 String::new()
    //             };
    //             col_data.push(value);
    //         }
            
    //         // Create StringSeries
    //         let series = Series::new(&col_name.clone().into(), col_data); // Use .into() for PlSmallStr
    //         series_vec.push(series);
    //     }
        
    //     // Create DataFrame from vector of Series
    //     match DataFrame::new(series_vec) { // DataFrame::new takes Vec<Series>
    //         Ok(df) => Ok(df.lazy()),
    //         Err(e) => Err(format!("Failed to create DataFrame: {}", e)),
    //     }
    // }
    
    // Function to preprocess CSV file (fix problematic lines, save to temporary file)
    // fn preprocess_csv_file(&self, path: &PathBuf, _separator: &str) -> io::Result<PathBuf> {
    //     let temp_path = PathBuf::from(format!("{}.tmp", path.display()));
        
    //     // Read the file as bytes
    //     let content = std::fs::read(path)?;
        
    //     // Check for BOM
    //     let mut start_index = 0;
    //     if content.len() >= 3 && content[0] == 0xEF && content[1] == 0xBB && content[2] == 0xBF {
    //         start_index = 3;
    //         eprintln!("Removed BOM from file: {}", path.display());
    //     }
        
    //     // Encoding detection and conversion
    //     let content_str = if let Ok(s) = String::from_utf8(content[start_index..].to_vec()) {
    //         // If successfully parsed as UTF-8
    //         s
    //     } else {
    //         // If cannot be parsed as UTF-8, consider SJIS possibility
    //         eprintln!("File is not valid UTF-8, trying SJIS encoding: {}", path.display());
            
    //         // Convert from SJIS to UTF-8
    //         let (cow, _encoding_used, had_errors) = SHIFT_JIS.decode(&content[start_index..]);
    //         if had_errors {
    //             eprintln!("Warning: Some characters could not be decoded properly from SJIS");
    //         }
    //         cow.into_owned()
    //     };
        
    //     // Process newlines within quotes
    //     let mut fixed_content = String::new();
    //     let mut in_quotes = false;
        
    //     for c in content_str.chars() {
    //         if c == '"' {
    //             in_quotes = !in_quotes;
    //             fixed_content.push(c);
    //         } else if c == '\n' && in_quotes {
    //             // Replace newlines within quotes with spaces
    //             fixed_content.push(' ');
    //         } else {
    //             fixed_content.push(c);
    //         }
    //     }
        
    //     // Write the corrected content to a temporary file
    //     std::fs::write(&temp_path, fixed_content)?;
        
    //     Ok(temp_path)
    // }
    
    // Function to concatenate multiple CSV files
    fn concat_csv_files(&self, separator: &str, low_memory: bool) -> LazyFrame {
        let mut dataframes = Vec::new();
        
        for path in &self.paths {
            dataframes.push(self.read_csv_file(path, separator, low_memory));
        }
        
        concat(dataframes, UnionArgs {
            parallel: true,
            rechunk: true,
            ..Default::default()
        }).unwrap_or_else(|e| {
            eprintln!("Error concatenating CSV files: {}", e);
            std::process::exit(1);
        })
    }
    
    // Function to handle glob patterns and concatenate matching files
    fn handle_glob_pattern(&self, pattern: &PathBuf, separator: &str, low_memory: bool) -> LazyFrame {
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
            },
            Err(e) => {
                eprintln!("Invalid glob pattern '{}': {}", pattern_str, e);
                std::process::exit(1);
            }
        }
        
        if paths.is_empty() {
            eprintln!("No files found matching pattern: {}", pattern_str);
            std::process::exit(1);
        }
        
        LogController::debug(&format!("Found {} files matching pattern: {}", paths.len(), pattern_str));
        
        let controller = CsvController::new(&paths);
        controller.get_dataframe(separator, low_memory)
    }
}

// Convert SJIS to UTF-8
#[allow(dead_code)] // Consider removing if unused later
pub fn convert_sjis_to_utf8(input_bytes: &[u8]) -> Vec<u8> {
    // Assuming LogController is in scope or crate::controllers::log::LogController;

    // Process newlines within quotes
    #[allow(dead_code)]
    fn process_quoted_newlines(line: &str) -> String {
        let mut fixed_line = String::new();
        let mut in_quotes = false;
        for c in line.chars() {
            if c == '"' {
                in_quotes = !in_quotes;
                fixed_line.push(c);
            } else if in_quotes && (c == '\r' || c == '\n') {
                // Replace newlines within quotes with spaces
                fixed_line.push(' ');
            } else {
                fixed_line.push(c);
            } // Closes: if/else block for char processing
        } // Closes: for loop
        fixed_line // Return from process_quoted_newlines
    } // Closes: fn process_quoted_newlines

    // Example usage if process_quoted_newlines was meant to be used here:
    // let s = String::from_utf8_lossy(input_bytes);
    // let processed_s = process_quoted_newlines(&s);
    // let input_bytes_for_decode = processed_s.as_bytes();
    // For now, assume direct decode as per previous structure:

    let (cow, _encoding_used, had_errors) = encoding_rs::SHIFT_JIS.decode(input_bytes);
    if had_errors {
        // Ensure LogController is accessible. If not, use eprintln or remove.
        // LogController::warn("Warning: Some characters could not be decoded properly from SJIS in convert_sjis_to_utf8");
        eprintln!("Warning: Some characters could not be decoded properly from SJIS in convert_sjis_to_utf8");
    }
    cow.into_owned().into_bytes() // New return for Vec<u8>
} // Closes: pub fn convert_sjis_to_utf8

// The following lines seemed out of place and are commented out to fix syntax errors.
// They might belong inside a different function or be part of an unfinished implementation.
//    // Write the modified content to a temporary file
//    let mut temp_file = File::create(&temp_path).map_err(|e| format!("Failed to create temporary file: {}", e))?;
//    // ... existing code ...
// } // This was Line 339 in the error, likely an extraneous closing brace.