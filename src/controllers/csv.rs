use polars::prelude::*;
use std::path::{PathBuf, Path};
use std::fs::File;
use std::io::{self, BufReader};
use glob::glob;
use crate::controllers::log::LogController;
use encoding_rs::SHIFT_JIS;

// ファイルパスの存在確認ユーティリティ関数
pub fn exists_path(paths: &[impl AsRef<Path>]) -> bool {
    for path in paths {
        if (!path.as_ref().exists()) {
            eprintln!("Error: File not found: {}", path.as_ref().display());
            return false;
        }
    }
    true
}

// パーサーモードを定義する列挙型 - implブロックの外に移動
#[derive(PartialEq)]
enum ParserMode {
    Polars,
    StandardCsv,
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
        // 複数のファイルが指定された場合を処理
        if self.paths.len() > 1 {
            self.concat_csv_files(separator, low_memory)
        } else if self.paths.len() == 1 {
            // 単一のファイルまたはパターンの場合
            let path = &self.paths[0];
            if path.to_string_lossy().contains('*') {
                // グロブパターンの場合
                self.handle_glob_pattern(path, separator, low_memory)
            } else {
                // 単一の明示的なファイル
                self.read_csv_file(path, separator, low_memory)
            }
        } else {
            // パスが指定されていない場合
            eprintln!("Error: No CSV files specified");
            std::process::exit(1);
        }
    }
    
    fn read_csv_file(&self, path: &PathBuf, separator: &str, low_memory: bool) -> LazyFrame {
        LogController::debug(&format!("Reading CSV file: {}", path.display()));
        
        // まずファイルの内容を調査して適切なパーサーを選択する
        let parser_mode = self.detect_best_parser_for_file(path);
        
        if parser_mode == ParserMode::StandardCsv {
            eprintln!("Complex CSV detected, using robust CSV parser...");
            match self.read_csv_with_standard_library(path, separator) {
                Ok(df) => return df,
                Err(e) => {
                    eprintln!("Error reading CSV file {}: {}", path.display(), e);
                    std::process::exit(1);
                }
            }
        }
        
        // 以下通常のPolarsパーサーを使用
        let sep_byte = separator.as_bytes()[0];
        
        // 通常の方法で読み込みを試みる
        let csv_reader = LazyCsvReader::new(path.clone())
            .with_delimiter(sep_byte)
            .has_header(true)
            .with_ignore_errors(true)
            .with_quote_char(Some(b'"'))
            .with_infer_schema_length(Some(1000))
            .low_memory(true)
            .truncate_ragged_lines(true);
            
        match csv_reader.finish() {
            Ok(df) => df,
            Err(e) => {
                eprintln!("Warning: Error with Polars CSV reader: {}. Trying alternative method...", e);
                
                // 標準のCSVクレートを使用して読み込む
                match self.read_csv_with_standard_library(path, separator) {
                    Ok(df) => df,
                    Err(e) => {
                        eprintln!("Error reading CSV file {}: {}", path.display(), e);
                        std::process::exit(1);
                    }
                }
            }
        }
    }
    
    // 最適なパーサーを検出する関数
    fn detect_best_parser_for_file(&self, path: &PathBuf) -> ParserMode {
        // ファイルの先頭部分を読み取り、複雑さを判断
        if let Ok(content) = std::fs::read(path) {
            // BOMチェック - BOМがあるファイルは複雑かもしれない
            if content.len() >= 3 && content[0] == 0xEF && content[1] == 0xBB && content[2] == 0xBF {
                return ParserMode::StandardCsv;
            }
            
            // ファイルサイズチェック - 非常に大きいファイルは標準パーサーで処理
            if content.len() > 10 * 1024 * 1024 { // 10MB以上
                return ParserMode::StandardCsv;
            }
            
            // エンコーディングチェック - UTF-8以外はより堅牢なパーサーを使用
            if String::from_utf8(content.clone()).is_err() {
                return ParserMode::StandardCsv;
            }
            
            // 引用符内の改行が多いかチェック
            let sample = String::from_utf8_lossy(&content[0..std::cmp::min(content.len(), 1024)]);
            let mut quote_count = 0;
            let mut newlines_in_quotes = 0;
            
            let mut in_quotes = false;
            for c in sample.chars() {
                if c == '"' {
                    in_quotes = !in_quotes;
                    quote_count += 1;
                } else if c == '\n' && in_quotes {
                    newlines_in_quotes += 1;
                }
            }
            
            // 引用符内に改行が多い場合は複雑なCSV
            if newlines_in_quotes > 0 || quote_count > 20 {
                return ParserMode::StandardCsv;
            }
        }
        
        // デフォルトではPolarsパーサーを使用
        ParserMode::Polars
    }
    
    // 標準CSVライブラリを使用して読み込み、Polars DataFrameに変換する
    fn read_csv_with_standard_library(&self, path: &PathBuf, separator: &str) -> Result<LazyFrame, String> {
        // 前処理済みの一時ファイルを作成
        let temp_path = match self.preprocess_csv_file(path, separator) {
            Ok(p) => p,
            Err(e) => return Err(format!("Failed to preprocess CSV: {}", e)),
        };
        
        // 標準CSVリーダーでファイルを開く
        let file = match File::open(&temp_path) {
            Ok(f) => f,
            Err(e) => return Err(format!("Failed to open preprocessed file: {}", e)),
        };
        
        let reader = BufReader::new(file);
        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(separator.as_bytes()[0])
            .has_headers(true)
            .flexible(true)
            .from_reader(reader);
        
        // ヘッダーを読み込む
        let headers = match csv_reader.headers() {
            Ok(h) => h.iter().map(|s| s.to_string()).collect::<Vec<String>>(),
            Err(e) => return Err(format!("Failed to read CSV headers: {}", e)),
        };
        
        // データを行ごとに読み込み
        let mut rows: Vec<Vec<String>> = Vec::new();
        for result in csv_reader.records() {
            match result {
                Ok(record) => {
                    let row: Vec<String> = record.iter().map(|s| s.to_string()).collect();
                    rows.push(row);
                },
                Err(e) => {
                    eprintln!("Warning: Skipping malformed row: {}", e);
                    continue;
                }
            }
        }
        
        // DataFrameに変換
        self.create_dataframe_from_rows(headers, rows)
    }
    
    // 行データからPolars DataFrameを作成
    fn create_dataframe_from_rows(&self, headers: Vec<String>, rows: Vec<Vec<String>>) -> Result<LazyFrame, String> {
        if rows.is_empty() {
            return Err("No data rows found in CSV".to_string());
        }
        
        // 各列のSeriesを作成
        let mut series_vec = Vec::new();
        let num_cols = headers.len();
        
        for (col_idx, col_name) in headers.iter().enumerate() {
            // 最初にデータを文字列として収集
            let mut col_data = Vec::new();
            
            for row in &rows {
                let value = if col_idx < row.len() {
                    row[col_idx].clone()
                } else {
                    // 列が足りない場合は空文字列
                    String::new()
                };
                col_data.push(value);
            }
            
            // StringSeriesを作成
            let series = Series::new(col_name, col_data);
            series_vec.push(series);
        }
        
        // SeriesのベクターからDataFrameを作成
        match DataFrame::new(series_vec) {
            Ok(df) => Ok(df.lazy()),
            Err(e) => Err(format!("Failed to create DataFrame: {}", e)),
        }
    }
    
    // CSVファイルを前処理する関数（問題のある行を修正し、一時ファイルに保存）
    fn preprocess_csv_file(&self, path: &PathBuf, separator: &str) -> io::Result<PathBuf> {
        let temp_path = PathBuf::from(format!("{}.tmp", path.display()));
        
        // ファイルをバイト列として読み込み
        let content = std::fs::read(path)?;
        
        // BOMをチェック
        let mut start_index = 0;
        if content.len() >= 3 && content[0] == 0xEF && content[1] == 0xBB && content[2] == 0xBF {
            start_index = 3;
            eprintln!("Removed BOM from file: {}", path.display());
        }
        
        // エンコーディング検出と変換
        let content_str = if let Ok(s) = String::from_utf8(content[start_index..].to_vec()) {
            // UTF-8として正常に解析できた場合
            s
        } else {
            // UTF-8として解析できない場合、SJISの可能性を検討
            eprintln!("File is not valid UTF-8, trying SJIS encoding: {}", path.display());
            
            // SJISからUTF-8への変換
            let (cow, _encoding_used, had_errors) = SHIFT_JIS.decode(&content[start_index..]);
            if had_errors {
                eprintln!("Warning: Some characters could not be decoded properly from SJIS");
            }
            cow.into_owned()
        };
        
        // 引用符内の改行を処理
        let mut fixed_content = String::new();
        let mut in_quotes = false;
        
        for c in content_str.chars() {
            if c == '"' {
                in_quotes = !in_quotes;
                fixed_content.push(c);
            } else if c == '\n' && in_quotes {
                // 引用符内の改行をスペースに置換
                fixed_content.push(' ');
            } else {
                fixed_content.push(c);
            }
        }
        
        // 修正した内容を一時ファイルに書き込み
        std::fs::write(&temp_path, fixed_content)?;
        
        Ok(temp_path)
    }
    
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