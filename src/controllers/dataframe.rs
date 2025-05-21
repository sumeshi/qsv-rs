use polars::prelude::*;
use std::path::PathBuf;
use crate::operations::initializers::load;
use crate::operations::chainables::{select, head, tail, isin, contains, sed, grep, sort, count, uniq, changetz, renamecol};
use crate::operations::finalizers::{headers, stats, showquery, show, showtable, dump};
use crate::operations::quilters::{quilt, quilt_visualize};
use crate::controllers::log::LogController;

pub struct DataFrameController {
    df: Option<LazyFrame>,
}

impl DataFrameController {
    pub fn new() -> Self {
        Self { df: None }
    }
    
    pub fn is_empty(&self) -> bool {
        self.df.is_none()
    }
    
    // -- initializers --
    pub fn load(&mut self, paths: &[PathBuf], separator: &str, low_memory: bool) -> &mut Self {
        self.df = Some(load::load(paths, separator, low_memory));
        self
    }
    
    // -- chainables --
    pub fn select(&mut self, colnames: &[String]) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(select::select(df, colnames));
        }
        self
    }
    
    pub fn isin(&mut self, colname: &str, values: &[String]) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(isin::isin(df, colname, values));
        }
        self
    }
    
    pub fn contains(&mut self, colname: &str, pattern: &str, ignorecase: bool) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(contains::contains(df, colname, pattern, ignorecase));
        }
        self
    }
    
    pub fn sed(&mut self, colname: &str, pattern: &str, replacement: &str, ignorecase: bool) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(sed::sed(df, colname, pattern, replacement, ignorecase));
        }
        self
    }
    
    pub fn grep(&mut self, pattern: &str, ignorecase: bool) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(grep::grep(df, pattern, ignorecase));
        }
        self
    }
    
    pub fn head(&mut self, number: usize) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(head::head(df, number));
        }
        self
    }
    
    pub fn tail(&mut self, number: usize) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(tail::tail(df, number));
        }
        self
    }
    
    pub fn sort(&mut self, colnames: &[String], desc: bool) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(sort::sort(df, colnames, desc));
        }
        self
    }
    
    pub fn count(&mut self) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(count::count(df));
        }
        self
    }
    
    pub fn uniq(&mut self, colnames: &[String]) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(uniq::uniq(df, colnames));
        }
        self
    }
    
    pub fn changetz(&mut self, colname: &str, tz_from: &str, tz_to: &str, dt_format: Option<&str>) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(changetz::changetz(df, colname, tz_from, tz_to, dt_format));
        }
        self
    }
    
    pub fn renamecol(&mut self, colname: &str, new_colname: &str) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(renamecol::renamecol(df, colname, new_colname));
        }
        self
    }
    
    #[allow(dead_code)]
    pub fn drop(&mut self) -> &mut Self {
        self.df = None;
        self
    }
    
    // -- finalizers --
    pub fn headers(&self, plain: bool) {
        if let Some(df) = &self.df {
            headers::headers(df, plain);
        }
    }
    
    pub fn stats(&self) {
        if let Some(df) = &self.df {
            stats::stats(df);
        }
    }
    
    pub fn showquery(&self) {
        if let Some(df) = &self.df {
            showquery::showquery(df);
        }
    }
    
    pub fn show(&self) {
        if let Some(df) = &self.df {
            show::show(df);
        }
    }
    
    pub fn showtable(&self) {
        if let Some(df) = &self.df {
            showtable::showtable(df);
        }
    }
    
    pub fn dump(&self, path: Option<&str>) {
        if let Some(df) = &self.df {
            dump::dump(df, path);
        }
    }
    
    // -- quilters --
    pub fn quilt(&mut self, config_path: &str, output_path: Option<&str>, title: Option<&str>) {
        // If data is empty, the quilt function will attempt to load from the config file
        quilt::quilt(self, config_path, output_path, title);
    }
    
    pub fn quilt_visualize(&self, config_path: &str, output_path: Option<&str>, title: Option<&str>) {
        quilt_visualize::quilt_visualize(config_path, output_path, title);
    }
}

// データフレームユーティリティ関数
pub fn exists_colname(df: &LazyFrame, colnames: &[String]) -> bool {
    // clone()を使用して所有権問題を解決
    let schema = match df.clone().schema() {
        Ok(schema) => schema,
        Err(e) => {
            eprintln!("Error getting schema: {}", e);
            return false;
        }
    };
    
    // SmartStringをStringに変換
    let headers: Vec<String> = schema.iter().map(|(name, _)| name.to_string()).collect();
    
    for colname in colnames {
        if colname.contains('-') {
            // "-"区切りの列範囲指定の場合
            let parts: Vec<&str> = colname.split('-').collect();
            if parts.len() == 2 {
                let start = parts[0];
                let end = parts[1];
                
                if !headers.contains(&start.to_string()) {
                    LogController::error(&format!("Column '{}' not found in headers. Available columns: {}", 
                        start, headers.join(", ")));
                    return false;
                }
                
                if !headers.contains(&end.to_string()) {
                    LogController::error(&format!("Column '{}' not found in headers. Available columns: {}", 
                        end, headers.join(", ")));
                    return false;
                }
            }
        } else if !headers.contains(colname) {
            // 通常の列名指定の場合
            LogController::error(&format!("Column '{}' not found in headers. Available columns: {}", 
                colname, headers.join(", ")));
            return false;
        }
    }
    
    true
}

pub fn parse_column_ranges(df: &LazyFrame, colnames: &[String]) -> Vec<String> {
    // clone()を使用して所有権問題を解決
    let schema = match df.clone().schema() {
        Ok(schema) => schema,
        Err(e) => {
            eprintln!("Error getting schema: {}", e);
            return vec![];
        }
    };
    
    // SmartStringをStringに変換
    let headers: Vec<String> = schema.iter().map(|(name, _)| name.to_string()).collect();
    let mut expanded_columns = Vec::new();
    
    for colname in colnames {
        if colname.contains(',') {
            // カンマ区切りの複数列指定
            for col in colname.split(',') {
                if !col.trim().is_empty() {
                    expanded_columns.push(col.trim().to_string());
                }
            }
        } else if colname.contains('-') {
            // ハイフン区切りの列範囲指定
            let parts: Vec<&str> = colname.split('-').collect();
            if parts.len() == 2 {
                let start = parts[0];
                let end = parts[1];
                
                let mut in_range = false;
                for header in &headers {
                    if header == start {
                        in_range = true;
                    }
                    
                    if in_range {
                        expanded_columns.push(header.clone());
                    }
                    
                    if header == end {
                        in_range = false;
                    }
                }
            }
        } else {
            // 通常の単一列指定
            expanded_columns.push(colname.clone());
        }
    }
    
    expanded_columns
}