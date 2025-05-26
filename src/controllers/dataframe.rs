use polars::prelude::*;
use std::path::PathBuf;
use crate::operations;
use crate::operations::initializers::load;
use crate::operations::chainables::{select, head, tail, isin, contains, sed, grep, sort, count, uniq, changetz, renamecol};
use crate::operations::finalizers::{headers, stats, showquery, show, showtable, dump};
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
    
    pub fn grep(&mut self, pattern: &str, ignorecase: bool, is_inverted: bool) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(grep::grep(df, pattern, ignorecase, is_inverted));
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
    
    pub fn uniq(&mut self, colnames: Option<Vec<String>>) -> &mut Self {
        if let Some(df) = &self.df {
            self.df = Some(uniq::uniq(df, colnames.as_deref()));
        }
        self
    }
    
    pub fn changetz(&mut self, colname: &str, tz_from: &str, tz_to: &str, dt_format: Option<&str>, ambiguous_time: Option<&str>) -> &mut Self {
        if let Some(df) = &self.df {
            let format_str = dt_format.unwrap_or("auto");
            let ambiguous_str = ambiguous_time.unwrap_or("earliest");
            self.df = Some(changetz::changetz(df, colname, tz_from, tz_to, format_str, ambiguous_str));
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
    
    pub fn dump(&self, path: Option<&str>, separator: Option<char>) {
        if let Some(df) = &self.df {
            let output_path_str = path.unwrap_or("output.csv");
            let sep_char = separator.unwrap_or(',');
            dump::dump(df, output_path_str, sep_char);
        }
    }
    
    pub fn set_df(&mut self, df: LazyFrame) {
        self.df = Some(df);
    }
    
    // -- quilters --
    #[allow(dead_code)]
    pub fn quilt(&mut self, config_path: &str, cli_input_files: Option<Vec<PathBuf>>, output_path: Option<&str>, title: Option<&str>) {
        // Ensure this matches the signature and logic of the actual quilt operation function
        // For now, this acts as a potential wrapper if controller needs to expose it directly.
        // The main CLI path currently calls operations::quilters::quilt::quilt directly.
        operations::quilters::quilt::quilt(self, config_path, cli_input_files, output_path, title);
    }
}

// DataFrame utility functions

// Function to parse column names including ranges like "col1-col3"
// clone() is used to resolve ownership issues when modifying self.df
#[allow(dead_code)]
pub fn parse_column_ranges(df: &LazyFrame, colnames_input: &[String]) -> Vec<String> {
    let mut final_colnames = Vec::new();
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            LogController::error(&format!("Failed to collect DataFrame for schema check in parse_column_ranges: {}", e));
            return Vec::new();
        }
    };
    let schema_ref = collected_df.schema(); // Get Schema from DataFrame
    let all_schema_colnames: Vec<String> = schema_ref.iter_names().map(|s| s.to_string()).collect();

    for colname_pattern in colnames_input {
        if colname_pattern.contains(',') {
            // Handling comma-separated multiple column specification
            // For example: "col1,col2,col5"
            final_colnames.extend(colname_pattern.split(',').map(|s| s.trim().to_string()));
        } else if colname_pattern.contains('-') {
            // Handling hyphen-separated column range specification
            // For example: "col1-col3" or "prefix1-prefix5"
            let parts: Vec<&str> = colname_pattern.split('-').collect();
            if parts.len() == 2 {
                let start = parts[0];
                let end = parts[1];
                
                let mut in_range = false;
                for header in &all_schema_colnames {
                    if header == start {
                        in_range = true;
                    }
                    
                    if in_range {
                        final_colnames.push(header.clone());
                    }
                    
                    if header == end {
                        in_range = false;
                    }
                }
            }
        } else {
            // Standard single column specification
            // For example: "col1"
            final_colnames.push(colname_pattern.to_string());
        }
    }
    
    final_colnames
}

// Method to apply a finalizer operation