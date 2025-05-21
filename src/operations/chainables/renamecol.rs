use polars::prelude::*;
use crate::controllers::log::LogController;
use crate::controllers::dataframe::exists_colname;

pub fn renamecol(df: &LazyFrame, colname: &str, new_colname: &str) -> LazyFrame {
    if !exists_colname(df, &[colname.to_string()]) {
        eprintln!("Error: Column '{}' not found in DataFrame", colname);
        std::process::exit(1);
    }
    
    LogController::debug(&format!("Renaming column '{}' to '{}'", colname, new_colname));
    
    // Polars 0.33では、renameメソッドはMapではなく2つの引数を取ります
    // dfの所有権問題を解決するためにcloneする
    df.clone().rename([colname], [new_colname])
}