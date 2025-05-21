use polars::prelude::*;
use crate::controllers::log::LogController;
use crate::controllers::dataframe::exists_colname;

pub fn uniq(df: &LazyFrame, colnames: &[String]) -> LazyFrame {
    if !exists_colname(df, colnames) {
        eprintln!("Error: One or more column names do not exist in the DataFrame");
        std::process::exit(1);
    }
    
    LogController::debug(&format!("Removing duplicates based on columns: [{}]", 
        colnames.join(", ")
    ));
    
    // 新しいAPIに合わせて修正
    // 文字列のカラム名をそのまま渡す
    let subset = colnames.to_vec();
    
    // Polars 0.33では、uniqueはOption<Vec<String>>を受け取ります
    df.clone().unique(Some(subset), UniqueKeepStrategy::First)
}