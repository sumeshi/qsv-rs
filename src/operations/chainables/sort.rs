use polars::prelude::*;
use crate::controllers::log::LogController;
use crate::controllers::dataframe::exists_colname;

pub fn sort(df: &LazyFrame, colnames: &[String], desc: bool) -> LazyFrame {
    if !exists_colname(df, colnames) {
        eprintln!("Error: One or more column names do not exist in the DataFrame");
        std::process::exit(1);
    }
    
    LogController::debug(&format!("Sorting by columns: [{}] (descending: {})", 
        colnames.join(", "), desc
    ));
    
    // ソート式を作成
    let sort_exprs: Vec<_> = colnames.iter()
        .map(|colname| {
            if desc {
                col(colname).sort(true)
            } else {
                col(colname).sort(false)
            }
        })
        .collect();
    
    // 新しいAPI: sort_by_exprsは4つの引数が必要で、descending引数はベクトルが必要
    // dfの所有権問題を解決するためにcloneする
    let descending = vec![desc; colnames.len()];
    df.clone().sort_by_exprs(sort_exprs, descending, false, false)
}