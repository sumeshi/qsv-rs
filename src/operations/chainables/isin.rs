use polars::prelude::*;
use crate::controllers::log::LogController;
use crate::controllers::dataframe::exists_colname;

pub fn isin(df: &LazyFrame, colname: &str, values: &[String]) -> LazyFrame {
    if !exists_colname(df, &[colname.to_string()]) {
        eprintln!("Error: Column '{}' not found in DataFrame", colname);
        std::process::exit(1);
    }
    
    LogController::debug(&format!("Filtering rows where '{}' column contains any of [{}]", 
        colname, 
        values.join(", ")
    ));
    
    // 基本的な論理演算を使用して「いずれかに等しい」条件を作成
    // OR演算子で複数の等価条件を結合
    let mut filter_expr = lit(false);  // 初期値はfalse
    
    for value in values {
        // 各値との等価条件を作成し、ORで結合
        filter_expr = filter_expr.or(col(colname).cast(DataType::Utf8).eq(lit(value.clone())));
    }
    
    df.clone().filter(filter_expr)
}