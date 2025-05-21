use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn count(df: &LazyFrame) -> LazyFrame {
    LogController::debug("Counting duplicate rows, grouping by all columns");
    
    // 新しいAPIに合わせて、スキーマをcloneして取得し、group_byメソッドを使用
    // 事前にスキーマを収集
    let schema = match df.schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema: {}", e);
            std::process::exit(1);
        }
    };
    
    // 全ての列でグループ化し、カウントする
    let cols: Vec<Expr> = schema.iter()
        .map(|(name, _)| col(name.as_ref()))
        .collect();
    
    // countは引数を取るのではなく、単に関数としてaggの中で使用
    // countは独自関数ではなく、polars::prelude::count()を使用
    df.clone().group_by(cols).agg([polars::prelude::count().alias("count")])
}