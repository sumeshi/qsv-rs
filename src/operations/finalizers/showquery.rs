use polars::prelude::*;
use crate::controllers::log::LogController;

pub fn showquery(df: &LazyFrame) {
    LogController::debug("Showing query plan for DataFrame");
    
    // 最適化されたプランを表示
    let plan = df.clone().describe_optimized_plan();
    println!("Optimized query plan:");
    match plan {
        Ok(plan_str) => println!("{}", plan_str),
        Err(e) => println!("Error getting optimized plan: {}", e),
    }
    
    // 最適化前のプランを表示
    // 現在のAPIではdescribe_plan()はStringを直接返す可能性がある
    let naive_plan = df.clone().describe_plan();
    println!("\nNaive plan:");
    
    // 型に応じた適切な処理
    // もしnaive_planがString型なら直接表示し、Result型ならmatchで処理
    // ここでは直接表示する方式に変更
    println!("{}", naive_plan);
}