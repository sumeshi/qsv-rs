use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn showquery(df: &LazyFrame) {
    LogController::debug("Showing query plan for DataFrame");

    // Logical plan
    let logical_plan_result = df.clone().describe_plan();
    println!("Logical query plan:");
    match logical_plan_result {
        Ok(logical_plan) => println!("{}", logical_plan),
        Err(e) => println!("Error getting logical plan: {}", e),
    }

    // Optimized plan
    let optimized_plan_result = df.clone().describe_optimized_plan();
    println!("\nOptimized query plan:");
    match optimized_plan_result {
        Ok(optimized_plan) => println!("{}", optimized_plan),
        Err(e) => println!("Error getting optimized plan: {}", e),
    }
}
