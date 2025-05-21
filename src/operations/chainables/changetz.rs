use polars::prelude::*;
use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use std::str::FromStr;

use crate::controllers::log::LogController;
use crate::controllers::dataframe::exists_colname;

pub fn changetz(df: &LazyFrame, colname: &str, tz_from: &str, tz_to: &str, dt_format: Option<&str>) -> LazyFrame {
    if !exists_colname(df, &[colname.to_string()]) {
        eprintln!("Error: Column '{}' not found in DataFrame", colname);
        std::process::exit(1);
    }
    
    // 元のタイムゾーンと変換先のタイムゾーンをパース
    let source_tz = match Tz::from_str(tz_from) {
        Ok(tz) => tz,
        Err(_) => {
            eprintln!("Error: Invalid source timezone '{}'", tz_from);
            std::process::exit(1);
        }
    };
    
    let target_tz = match Tz::from_str(tz_to) {
        Ok(tz) => tz,
        Err(_) => {
            eprintln!("Error: Invalid target timezone '{}'", tz_to);
            std::process::exit(1);
        }
    };
    
    LogController::debug(&format!(
        "Changing timezone of '{}' column from {} to {}", 
        colname, tz_from, tz_to
    ));
    
    // dt_formatをクローンして'staticライフタイムの問題を回避
    let dt_format_owned: Option<String> = dt_format.map(|s| s.to_string());
    
    // 時間文字列を処理する関数を作成
    let time_conversion = move |s: &str| -> String {
        if s.is_empty() {
            return String::new();
        }
        
        // 日時をパース
        let naive_dt = if let Some(fmt) = &dt_format_owned {
            match NaiveDateTime::parse_from_str(s, fmt) {
                Ok(dt) => dt,
                Err(e) => {
                    LogController::debug(&format!("Error parsing date '{}' with format '{}': {}", s, fmt, e));
                    return s.to_string();
                }
            }
        } else {
            // 自動フォーマット検出（簡易実装）
            // ISO 8601形式を試してみる
            match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
                Ok(dt) => dt,
                Err(_) => {
                    // 一般的な日付フォーマットを試す
                    match NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                        Ok(dt) => dt,
                        Err(_) => {
                            // さらに別のフォーマットを試す
                            match NaiveDateTime::parse_from_str(s, "%m/%d/%Y %I:%M:%S %p") {
                                Ok(dt) => dt,
                                Err(_) => {
                                    // MM/DD/YYYY HH:MM形式を試す (Security.csvファイル形式)
                                    match NaiveDateTime::parse_from_str(s, "%m/%d/%Y %H:%M") {
                                        Ok(dt) => dt,
                                        Err(e) => {
                                            LogController::debug(&format!("Error auto-detecting date format for '{}': {}", s, e));
                                            return s.to_string();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
        
        // タイムゾーンを適用してからターゲットタイムゾーンに変換
        let dt_utc = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);
        let dt_with_source_tz = dt_utc.with_timezone(&source_tz);
        let converted = dt_with_source_tz.with_timezone(&target_tz);
        
        // フォーマットして返す
        if let Some(fmt) = &dt_format_owned {
            converted.format(fmt).to_string()
        } else {
            converted.to_rfc3339()
        }
    };
    
    // UDFを作成してDataFrameに適用
    let timezone_udf = move |s: Series| -> PolarsResult<Option<Series>> {
        // utf8メソッドでUTF8データを取得
        let ca = s.utf8()?;
        let result: Vec<String> = ca.into_iter()
            .map(|opt_s| opt_s.map(|s| time_conversion(s)).unwrap_or_default())
            .collect();
        
        // 変換したデータのサンプルをログに出力
        if !result.is_empty() {
            LogController::debug(&format!("Sample converted time: {}", result[0]));
        }
        
        Ok(Some(Series::new(s.name(), result)))
    };
    
    // dfの所有権問題を解決するためにcloneする
    df.clone().with_column(col(colname).map(timezone_udf, GetOutput::from_type(DataType::Utf8)).alias(colname))
}