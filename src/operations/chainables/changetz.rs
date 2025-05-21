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
    
    // デバッグ情報を追加
    LogController::info(&format!("Attempting to change timezone for column {} from {} to {}", colname, tz_from, tz_to));
    
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
        
        // 入力文字列のロギング
        LogController::debug(&format!("Processing date string: '{}'", s));
        
        // 日時をパース
        let naive_dt = if let Some(fmt) = &dt_format_owned {
            // フォーマット指定がある場合は、それを使ってパース
            LogController::debug(&format!("Parsing with specified format: '{}'", fmt));
            
            match NaiveDateTime::parse_from_str(s, fmt) {
                Ok(dt) => dt,
                Err(e) => {
                    LogController::debug(&format!("Failed with specified format, trying default formats: {}", e));
                    // 指定フォーマットで失敗した場合は標準フォーマットを試す
                    parse_with_standard_formats(s)
                }
            }
        } else {
            // フォーマット指定がない場合は標準フォーマットを試す
            parse_with_standard_formats(s)
        };
        
        // タイムゾーンを適用してからターゲットタイムゾーンに変換
        let dt_utc = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);
        let dt_with_source_tz = dt_utc.with_timezone(&source_tz);
        let converted = dt_with_source_tz.with_timezone(&target_tz);
        
        // フォーマットして返す
        if let Some(fmt) = &dt_format_owned {
            // 出力フォーマットを使用
            let result = converted.format(fmt).to_string();
            LogController::debug(&format!("Formatted result: '{}'", result));
            result
        } else {
            // デフォルトフォーマットで出力
            let result = converted.format("%Y-%m-%d %H:%M:%S %Z").to_string();
            LogController::debug(&format!("Default formatted result: '{}'", result));
            result
        }
    };
    
    // 標準フォーマットでのパース関数
    fn parse_with_standard_formats(s: &str) -> NaiveDateTime {
        // 日本語形式のCSVで使われる形式 (YYYY/MM/DD HH:MM:SS)
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S") {
            LogController::debug(&format!("Parsed with YYYY/MM/DD HH:MM:SS"));
            return dt;
        }
        
        // ISO 8601形式
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
            return dt;
        }
        
        // 一般的な日付フォーマット
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return dt;
        }
        
        // アメリカ形式
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%m/%d/%Y %H:%M:%S") {
            return dt;
        }
        
        // 短い時間形式
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M") {
            return dt;
        }
        
        // 最終手段
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%m/%d/%Y %H:%M") {
            return dt;
        }
        
        // どのフォーマットでもパースできなかった場合はエラーメッセージを表示して現在時刻を返す
        LogController::error(&format!("Failed to parse date '{}' with any format, using current time", s));
        Utc::now().naive_utc()
    }
    
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