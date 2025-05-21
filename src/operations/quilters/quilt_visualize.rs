use std::path::Path;
use std::fs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::controllers::dataframe::DataFrameController;
use crate::controllers::log::LogController;
use crate::operations::quilters::quilt::{QuiltConfig, StageConfig};

pub fn quilt_visualize(config_path: &str, output_path: Option<&str>, title: Option<&str>) {
    // YAMLファイルを読み込み
    let config_path = Path::new(config_path);
    let config_content = match fs::read_to_string(config_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading config file {}: {}", config_path.display(), e);
            std::process::exit(1);
        }
    };

    // YAMLをパース
    let mut quilt_config: QuiltConfig = match serde_yaml::from_str(&config_content) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error parsing YAML config: {}", e);
            std::process::exit(1);
        }
    };

    // タイトルが指定されていれば上書き
    if let Some(t) = title {
        quilt_config.title = t.to_string();
    }

    // データを読み込む
    let mut controller = DataFrameController::new();
    
    // 設定ファイルからデータソース（CSV）を読み込む
    let mut data_loaded = false;
    for (_, stage_config) in &quilt_config.stages {
        if stage_config.stage_type == "load" {
            if let Some(source) = &stage_config.source {
                // ソースファイルのパスを解決
                let source_path = Path::new(source);
                let path = if source_path.is_absolute() {
                    source_path.to_path_buf()
                } else {
                    // 相対パスの場合は設定ファイルからの相対パスとして解決
                    config_path.parent()
                        .unwrap_or(Path::new("."))
                        .join(source_path)
                };
                
                LogController::debug(&format!("Loading data from: {}", path.display()));
                controller.load(&[path], ",", false);
                data_loaded = true;
                break; // 最初に見つかったloadステージだけを処理
            }
        }
    }

    // データが読み込まれなかった場合、テスト用のデフォルトデータをロード
    if !data_loaded {
        LogController::warn("No data was loaded from config, using default test data");
        let default_data = config_path.parent()
            .unwrap_or(Path::new("."))
            .join("../sample/simple.csv");
        
        if default_data.exists() {
            LogController::info(&format!("Loading default test data from: {}", default_data.display()));
            controller.load(&[default_data], ",", false);
        } else {
            eprintln!("Error: No data loaded and default test data not found");
            std::process::exit(1);
        }
    }

    LogController::info(&format!("Visualizing quilt '{}'", quilt_config.title));
    
    // 可視化処理のプレースホルダ - 実際の実装はここに追加
    LogController::info("Generating visualization...");
    
    // 結果の表示
    controller.showtable();
    
    // 出力パスが指定されていれば結果を保存
    if let Some(path) = output_path {
        LogController::info(&format!("Saving visualization to: {}", path));
        controller.dump(Some(path));
    }
}