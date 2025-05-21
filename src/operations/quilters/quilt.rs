use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use crate::controllers::dataframe::DataFrameController;
use crate::controllers::log::LogController;
use crate::controllers::yaml::YamlController;

#[derive(Debug, Serialize, Deserialize)]
pub struct QuiltConfig {
    pub title: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub author: Option<String>,
    pub stages: HashMap<String, StageConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StageConfig {
    #[serde(rename = "type")]
    pub stage_type: String,
    pub source: Option<String>,
    pub steps: Option<HashMap<String, serde_yaml::Value>>,
    pub sources: Option<Vec<String>>,
    pub params: Option<HashMap<String, serde_yaml::Value>>,
}

pub fn quilt(controller: &mut DataFrameController, config_path: &str, output_path: Option<&str>, title: Option<&str>) {
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

    LogController::info(&format!(
        "Executing quilt '{}' with {} stages",
        quilt_config.title,
        quilt_config.stages.len()
    ));

    // データがまだ読み込まれていない場合はデフォルト値を設定
    let mut data_loaded = false;

    // ステージを処理
    for (stage_name, stage_config) in &quilt_config.stages {
        LogController::debug(&format!("Processing stage: {}", stage_name));
        
        // 各ステージタイプに応じた処理
        match stage_config.stage_type.as_str() {
            "load" => {
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
                }
            },
            "transform" => {
                if let Some(steps) = &stage_config.steps {
                    for (step_name, _) in steps {
                        LogController::debug(&format!("Applying transformation: {}", step_name));
                        // 実際の変換処理はここに実装
                    }
                }
            },
            "visualize" => {
                LogController::debug("Generating visualization");
                // 可視化処理はここに実装
            },
            _ => {
                LogController::warn(&format!("Unknown stage type: {}", stage_config.stage_type));
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

    LogController::info("Quilt execution completed");
    
    // 出力パスが指定されていれば結果を保存
    if let Some(path) = output_path {
        LogController::info(&format!("Saving results to: {}", path));
        controller.dump(Some(path));
    } else {
        // 指定がなければ結果を表示
        controller.showtable();
    }
}