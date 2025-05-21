use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use crate::controllers::dataframe::DataFrameController;
use crate::controllers::log::LogController;

#[derive(Debug, Serialize, Deserialize)]
struct QuiltConfig {
    title: String,
    description: Option<String>,
    version: Option<String>,
    author: Option<String>,
    stages: HashMap<String, StageConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StageConfig {
    #[serde(rename = "type")]
    stage_type: String,
    source: Option<String>,
    steps: Option<HashMap<String, serde_yaml::Value>>,
    sources: Option<Vec<String>>,
    params: Option<HashMap<String, serde_yaml::Value>>,
}

pub fn quilt(controller: &mut DataFrameController, config: &str, paths: &[PathBuf], debug: bool) {
    // YAMLファイルを読み込み
    let config_path = Path::new(config);
    let config_content = match fs::read_to_string(config_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading config file {}: {}", config_path.display(), e);
            std::process::exit(1);
        }
    };

    // YAMLをパース
    let quilt_config: QuiltConfig = match serde_yaml::from_str(&config_content) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error parsing YAML config: {}", e);
            std::process::exit(1);
        }
    };

    LogController::info(&format!(
        "Executing quilt '{}' with {} stages",
        quilt_config.title,
        quilt_config.stages.len()
    ));

    if debug {
        LogController::debug(&format!("Config details: {:?}", quilt_config));
    }

    // とりあえず最初にCSVファイルを読み込むだけの簡易実装
    controller.load(paths, ",", false);
    
    LogController::info("Quilt execution completed");
    
    // 結果を表示
    controller.showtable();
}