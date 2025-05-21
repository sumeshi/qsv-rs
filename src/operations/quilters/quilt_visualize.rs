use std::path::Path;
use std::fs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

pub fn quilt_visualize(config: &str) {
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

    // 構造の可視化
    println!("Quilt Configuration: {}", quilt_config.title);
    if let Some(desc) = &quilt_config.description {
        println!("Description: {}", desc);
    }
    if let Some(ver) = &quilt_config.version {
        println!("Version: {}", ver);
    }
    if let Some(author) = &quilt_config.author {
        println!("Author: {}", author);
    }
    
    println!("\nStages:");
    for (name, stage) in &quilt_config.stages {
        println!("  - {} (type: {})", name, stage.stage_type);
        
        if let Some(source) = &stage.source {
            println!("    source: {}", source);
        }
        
        if let Some(sources) = &stage.sources {
            println!("    sources: {}", sources.join(", "));
        }
        
        if let Some(steps) = &stage.steps {
            println!("    steps:");
            for (step_name, _) in steps {
                println!("      - {}", step_name);
            }
        }
        
        if let Some(params) = &stage.params {
            println!("    params:");
            for (param_name, _) in params {
                println!("      - {}", param_name);
            }
        }
    }
    
    LogController::info("Quilt visualization completed");
}