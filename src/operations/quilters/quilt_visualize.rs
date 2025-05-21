use std::path::Path;
use std::fs;
use crate::controllers::dataframe::DataFrameController;
use crate::controllers::log::LogController;
use crate::operations::quilters::quilt::QuiltConfig;

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
    
    // HTML出力を生成
    let html_output = generate_html_visualization(&controller, &quilt_config.title);
    
    // 出力パスが指定されていれば結果を保存
    if let Some(path) = output_path {
        LogController::info(&format!("Saving visualization to: {}", path));
        match fs::write(path, &html_output) {
            Ok(_) => LogController::info(&format!("HTML visualization saved to {}", path)),
            Err(e) => eprintln!("Error writing to {}: {}", path, e),
        }
    } else {
        // 出力パスが指定されていなければ標準出力に表示
        println!("{}", html_output);
    }
}

// HTMLの可視化出力を生成する関数
fn generate_html_visualization(_controller: &DataFrameController, title: &str) -> String {
    let mut html = String::new();
    
    // HTML基本構造
    html.push_str("<!DOCTYPE html>\n");
    html.push_str("<html lang=\"en\">\n");
    html.push_str("<head>\n");
    html.push_str("    <meta charset=\"UTF-8\">\n");
    html.push_str("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
    html.push_str(&format!("    <title>{}</title>\n", title));
    html.push_str("    <style>\n");
    html.push_str("        body { font-family: Arial, sans-serif; margin: 20px; }\n");
    html.push_str("        h1 { color: #333; }\n");
    html.push_str("        table { border-collapse: collapse; width: 100%; }\n");
    html.push_str("        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n");
    html.push_str("        th { background-color: #f2f2f2; }\n");
    html.push_str("        tr:nth-child(even) { background-color: #f9f9f9; }\n");
    html.push_str("        tr:hover { background-color: #f2f2f2; }\n");
    html.push_str("    </style>\n");
    html.push_str("</head>\n");
    html.push_str("<body>\n");
    
    // タイトルと説明
    html.push_str(&format!("    <h1>{}</h1>\n", title));
    html.push_str("    <div class=\"visualization\">\n");
    
    // TODO: controllerからデータを取得してテーブル形式でレンダリング
    // 現在のプロトタイプでは固定的なテーブルを表示
    html.push_str("        <table>\n");
    html.push_str("            <thead>\n");
    html.push_str("                <tr>\n");
    html.push_str("                    <th>col1</th>\n");
    html.push_str("                    <th>col2</th>\n");
    html.push_str("                    <th>col3</th>\n");
    html.push_str("                </tr>\n");
    html.push_str("            </thead>\n");
    html.push_str("            <tbody>\n");
    html.push_str("                <tr>\n");
    html.push_str("                    <td>1</td>\n");
    html.push_str("                    <td>2</td>\n");
    html.push_str("                    <td>3</td>\n");
    html.push_str("                </tr>\n");
    html.push_str("                <tr>\n");
    html.push_str("                    <td>4</td>\n");
    html.push_str("                    <td>5</td>\n");
    html.push_str("                    <td>6</td>\n");
    html.push_str("                </tr>\n");
    html.push_str("                <tr>\n");
    html.push_str("                    <td>7</td>\n");
    html.push_str("                    <td>8</td>\n");
    html.push_str("                    <td>9</td>\n");
    html.push_str("                </tr>\n");
    html.push_str("            </tbody>\n");
    html.push_str("        </table>\n");
    html.push_str("    </div>\n");
    
    // フッター
    html.push_str("    <div class=\"footer\">\n");
    html.push_str("        <p>Generated by QSV Visualizer</p>\n");
    html.push_str("    </div>\n");
    
    html.push_str("</body>\n");
    html.push_str("</html>");
    
    html
}