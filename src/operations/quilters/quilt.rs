use crate::controllers::dataframe::DataFrameController;
use crate::controllers::log::LogController;
use polars::prelude::{col, JoinType, LazyFrame};
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

// Re-import operations to call them directly with LazyFrame
use crate::operations::chainables::{
    changetz, contains, count, grep, head, isin, renamecol, sed, select, sort, tail, uniq,
};
use crate::operations::finalizers::{
    dump as dump_op, headers as headers_op, show as show_op, showquery as showquery_op,
    showtable as showtable_op, stats as stats_op,
};
use crate::operations::initializers::load as load_op;

#[derive(Debug, Serialize, Deserialize)]
pub struct QuiltConfig {
    pub title: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub author: Option<String>,
    pub stages: serde_yaml::Mapping,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StageConfig {
    #[serde(rename = "type")]
    pub stage_type: String,
    pub source: Option<String>,
    pub sources: Option<Vec<String>>,
    pub params: Option<Value>,
    pub steps: Option<serde_yaml::Mapping>,
}

fn get_string_from_value(val: &Value, key: &str) -> Option<String> {
    val.get(key).and_then(|v| v.as_str().map(String::from))
}

fn get_string_vec_from_value(val: &Value, key: &str) -> Option<Vec<String>> {
    val.get(key).and_then(|v| v.as_sequence()).map(|seq| {
        seq.iter()
            .filter_map(|item| item.as_str().map(String::from))
            .collect()
    })
}

fn get_bool_from_value(val: &Value, key: &str) -> bool {
    val.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
}

fn get_usize_from_value(val: &Value, key: &str) -> Option<usize> {
    val.get(key)
        .and_then(|v| v.as_u64().and_then(|u| usize::try_from(u).ok()))
}

pub fn quilt(
    controller: &mut DataFrameController,
    config_path_str: &str,
    cli_input_files: Option<Vec<PathBuf>>,
    output_path_str: Option<&str>,
    title_override: Option<&str>,
) {
    let config_path = Path::new(config_path_str);
    let config_content = match fs::read_to_string(config_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading config file {}: {}", config_path.display(), e);
            std::process::exit(1);
        }
    };

    let mut quilt_config: QuiltConfig = match serde_yaml::from_str(&config_content) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error parsing YAML config: {}", e);
            std::process::exit(1);
        }
    };

    if let Some(t) = title_override {
        quilt_config.title = t.to_string();
    }

    LogController::info(&format!(
        "Executing quilt '{}' with {} stage entries in YAML",
        quilt_config.title,
        quilt_config.stages.len()
    ));

    let mut stage_results: HashMap<String, LazyFrame> = HashMap::new();
    let mut last_processed_df: Option<LazyFrame> = None;

    for (stage_name_val, stage_config_val) in &quilt_config.stages {
        let stage_name = stage_name_val
            .as_str()
            .unwrap_or("unknown_stage")
            .to_string();
        let stage_config: StageConfig = match serde_yaml::from_value(stage_config_val.clone()) {
            Ok(sc) => sc,
            Err(e) => {
                LogController::error(&format!(
                    "Error parsing config for stage '{}': {}. Skipping.",
                    stage_name, e
                ));
                continue;
            }
        };

        LogController::debug(&format!(
            "Processing stage: {} (type: {})",
            stage_name, stage_config.stage_type
        ));

        let mut current_stage_input_df: Option<LazyFrame> = None;

        if let Some(source_name) = &stage_config.source {
            if let Some(df) = stage_results.get(source_name) {
                current_stage_input_df = Some(df.clone());
                LogController::debug(&format!(
                    "Stage '{}' is using data from source stage '{}'",
                    stage_name, source_name
                ));
            } else {
                LogController::error(&format!(
                    "Source stage '{}' not found for stage '{}'. Skipping stage.",
                    source_name, stage_name
                ));
                continue;
            }
        }

        let mut stage_output_df: Option<LazyFrame> = current_stage_input_df.clone();

        if stage_config.stage_type == "process" {
            if let Some(steps) = &stage_config.steps {
                for (command_name_val, command_args_val) in steps {
                    let command_name = command_name_val.as_str().unwrap_or("");
                    LogController::debug(&format!(
                        "Applying step: {} to stage '{}'",
                        command_name, stage_name
                    ));

                    if command_name != "load" && stage_output_df.is_none() {
                        LogController::error(&format!("No DataFrame available for step '{}' in stage '{}'. Load data first or specify a valid source. Skipping step.", command_name, stage_name));
                        continue;
                    }

                    match command_name {
                        "load" => {
                            let file_to_load_str = get_string_from_value(command_args_val, "path");
                            let mut loaded_df: Option<LazyFrame> = None;

                            if let Some(file_str) = file_to_load_str {
                                let source_path = Path::new(&file_str);
                                let path_to_load = if source_path.is_absolute() {
                                    source_path.to_path_buf()
                                } else {
                                    config_path
                                        .parent()
                                        .unwrap_or_else(|| Path::new("."))
                                        .join(source_path)
                                };
                                LogController::debug(&format!("Loading data from: {} (specified in quilt YAML for stage '{}')", path_to_load.display(), stage_name));
                                loaded_df = Some(load_op::load(&[path_to_load], ",", false));
                            } else if let Some(ref cli_files) = cli_input_files {
                                if stage_output_df.is_none() && !cli_files.is_empty() {
                                    LogController::debug(&format!(
                                        "Loading data from CLI for stage '{}': {:?}",
                                        stage_name, cli_files
                                    ));
                                    loaded_df = Some(load_op::load(cli_files, ",", false));
                                } else if stage_output_df.is_some() {
                                    LogController::debug(&format!("Stage '{}' already has data from source, 'load' step without path will not use CLI files.", stage_name));
                                } else {
                                    LogController::warn(&format!("Load step in YAML for stage '{}' has no path, and no files provided via CLI for this quilt command, or stage already sourced.", stage_name));
                                }
                            } else {
                                LogController::warn(&format!("No data source specified for load in stage '{}'. Trying default test data.", stage_name));
                                let default_data_path = config_path
                                    .parent()
                                    .unwrap_or_else(|| Path::new("."))
                                    .join("../sample/simple.csv");
                                if default_data_path.exists() {
                                    loaded_df =
                                        Some(load_op::load(&[default_data_path], ",", false));
                                }
                            }
                            if let Some(ref new_lf) = loaded_df {
                                stage_output_df = Some(new_lf.clone());
                            } else if stage_output_df.is_none() {
                                LogController::error(&format!("Failed to load any data for stage '{}' via 'load' step and no prior data for stage.", stage_name));
                                continue;
                            }
                        }
                        "select" => {
                            if let Some(ref df) = stage_output_df {
                                if let Some(colnames_str) =
                                    get_string_from_value(command_args_val, "colnames")
                                {
                                    let colnames: Vec<String> = colnames_str
                                        .split(',')
                                        .map(|s| s.trim().to_string())
                                        .collect();
                                    stage_output_df = Some(select::select(df, &colnames));
                                } else if let Some(colnames_vec) =
                                    get_string_vec_from_value(command_args_val, "colnames")
                                {
                                    stage_output_df = Some(select::select(df, &colnames_vec));
                                } else {
                                    LogController::warn("Select step has no 'colnames'.");
                                }
                            }
                        }
                        "isin" => {
                            if let Some(ref df) = stage_output_df {
                                if let (Some(colname), Some(values)) = (
                                    get_string_from_value(command_args_val, "colname"),
                                    get_string_vec_from_value(command_args_val, "values"),
                                ) {
                                    stage_output_df = Some(isin::isin(df, &colname, &values));
                                } else {
                                    LogController::warn("Isin step missing 'colname' or 'values'.");
                                }
                            }
                        }
                        "head" => {
                            if let Some(ref df) = stage_output_df {
                                if let Some(num) = get_usize_from_value(command_args_val, "number")
                                    .or_else(|| {
                                        command_args_val
                                            .as_u64()
                                            .and_then(|u| usize::try_from(u).ok())
                                    })
                                {
                                    stage_output_df = Some(head::head(df, num));
                                } else {
                                    LogController::warn("Head step missing 'number' or invalid value. Defaulting to 5 for head.");
                                    stage_output_df = Some(head::head(df, 5)); // Default to 5
                                }
                            }
                        }
                        "tail" => {
                            if let Some(ref df) = stage_output_df {
                                if let Some(num) = get_usize_from_value(command_args_val, "number")
                                    .or_else(|| {
                                        command_args_val
                                            .as_u64()
                                            .and_then(|u| usize::try_from(u).ok())
                                    })
                                {
                                    stage_output_df = Some(tail::tail(df, num));
                                } else {
                                    LogController::warn("Tail step missing 'number' or invalid value. Defaulting to 5 for tail.");
                                    stage_output_df = Some(tail::tail(df, 5)); // Default to 5
                                }
                            }
                        }
                        "sort" => {
                            if let Some(ref df) = stage_output_df {
                                if let Some(colnames_str) =
                                    get_string_from_value(command_args_val, "colnames")
                                {
                                    let colnames: Vec<String> = colnames_str
                                        .split(',')
                                        .map(|s| s.trim().to_string())
                                        .collect();
                                    let desc = get_bool_from_value(command_args_val, "desc");
                                    stage_output_df = Some(sort::sort(df, &colnames, desc));
                                } else if let Some(colnames_vec) =
                                    get_string_vec_from_value(command_args_val, "colnames")
                                {
                                    let desc = get_bool_from_value(command_args_val, "desc");
                                    stage_output_df = Some(sort::sort(df, &colnames_vec, desc));
                                } else {
                                    LogController::warn("Sort step has no 'colnames'.");
                                }
                            }
                        }
                        "changetz" => {
                            if let Some(ref df) = stage_output_df {
                                if let (Some(colname), Some(tz_from), Some(tz_to)) = (
                                    get_string_from_value(command_args_val, "colname"),
                                    get_string_from_value(command_args_val, "tz_from"),
                                    get_string_from_value(command_args_val, "tz_to"),
                                ) {
                                    let dt_format =
                                        get_string_from_value(command_args_val, "dt_format");
                                    let ambiguous =
                                        get_string_from_value(command_args_val, "ambiguous");
                                    stage_output_df = Some(changetz::changetz(
                                        df,
                                        &colname,
                                        &tz_from,
                                        &tz_to,
                                        dt_format.as_deref().unwrap_or("auto"),
                                        ambiguous.as_deref().unwrap_or("earliest"),
                                    ));
                                } else {
                                    LogController::warn(
                                        "Changetz step missing 'colname', 'tz_from', or 'tz_to'.",
                                    );
                                }
                            }
                        }
                        "renamecol" => {
                            if let Some(ref df) = stage_output_df {
                                if let (Some(old_name), Some(new_name)) = (
                                    get_string_from_value(command_args_val, "from").or_else(|| {
                                        get_string_from_value(command_args_val, "old_name")
                                    }),
                                    get_string_from_value(command_args_val, "to").or_else(|| {
                                        get_string_from_value(command_args_val, "new_name")
                                    }),
                                ) {
                                    stage_output_df =
                                        Some(renamecol::renamecol(df, &old_name, &new_name));
                                } else {
                                    LogController::warn("Renamecol step missing 'from'/'old_name' or 'to'/'new_name'.");
                                }
                            }
                        }
                        "count" => {
                            if let Some(ref df) = stage_output_df {
                                stage_output_df = Some(count::count(df));
                            }
                        }
                        "uniq" => {
                            if let Some(ref df) = stage_output_df {
                                let colnames = get_string_from_value(command_args_val, "colnames")
                                    .map(|s| s.split(',').map(|s| s.trim().to_string()).collect())
                                    .or_else(|| {
                                        get_string_vec_from_value(command_args_val, "colnames")
                                    });
                                stage_output_df = Some(uniq::uniq(df, colnames.as_deref()));
                            }
                        }
                        "grep" => {
                            if let Some(ref df) = stage_output_df {
                                if let Some(pattern) =
                                    get_string_from_value(command_args_val, "pattern")
                                {
                                    let ignorecase =
                                        get_bool_from_value(command_args_val, "ignorecase");
                                    let inverted =
                                        get_bool_from_value(command_args_val, "invert_match");
                                    stage_output_df =
                                        Some(grep::grep(df, &pattern, ignorecase, inverted));
                                } else {
                                    LogController::warn("Grep step missing 'pattern'.");
                                }
                            }
                        }
                        "contains" => {
                            if let Some(ref df) = stage_output_df {
                                if let (Some(colname), Some(pattern)) = (
                                    get_string_from_value(command_args_val, "colname"),
                                    get_string_from_value(command_args_val, "pattern"),
                                ) {
                                    let ignorecase =
                                        get_bool_from_value(command_args_val, "ignorecase");
                                    stage_output_df = Some(contains::contains(
                                        df, &colname, &pattern, ignorecase,
                                    ));
                                } else {
                                    LogController::warn(
                                        "Contains step missing 'colname' or 'pattern'.",
                                    );
                                }
                            }
                        }
                        "sed" => {
                            if let Some(ref df) = stage_output_df {
                                if let (Some(colname), Some(pattern), Some(replacement)) = (
                                    get_string_from_value(command_args_val, "colname"),
                                    get_string_from_value(command_args_val, "pattern"),
                                    get_string_from_value(command_args_val, "replacement"),
                                ) {
                                    let ignorecase =
                                        get_bool_from_value(command_args_val, "ignorecase");
                                    stage_output_df = Some(sed::sed(
                                        df,
                                        &colname,
                                        &pattern,
                                        &replacement,
                                        ignorecase,
                                    ));
                                } else {
                                    LogController::warn(
                                        "Sed step missing 'colname', 'pattern', or 'replacement'.",
                                    );
                                }
                            }
                        }
                        "showtable" => {
                            if let Some(df) = &stage_output_df {
                                showtable_op::showtable(df);
                            } else {
                                LogController::warn("No DataFrame to showtable for stage.");
                            }
                        }
                        "show" => {
                            if let Some(df) = &stage_output_df {
                                show_op::show(df);
                            } else {
                                LogController::warn("No DataFrame to show for stage.");
                            }
                        }
                        "headers" => {
                            if let Some(df) = &stage_output_df {
                                let plain = get_bool_from_value(command_args_val, "plain");
                                headers_op::headers(df, plain);
                            } else {
                                LogController::warn("No DataFrame for headers for stage.");
                            }
                        }
                        "stats" => {
                            if let Some(df) = &stage_output_df {
                                stats_op::stats(df);
                            } else {
                                LogController::warn("No DataFrame for stats for stage.");
                            }
                        }
                        "showquery" => {
                            if let Some(df) = &stage_output_df {
                                showquery_op::showquery(df);
                            } else {
                                LogController::warn("No DataFrame for showquery for stage.");
                            }
                        }
                        "dump" => {
                            if let Some(df) = &stage_output_df {
                                let path_from_yaml =
                                    get_string_from_value(command_args_val, "path").or_else(|| {
                                        get_string_from_value(command_args_val, "output")
                                    });

                                let separator_char =
                                    get_string_from_value(command_args_val, "separator")
                                        .and_then(|s| s.chars().next());

                                if let Some(p) = path_from_yaml {
                                    let dump_path_resolved = if Path::new(&p).is_absolute() {
                                        PathBuf::from(p)
                                    } else {
                                        config_path
                                            .parent()
                                            .unwrap_or_else(|| Path::new("."))
                                            .join(p)
                                    };
                                    dump_op::dump(
                                        df,
                                        dump_path_resolved.to_str().unwrap_or_default(),
                                        separator_char.unwrap_or(','),
                                    );
                                } else {
                                    LogController::warn("Dump step in YAML missing 'path' or 'output'. Will not dump from this step.");
                                }
                            } else {
                                LogController::warn("No DataFrame to dump for stage.");
                            }
                        }
                        _ => {
                            // LogController::warn(&format!("Unknown or unsupported step in 'process' stage: {}", command_name));
                            LogController::error(&format!("Error: Unknown or unsupported step '{}' in 'process' stage '{}'. Halting quilt execution.", command_name, stage_name));
                            eprintln!("Error: Unknown or unsupported step '{}' in 'process' stage '{}'. See qsv logs for more details.", command_name, stage_name);
                            std::process::exit(1); // Halt processing
                        }
                    }
                }
            } else {
                LogController::warn(&format!(
                    "Stage '{}' is of type 'process' but has no steps defined.",
                    stage_name
                ));
            }
        } else if stage_config.stage_type == "concat" {
            LogController::warn(&format!(
                "Stage type 'concat' for stage '{}' is not yet implemented.",
                stage_name
            ));
        } else if stage_config.stage_type == "join" {
            if let Some(sources_string_vec) = &stage_config.sources {
                if sources_string_vec.len() == 2 {
                    let left_name: &str = sources_string_vec[0].as_str();
                    let right_name: &str = sources_string_vec[1].as_str();

                    if left_name.is_empty() || right_name.is_empty() {
                        LogController::error(&format!(
                            "Join stage '{}' has empty source names. Skipping.",
                            stage_name
                        ));
                        continue;
                    }

                    if let (Some(left_df), Some(right_df)) =
                        (stage_results.get(left_name), stage_results.get(right_name))
                    {
                        let join_params = stage_config.params.as_ref();
                        let how_str = join_params
                            .and_then(|p| get_string_from_value(p, "how"))
                            .unwrap_or_else(|| "inner".to_string());
                        let key_col_name = join_params
                            .and_then(|p| get_string_from_value(p, "key"))
                            .or_else(|| join_params.and_then(|p| get_string_from_value(p, "on")));

                        if key_col_name.is_none() {
                            LogController::error(&format!(
                                "Join stage '{}' missing 'key' (or 'on') parameter. Skipping.",
                                stage_name
                            ));
                            continue;
                        }
                        let key = key_col_name.unwrap();

                        let join_type = match how_str.to_lowercase().as_str() {
                            "inner" => JoinType::Inner,
                            "left" => JoinType::Left,
                            "outer" | "full" => JoinType::Full,
                            _ => {
                                LogController::warn(&format!("Unsupported join type '{}' for stage '{}'. Defaulting to inner join.", how_str, stage_name));
                                JoinType::Inner
                            }
                        };

                        let coalesce = join_params
                            .and_then(|p| p.get("coalesce"))
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);

                        let mut join_args = polars::prelude::JoinArgs::new(join_type);
                        if coalesce {
                            join_args = join_args
                                .with_coalesce(polars::prelude::JoinCoalesce::CoalesceColumns);
                        }

                        let joined_df_result = left_df.clone().join(
                            right_df.clone(),
                            &[col(&key)],
                            &[col(&key)],
                            join_args,
                        );
                        stage_output_df = Some(joined_df_result); // Result is a LazyFrame, not Result<LazyFrame, Error>
                        LogController::debug(&format!(
                            "Join stage '{}' completed using key '{}', type '{}', coalesce: {}",
                            stage_name, key, how_str, coalesce
                        ));
                    } else {
                        let mut missing_sources = Vec::new();
                        if !stage_results.contains_key(left_name) {
                            missing_sources.push(left_name);
                        }
                        if !stage_results.contains_key(right_name) {
                            missing_sources.push(right_name);
                        }
                        LogController::error(&format!("Could not find source DataFrame(s): {:?} for join stage '{}'. Skipping.", missing_sources, stage_name));
                        continue;
                    }
                } else {
                    LogController::error(&format!(
                        "Join stage '{}' must have exactly two sources. Found {}. Skipping.",
                        stage_name,
                        sources_string_vec.len()
                    ));
                    continue;
                }
            } else {
                LogController::error(&format!(
                    "Join stage '{}' is missing 'sources' attribute. Skipping.",
                    stage_name
                ));
                continue;
            }
        } else {
            LogController::warn(&format!(
                "Unknown stage type: {} for stage '{}'",
                stage_config.stage_type, stage_name
            ));
        }

        if let Some(df_to_store) = &stage_output_df {
            stage_results.insert(stage_name.clone(), df_to_store.clone());
            last_processed_df = Some(df_to_store.clone());
            LogController::debug(&format!(
                "Finished processing stage '{}'. Result stored.",
                stage_name
            ));
        } else {
            LogController::warn(&format!(
                "Stage '{}' did not produce a DataFrame.",
                stage_name
            ));
        }
    }

    LogController::info(&format!(
        "Quilt '{}' execution processing finished.",
        quilt_config.title
    ));

    if let Some(path_str) = output_path_str {
        if let Some(final_df_to_dump) = last_processed_df {
            LogController::info(&format!("Saving final quilt output to: {}", path_str));
            let final_output_path = Path::new(path_str);
            let absolute_path = if final_output_path.is_absolute() {
                final_output_path.to_path_buf()
            } else {
                std::env::current_dir()
                    .unwrap_or_else(|_| Path::new(".").to_path_buf())
                    .join(final_output_path)
            };

            if let Some(parent) = absolute_path.parent() {
                if !parent.exists() {
                    if let Err(e) = std::fs::create_dir_all(parent) {
                        eprintln!("Error creating directory {}: {}", parent.display(), e);
                    }
                }
            }
            dump_op::dump(
                &final_df_to_dump,
                absolute_path.to_str().unwrap_or(path_str),
                ',',
            );
        } else {
            LogController::warn(
                "No final DataFrame from quilt execution to save for --output CLI option.",
            );
        }
    } else {
        // If no CLI output, the last stage might have a showtable or show.
        // If not, and if the main qsv CLI expects something in controller.df, we might set it.
        // For now, if no --output, rely on YAML steps for display.
        if let Some(final_df_state) = last_processed_df {
            // If no output path and no explicit display in last stage, perhaps default to showtable?
            // This depends on how quilt is meant to integrate with the main qsv loop's default display.
            // For now, we ensure the main `controller`'s `df` is updated so `qsv` can show it if quilt is the last command.
            controller.set_df(final_df_state);
        }
        LogController::debug("Quilt finished. Output handled by steps in YAML or by main CLI flow if no explicit output/show in YAML.");
    }
}
