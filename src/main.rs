use std::process;
use std::path::PathBuf;

mod controllers;
mod operations;

use controllers::dataframe::DataFrameController;
use controllers::command::{parse_commands, Command, print_help, print_chainable_help};
use controllers::log::LogController;

fn main() {
    // Initialize logger to only show errors by default
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error")).init();
    
    // Get command line arguments
    let args: Vec<String> = std::env::args().collect();
    
    // -h, --help
    if args.len() == 2 && (args[1] == "-h" || args[1] == "--help") {
        print_help();
        return;
    }
    // -v, --version
    if args.len() == 2 && (args[1] == "-v" || args[1] == "--version") {
        println!("qsv version {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    
    // Subcommand help: e.g., qsv select -h
    if args.len() >= 3 && (args[2] == "-h" || args[2] == "--help") {
        print_chainable_help(&args[1]);
        return;
    }
    
    // Parse commands using dedicated command parser module
    let commands = parse_commands(&args[1..]);
    
    if commands.is_empty() {
        eprintln!("Error: No commands provided. Use the format: qsv load file.csv - select col1,col2 - head 5");
        process::exit(1);
    }
    
    // Initialize dataframe controller
    let mut controller = DataFrameController::new();
    
    // Process commands sequentially
    process_commands(&mut controller, &commands);
}

// Process all commands in sequence
fn process_commands(controller: &mut DataFrameController, commands: &[Command]) {
    // Process each command in sequence
    for (i, cmd) in commands.iter().enumerate() {
        // Process the command
        process_command(controller, cmd);
        
        // Display results for the last command if it's not already a display command
        let is_last_command = i == commands.len() - 1;
        let is_display_command = is_display_command(&cmd.name);
        
        if is_last_command && !is_display_command {
            // Show the results by default
            controller.showtable();
        }
    }
}

// Check if data is loaded
fn check_data_loaded(controller: &DataFrameController, cmd_name: &str) {
    if controller.is_empty() {
        eprintln!("Error: No data loaded. Please load data first before using '{}'.", cmd_name);
        process::exit(1);
    }
}

// Check if command is a display command
fn is_display_command(cmd_name: &str) -> bool {
    matches!(cmd_name, "showtable" | "headers" | "show" | "stats" | "showquery" | "dump")
}

// Parse column names from comma-separated string
fn parse_column_names(input: &str) -> Vec<String> {
    if input.contains(',') {
        return input.split(',')
            .map(|s| s.trim().to_string())
            .collect();
    }
    
    vec![input.to_string()]
}

// Process a single command
fn process_command(controller: &mut DataFrameController, cmd: &Command) {
    match cmd.name.as_str() {
        // Initializers
        "load" => {
            if cmd.args.is_empty() {
                eprintln!("Error: 'load' command requires at least one file path");
                process::exit(1);
            }
            
            let mut paths = Vec::new();
            let separator = match cmd.options.get("s").or(cmd.options.get("separator")) {
                Some(Some(sep)) => sep.clone(),
                _ => ",".to_string(),
            };
            
            let low_memory = cmd.options.contains_key("low-memory");
            
            for path_str in &cmd.args {
                paths.push(PathBuf::from(path_str));
            }
            
            controller.load(&paths, &separator, low_memory);
        },
        
        // Chainables
        "select" => {
            check_data_loaded(controller, "select");
            
            if cmd.args.is_empty() {
                eprintln!("Error: 'select' command requires column names");
                process::exit(1);
            }
            
            let colnames = if cmd.args.len() == 1 {
                parse_column_names(&cmd.args[0])
            } else {
                cmd.args.clone()
            };
            
            controller.select(&colnames);
        },
        
        "isin" => {
            check_data_loaded(controller, "isin");
            
            if cmd.args.len() < 2 {
                eprintln!("Error: 'isin' command requires a column name and at least one value");
                process::exit(1);
            }
            
            let colname = &cmd.args[0];
            let values = cmd.args[1..].to_vec();
            
            controller.isin(colname, &values);
        },
        
        "contains" => {
            check_data_loaded(controller, "contains");
            
            if cmd.args.len() < 2 {
                eprintln!("Error: 'contains' command requires a column name and a pattern");
                process::exit(1);
            }
            
            let colname = &cmd.args[0];
            let pattern = &cmd.args[1];
            let ignorecase = cmd.options.contains_key("i") || cmd.options.contains_key("ignorecase");
            
            controller.contains(colname, pattern, ignorecase);
        },
        
        "sed" => {
            check_data_loaded(controller, "sed");
            
            if cmd.args.len() < 3 {
                eprintln!("Error: 'sed' command requires a column name, pattern, and replacement");
                process::exit(1);
            }
            
            let colname = &cmd.args[0];
            let pattern = &cmd.args[1];
            let replacement = &cmd.args[2];
            let ignorecase = cmd.options.contains_key("i") || cmd.options.contains_key("ignorecase");
            
            controller.sed(colname, pattern, replacement, ignorecase);
        },
        
        "grep" => {
            check_data_loaded(controller, "grep");
            
            if cmd.args.is_empty() {
                eprintln!("Error: 'grep' command requires a pattern");
                process::exit(1);
            }
            
            let pattern = &cmd.args[0];
            let ignorecase = cmd.options.contains_key("i") || cmd.options.contains_key("ignorecase");
            
            controller.grep(pattern, ignorecase);
        },
        
        "head" => {
            check_data_loaded(controller, "head");
            
            let number = if !cmd.args.is_empty() {
                cmd.args[0].parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("Error: 'head' command requires a valid number");
                    process::exit(1);
                })
            } else if let Some(Some(n)) = cmd.options.get("n").or(cmd.options.get("number")) {
                n.parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("Error: 'head' command requires a valid number");
                    process::exit(1);
                })
            } else {
                5 // Default value
            };
            
            controller.head(number);
        },
        
        "tail" => {
            check_data_loaded(controller, "tail");
            
            let number = if !cmd.args.is_empty() {
                cmd.args[0].parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("Error: 'tail' command requires a valid number");
                    process::exit(1);
                })
            } else if let Some(Some(n)) = cmd.options.get("n").or(cmd.options.get("number")) {
                n.parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("Error: 'tail' command requires a valid number");
                    process::exit(1);
                })
            } else {
                5 // Default value
            };
            
            controller.tail(number);
        },
        
        "sort" => {
            check_data_loaded(controller, "sort");
            
            if cmd.args.is_empty() {
                eprintln!("Error: 'sort' command requires column names");
                process::exit(1);
            }
            
            let colnames = if cmd.args.len() == 1 {
                parse_column_names(&cmd.args[0])
            } else {
                cmd.args.clone()
            };
            
            let desc = cmd.options.contains_key("d") || cmd.options.contains_key("desc");
            
            controller.sort(&colnames, desc);
        },
        
        "count" => {
            check_data_loaded(controller, "count");
            controller.count();
        },
        
        "uniq" => {
            check_data_loaded(controller, "uniq");
            
            if cmd.args.is_empty() {
                eprintln!("Error: 'uniq' command requires column names");
                process::exit(1);
            }
            
            let colnames = if cmd.args.len() == 1 {
                parse_column_names(&cmd.args[0])
            } else {
                cmd.args.clone()
            };
            
            controller.uniq(&colnames);
        },
        
        "changetz" => {
            check_data_loaded(controller, "changetz");
            
            if cmd.args.len() < 3 {
                eprintln!("Error: 'changetz' command requires a column name, source timezone, and target timezone");
                process::exit(1);
            }
            
            let colname = &cmd.args[0];
            let tz_from = &cmd.args[1];
            let tz_to = &cmd.args[2];
            
            // Remove quotes if format is specified
            let dt_format = cmd.args.get(3).map(|s| {
                let cleaned = s.trim_matches('\'');
                LogController::debug(&format!("Using date format: {}", cleaned));
                cleaned
            });
            
            controller.changetz(colname, tz_from, tz_to, dt_format);
        },
        
        "renamecol" => {
            check_data_loaded(controller, "renamecol");
            
            if cmd.args.len() < 2 {
                eprintln!("Error: 'renamecol' command requires an old column name and a new column name");
                process::exit(1);
            }
            
            let old_colname = &cmd.args[0];
            let new_colname = &cmd.args[1];
            
            controller.renamecol(old_colname, new_colname);
        },
        
        // Finalizers
        "showtable" => {
            check_data_loaded(controller, "showtable");
            controller.showtable();
        },
        
        "headers" => {
            check_data_loaded(controller, "headers");
            
            let plain = cmd.options.contains_key("p") || cmd.options.contains_key("plain");
            controller.headers(plain);
        },
        
        "show" => {
            check_data_loaded(controller, "show");
            controller.show();
        },
        
        "stats" => {
            check_data_loaded(controller, "stats");
            controller.stats();
        },
        
        "showquery" => {
            check_data_loaded(controller, "showquery");
            controller.showquery();
        },
        
        "dump" => {
            check_data_loaded(controller, "dump");
            
            let output_path = if !cmd.args.is_empty() {
                Some(&cmd.args[0] as &str)
            } else {
                None
            };
            
            controller.dump(output_path);
        },
        
        // Quilters
        "quilt" => {
            // No pre-load check needed; config file is processed directly
            
            if cmd.args.is_empty() {
                eprintln!("Error: 'quilt' command requires a config file path");
                process::exit(1);
            }
            
            // Parse config file path
            let config_path = &cmd.args[0];
            
            // Output path option
            let output_path = cmd.options.get("o").or(cmd.options.get("output"))
                .and_then(|opt| opt.as_ref())
                .map(|s| s.as_str());
            
            // Title option
            let title = cmd.options.get("t").or(cmd.options.get("title"))
                .and_then(|opt| opt.as_ref())
                .map(|s| s.as_str());
            
            // quilt execution
            operations::quilters::quilt::quilt(controller, config_path, output_path, title);
        },
        
        "quilt-visualize" => {
            // No pre-load check needed; config file is processed directly
            
            if cmd.args.is_empty() {
                eprintln!("Error: 'quilt-visualize' command requires a config file path");
                process::exit(1);
            }
            
            // Config file path
            let config_path = &cmd.args[0];
            
            // Output path option
            let output_path = cmd.options.get("o").or(cmd.options.get("output"))
                .and_then(|opt| opt.as_ref())
                .map(|s| s.as_str());
            
            // Title option
            let title = cmd.options.get("t").or(cmd.options.get("title"))
                .and_then(|opt| opt.as_ref())
                .map(|s| s.as_str());
            
            // quilt-visualize execution
            operations::quilters::quilt_visualize::quilt_visualize(config_path, output_path, title);
        },
        
        // Unsupported commands
        _ => {
            eprintln!("Error: Unsupported command '{}'.", cmd.name);
            process::exit(1);
        }
    }
}