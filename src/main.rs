use std::env;
use std::path::PathBuf;
use std::process;

mod controllers;
mod operations;

use controllers::command::{parse_commands, print_chainable_help, print_help, Command};
use controllers::dataframe::DataFrameController;
use once_cell::sync::Lazy;
use regex::Regex;

// Define the static Regex for column range parsing
static RE_COL_RANGE: Lazy<Regex> = Lazy::new(|| {
    // This regex captures:
    // p1: The prefix of the start of the range (e.g., "col")
    // n1: The number of the start of the range (e.g., "1")
    // p2: (Optional) The prefix of the end of the range if specified (e.g., "col" in "col1-col3")
    // n2: (Conditional) The number of the end of the range if p2 is specified (e.g., "3" in "col1-col3")
    // n3: (Conditional) The number of the end of the range if p2 is NOT specified (e.g., "3" in "col1-3")
    Regex::new(r"^(?P<p1>[a-zA-Z_][a-zA-Z_0-9]*)(?P<n1>\d+)-(?:(?P<p2>[a-zA-Z_][a-zA-Z_0-9]*)(?P<n2>\d+)|(?P<n3>\d+))$").unwrap()
});

fn main() {
    // Initialize logger to only show errors by default
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error")).init();

    // Get command line arguments
    let args: Vec<String> = std::env::args().collect();

    // No arguments provided - show help
    if args.len() == 1 {
        print_help();
        return;
    }

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
    for cmd in commands.iter() {
        process_command(controller, cmd);
    }

    // Check if the last command was a finalizer
    if let Some(last_cmd) = commands.last() {
        let finalizer_commands = [
            "show",
            "showtable",
            "headers",
            "stats",
            "showquery",
            "dump",
            "partition",
            "quilt",
        ];

        if !finalizer_commands.contains(&last_cmd.name.as_str()) {
            // Last command was not a finalizer, so call showtable as default
            if !controller.is_empty() {
                controller.showtable();
            }
        }
    }
}

// Check if data is loaded
fn check_data_loaded(controller: &DataFrameController, cmd_name: &str) {
    if controller.is_empty() {
        eprintln!(
            "Error: No data loaded. Please load data first before using '{}'.",
            cmd_name
        );
        process::exit(1);
    }
}

// New parse_column_names function with range expansion
fn parse_column_names(input: &str) -> Vec<String> {
    let mut expanded_colnames = Vec::new();

    for part in input.split(',') {
        let token = part.trim();
        if token.is_empty() {
            continue;
        }

        if let Some(caps) = RE_COL_RANGE.captures(token) {
            // These .unwrap() calls are safe because the groups are mandatory in the regex if it matches.
            let p1 = caps.name("p1").unwrap().as_str();
            let n1_str = caps.name("n1").unwrap().as_str();

            let n1 = match n1_str.parse::<usize>() {
                Ok(num) => num,
                Err(_) => {
                    eprintln!(
                        "Warning: Invalid start number in range token '{}'. Treating as literal.",
                        token
                    );
                    expanded_colnames.push(token.to_string());
                    continue;
                }
            };

            let (n_end, effective_prefix) = if let (Some(p2_match), Some(n2_match)) =
                (caps.name("p2"), caps.name("n2"))
            {
                // Case: p1n1-p2n2 (e.g., col1-col3, data1-data5)
                let p2 = p2_match.as_str();
                if p1 != p2 {
                    eprintln!("Warning: Mismatched prefixes ('{}' and '{}') in range token '{}'. Treating as literal.", p1, p2, token);
                    expanded_colnames.push(token.to_string());
                    continue;
                }
                match n2_match.as_str().parse::<usize>() {
                    Ok(num) => (num, p1), // Use p1 as the effective prefix
                    Err(_) => {
                        eprintln!("Warning: Invalid end number in range token '{}' (with explicit end prefix). Treating as literal.", token);
                        expanded_colnames.push(token.to_string());
                        continue;
                    }
                }
            } else if let Some(n3_match) = caps.name("n3") {
                // Case: p1n1-n3 (e.g., col1-3)
                match n3_match.as_str().parse::<usize>() {
                    Ok(num) => (num, p1), // Use p1 as the effective prefix
                    Err(_) => {
                        eprintln!("Warning: Invalid end number in range token '{}' (with implicit end prefix). Treating as literal.", token);
                        expanded_colnames.push(token.to_string());
                        continue;
                    }
                }
            } else {
                // This case should ideally not be reached if the regex matches,
                // as one of the OR branches for the end part should capture.
                eprintln!(
                    "Warning: Unparsable range format for token '{}'. Treating as literal.",
                    token
                );
                expanded_colnames.push(token.to_string());
                continue;
            };

            if n1 <= n_end {
                for i in n1..=n_end {
                    expanded_colnames.push(format!("{}{}", effective_prefix, i));
                }
            } else {
                // Example: col5-col1. Decide behavior: error, single item, or empty.
                // For now, warn and treat as literal, consistent with other parsing errors.
                eprintln!("Warning: Start of range ({}{}) is greater than end ({}{}) in token '{}'. Treating as literal.", effective_prefix, n1, effective_prefix, n_end, token);
                expanded_colnames.push(token.to_string());
            }
        } else {
            // Does not match the range pattern, add token as literal
            expanded_colnames.push(token.to_string());
        }
    }
    expanded_colnames
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
            let separator = match cmd.options.get("separator") {
                Some(Some(sep)) => sep.clone(),
                _ => ",".to_string(),
            };

            let low_memory =
                cmd.options.contains_key("low-memory") || cmd.options.contains_key("low_memory");

            let no_headers =
                cmd.options.contains_key("no-headers") || cmd.options.contains_key("no_headers");

            for path_str in &cmd.args {
                paths.push(PathBuf::from(path_str));
            }

            controller.load(&paths, &separator, low_memory, no_headers);
        }

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
        }

        "isin" => {
            check_data_loaded(controller, "isin");

            if cmd.args.len() < 2 {
                eprintln!("Error: 'isin' command requires a column name and at least one value string (e.g., isin colname val1,val2,val3)");
                process::exit(1);
            }

            let colname = &cmd.args[0];

            let values_str = &cmd.args[1];
            let values: Vec<String> = values_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if values.is_empty() {
                eprintln!("Error: 'isin' command requires at least one value after splitting the value string by comma.");
                process::exit(1);
            }

            controller.isin(colname, &values);
        }

        "contains" => {
            check_data_loaded(controller, "contains");

            if cmd.args.len() < 2 {
                eprintln!("Error: 'contains' command requires a column name and a pattern");
                process::exit(1);
            }

            let colname = &cmd.args[0];
            let pattern = &cmd.args[1];
            let ignorecase = cmd.options.contains_key("ignorecase");

            controller.contains(colname, pattern, ignorecase);
        }

        "sed" => {
            check_data_loaded(controller, "sed");

            if cmd.args.len() < 3 {
                eprintln!("Error: 'sed' command requires a column name, pattern, and replacement");
                process::exit(1);
            }

            let colname = &cmd.args[0];
            let pattern = &cmd.args[1];
            let replacement = &cmd.args[2];
            let ignorecase = cmd.options.contains_key("ignorecase");

            controller.sed(colname, pattern, replacement, ignorecase);
        }

        "grep" => {
            check_data_loaded(controller, "grep");

            if cmd.args.is_empty() {
                eprintln!("Error: 'grep' command requires a pattern.");
                process::exit(1);
            }

            let pattern = &cmd.args[0];

            let ignorecase =
                cmd.options.contains_key("ignorecase") || cmd.options.contains_key("i");
            let is_inverted =
                cmd.options.contains_key("invert-match") || cmd.options.contains_key("v");

            controller.grep(pattern, ignorecase, is_inverted);
        }

        "head" => {
            check_data_loaded(controller, "head");

            let number = if !cmd.args.is_empty() {
                cmd.args[0].parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("Error: 'head' command requires a valid number");
                    process::exit(1);
                })
            } else if let Some(Some(n_str)) = cmd.options.get("number") {
                n_str.parse::<usize>().unwrap_or_else(|_| {
                    eprintln!(
                        "Error: 'head' command requires a valid number for --number or -n option"
                    );
                    process::exit(1);
                })
            } else {
                5 // Default value
            };

            controller.head(number);
        }

        "tail" => {
            check_data_loaded(controller, "tail");

            let number = if !cmd.args.is_empty() {
                cmd.args[0].parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("Error: 'tail' command requires a valid number");
                    process::exit(1);
                })
            } else if let Some(Some(n_str)) = cmd.options.get("number") {
                n_str.parse::<usize>().unwrap_or_else(|_| {
                    eprintln!(
                        "Error: 'tail' command requires a valid number for --number or -n option"
                    );
                    process::exit(1);
                })
            } else {
                5 // Default value
            };

            controller.tail(number);
        }

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

            let desc = cmd.options.contains_key("desc");

            controller.sort(&colnames, desc);
        }

        "count" => {
            check_data_loaded(controller, "count");
            controller.count();
        }

        "uniq" => {
            check_data_loaded(controller, "uniq");
            let colnames = if cmd.args.is_empty() {
                None
            } else {
                Some(parse_column_names(&cmd.args[0]))
            };
            controller.uniq(colnames);
        }

        "changetz" => {
            check_data_loaded(controller, "changetz");

            if cmd.args.is_empty() {
                eprintln!("Error: 'changetz' command requires a column name");
                process::exit(1);
            }

            let colname = &cmd.args[0];

            let tz_from = match cmd.options.get("from_tz") {
                Some(Some(tz)) => tz,
                _ => {
                    eprintln!("Error: 'changetz' command requires --from_tz option");
                    process::exit(1);
                }
            };

            let tz_to = match cmd.options.get("to_tz") {
                Some(Some(tz)) => tz,
                _ => {
                    eprintln!("Error: 'changetz' command requires --to_tz option");
                    process::exit(1);
                }
            };

            let dt_format = cmd
                .options
                .get("format")
                .and_then(|opt_val| opt_val.as_deref());
            let ambiguous_time = cmd
                .options
                .get("ambiguous")
                .and_then(|opt_val| opt_val.as_deref());

            controller.changetz(colname, tz_from, tz_to, dt_format, ambiguous_time);
        }

        "renamecol" => {
            check_data_loaded(controller, "renamecol");
            if cmd.args.len() < 2 {
                eprintln!("Error: 'renamecol' command requires the current column name and the new column name.");
                process::exit(1);
            }
            let colname = &cmd.args[0];
            let new_colname = &cmd.args[1];
            controller.renamecol(colname, new_colname);
        }

        "convert" => {
            check_data_loaded(controller, "convert");
            if cmd.args.is_empty() {
                eprintln!("Error: 'convert' command requires a column name");
                process::exit(1);
            }
            let colname = &cmd.args[0];

            let from_format = match cmd.options.get("from") {
                Some(Some(format)) => format,
                _ => {
                    eprintln!("Error: 'convert' command requires --from option");
                    process::exit(1);
                }
            };

            let to_format = match cmd.options.get("to") {
                Some(Some(format)) => format,
                _ => {
                    eprintln!("Error: 'convert' command requires --to option");
                    process::exit(1);
                }
            };

            controller.convert(colname, from_format, to_format);
        }

        "timeline" => {
            check_data_loaded(controller, "timeline");

            if cmd.args.is_empty() {
                eprintln!("Error: 'timeline' command requires a time column name");
                process::exit(1);
            }

            let time_column = &cmd.args[0];

            let interval = match cmd.options.get("interval") {
                Some(Some(interval)) => interval,
                _ => {
                    eprintln!("Error: 'timeline' command requires --interval option (e.g., --interval 1h)");
                    process::exit(1);
                }
            };

            // Determine aggregation type and column
            let (agg_type, agg_column) = if let Some(Some(col)) = cmd.options.get("sum") {
                ("sum", Some(col.as_str()))
            } else if let Some(Some(col)) = cmd.options.get("avg") {
                ("avg", Some(col.as_str()))
            } else if let Some(Some(col)) = cmd.options.get("min") {
                ("min", Some(col.as_str()))
            } else if let Some(Some(col)) = cmd.options.get("max") {
                ("max", Some(col.as_str()))
            } else if let Some(Some(col)) = cmd.options.get("std") {
                ("std", Some(col.as_str()))
            } else {
                ("count", None) // Default to count
            };

            controller.timeline(time_column, interval, agg_type, agg_column);
        }

        "timeslice" => {
            check_data_loaded(controller, "timeslice");

            if cmd.args.is_empty() {
                eprintln!("Error: 'timeslice' command requires a time column name");
                process::exit(1);
            }

            let time_column = &cmd.args[0];

            let start_time = cmd.options.get("start").and_then(|opt| opt.as_deref());
            let end_time = cmd.options.get("end").and_then(|opt| opt.as_deref());

            if start_time.is_none() && end_time.is_none() {
                eprintln!(
                    "Error: 'timeslice' command requires at least one of --start or --end options"
                );
                process::exit(1);
            }

            controller.timeslice(time_column, start_time, end_time);
        }

        "partition" => {
            check_data_loaded(controller, "partition");

            if cmd.args.is_empty() {
                eprintln!("Error: 'partition' command requires a column name");
                process::exit(1);
            }

            let colname = &cmd.args[0];
            let output_dir = if cmd.args.len() > 1 {
                &cmd.args[1]
            } else {
                "./partitions"
            };

            controller.partition(colname, output_dir);
        }

        "pivot" => {
            check_data_loaded(controller, "pivot");

            let rows_str = cmd
                .options
                .get("rows")
                .and_then(|opt| opt.as_deref())
                .unwrap_or("");
            let cols_str = cmd
                .options
                .get("cols")
                .and_then(|opt| opt.as_deref())
                .unwrap_or("");
            let values = cmd
                .options
                .get("values")
                .and_then(|opt| opt.as_deref())
                .unwrap_or_else(|| {
                    eprintln!("Error: 'pivot' command requires --values option");
                    process::exit(1);
                });
            let agg_func = cmd
                .options
                .get("agg")
                .and_then(|opt| opt.as_deref())
                .unwrap_or("sum");

            if rows_str.is_empty() && cols_str.is_empty() {
                eprintln!(
                    "Error: 'pivot' command requires at least one of --rows or --cols options"
                );
                process::exit(1);
            }

            let rows: Vec<String> = if rows_str.is_empty() {
                Vec::new()
            } else {
                rows_str.split(',').map(|s| s.trim().to_string()).collect()
            };

            let columns: Vec<String> = if cols_str.is_empty() {
                Vec::new()
            } else {
                cols_str.split(',').map(|s| s.trim().to_string()).collect()
            };

            controller.pivot(&rows, &columns, values, agg_func);
        }

        // Quilters
        "quilt" => {
            if cmd.args.is_empty() {
                eprintln!("Error: 'quilt' command requires a config_path argument.");
                process::exit(1);
            }
            let config_path_str = &cmd.args[0];

            let cli_input_files = if cmd.args.len() > 1 {
                Some(
                    cmd.args[1..]
                        .iter()
                        .map(PathBuf::from)
                        .collect::<Vec<PathBuf>>(),
                )
            } else {
                None
            };

            let output_path_str = cmd.options.get("output").and_then(|o| o.as_deref());

            // quilt operation is destructive / stateful for the controller for now
            operations::quilters::quilt::quilt(
                controller,
                config_path_str,
                cli_input_files,
                output_path_str,
            );
        }

        // Finalizers
        "showtable" => {
            check_data_loaded(controller, "showtable");
            controller.showtable();
        }

        "headers" => {
            check_data_loaded(controller, "headers");
            let plain = cmd.options.contains_key("plain");
            controller.headers(plain);
        }

        "show" => {
            check_data_loaded(controller, "show");
            controller.show();
        }

        "stats" => {
            check_data_loaded(controller, "stats");
            controller.stats();
        }

        "showquery" => {
            check_data_loaded(controller, "showquery");
            controller.showquery();
        }

        "dump" => {
            check_data_loaded(controller, "dump");
            let output_path = if !cmd.args.is_empty() {
                Some(cmd.args[0].as_str())
            } else {
                cmd.options
                    .get("output")
                    .and_then(|opt_val| opt_val.as_deref())
            };

            let separator = cmd
                .options
                .get("separator")
                .and_then(|opt_val| opt_val.as_ref().and_then(|s| s.chars().next()));

            controller.dump(output_path, separator);
        }

        // Unsupported commands
        _ => {
            eprintln!("Error: Unknown command '{}'", cmd.name);
            print_help();
            process::exit(1);
        }
    }
}
