// filepath: /workspaces/qsv-rs/src/controllers/command.rs
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Command {
    pub name: String,
    pub args: Vec<String>,
    pub options: HashMap<String, Option<String>>,
}

impl Command {
    pub fn new(name: String) -> Self {
        Command {
            name,
            args: Vec::new(),
            options: HashMap::new(),
        }
    }
}

pub fn parse_commands(args: &[String]) -> Vec<Command> {
    let mut commands = Vec::new();
    let mut current_command = Command::new(String::new());
    let mut is_first_arg = true;
    let mut i = 0; // Index for iterating through args

    while i < args.len() {
        let arg = &args[i];

        if arg == "-" {
            if !current_command.name.is_empty() {
                commands.push(current_command);
                current_command = Command::new(String::new());
                is_first_arg = true;
            }
            i += 1;
            continue;
        }

        if is_first_arg {
            current_command.name = arg.clone();
            is_first_arg = false;
            i += 1;
            continue;
        }

        if let Some(option_str) = arg.strip_prefix("--") {
            // Long option format: --option[=value] or --option value

            // Check if it's --option=value format
            if option_str.contains('=') {
                parse_option(&mut current_command, option_str);
                i += 1;
            } else {
                // Check if this is a long option that expects a value
                let needs_value = matches!(
                    option_str,
                    "from-tz"
                        | "to-tz"
                        | "input-format" | "input_format"
                        | "output-format" | "output_format"
                        | "ambiguous"
                        | "output"
                        | "separator"
                        | "number"
                        | "interval"
                        | "sum"
                        | "avg"
                        | "min"
                        | "max"
                        | "std"
                        | "start"
                        | "end"
                        | "rows"
                        | "cols"
                        | "values"
                        | "agg"
                        | "from"
                        | "to"
                        | "column"
                );

                if needs_value && i + 1 < args.len() && !args[i + 1].starts_with('-') {
                    // --option value format
                    let value = args[i + 1].clone();
                    current_command
                        .options
                        .insert(option_str.to_string(), Some(value));
                    i += 2; // Consumed option and its value
                } else {
                    // It's a flag option
                    parse_option(&mut current_command, option_str);
                    i += 1;
                }
            }
        } else if let Some(stripped) = arg.strip_prefix('-') {
            let opt_key_to_parse = stripped.to_string(); // Make it mutable

            // Handle cases like -sValue or -s=Value directly attached
            if opt_key_to_parse.len() > 1
                && (opt_key_to_parse.starts_with('s')
                    || opt_key_to_parse.starts_with('n')
                    || opt_key_to_parse.starts_with('o'))
            {
                let (actual_key, actual_value) = if opt_key_to_parse.contains('=') {
                    // Case: -s=value
                    let parts: Vec<&str> = opt_key_to_parse.splitn(2, '=').collect();
                    (parts[0].to_string(), parts.get(1).map(|s| s.to_string()))
                } else {
                    // Case: -sValue (no equals)
                    (
                        opt_key_to_parse[0..1].to_string(),
                        Some(opt_key_to_parse[1..].to_string()),
                    )
                };

                if (actual_key == "s" || actual_key == "n" || actual_key == "o")
                    && actual_value.is_some()
                {
                    let full_key = match actual_key.as_str() {
                        "s" => "separator".to_string(),
                        "n" => "number".to_string(),
                        "o" => "output".to_string(),
                        _ => actual_key.clone(), // Should not happen
                    };
                    current_command.options.insert(full_key, actual_value);
                    i += 1;
                    continue; // Move to next argument
                }
                // If not s or n, or no value, fall through to general short opt parsing
            }

            // Standard short option handling (e.g. -s value, or -f flag)
            let opt_char_str = if arg.len() >= 2 { &arg[1..2] } else { "" }; // Get the char e.g. "s"

            if (opt_char_str == "s" || opt_char_str == "n" || opt_char_str == "o") && // It's -s, -n, or -o
               i + 1 < args.len() && // Next argument exists
               !args[i+1].starts_with('-')
            // Next argument is not another option
            {
                let value = args[i + 1].clone();
                let full_key = match opt_char_str {
                    "s" => "separator".to_string(),
                    "n" => "number".to_string(),
                    "o" => "output".to_string(),
                    _ => opt_char_str.to_string(), // Fallback
                };
                current_command.options.insert(full_key, Some(value));
                i += 2; // Consumed option and its value
            } else {
                // It's a flag (e.g., -f) or an option with '=' (e.g., -o=value),
                // or -s/-n without a following value that looks like a value.
                let option_str_for_parse_option = stripped;
                parse_option(&mut current_command, option_str_for_parse_option);
                i += 1;
            }
        } else {
            current_command.args.push(arg.clone());
            i += 1;
        }
    }

    if !current_command.name.is_empty() {
        commands.push(current_command);
    }

    commands
}

fn parse_option(cmd: &mut Command, option_str: &str) {
    if let Some((key, value)) = option_str.split_once('=') {
        // For short options like -s=val, key would be "s"
        // We want to store it as "separator"
        let final_key = match key {
            "s" => "separator".to_string(),
            "n" => "number".to_string(),
            "o" => "output".to_string(),
            _ => key.to_string(),
        };
        cmd.options.insert(final_key, Some(value.to_string()));
    } else {
        // This is a flag option (e.g., -i, --ignorecase) or a short option passed without '=' that wasn't -s or -n
        // Or it's a key that parse_commands decided should be treated as a flag (e.g. -s at end of args)
        let final_key = match option_str {
            "s" => "separator".to_string(), // if -s is passed as a flag, store as separator: None
            "n" => "number".to_string(),    // if -n is passed as a flag, store as number: None
            "o" => "output".to_string(),    // if -o is passed as a flag, store as output: None
            // Add other short options that are flags here if necessary
            "i" => "ignorecase".to_string(), // Example for grep -i
            "d" => "desc".to_string(),       // Example for sort -d
            "p" => "plain".to_string(),      // Example for headers -p
            _ => option_str.to_string(),
        };
        // If it's a known flag that should be stored with its full name, do so.
        // Otherwise, it's a flag option (value is None).
        cmd.options.insert(final_key, None);
    }
}

// Help functions for CLI
pub fn print_help() {
    println!("qsv: Elastic and rapid filtering of huge CSV files\n");
    println!("Usage: qsv load <file.csv> - <chainable> <args> - <finalizer> <args>\n");
    println!("Initializers:");
    println!("  load         Load CSV file(s)");
    println!();
    println!("Chainables:");
    println!("  select       Select columns");
    println!("  isin         Filter rows by values");
    println!("  contains     Filter rows by pattern");
    println!("  sed          Replace values by pattern");
    println!("  grep         Filter rows by regex (any column)");
    println!("  head         Show first N rows");
    println!("  tail         Show last N rows");
    println!("  sort         Sort rows");
    println!("  count        Count duplicate rows");
    println!("  uniq         Remove duplicate rows");
    println!("  changetz     Change timezone");
    println!("  renamecol    Rename column");
    println!("  convert      Convert data formats (JSON, YAML, XML, etc.)");
    println!("  timeline     Aggregate data by time intervals");
    println!("  timeslice    Filter data by time range");
    println!("  pivot        Create pivot tables with cross-tabulation");
    println!();
    println!("Finalizers:");
    println!("  show         Print as CSV");
    println!("  showtable    Print as table");
    println!("  headers      Show column names");
    println!("  stats        Show statistics");
    println!("  showquery    Show query plan");
    println!("  dump         Save as CSV");
    println!("  partition    Split data into separate files by column values");
    println!();
    println!("Quilters:");
    println!("  quilt        Execute a quilt (data processing pipeline from YAML)");
    println!();
    println!("Examples:");
    println!("  qsv load data.csv - select col1,col2 - head 10 - show");
    println!("  qsv load data.csv - select 1:3 - show");
    println!("  qsv load data.csv - grep pattern - showtable");
    println!("  qsv load data.csv - sort col1 -d - show");
    println!("  qsv load data.csv - isin col1 1,2,3 - uniq col1 - show");
    println!("  qsv load data.csv - changetz datetime --from_tz UTC --to_tz Asia/Tokyo - show");
    println!("  qsv load data.csv - partition category ./partitions/");
    println!();
    println!("For more details, see README.md or --help");
}

pub fn print_chainable_help(cmd: &str) {
    match cmd {
        "load" => print_load_help(),
        "select" => print_select_help(),
        "isin" => print_isin_help(),
        "contains" => print_contains_help(),
        "sed" => print_sed_help(),
        "grep" => print_grep_help(),
        "head" => print_head_help(),
        "tail" => print_tail_help(),
        "sort" => print_sort_help(),
        "count" => print_count_help(),
        "uniq" => print_uniq_help(),
        "changetz" => print_changetz_help(),
        "renamecol" => print_renamecol_help(),
        "convert" => print_convert_help(),
        "timeline" => print_timeline_help(),
        "timeslice" => print_timeslice_help(),
        "partition" => print_partition_help(),
        "pivot" => print_pivot_help(),
        "show" => print_show_help(),
        "showtable" => print_showtable_help(),
        "headers" => print_headers_help(),
        "stats" => print_stats_help(),
        "showquery" => print_showquery_help(),
        "dump" => print_dump_help(),
        "quilt" => print_quilt_help(),
        _ => println!("No detailed help available for this command."),
    }
}

fn print_load_help() {
    println!("load: Load one or more CSV files\n");
    println!("Usage: load <file1.csv> [file2.csv ...] [-s|--separator <char>] [--low-memory] [--no-headers]\n");
    println!("Options:");
    println!("  -s, --separator <char>  Field separator character (default: ',')");
    println!("  --low-memory            Enable low-memory mode for very large files");
    println!("  --no-headers            Treat first row as data, not headers");
    println!("\nExamples:");
    println!("  qsv load data.csv - show");
    println!("  qsv load data1.csv data2.csv data3.csv - show");
    println!("  qsv load logs/*.tsv -s '\\t' - show");
    println!("  qsv load data.csv --low-memory - show");
    println!("  qsv load data.csv --no-headers - show");
    println!("  qsv load data.csv.gz - show  # Automatically detects gzip files");
}

fn print_select_help() {
    println!("select: Select columns from the DataFrame\n");
    println!("Usage: select <col1>[,<col2>,...]\n");
    println!("Column Selection:");
    println!("  - Individual columns: col1,col2,col3");
    println!("  - Range notation: col1-col3 (hyphen-separated)");
    println!("  - Colon notation: col1:col3 (colon-separated)");
    println!("  - Quoted colon notation: \"col:1\":\"col:3\" (for columns with colons)");
    println!("  - Numeric colon notation: 1:3 (selects col1, col2, col3)");
    println!("\nExamples:");
    println!("  qsv load data.csv - select col1 - show");
    println!("  qsv load data.csv - select col1,col2 - show");
    println!("  qsv load data.csv - select col1-col3 - show");
    println!("  qsv load data.csv - select col1:col3 - show");
    println!("  qsv load data.csv - select 1:3 - show  # Select col1, col2, col3");
    println!("  qsv load data.csv - select \"col:1\":\"col:3\" - show  # Quoted for colons");
}

fn print_isin_help() {
    println!("isin: Filter rows by values in a column\n");
    println!("Usage: isin <colname> <value1>[,<value2>,...]\n");
    println!("Examples:");
    println!("  qsv load data.csv - isin col1 1,2,3 - show");
}

fn print_contains_help() {
    println!("contains: Filter rows by substring or pattern in a column\n");
    println!("Usage: contains <colname> <pattern> [-i]\n");
    println!("Examples:");
    println!("  qsv load data.csv - contains col1 foo - show");
    println!("  qsv load data.csv - contains col1 bar -i - show");
}

fn print_sed_help() {
    println!("sed: Replace values in column(s) using a pattern\n");
    println!("Usage:");
    println!("  sed <pattern> <replacement> [-i]                    (apply to all columns)");
    println!("  sed <pattern> <replacement> --column <colname> [-i] (apply to specific column)\n");
    println!("Options:");
    println!("  -i, --ignorecase    Case-insensitive matching");
    println!("  --column <colname>  Apply replacement to specific column only\n");
    println!("Examples:");
    println!("  qsv load data.csv - sed foo bar - show                       # Replace 'foo' with 'bar' in all columns");
    println!("  qsv load data.csv - sed foo bar --column str - show          # Replace 'foo' with 'bar' in 'str' column only");
    println!("  qsv load data.csv - sed john JOHN -i - show                  # Case-insensitive replacement");
}

fn print_grep_help() {
    println!("grep: Filter rows by regex pattern (any column)\n");
    println!("Usage: grep <pattern> [-i]\n");
    println!("Examples:");
    println!("  qsv load data.csv - grep foo - show");
    println!("  qsv load data.csv - grep bar -i - show");
}

fn print_head_help() {
    println!("head: Show first N rows\n");
    println!("Usage: head <number>\n");
    println!("Examples:");
    println!("  qsv load data.csv - head 10 - show");
}

fn print_tail_help() {
    println!("tail: Show last N rows\n");
    println!("Usage: tail <number>\n");
    println!("Examples:");
    println!("  qsv load data.csv - tail 10 - show");
}

fn print_sort_help() {
    println!("sort: Sort rows by column(s)\n");
    println!("Usage: sort <col1>[,<col2>,...] [-d]\n");
    println!("Options: -d (descending order)\n");
    println!("Examples:");
    println!("  qsv load data.csv - sort col1 - show");
    println!("  qsv load data.csv - sort col1,col2 -d - show");
}

fn print_count_help() {
    println!("count: Count duplicate rows, grouping by all columns\n");
    println!("Usage: count\n");
    println!("Examples:");
    println!("  qsv load data.csv - count - show");
}

fn print_uniq_help() {
    println!("uniq: Remove duplicate rows based on column(s)\n");
    println!("Usage: uniq <col1>[,<col2>,...]\n");
    println!("Examples:");
    println!("  qsv load data.csv - uniq col1 - show");
}

fn print_changetz_help() {
    println!("changetz: Change timezone of a datetime column\n");
    println!("Usage: changetz <colname> --from-tz <from_tz> --to-tz <to_tz> [--input-format <format>] [--output-format <format>] [--ambiguous <strategy>]\n");
    println!("Options:");
    println!("  --from-tz       Source timezone (e.g., UTC, America/New_York, local)");
    println!("  --to-tz         Target timezone (e.g., Asia/Tokyo)");
    println!("  --input-format  Input datetime format (default: auto)");
    println!("  --output-format Output datetime format (default: auto - ISO8601)");
    println!("  --ambiguous     Strategy for ambiguous times: earliest or latest (default: earliest)");
    println!("\nExamples:");
    println!("  qsv load data.csv - changetz datetime --from-tz UTC --to-tz Asia/Tokyo - show");
    println!("  qsv load data.csv - changetz datetime --from-tz UTC --to-tz Asia/Tokyo --input-format '%Y/%m/%d %H:%M' - show");
    println!("  qsv load data.csv - changetz datetime --from-tz America/New_York --to-tz UTC --ambiguous latest - show");
}

fn print_renamecol_help() {
    println!("renamecol: Rename a column\n");
    println!("Usage: renamecol <old_colname> <new_colname>\n");
    println!("Examples:");
    println!("  qsv load data.csv - renamecol col1 new_col - show");
}

fn print_convert_help() {
    println!("convert: Convert data formats (JSON, YAML, XML, etc.)\n");
    println!("Usage: convert <colname> --from <format> --to <format>\n");
    println!("Options:");
    println!("  --from <format>  Source format (json, yaml, xml)");
    println!("  --to <format>    Target format (json, yaml, xml)");
    println!("\nSupported conversions:");
    println!("  json -> yaml     Convert JSON to YAML format");
    println!("  yaml -> json     Convert YAML to JSON format");
    println!("  json -> xml      Convert JSON to XML format");
    println!("  xml -> json      Convert XML to JSON format");
    println!("  yaml -> xml      Convert YAML to XML format");
    println!("  xml -> yaml      Convert XML to YAML format");
    println!("\nFormatting (same format conversions):");
    println!("  json -> json     Format/prettify JSON");
    println!("  yaml -> yaml     Format/prettify YAML");
    println!("  xml -> xml       Format/prettify XML");
    println!("\nExamples:");
    println!("  qsv load data.csv - convert col1 --from json --to yaml - show");
    println!("  qsv load data.csv - convert config --from yaml --to json - show");
    println!("  qsv load data.csv - convert data --from json --to xml - show");
    println!("  qsv load data.csv - convert xml_data --from xml --to json - show");
    println!("  qsv load data.csv - convert json_data --from json --to json - show  # Format JSON");
    println!("\nNote: Handles malformed JSON with extra quotes automatically.");
}

fn print_timeline_help() {
    println!("timeline: Aggregate data by time intervals\n");
    println!("Usage: timeline <time_column> --interval <interval> [--sum|--avg|--min|--max|--std <column>]\n");
    println!("Options:");
    println!("  --interval   Time interval (e.g., 1h, 5m, 30s, 1d)");
    println!("  --sum        Sum values in specified column");
    println!("  --avg        Average values in specified column");
    println!("  --min        Minimum values in specified column");
    println!("  --max        Maximum values in specified column");
    println!("  --std        Standard deviation of values in specified column");
    println!("\nExamples:");
    println!("  qsv load access.log - timeline timestamp --interval 1h - show");
    println!("  qsv load metrics.csv - timeline time --interval 5m --avg cpu_usage - show");
    println!("  qsv load sales.csv - timeline date --interval 1d --sum amount - show");
}

fn print_timeslice_help() {
    println!("timeslice: Filter data by time range\n");
    println!("Usage: timeslice <time_column> [--start <start_time>] [--end <end_time>]\n");
    println!("Options:");
    println!("  --start      Start time (inclusive)");
    println!("  --end        End time (inclusive)");
    println!("\nExamples:");
    println!("  qsv load data.csv - timeslice timestamp --start '2023-01-01 00:00:00' - show");
    println!("  qsv load data.csv - timeslice timestamp --end '2023-12-31 23:59:59' - show");
    println!(
        "  qsv load data.csv - timeslice timestamp --start '2023-06-01' --end '2023-06-30' - show"
    );
}

fn print_partition_help() {
    println!("partition: Split data into separate files by column values\n");
    println!("Usage: partition <colname> <output_directory>\n");
    println!("Arguments:");
    println!("  <colname>           Column name to partition by");
    println!("  <output_directory>  Directory to save partitioned files");
    println!("\nExamples:");
    println!("  qsv load data.csv - partition category ./partitions/ - show");
    println!("  qsv load sales.csv - partition region ./by_region/ - show");
    println!("  qsv load logs.csv - partition date ./daily_logs/ - show");
    println!("\nNote: Creates one CSV file per unique value in the specified column.");
}

fn print_pivot_help() {
    println!("pivot: Create pivot tables with cross-tabulation\n");
    println!(
        "Usage: pivot --rows <columns> --cols <columns> --values <column> [--agg <function>]\n"
    );
    println!("Options:");
    println!("  --rows <columns>    Comma-separated list of columns for rows");
    println!("  --cols <columns>    Comma-separated list of columns for columns");
    println!("  --values <column>   Column to aggregate");
    println!(
        "  --agg <function>    Aggregation function (sum, mean, count, min, max, median, std)"
    );
    println!("\nExamples:");
    println!("  qsv load sales.csv - pivot --rows region --cols product --values sales_amount --agg sum - show");
    println!("  qsv load data.csv - pivot --rows category --cols year --values revenue --agg mean - show");
    println!("  qsv load logs.csv - pivot --rows date --cols error_type --values count --agg count - show");
    println!("\nNote: Creates a cross-tabulation table with specified rows and columns.");
}

fn print_show_help() {
    println!("show: Print result as CSV\n");
    println!("Usage: show\n");
    println!("Examples:");
    println!("  qsv load data.csv - show");
}

fn print_showtable_help() {
    println!("showtable: Print result as a table\n");
    println!("Usage: showtable\n");
    println!("Examples:");
    println!("  qsv load data.csv - showtable");
}

fn print_headers_help() {
    println!("headers: Show column names\n");
    println!("Usage: headers [-p]\n");
    println!("Options: -p (plain format)\n");
    println!("Examples:");
    println!("  qsv load data.csv - headers");
    println!("  qsv load data.csv - headers -p");
}

fn print_stats_help() {
    println!("stats: Show statistics of the data\n");
    println!("Usage: stats\n");
    println!("Examples:");
    println!("  qsv load data.csv - stats");
}

fn print_showquery_help() {
    println!("showquery: Show query plan\n");
    println!("Usage: showquery\n");
    println!("Examples:");
    println!("  qsv load data.csv - showquery");
}

fn print_dump_help() {
    println!("dump: Save DataFrame as CSV\n");
    println!("Usage: dump [output_path] [--separator <char>]\n");
    println!("Examples:");
    println!("  qsv load data.csv - dump results.csv");
    println!("  qsv load data.csv - dump --separator ';' results.csv");
}

fn print_quilt_help() {
    println!("quilt: Execute a quilt (data processing pipeline from YAML)\n");
    println!("Usage: quilt <config_path> [csv_file_paths...] [-o <output_file>]\n");
    println!("Arguments:");
    println!("  <config_path>    Path to the Quilt YAML configuration file. (Required)");
    println!("  [csv_file_paths...] Optional paths to CSV files to be processed if not specified in YAML's load steps.");
    println!("Options:");
    println!("  -o, --output <output_file>  Optional path to save the result as CSV.");
    println!("                              If not provided, output is printed to console.");
    println!("Examples:");
    println!("  qsv quilt my_pipeline.yaml");
    println!("  qsv quilt my_pipeline.yaml -o result.csv");
}
