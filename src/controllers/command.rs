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
    
    for arg in args {
        if arg == "-" {
            // Finalize the current command if it has a name
            if !current_command.name.is_empty() {
                commands.push(current_command);
                current_command = Command::new(String::new());
                is_first_arg = true;
            }
            continue;
        }
        
        // Handle command name
        if is_first_arg {
            current_command.name = arg.clone();
            is_first_arg = false;
            continue;
        }
        
        // Parse options and arguments
        if arg.starts_with("--") {
            // Long option format: --option[=value]
            let option_str = &arg[2..];
            parse_option(&mut current_command, option_str);
        } else if arg.starts_with('-') {
            // Short option format: -o[=value]
            let option_str = &arg[1..];
            parse_option(&mut current_command, option_str);
        } else {
            // Regular argument
            current_command.args.push(arg.clone());
        }
    }
    
    // Add the last command if it has a name
    if !current_command.name.is_empty() {
        commands.push(current_command);
    }
    
    commands
}

fn parse_option(cmd: &mut Command, option_str: &str) {
    if let Some((key, value)) = option_str.split_once('=') {
        // キーと値がある場合 (--key=value または -k=value)
        cmd.options.insert(key.to_string(), Some(value.to_string()));
    } else if option_str.contains('=') {
        // =が含まれているが、分割できない場合（例：-o=value）
        let parts: Vec<&str> = option_str.split('=').collect();
        if parts.len() >= 2 {
            let key = parts[0];
            let value = if parts.len() > 2 {
                parts[1..].join("=")
            } else {
                parts[1].to_string()
            };
            cmd.options.insert(key.to_string(), Some(value));
        } else {
            cmd.options.insert(option_str.to_string(), None);
        }
    } else {
        // フラグオプション（値なし）
        cmd.options.insert(option_str.to_string(), None);
    }
}

// Help functions for CLI
pub fn print_help() {
    println!("qsv: Elastic and rapid filtering of huge CSV files\n");
    println!("Usage: qsv load <file.csv> - <chainable> <args> - <finalizer> <args>\n");
    println!("Initializers:");
    println!("  load         Load CSV file(s)");
    println!("");
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
    println!("");
    println!("Finalizers:");
    println!("  show         Print as CSV");
    println!("  showtable    Print as table");
    println!("  headers      Show column names");
    println!("  stats        Show statistics");
    println!("  showquery    Show query plan");
    println!("  dump         Save as CSV");
    println!("");
    println!("Examples:");
    println!("  qsv load data.csv - select col1,col2 - head 10 - show");
    println!("  qsv load data.csv - grep pattern - showtable");
    println!("  qsv load data.csv - sort col1 -d - show");
    println!("  qsv load data.csv - isin col1 1,2,3 - uniq col1 - show");
    println!("  qsv load data.csv - changetz datetime UTC JST - show");
    println!("");
    println!("For more details, see README.md or --help");
}

pub fn print_chainable_help(cmd: &str) {
    match cmd {
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
        "show" => print_show_help(),
        "showtable" => print_showtable_help(),
        "headers" => print_headers_help(),
        "stats" => print_stats_help(),
        "showquery" => print_showquery_help(),
        "dump" => print_dump_help(),
        _ => println!("No detailed help available for this command."),
    }
}

fn print_select_help() {
    println!("select: Select columns from the DataFrame\n");
    println!("Usage: select <col1>[,<col2>,...]\n");
    println!("Examples:");
    println!("  qsv load data.csv - select col1 - show");
    println!("  qsv load data.csv - select col1,col2 - show");
    println!("  qsv load data.csv - select col1-col3 - show");
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
    println!("sed: Replace values in a column using a pattern\n");
    println!("Usage: sed <colname> <pattern> <replacement> [-i]\n");
    println!("Examples:");
    println!("  qsv load data.csv - sed col1 foo bar - show");
    println!("  qsv load data.csv - sed col1 foo bar -i - show");
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
    println!("Usage: changetz <colname> <from_tz> <to_tz> [format]\n");
    println!("Examples:");
    println!("  qsv load data.csv - changetz datetime UTC JST - show");
    println!("  qsv load data.csv - changetz datetime UTC JST '%Y-%m-%d %H:%M:%S' - show");
}
fn print_renamecol_help() {
    println!("renamecol: Rename a column\n");
    println!("Usage: renamecol <old_colname> <new_colname>\n");
    println!("Examples:");
    println!("  qsv load data.csv - renamecol col1 new_col - show");
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
    println!("dump: Save result as CSV file\n");
    println!("Usage: dump <path>\n");
    println!("Examples:");
    println!("  qsv load data.csv - dump result.csv");
}