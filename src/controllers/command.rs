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