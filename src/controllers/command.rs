// filepath: /workspaces/qsv-rs/src/controllers/command.rs
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Command {
    pub name: String,
    pub args: Vec<String>,
    pub options: HashMap<String, Option<String>>,
}

pub fn parse_commands(args: &[String]) -> Vec<Command> {
    let mut result = Vec::new();
    let mut current_group = Vec::new();
    
    // コマンド区切り文字として `-` を使用
    let delimiter = "-";
    
    for arg in args {
        if arg == delimiter {
            if !current_group.is_empty() {
                if let Some(cmd) = parse_command_group(&current_group) {
                    result.push(cmd);
                }
                current_group = Vec::new();
            }
        } else {
            current_group.push(arg.clone());
        }
    }
    
    if !current_group.is_empty() {
        if let Some(cmd) = parse_command_group(&current_group) {
            result.push(cmd);
        }
    }
    
    result
}

fn parse_command_group(args: &[String]) -> Option<Command> {
    if args.is_empty() {
        return None;
    }
    
    let name = args[0].clone();
    let mut cmd_args = Vec::new();
    let mut options = HashMap::new();
    let mut i = 1;
    
    while i < args.len() {
        let arg = &args[i];
        
        if arg.starts_with("--") {
            let option_name = arg[2..].to_string();
            
            if i + 1 < args.len() && !args[i + 1].starts_with("-") {
                options.insert(option_name, Some(args[i + 1].clone()));
                i += 2;
            } else {
                options.insert(option_name, None);
                i += 1;
            }
        } else if arg.starts_with("-") {
            let option_name = arg[1..].to_string();
            
            if i + 1 < args.len() && !args[i + 1].starts_with("-") {
                options.insert(option_name, Some(args[i + 1].clone()));
                i += 2;
            } else {
                options.insert(option_name, None);
                i += 1;
            }
        } else {
            cmd_args.push(arg.clone());
            i += 1;
        }
    }
    
    Some(Command {
        name,
        args: cmd_args,
        options,
    })
}