use serde_yaml::Value;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use crate::controllers::log::LogController;

#[allow(dead_code)]
pub struct YamlController;

impl YamlController {
    /// YAML file to read and return Value object
    #[allow(dead_code)]
    pub fn load_yaml<P: AsRef<Path>>(path: P) -> Result<Value, String> {
        let path_str = path.as_ref().to_string_lossy();
        LogController::debug(&format!("Loading YAML file: {}", path_str));
        
        let mut file = match File::open(&path) {
            Ok(f) => f,
            Err(e) => return Err(format!("Failed to open YAML file: {}", e)),
        };
        
        let mut content = String::new();
        if let Err(e) = file.read_to_string(&mut content) {
            return Err(format!("Failed to read YAML file: {}", e));
        }
        
        match serde_yaml::from_str(&content) {
            Ok(value) => Ok(value),
            Err(e) => Err(format!("Failed to parse YAML file: {}", e)),
        }
    }
    
    /// Save Value object to YAML file
    #[allow(dead_code)]
    pub fn save_yaml<P: AsRef<Path>>(path: P, value: &Value) -> Result<(), String> {
        let path_str = path.as_ref().to_string_lossy();
        LogController::debug(&format!("Saving YAML file: {}", path_str));
        
        let content = match serde_yaml::to_string(value) {
            Ok(c) => c,
            Err(e) => return Err(format!("Failed to serialize YAML: {}", e)),
        };
        
        if let Err(e) = std::fs::write(&path, content) {
            return Err(format!("Failed to write YAML file: {}", e));
        }
        
        Ok(())
    }
    
    /// Parse YAML string to Value object
    #[allow(dead_code)]
    pub fn parse_yaml(content: &str) -> Result<Value, String> {
        match serde_yaml::from_str(content) {
            Ok(value) => Ok(value),
            Err(e) => Err(format!("Failed to parse YAML: {}", e)),
        }
    }
    
    /// Convert Value object to YAML string
    #[allow(dead_code)]
    pub fn to_string(value: &Value) -> Result<String, String> {
        match serde_yaml::to_string(value) {
            Ok(s) => Ok(s),
            Err(e) => Err(format!("Failed to serialize YAML: {}", e)),
        }
    }
}