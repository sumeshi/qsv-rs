use crate::controllers::log::LogController;
use polars::prelude::*;
use serde_json::Value as JsonValue;
use serde_xml_rs::from_str as xml_from_str;
use serde_yml;

pub fn convert(df: &LazyFrame, colname: &str, from_format: &str, to_format: &str) -> LazyFrame {
    LogController::debug(&format!(
        "Converting column '{colname}' from {from_format} to {to_format}"
    ));

    let schema = match df.clone().collect_schema() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error getting schema for convert operation: {e}");
            std::process::exit(1);
        }
    };

    if !schema.iter_names().any(|s| s == colname) {
        eprintln!("Error: Column '{colname}' not found in DataFrame for convert operation");
        std::process::exit(1);
    }

    // Create the conversion expression - replace the original column
    let from_format_owned = from_format.to_string();
    let to_format_owned = to_format.to_string();
    let conversion_expr = col(colname)
        .cast(DataType::String)
        .map(
            move |s_col: Column| {
                let ca = s_col.str()?;
                let mut converted_values: Vec<Option<String>> = Vec::new();
                for opt_str in ca.into_iter() {
                    if let Some(input_str) = opt_str {
                        let converted_result =
                            convert_format(input_str, &from_format_owned, &to_format_owned);
                        converted_values.push(Some(converted_result));
                    } else {
                        converted_values.push(None);
                    }
                }
                Ok(Some(
                    Series::new("converted".into(), converted_values).into(),
                ))
            },
            GetOutput::from_type(DataType::String),
        )
        .alias(colname); // Use original column name to replace it
    df.clone().with_column(conversion_expr)
}
fn convert_format(input_str: &str, from_format: &str, to_format: &str) -> String {
    match (
        from_format.to_lowercase().as_str(),
        to_format.to_lowercase().as_str(),
    ) {
        ("json", "yaml") => convert_json_to_yaml(input_str),
        ("yaml", "json") => convert_yaml_to_json(input_str),
        ("json", "xml") => convert_json_to_xml(input_str),
        ("xml", "json") => convert_xml_to_json(input_str),
        ("yaml", "xml") => convert_yaml_to_xml(input_str),
        ("xml", "yaml") => convert_xml_to_yaml(input_str),
        ("json", "json") => format_json(input_str),
        ("yaml", "yaml") => format_yaml(input_str),
        ("xml", "xml") => format_xml(input_str),
        _ => {
            LogController::debug(&format!(
                "Unsupported conversion: {from_format} to {to_format}"
            ));
            format!(
                "# Unsupported conversion: {from_format} to {to_format}\n# Original: {input_str}"
            )
        }
    }
}
fn convert_json_to_yaml(json_str: &str) -> String {
    // First, try to clean up the JSON string
    let cleaned_json = clean_json_string(json_str);
    // Try to parse as JSON
    match serde_json::from_str::<JsonValue>(&cleaned_json) {
        Ok(json_value) => {
            // Convert JSON to YAML
            match serde_yml::to_string(&json_value) {
                Ok(yaml_str) => {
                    // Remove the trailing newline and document separator if present
                    yaml_str
                        .trim_end_matches('\n')
                        .trim_end_matches("---")
                        .trim()
                        .to_string()
                }
                Err(e) => {
                    LogController::debug(&format!("Failed to convert JSON to YAML: {e}"));
                    format!("# YAML conversion error: {e}\n# Original: {json_str}")
                }
            }
        }
        Err(e) => {
            LogController::debug(&format!("Failed to parse JSON: {e}"));
            format!("# JSON parse error: {e}\n# Original: {json_str}")
        }
    }
}
fn convert_yaml_to_json(yaml_str: &str) -> String {
    // Try to parse as YAML
    match serde_yml::from_str::<JsonValue>(yaml_str) {
        Ok(yaml_value) => {
            // Convert YAML to JSON
            match serde_json::to_string_pretty(&yaml_value) {
                Ok(json_str) => json_str,
                Err(e) => {
                    LogController::debug(&format!("Failed to convert YAML to JSON: {e}"));
                    format!("# JSON conversion error: {e}\n# Original: {yaml_str}")
                }
            }
        }
        Err(e) => {
            LogController::debug(&format!("Failed to parse YAML: {e}"));
            format!("# YAML parse error: {e}\n# Original: {yaml_str}")
        }
    }
}
fn convert_json_to_xml(json_str: &str) -> String {
    // First, try to clean up the JSON string
    let cleaned_json = clean_json_string(json_str);
    // Try to parse as JSON
    match serde_json::from_str::<JsonValue>(&cleaned_json) {
        Ok(json_value) => {
            // Convert JSON to XML manually
            json_value_to_xml(&json_value, "root")
        }
        Err(e) => {
            LogController::debug(&format!("Failed to parse JSON: {e}"));
            format!("<!-- JSON parse error: {e} -->\n<!-- Original: {json_str} -->")
        }
    }
}
fn convert_xml_to_json(xml_str: &str) -> String {
    // Try to parse as XML
    match xml_from_str::<JsonValue>(xml_str) {
        Ok(xml_value) => {
            // Convert XML to JSON
            match serde_json::to_string_pretty(&xml_value) {
                Ok(json_str) => json_str,
                Err(e) => {
                    LogController::debug(&format!("Failed to convert XML to JSON: {e}"));
                    format!("# JSON conversion error: {e}\n# Original: {xml_str}")
                }
            }
        }
        Err(e) => {
            LogController::debug(&format!("Failed to parse XML: {e}"));
            format!("# XML parse error: {e}\n# Original: {xml_str}")
        }
    }
}
fn convert_yaml_to_xml(yaml_str: &str) -> String {
    // YAML -> JSON -> XML
    match serde_yml::from_str::<JsonValue>(yaml_str) {
        Ok(yaml_value) => {
            // Convert YAML to XML manually
            json_value_to_xml(&yaml_value, "root")
        }
        Err(e) => {
            LogController::debug(&format!("Failed to parse YAML: {e}"));
            format!("<!-- YAML parse error: {e} -->\n<!-- Original: {yaml_str} -->")
        }
    }
}
fn convert_xml_to_yaml(xml_str: &str) -> String {
    // XML -> JSON -> YAML
    match xml_from_str::<JsonValue>(xml_str) {
        Ok(xml_value) => match serde_yml::to_string(&xml_value) {
            Ok(yaml_str) => yaml_str
                .trim_end_matches('\n')
                .trim_end_matches("---")
                .trim()
                .to_string(),
            Err(e) => {
                LogController::debug(&format!("Failed to convert XML to YAML: {e}"));
                format!("# YAML conversion error: {e}\n# Original: {xml_str}")
            }
        },
        Err(e) => {
            LogController::debug(&format!("Failed to parse XML: {e}"));
            format!("# XML parse error: {e}\n# Original: {xml_str}")
        }
    }
}
fn clean_json_string(json_str: &str) -> String {
    let trimmed = json_str.trim();

    // First, try to parse the string as-is
    if serde_json::from_str::<JsonValue>(trimmed).is_ok() {
        // If it's already valid JSON, don't modify it
        return trimmed.to_string();
    }

    let mut cleaned = trimmed.to_string();

    // Only apply cleaning if the original parse failed
    // Remove surrounding quotes if they exist and contain JSON
    if cleaned.starts_with('"') && cleaned.ends_with('"') && cleaned.len() > 1 {
        let inner = &cleaned[1..cleaned.len() - 1];
        // Try parsing the inner content
        if serde_json::from_str::<JsonValue>(inner).is_ok() {
            cleaned = inner.to_string();
        } else {
            // Try unescaping the inner content
            let unescaped = inner.replace("\\\"", "\"").replace("\\\\", "\\");
            if serde_json::from_str::<JsonValue>(&unescaped).is_ok() {
                cleaned = unescaped;
            }
        }
    }

    // If it's still not valid JSON after basic cleaning, try more aggressive cleaning
    if serde_json::from_str::<JsonValue>(&cleaned).is_err() {
        // Handle HTML/XML entities
        cleaned = cleaned.replace("&amp;", "&");
        cleaned = cleaned.replace("&lt;", "<");
        cleaned = cleaned.replace("&gt;", ">");
        cleaned = cleaned.replace("&quot;", "\"");
        cleaned = cleaned.replace("&apos;", "'");

        // If it still doesn't look like JSON, wrap it as a string value
        if !cleaned.starts_with('{') && !cleaned.starts_with('[') && !cleaned.starts_with('"') {
            cleaned = format!("\"{}\"", cleaned.replace("\"", "\\\""));
        }
    }

    cleaned
}
fn json_value_to_xml(value: &JsonValue, _tag_name: &str) -> String {
    match value {
        JsonValue::Object(map) => {
            let mut xml = String::new();
            for (key, val) in map {
                xml.push_str(&format!("<{key}>"));
                xml.push_str(&json_value_to_xml(val, key));
                xml.push_str(&format!("</{key}>"));
            }
            xml
        }
        JsonValue::Array(arr) => {
            let mut xml = String::new();
            for (i, val) in arr.iter().enumerate() {
                let item_tag = format!("item{i}");
                xml.push_str(&format!("<{item_tag}>"));
                xml.push_str(&json_value_to_xml(val, &item_tag));
                xml.push_str(&format!("</{item_tag}>"));
            }
            xml
        }
        JsonValue::String(s) => xml_escape(s),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => String::new(),
    }
}
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
fn format_json(json_str: &str) -> String {
    // Clean and format JSON
    let cleaned_json = clean_json_string(json_str);
    match serde_json::from_str::<JsonValue>(&cleaned_json) {
        Ok(json_value) => match serde_json::to_string_pretty(&json_value) {
            Ok(formatted) => formatted,
            Err(e) => {
                LogController::debug(&format!("Failed to format JSON: {e}"));
                format!("# JSON format error: {e}\n# Original: {json_str}")
            }
        },
        Err(e) => {
            LogController::debug(&format!("Failed to parse JSON for formatting: {e}"));
            format!("# JSON parse error: {e}\n# Original: {json_str}")
        }
    }
}
fn format_yaml(yaml_str: &str) -> String {
    // Re-parse and format YAML
    match serde_yml::from_str::<JsonValue>(yaml_str) {
        Ok(yaml_value) => match serde_yml::to_string(&yaml_value) {
            Ok(formatted) => formatted
                .trim_end_matches('\n')
                .trim_end_matches("---")
                .trim()
                .to_string(),
            Err(e) => {
                LogController::debug(&format!("Failed to format YAML: {e}"));
                format!("# YAML format error: {e}\n# Original: {yaml_str}")
            }
        },
        Err(e) => {
            LogController::debug(&format!("Failed to parse YAML for formatting: {e}"));
            format!("# YAML parse error: {e}\n# Original: {yaml_str}")
        }
    }
}
fn format_xml(xml_str: &str) -> String {
    // Simple XML formatting - parse and regenerate
    match xml_from_str::<JsonValue>(xml_str) {
        Ok(xml_value) => {
            // Convert back to XML with our formatter
            json_value_to_xml(&xml_value, "root")
        }
        Err(e) => {
            LogController::debug(&format!("Failed to parse XML for formatting: {e}"));
            format!("<!-- XML parse error: {e} -->\n<!-- Original: {xml_str} -->")
        }
    }
}
