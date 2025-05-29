use crate::controllers::log::LogController;
use polars::prelude::*;

pub fn select(df: &LazyFrame, colnames: &[String]) -> LazyFrame {
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!(
                "Error collecting DataFrame for schema check in select: {}",
                e
            );
            std::process::exit(1);
        }
    };
    let schema = collected_df.schema();
    let available_columns: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();

    // Expand column names to handle colon notation, quoted colon notation, and numeric indices
    let mut expanded_colnames = Vec::new();
    for colname in colnames {
        if colname.contains(':') && !colname.starts_with('"') {
            // Check if it's a numeric range (e.g., "1:3")
            if is_numeric_range(colname) {
                let range_cols = parse_numeric_range(colname, &available_columns);
                expanded_colnames.extend(range_cols);
            } else {
                // Handle regular colon-separated range (col1:col3)
                let range_cols = parse_colon_range(colname, &available_columns);
                expanded_colnames.extend(range_cols);
            }
        } else if colname.starts_with('"') && colname.contains("\":\"") && colname.ends_with('"') {
            // Handle quoted colon notation: "col1":"col3"
            let inner = &colname[1..colname.len() - 1]; // Remove outer quotes
            if let Some((start_col, end_col)) = inner.split_once("\":\"") {
                let range_cols = parse_quoted_colon_range(start_col, end_col, &available_columns);
                expanded_colnames.extend(range_cols);
            } else {
                expanded_colnames.push(colname.clone());
            }
        } else {
            // Check if it's a single numeric index
            if is_numeric_index(colname) {
                if let Some(col_name) = parse_single_numeric_index(colname, &available_columns) {
                    expanded_colnames.push(col_name);
                } else {
                    eprintln!("Error: Invalid column index '{}'", colname);
                    std::process::exit(1);
                }
            } else {
                expanded_colnames.push(colname.clone());
            }
        }
    }

    // Validate all expanded column names exist
    for colname in &expanded_colnames {
        if !schema.iter_names().any(|s| s == colname) {
            eprintln!(
                "Error: Column '{}' not found in DataFrame for select operation",
                colname
            );
            std::process::exit(1);
        }
    }

    let mut selected_cols: Vec<Expr> = Vec::new();

    for name in &expanded_colnames {
        if available_columns.contains(name) {
            selected_cols.push(col(name));
        } else {
            LogController::warn(&format!("Column '{}' not found in DataFrame.", name));
        }
    }

    if selected_cols.is_empty() {
        LogController::warn("No valid columns selected. Returning original DataFrame.");
        return df.clone();
    }

    df.clone().select(&selected_cols)
}

// Helper function to check if a string is a numeric index
fn is_numeric_index(s: &str) -> bool {
    s.parse::<usize>().is_ok()
}

// Helper function to check if a string is a numeric range (e.g., "1:3")
fn is_numeric_range(s: &str) -> bool {
    if let Some((start, end)) = s.split_once(':') {
        start.trim().parse::<usize>().is_ok() && end.trim().parse::<usize>().is_ok()
    } else {
        false
    }
}

// Helper function to parse a single numeric index to column name
fn parse_single_numeric_index(index_str: &str, available_columns: &[String]) -> Option<String> {
    if let Ok(index) = index_str.parse::<usize>() {
        if index >= 1 && index <= available_columns.len() {
            Some(available_columns[index - 1].clone()) // Convert 1-based to 0-based
        } else {
            None
        }
    } else {
        None
    }
}

// Helper function to parse numeric ranges (1:3)
fn parse_numeric_range(range_str: &str, available_columns: &[String]) -> Vec<String> {
    if let Some((start_str, end_str)) = range_str.split_once(':') {
        let start_str = start_str.trim();
        let end_str = end_str.trim();

        if let (Ok(start_idx), Ok(end_idx)) = (start_str.parse::<usize>(), end_str.parse::<usize>())
        {
            // Convert 1-based indices to 0-based
            let start_zero_based = if start_idx > 0 { start_idx - 1 } else { 0 };
            let end_zero_based = if end_idx > 0 { end_idx - 1 } else { 0 };

            if start_zero_based < available_columns.len()
                && end_zero_based < available_columns.len()
                && start_zero_based <= end_zero_based
            {
                return available_columns[start_zero_based..=end_zero_based].to_vec();
            } else {
                LogController::warn(&format!(
                    "Invalid numeric range: indices out of bounds or invalid order: {}",
                    range_str
                ));
            }
        } else {
            LogController::warn(&format!("Invalid numeric range format: {}", range_str));
        }
    }

    // If parsing fails, return empty vector
    vec![]
}

// Helper function to parse colon-separated ranges (col1:col3)
pub fn parse_colon_range(range_str: &str, available_columns: &[String]) -> Vec<String> {
    if let Some((start_col, end_col)) = range_str.split_once(':') {
        let start_col = start_col.trim();
        let end_col = end_col.trim();

        // Find indices of start and end columns
        if let (Some(start_idx), Some(end_idx)) = (
            available_columns.iter().position(|c| c == start_col),
            available_columns.iter().position(|c| c == end_col),
        ) {
            if start_idx <= end_idx {
                return available_columns[start_idx..=end_idx].to_vec();
            } else {
                LogController::warn(&format!(
                    "Invalid range: '{}' comes after '{}' in column order",
                    start_col, end_col
                ));
            }
        } else {
            LogController::warn(&format!(
                "Column range '{}' contains invalid column names",
                range_str
            ));
        }
    }

    // If parsing fails, return the original string as a single column
    vec![range_str.to_string()]
}

// Helper function to parse quoted colon-separated ranges ("col1":"col3")
pub fn parse_quoted_colon_range(
    start_col: &str,
    end_col: &str,
    available_columns: &[String],
) -> Vec<String> {
    // Find indices of start and end columns
    if let (Some(start_idx), Some(end_idx)) = (
        available_columns.iter().position(|c| c == start_col),
        available_columns.iter().position(|c| c == end_col),
    ) {
        if start_idx <= end_idx {
            return available_columns[start_idx..=end_idx].to_vec();
        } else {
            LogController::warn(&format!(
                "Invalid quoted range: '{}' comes after '{}' in column order",
                start_col, end_col
            ));
        }
    } else {
        LogController::warn(&format!(
            "Quoted column range '\"{}\":\"{}\"' contains invalid column names",
            start_col, end_col
        ));
    }

    // If parsing fails, return the original column names
    vec![start_col.to_string(), end_col.to_string()]
}

// Function to select rows by indices (1-based)
pub fn select_rows(df: &LazyFrame, row_indices: &[usize]) -> LazyFrame {
    // Convert 1-based indices to 0-based for internal use
    let zero_based_indices: Vec<usize> = row_indices
        .iter()
        .map(|&i| if i > 0 { i - 1 } else { 0 })
        .collect();

    if zero_based_indices.is_empty() {
        return df.clone().limit(0);
    }

    // Collect the DataFrame first to access specific rows
    let collected_df = match df.clone().collect() {
        Ok(df) => df,
        Err(e) => {
            eprintln!("Error collecting DataFrame for row selection: {}", e);
            std::process::exit(1);
        }
    };

    let total_rows = collected_df.height();
    let mut valid_indices = Vec::new();

    // Filter out invalid indices
    for &idx in &zero_based_indices {
        if idx < total_rows {
            valid_indices.push(idx);
        }
    }

    if valid_indices.is_empty() {
        return df.clone().limit(0);
    }

    // Select specific rows by slicing and concatenating
    let mut selected_rows = Vec::new();

    for &idx in &valid_indices {
        let single_row = collected_df.slice(idx as i64, 1);
        selected_rows.push(single_row.lazy());
    }

    if selected_rows.is_empty() {
        return df.clone().limit(0);
    }

    // Concatenate all selected rows
    let result_lazy = concat(
        selected_rows,
        UnionArgs {
            parallel: false,
            rechunk: true,
            ..Default::default()
        },
    )
    .unwrap_or_else(|e| {
        eprintln!("Error concatenating selected rows: {}", e);
        std::process::exit(1);
    });

    result_lazy
}
