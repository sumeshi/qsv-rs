# CLI Development Guidelines

## Command Structure Design

### Pipeline Architecture
The qsv tool follows a pipeline pattern with three types of commands:

1. **Initializers** - Load data (e.g., `load`)
2. **Chainable Operations** - Transform/filter data (e.g., `select`, `grep`, `sort`)
3. **Finalizers** - Output results (e.g., `show`, `showtable`, `headers`)

### Command Implementation Pattern
```rust
// Each command should implement a consistent pattern
pub struct SelectCommand {
    pub colnames: String,
}

impl SelectCommand {
    pub fn execute(&self, df: LazyFrame) -> anyhow::Result<LazyFrame> {
        // Implementation logic here
        Ok(df.select(columns))
    }
}
```

### Argument Parsing Guidelines
- Use `clap` with derive macros for consistent CLI interface
- Provide clear help text for all arguments
- Use sensible defaults where possible
- Support both short and long argument forms

```rust
#[derive(Parser)]
pub struct LoadArgs {
    /// Path to CSV file(s). Supports glob patterns.
    pub path: Vec<String>,
    
    /// Field separator character
    #[arg(short = 's', long = "separator", default_value = ",")]
    pub separator: char,
    
    /// Enable low-memory mode for very large files
    #[arg(long)]
    pub low_memory: bool,
}
```

## Error Handling for CLI

### User-Friendly Error Messages
- Provide actionable error messages
- Include file paths and line numbers when relevant
- Suggest solutions when possible

```rust
// Good error message
return Err(anyhow::anyhow!(
    "Failed to read CSV file '{}': {}. Check if the file exists and has proper permissions.",
    path.display(),
    e
));

// Poor error message
return Err(anyhow::anyhow!("CSV error: {}", e));
```

### Exit Codes
- Use standard exit codes (0 for success, 1 for general errors)
- Consider using specific exit codes for different error types

## Input/Output Handling

### File Support
- Support both single files and glob patterns
- Handle compressed files (.gz) automatically
- Provide progress indicators for large files

### CSV Processing
- Support different separators (comma, tab, pipe, etc.)
- Handle files with and without headers
- Support different encodings
- Gracefully handle malformed CSV data

### Output Formatting
- Default to table format for human readability
- Support machine-readable output formats
- Allow output redirection to files

## Performance & UX
- Use streaming/low-memory mode for large files with progress indicators
- Polars lazy evaluation by default, collect only for output

## Performance Considerations
- Use lazy evaluation with Polars LazyFrame when possible
- Avoid unnecessary data collection with `.collect()`
- Implement streaming/batching for large datasets (>1GB)
- Use parallel processing with rayon for multi-file operations

## Memory Management
- Default batch size: 1GB for streaming operations
- Support configurable batch sizes (1MB-10GB range)
- Use traditional methods for datasets that fit in memory
- Implement fallback mechanisms for large dataset operations

## Testing CLI Commands

### Integration Tests
```rust
#[test]
fn test_load_and_select_command() {
    let output = Command::new("target/debug/qsv")
        .args(&["load", "test.csv", "-", "select", "col1,col2"])
        .output()
        .expect("Failed to execute command");
    
    assert!(output.status.success());
    // Validate output content
}
```

### Test Data
- Maintain a set of test CSV files with different characteristics
- Include edge cases: empty files, single column, special characters
- Test with different file sizes and formats

## Integration Test Commands

### Test Development Workflow
```bash
# 1. Run formatting and linting
cargo fmt --all
cargo clippy --all-targets --all-features

# 2. Build the project first
cargo build

# 3. Run integration tests
python3 tests/run_tests.py

``` 

### Python Integration Tests
The project uses Python for integration testing of CLI commands:

```bash
# Run all integration tests
python3 tests/run_tests.py

# Run specific test file
python3 -m pytest tests/test_chainables_select.py -v
```
