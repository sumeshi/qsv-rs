# Coding Standards

## Core Principles
- Clean, readable, maintainable code with self-documenting naming
- Follow principle of least surprise, prefer composition over inheritance

## Rust-Specific Guidelines

### Naming Conventions
- Use `snake_case` for variables, functions, and modules
- Use `PascalCase` for types, structs, enums, and traits
- Use `SCREAMING_SNAKE_CASE` for constants and static variables
- Use descriptive names that explain intent

### Function Design
- Functions should do one thing well
- Keep functions small and focused
- Use early returns to reduce nesting
- Return `Result<T, E>` for operations that can fail

### Error Handling Best Practices
```rust
// Prefer this:
fn process_csv(path: &str) -> anyhow::Result<DataFrame> {
    let df = LazyFrame::scan_csv(path, ScanArgsCSV::default())?
        .collect()?;
    Ok(df)
}

// Instead of this:
fn process_csv(path: &str) -> DataFrame {
    let df = LazyFrame::scan_csv(path, ScanArgsCSV::default())
        .unwrap()
        .collect()
        .unwrap();
    df
}
```

### Pattern Matching
- Use pattern matching instead of multiple if-else chains
- Handle all cases explicitly, avoid catch-all patterns when possible
- Use `if let` for simple optional value handling

### Documentation
- Use `///` for public APIs, `//` for complex logic
- Comments explain "why", not "what"

## Code Organization

### Module Structure
- One concept per module
- Use `mod.rs` files for module organization
- Keep public APIs minimal and well-documented
- Use `pub(crate)` for internal APIs

### Import Organization
```rust
// Standard library imports first
use std::path::Path;
use std::fs::File;

// External crate imports
use anyhow::{Context, Result};
use clap::Parser;
use polars::prelude::*;

// Internal imports last
use crate::operations::Operations;
use crate::controllers::Controller;
```

## Performance Guidelines
- Prefer borrowing over cloning when possible
- Use `&str` instead of `String` for read-only string parameters  
- Use iterators instead of manual loops
- Profile before optimizing
- Consider memory allocation patterns for large data processing

## Development Commands

### Building and Formatting
```bash
# Build the project
cargo build

# Build for release
cargo build --release

# Format all code
cargo fmt --all

# Run clippy with all targets and features
cargo clippy --all-targets --all-features
```
