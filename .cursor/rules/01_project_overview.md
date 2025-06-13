# qsv-rs Project Rules for Cursor

## Core Development Rules
- Follow established Rust and CLI development standards
- Auto-add beneficial rules to `.cursor/rules` or confirm with user

## Project Overview
This is `qsv` - A fast, flexible, and memory-efficient command-line tool written in Rust for processing large CSV files. Built on Polars, designed for handling tens or hundreds of gigabytes of CSV data efficiently.

## Rust Development Standards

### Code Style
- Follow Rust standard formatting with `rustfmt`
- Use `clippy` recommendations for code quality
- Prefer explicit error handling with `Result<T, E>` and `anyhow::Error`
- Use meaningful variable and function names in English
- Write comprehensive documentation comments using `///`

### Dependencies Management
- Current key dependencies: `polars`, `clap`, `anyhow`, `serde`, `chrono`
- When adding new dependencies, justify the choice and check compatibility
- Prefer established, well-maintained crates

### Architecture Patterns
- Main binary is CLI-focused with command chaining pipeline
- Commands are divided into: Initializers, Chainable functions, and Finalizers
- Use the controller pattern for command organization
- Operations should be modular and composable

### Error Handling & Performance
- Use `anyhow::Result` with meaningful user-facing error messages
- Prioritize memory efficiency and Polars lazy evaluation

## CLI Design Principles
- Commands should be chainable with `-` separator
- Support glob patterns for file inputs
- Provide sensible defaults (e.g., automatic `showtable` finalizer)
- Support both compressed (.gz) and uncompressed CSV files
- Make column selection flexible (names, indices, ranges)

## Testing & Documentation
- Write comprehensive unit/integration tests for various CSV formats and edge cases
- Keep README.md and documentation updated with examples 
