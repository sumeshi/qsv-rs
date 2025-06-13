# Contributing to Quilter-CSV

Thank you for your interest in contributing to Quilter-CSV!

## Development Setup

### Using Dev Container

1. Clone the repository and open in VS Code
2. Click "Reopen in Container" when prompted
3. The development environment will be set up automatically

## Project Structure

```
src/
├── controllers/     # Command parsing and control
├── operations/      # Data processing operations
│   ├── chainables/  # Transformations (select, filter, etc.)
│   ├── finalizers/  # Output operations (show, dump, etc.)
│   └── initializers/ # Data loading
└── main.rs         # Entry point
tests/              # Python test suite
```

## Testing

```bash
# Run all tests
python3 tests/run_tests.py

# Run specific test
python3 -m pytest tests/test_chainables_select.py -v
```

## Code Standards

```bash
# Format and lint
cargo fmt --all
cargo clippy --all-targets --all-features

# Check before submitting
cargo build --release
python3 tests/run_tests.py
```

## Submitting Changes

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make your changes with clear commit messages
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request with a clear description

## Adding New Operations

1. Create operation in `src/operations/chainables/yourfeature.rs`
2. Add to module exports in `mod.rs`
3. Add to dataframe controller
4. Add command parsing in `main.rs`
5. Write tests in `tests/test_chainables_yourfeature.py`

## Getting Help

- Report issues: [GitHub Issues](https://github.com/sumeshi/qsv-rs/issues)
- Ask questions: [GitHub Discussions](https://github.com/sumeshi/qsv-rs/discussions)
