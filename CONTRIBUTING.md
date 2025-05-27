# Contributing to Quilter-CSV

Thank you for your interest in contributing to Quilter-CSV! This document provides guidelines and information for developers who want to contribute to the project.

## Table of Contents

- [Development Environment Setup](#development-environment-setup)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Code Style and Standards](#code-style-and-standards)
- [Submitting Changes](#submitting-changes)
- [Development Workflow](#development-workflow)

## Development Environment Setup

### Prerequisites

- [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/)
- [Visual Studio Code](https://code.visualstudio.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

### Using Dev Container (Recommended)

This project includes a complete development environment using VS Code Dev Containers:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/sumeshi/qsv-rs.git
   cd qsv-rs
   ```

2. **Open in VS Code:**
   ```bash
   code .
   ```

3. **Reopen in Container:**
   - When prompted, click "Reopen in Container"
   - Or use Command Palette (`Ctrl+Shift+P`) → "Dev Containers: Reopen in Container"

4. **Wait for setup:**
   - The container will automatically install Rust, Python, and all dependencies
   - This may take a few minutes on first run

### Manual Setup (Alternative)

If you prefer not to use Dev Containers:

1. **Install Rust:**
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   source ~/.cargo/env
   ```

2. **Install Python 3.8+ and dependencies:**
   ```bash
   # Ubuntu/Debian
   sudo apt update
   sudo apt install python3 python3-pip fish

       # macOS (if needed)
    brew install python fish

   # Install Python dependencies
   pip3 install polars pytest
   ```

3. **Build the project:**
   ```bash
   cargo build
   ```

## Project Structure

```
qsv-rs/
├── src/                          # Rust source code
│   ├── controllers/              # Command parsing and control logic
│   │   ├── command.rs           # Command line argument parsing
│   │   ├── dataframe.rs         # Main DataFrame controller
│   │   └── log.rs               # Logging utilities
│   ├── operations/              # Core data processing operations
│   │   ├── chainables/          # Chainable operations (select, filter, etc.)
│   │   ├── finalizers/          # Output operations (show, dump, etc.)
│   │   ├── initializers/        # Data loading operations
│   │   └── quilters/            # YAML workflow processing
│   └── main.rs                  # Application entry point
├── tests/                       # Test suite
│   ├── test_*.py               # Python test files
│   ├── test_all.py             # Main test runner
│   ├── simpletest.fish         # Fish shell test script
│   └── test_base.py            # Test base classes and utilities
├── sample/                      # Sample data and configurations
│   ├── simple.csv              # Test CSV data
│   ├── quilt-test.yaml         # Basic quilt configuration
│   └── quilt-complex.yaml      # Complex quilt example
├── .devcontainer/              # Dev Container configuration
└── README.md                   # Project documentation
```

## Testing

### Running All Tests

The project includes comprehensive test suites in both Python and Fish shell:

#### Python Test Suite (Recommended)

```bash
# Run all tests
python3 tests/run_tests.py

# Run specific test file
python3 -m pytest tests/test_chainables_select.py -v

# Run tests with verbose output
python3 tests/run_tests.py --verbose
```

#### Fish Shell Test Suite

```bash
# Make sure fish is installed and qsv is built
cargo build

# Run simple tests
fish tests/simpletest.fish
```

### Test Categories

1. **Initializer Tests** (`test_initializers_*.py`)
   - Data loading functionality
   - File format support
   - Error handling

2. **Chainable Operation Tests** (`test_chainables_*.py`)
   - Data transformation operations
   - Filtering and selection
   - Sorting and aggregation

3. **Finalizer Tests** (`test_finalizers_*.py`)
   - Output formatting
   - File export functionality
   - Display operations

### Writing New Tests

When adding new functionality, please include tests:

1. **Create a new test file** following the naming convention:
   ```python
   # tests/test_chainables_newfeature.py
   from test_base import QSVTestBase

   class TestNewFeature(QSVTestBase):
       def test_basic_functionality(self):
           result = self.run_qsv_command([
               "load", self.sample_csv,
               "-", "newfeature", "arg1", "arg2",
               "-", "show"
           ])
           self.assertIn("expected_output", result)
   ```

2. **Add the test to the main runner** in `tests/run_tests.py`

3. **Test edge cases** including:
   - Empty data
   - Invalid arguments
   - Large datasets
   - Error conditions

## Code Style and Standards

### Rust Code Style

- Follow standard Rust formatting with `rustfmt`
- Use `cargo clippy` for linting
- Write comprehensive documentation comments
- Handle errors appropriately (avoid `unwrap()` in production code)

```bash
# Format code
cargo fmt

# Run linter
cargo clippy

# Run tests
cargo test
```

### Python Test Style

- Follow PEP 8 style guidelines
- Use descriptive test method names
- Include docstrings for complex test cases
- Use the provided `QSVTestBase` class for consistency

### Documentation

- Update README.md for user-facing changes
- Add inline comments for complex logic
- Update help text for new commands
- Include examples in documentation

## Submitting Changes

### Before Submitting

1. **Run the full test suite:**
   ```bash
   python3 tests/run_tests.py
   ```

2. **Check code formatting:**
   ```bash
   cargo fmt --check
   cargo clippy
   ```

3. **Build successfully:**
   ```bash
   cargo build --release
   ```

4. **Test manually with sample data:**
   ```bash
   ./target/release/qsv load sample/simple.csv - select col1,col2 - showtable
   ```

### Pull Request Guidelines

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make focused commits:**
   - One logical change per commit
   - Clear, descriptive commit messages
   - Reference issues when applicable

3. **Update documentation:**
   - Add/update command help text
   - Update README.md if needed
   - Add examples for new features

4. **Include tests:**
   - Add tests for new functionality
   - Ensure existing tests still pass
   - Test edge cases and error conditions

5. **Submit pull request:**
   - Provide clear description of changes
   - Reference related issues
   - Include testing instructions

## Development Workflow

### Adding a New Chainable Operation

1. **Create the operation module:**
   ```rust
   // src/operations/chainables/newop.rs
   use polars::prelude::*;

   pub fn newop(df: &LazyFrame, param: &str) -> LazyFrame {
       // Implementation here
   }
   ```

2. **Add to the module exports:**
   ```rust
   // src/operations/chainables/mod.rs
   pub mod newop;
   ```

3. **Add to the controller:**
   ```rust
   // src/controllers/dataframe.rs
   pub fn newop(&mut self, param: &str) {
       if let Some(df) = &self.df {
           self.df = Some(operations::chainables::newop::newop(df, param));
       }
   }
   ```

4. **Add command parsing:**
   ```rust
   // src/main.rs
   "newop" => {
       check_data_loaded(controller, "newop");
       if cmd.args.is_empty() {
           eprintln!("Error: 'newop' command requires a parameter");
           process::exit(1);
       }
       controller.newop(&cmd.args[0]);
   },
   ```

5. **Add help text:**
   ```rust
   // src/controllers/command.rs
   fn print_newop_help() {
       println!("newop: Description of the operation\n");
       println!("Usage: newop <parameter>\n");
       println!("Examples:");
       println!("  qsv load data.csv - newop value - show");
   }
   ```

6. **Write tests:**
   ```python
   # tests/test_chainables_newop.py
   class TestNewOp(QSVTestBase):
       def test_basic_newop(self):
           # Test implementation
   ```

### Debugging Tips

1. **Enable debug logging:**
   ```bash
   RUST_LOG=debug ./target/debug/qsv your_command
   ```

2. **Use the query planner:**
   ```bash
   qsv load data.csv - your_operations - showquery
   ```

3. **Test with small datasets first:**
   ```bash
   qsv load sample/simple.csv - your_operations - showtable
   ```

4. **Check intermediate results:**
   ```bash
   qsv load data.csv - step1 - showtable  # Check step1 output
   qsv load data.csv - step1 - step2 - showtable  # Check step2 output
   ```

## Release Process

### Creating a New Release

Releases are automated through GitHub Actions. To create a new release:

1. **Ensure all changes are merged to main:**
   ```bash
   git checkout main
   git pull origin main
   ```

2. **Run the release script:**
   ```bash
   ./scripts/create-release.sh v0.1.0
   ```

3. **Monitor the build:**
   - The script will create a git tag and push it
       - GitHub Actions will automatically:
      - Run all tests on multiple platforms
      - Build binaries for Linux and Windows
      - Create a GitHub release with downloadable binaries
      - Generate SHA256 checksums for verification

### Release Artifacts

Each release includes:
- `qsv-rs-linux-x86_64` - Linux binary
- `qsv-rs-windows-x86_64.exe` - Windows binary  
- `checksums-*.txt` - SHA256 checksums for verification

### Version Numbering

We follow [Semantic Versioning](https://semver.org/):
- `MAJOR.MINOR.PATCH` (e.g., `1.2.3`)
- **MAJOR**: Incompatible API changes
- **MINOR**: New functionality (backwards compatible)
- **PATCH**: Bug fixes (backwards compatible)

### Pre-release Testing

Before creating a release:

1. **Run full test suite:**
   ```bash
   python3 tests/run_tests.py
   cargo test
   ```

2. **Test cross-platform builds locally:**
   ```bash
   # Test different targets
   cargo build --target x86_64-unknown-linux-gnu
   cargo build --target x86_64-pc-windows-msvc  # Requires Windows or cross-compilation
   ```

3. **Verify documentation is up to date:**
   - README.md examples work
   - Help text matches actual behavior
   - CONTRIBUTING.md reflects current process

## Getting Help

- **Issues:** Report bugs and request features on [GitHub Issues](https://github.com/sumeshi/qsv-rs/issues)
- **Discussions:** Ask questions in [GitHub Discussions](https://github.com/sumeshi/qsv-rs/discussions)
- **Documentation:** Check the [README.md](README.md) for usage examples

## License

By contributing to Quilter-CSV, you agree that your contributions will be licensed under the same [MIT License](LICENSE) that covers the project. 