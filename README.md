# Quilter-CSV
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg?style=flat)](LICENSE)
[![CI/CD Pipeline](https://github.com/sumeshi/qsv-rs/actions/workflows/ci-cd.yml/badge.svg?branch=main)](https://github.com/sumeshi/qsv-rs/actions/workflows/ci-cd.yml)

![Quilter-CSV](https://gist.githubusercontent.com/sumeshi/644af27c8960a9b6be6c7470fe4dca59/raw/00d774e6814a462eb48e68f29fc6226976238777/quilter-csv.svg)

A fast, flexible, and memory-efficient command-line tool written in Rust for processing large CSV files. Inspired by [xsv](https://github.com/BurntSushi/xsv) and built on [Polars](https://www.pola.rs/), it's designed for handling tens or hundreds of gigabytes of CSV data efficiently in workflows like log analysis and digital forensics.

> [!IMPORTANT]  
> This project is in the early stages of development. Please be aware that frequent changes and updates are likely to occur.

> [!NOTE]
> The original version of this project was implemented in Python and can be found at [sumeshi/quilter-csv](https://github.com/sumeshi/quilter-csv). This Rust version is a complete rewrite.

## Features

- **Pipeline-style command chaining**: Chain multiple commands in a single line for fast and efficient data processing
- **Flexible filtering and transformation**: Perform operations like select, filter, sort, deduplicate, and timezone conversion
- **YAML-based batch processing (Quilt)**: Automate complex workflows using YAML configuration files

## Usage
![](https://gist.githubusercontent.com/sumeshi/644af27c8960a9b6be6c7470fe4dca59/raw/2a19fafd4f4075723c731e4a8c8d21c174cf0ffb/qsv.svg)

### Getting Help

To see available commands and options, run `qsv` without any arguments:

```bash
$ qsv -h
```

### Example

Here's an example of reading a CSV file, extracting rows that contain 4624 in the 'Event ID' column, and displaying the top 3 rows sorted by the 'Date and Time' column:

```bash
$ qsv load Security.csv - isin 'Event ID' 4624 - sort 'Date and Time' - head 3 - showtable
```

This command:
1. Loads `Security.csv`
2. Filters rows where `Event ID` is 4624
3. Sorts by `Date and Time`
4. Shows the first 3 rows as a table

### Command Structure

Quilter-CSV commands are composed of three types of steps:

- **Initializer**: Loads data (e.g., `load`)
- **Chainable**: Transforms or filters data (e.g., `select`, `grep`, `sort`, etc.)
- **Finalizer**: Outputs or summarizes data (e.g., `show`, `showtable`, `headers`, etc.)

Each step is separated by a hyphen (`-`):

```bash
$ qsv <INITIALIZER> <args> - <CHAINABLE> <args> - <FINALIZER> <args>
```

**Note:** If no finalizer is explicitly specified, `showtable` is automatically used as the default finalizer, making it easy to quickly view results:

```bash
$ qsv load data.csv - select col1,col2 - head 5
# Equivalent to:
$ qsv load data.csv - select col1,col2 - head 5 - showtable
```

## Command Reference

### Initializers

#### `load`
Load one or more CSV files.

| Parameter     | Type        | Default | Description                                      |
|---------------|-------------|---------|--------------------------------------------------|
| path          | list[str] |         | One or more paths to CSV files. Glob patterns are supported. Gzip files (.gz) are automatically detected and decompressed. |
| -s, --separator | str       | `,`     | Field separator character.                       |
| --low-memory  | flag    | `false` | Enable low-memory mode for very large files.     |
| --no-headers  | flag    | `false` | Treat the first row as data, not headers. When enabled, columns will be named automatically (column_0, column_1, etc.). |

Example:
```bash
$ qsv load data.csv
$ qsv load data.csv.gz
$ qsv load data1.csv data2.csv data3.csv
$ qsv load "logs/*.tsv" -s \t
$ qsv load logs/*.tsv --separator=\t
$ qsv load data.csv --low-memory
$ qsv load data.csv --no-headers
```

### Chainable Functions

#### `select`
Select columns by name or range.

| Parameter | Type                | Default | Description                                                                                                |
|-----------|---------------------|---------|------------------------------------------------------------------------------------------------------------|
| colnames  | str/list/range      |         | Column name(s). Use comma-separated for specific columns (e.g., `col1,col3`) or hyphen-separated for a range (e.g., `col1-col3`). This is a required argument. |

```bash
$ qsv load data.csv - select datetime
$ qsv load data.csv - select col1,col3
$ qsv load data.csv - select col1-col3
```

#### `isin`
Filter rows where a column matches any of the given values.

| Parameter | Type   | Default | Description                                                                          |
|-----------|--------|---------|--------------------------------------------------------------------------------------|
| colname   | str    |         | Column name to filter. Required.                                                     |
| values    | list   |         | Comma-separated values. Filters rows where the column matches any of these values (OR condition). Required. |

```bash
$ qsv load data.csv - isin col1 1
$ qsv load data.csv - isin col1 1,4
```

#### `contains`
Filter rows where a column contains a specific literal substring.

| Parameter   | Type   | Default | Description                                 |
|-------------|--------|---------|---------------------------------------------|
| colname     | str    |         | Column name to search. Required.            |
| substring   | str    |         | The literal substring to search for. Required. |
| -i, --ignorecase | flag | `false` | Perform case-insensitive matching.          |

```bash
$ qsv load data.csv - contains str ba
$ qsv load data.csv - contains str BA -i
$ qsv load data.csv - contains str BA --ignorecase
```

#### `sed`
Replace values in a column using a Regex pattern.

| Parameter   | Type   | Default | Description                                 |
|-------------|--------|---------|---------------------------------------------|
| colname     | str    |         | Column name to modify. Required.            |
| pattern     | str    |         | Regex pattern to search for. Required.      |
| replacement | str    |         | Replacement string. Required.               |
| -i, --ignorecase | flag | `false` | Perform case-insensitive matching.          |

```bash
$ qsv load data.csv - sed str foo foooooo
$ qsv load data.csv - sed str FOO foooooo -i
$ qsv load data.csv - sed str ".*o.*" foooooo
```

#### `grep`
Filter rows where any column matches a regex pattern.

| Parameter | Type | Default | Description |
|---|---|---|---|
| pattern | str |         | Regex pattern to search for in any column. Required. |
| -i, --ignorecase | flag | `false` | Perform case-insensitive matching. |
| -v, --invert-match | flag | `false` | Invert the sense of matching, to select non-matching lines. |

Example:
```bash
$ qsv load data.csv - grep foo 
$ qsv load data.csv - grep "^FOO" -i
$ qsv load data.csv - grep "^FOO" -i -v
```

#### `head`
Displays the first N rows of the dataset.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| N         | int  | 5       | Number of rows to display. This can be provided as a direct argument or via `-n`/`--number` option. |

```bash
$ qsv load data.csv - head 3
$ qsv load data.csv - head -n 7 
```

#### `tail`
Displays the last N rows of the dataset.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| N         | int  | 5       | Number of rows to display. This can be provided as a direct argument or via `-n`/`--number` option. |

```bash
$ qsv load data.csv - tail 3
$ qsv load data.csv - tail -n 7
```

#### `sort`
Sorts the dataset based on the specified column(s).

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| colnames  | str/list |         | Column name(s) to sort by. Comma-separated for multiple columns (e.g., `col1,col3`) or a single column name. Required. |
| --desc    | flag | `false` | Sort in descending order. Applies to all specified columns. |

```bash
$ qsv load data.csv - sort str
$ qsv load data.csv - sort str --desc
$ qsv load data.csv - sort col1,col2,col3 --desc
```

#### `count`
Counts duplicate rows, grouping by all columns, and adds a 'count' column.

This command does not take any arguments or options.

```bash
$ qsv load data.csv - count
```

#### `uniq`
Filters unique rows. If `colnames` is specified, uniqueness is based on those columns. If no `colnames` are specified, uniqueness is based on all columns.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| colnames  | str/list | all columns | Optional. Column name(s) to consider for uniqueness. Can be a single column name, or comma-separated for multiple columns. If omitted, all columns are used. |

```bash
$ qsv load data.csv - uniq
$ qsv load data.csv - uniq col1
$ qsv load data.csv - uniq "col1,col2"
```

#### `changetz`
Changes the timezone of a datetime column.

| Parameter | Type | Default | Description |
|---|---|---|---|
| colname | str |         | Name of the datetime column. Required. |
| from_tz | str |         | Source timezone (e.g., `UTC`, `America/New_York`, `local`). Required. |
| to_tz | str |         | Target timezone (e.g., `Asia/Tokyo`). Required. |
| --format | str | `auto` | Input datetime format string (e.g., `%Y-%m-%d %H:%M:%S%.f`). `auto` attempts to parse common formats. |
| --ambiguous | str | `earliest` | Strategy for ambiguous times during DST transitions: `earliest` or `latest`. |

Example:
```bash
$ qsv load data.csv - changetz datetime --from_tz UTC --to_tz Asia/Tokyo
$ qsv load data.csv - changetz datetime --from_tz UTC --to_tz Asia/Tokyo --format "%Y/%m/%d %H:%M"
$ qsv load data.csv - changetz datetime --from_tz UTC --to_tz Asia/Tokyo --format "%Y/%m/%d %H:%M" --ambiguous latest
```

#### `renamecol`
Renames a specific column.

| Parameter   | Type | Default | Description             |
|-------------|------|---------|-------------------------|
| old_name    | str  |         | The current column name. Required. |
| new_name    | str  |         | The new column name. Required.   |

```bash
$ qsv load data.csv - renamecol current_name new_name
```

#### `convert`
Converts data formats between JSON, YAML, and XML. Also supports formatting/prettifying data in the same format.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| colname | str |         | Column name containing the data to convert. Required. |
| --from | str |         | Source format: `json`, `yaml`, or `xml`. Required. |
| --to | str |         | Target format: `json`, `yaml`, or `xml`. Required. |

**Supported conversions:**
- Cross-format: `json ↔ yaml`, `json ↔ xml`, `yaml ↔ xml`
- Same-format (formatting): `json → json`, `yaml → yaml`, `xml → xml`

**Features:**
- Automatically handles malformed JSON with extra quotes
- Prettifies and formats data for better readability
- Preserves data structure during conversion

Example:
```bash
$ qsv load data.csv - convert json_col --from json --to yaml
$ qsv load data.csv - convert config --from yaml --to json
$ qsv load data.csv - convert data --from json --to xml
$ qsv load data.csv - convert messy_json --from json --to json  # Format/prettify JSON
$ qsv load data.csv - convert compact_yaml --from yaml --to yaml  # Format YAML
```

#### `timeline`
Aggregates data by time intervals, creating time-based summaries.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| time_column | str |         | Name of the datetime column to use for time bucketing. Required. |
| --interval | str |         | Time interval for aggregation (e.g., `1h`, `30m`, `5s`, `1d`). Required. |
| --sum | str | | Column name to sum within each time bucket. Optional. |
| --avg | str | | Column name to average within each time bucket. Optional. |
| --min | str | | Column name to find minimum within each time bucket. Optional. |
| --max | str | | Column name to find maximum within each time bucket. Optional. |
| --std | str | | Column name to calculate standard deviation within each time bucket. Optional. |

**Features:**
- Creates a time bucket column named `timeline_{interval}` (e.g., `timeline_1h`, `timeline_30m`)
- If no aggregation column is specified, only row counts are provided for each time bucket
- Supports various time interval formats: hours (`1h`), minutes (`30m`), seconds (`5s`), days (`1d`)

Example:
```bash
$ qsv load access.log - timeline timestamp --interval 1h
# Creates column: timeline_1h

$ qsv load metrics.csv - timeline time --interval 5m --avg cpu_usage
# Creates columns: timeline_5m, count, avg_cpu_usage

$ qsv load sales.csv - timeline date --interval 1d --sum amount
# Creates columns: timeline_1d, count, sum_amount

$ qsv load server.log - timeline timestamp --interval 30s --max response_time
# Creates columns: timeline_30s, count, max_response_time
```

#### `timeslice`
Filters data based on time ranges, extracting records within specified time boundaries.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| time_column | str |         | Name of the datetime column to filter on. Required. |
| --start | str | | Start time (inclusive). Optional. |
| --end | str | | End time (inclusive). Optional. |

At least one of `--start` or `--end` must be specified. Supports various datetime formats including ISO8601, timestamps, and common log formats.

Example:
```bash
$ qsv load data.csv - timeslice timestamp --start "2023-01-01 00:00:00"
$ qsv load data.csv - timeslice timestamp --end "2023-12-31 23:59:59"
$ qsv load data.csv - timeslice timestamp --start "2023-06-01" --end "2023-06-30"
$ qsv load access.log - timeslice timestamp --start "2023-01-01T10:00:00"
```

#### `partition`
Splits data into separate CSV files based on unique values in a specified column. Each unique value creates its own file.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| colname | str |         | Column name to partition by. Required. |
| output_directory | str |         | Directory to save partitioned files. Required. |

The output directory will be created if it doesn't exist. Each file is named after the unique value in the partition column (with invalid filename characters replaced by underscores).

Example:
```bash
$ qsv load data.csv - partition category ./partitions/
$ qsv load sales.csv - partition region ./by_region/
$ qsv load logs.csv - partition date ./daily_logs/
$ qsv load data.csv - select col1,col2 - partition col1 ./numeric_partitions/
```

### Finalizers

Finalizers are used to output or summarize the processed data. They are typically the last command in a chain.

#### `headers`
Displays the column headers of the current dataset.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| --plain   | flag | `false` | Display headers as plain text, one per line, instead of a formatted table. |

Example:
```bash
$ qsv load data.csv - headers
$ qsv load data.csv - headers --plain
```

#### `stats`
Displays summary statistics for each column in the dataset (e.g., count, null_count, mean, std, min, max).

This command does not take any arguments or options.

Example:
```bash
$ qsv load data.csv - stats
```

#### `showquery`
Displays the Polars LazyFrame query plan. This is useful for debugging and understanding the operations being performed.

This command does not take any arguments or options.

Example:
```bash
$ qsv load data.csv - select col1 - showquery
```

#### `show`
Displays the resulting data as CSV to standard output. Header is included by default.

This command does not take any arguments or options.

Example:
```bash
$ qsv load data.csv - head 5 - show
```

#### `showtable`
Displays the resulting data in a formatted table to standard output. Shows table dimensions and intelligently truncates large datasets.

**Features:**
- Displays table size information (rows × columns) like Python Polars
- For datasets with 8+ rows: shows first 3 rows, truncation indicator (`…`), and last 3 rows
- For datasets with 7 or fewer rows: shows all rows without truncation
- Automatically used as default finalizer when no explicit finalizer is specified

This command does not take any arguments or options.

Example:
```bash
$ qsv load data.csv - select col1,col2 - head 3 - showtable
# Output includes: shape: (3, 2) followed by formatted table

$ qsv load large_data.csv - select col1,col2
# Automatically calls showtable if no finalizer specified
```

#### `dump`
Outputs the processing results to a CSV file.

| Parameter | Type | Default | Description |
|---|---|---|---|
| output_path | str | `output.csv` | File path to save the CSV data. Can be provided as a positional argument (e.g., `dump my_file.csv`) or via `--output <path>` / `-o <path>` option. If omitted, defaults to `output.csv`. |
| --separator, --sep | char | `,` | Field separator character for the output CSV file. |

Example:
```bash
$ qsv load data.csv - head 100 - dump
$ qsv load data.csv - head 100 - dump --output result.csv
$ qsv load data.csv - head 100 - dump --output result.tsv --separator=\t
```

### Quilt (YAML Workflows)

Quilt allows you to define complex data processing workflows in YAML configuration files. This is useful for automating repetitive tasks or creating reusable data processing pipelines.

#### Usage
The `quilt` command itself takes the path to a YAML configuration file. Input data sources and other parameters are typically defined within the YAML file.

```bash
$ qsv quilt <config_file_path.yaml> [options]
```
| Parameter | Type | Description |
|---|---|---|
| config_file_path.yaml | str | Path to the YAML configuration file defining the pipeline stages. Required. |
| -o, --output | str | Overrides the output path defined in the YAML config for the final dump operation (if any). |


#### Example: Running a Quilt File
```bash
$ qsv quilt rules/my_workflow.yaml
$ qsv quilt rules/my_analysis.yaml -o custom_output.csv
```

The YAML configuration file (e.g., `rules/my_workflow.yaml`) defines the stages and steps. For example, the `Sample YAML (rules/test.yaml)` below defines a pipeline that:
1. Loads data (implicitly or explicitly via a `load` step in a `process` stage).
2. Performs selections and a join operation across different stages.
3. Displays the final result as a table.

#### Pipeline Operations in YAML
Within a Quilt YAML file, stages can be of different types to orchestrate the flow.

| Operation Type | Description                                                | Key Parameters (within `params` or stage-specific)                                                                                                                                    |
| -------------- | ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `process`      | Executes a series of qsv operations on a dataset.          | `steps`: A dictionary of operations (e.g., `load`, `select`, `head`, `showtable`). Each key is a qsv command, and its value contains its arguments/options. <br> `source` (optional): Specifies the output of a previous stage as input. |
| `concat`       | Concatenates multiple datasets (stages).                   | `sources`: List of stage names whose outputs to concatenate. <br>`params.how` (optional): Method for concatenation, e.g., `vertical` (default), `horizontal`. Polars `UnionArgs` can be used. |
| `join`         | Joins datasets from multiple stages based on keys.         | `sources`: List of two stage names whose outputs to join. <br>`params.left_on`/`params.right_on` or `params.on`: Column(s) for joining. <br>`params.how`: Type of join, e.g., `inner` (default), `left`, `outer`, `cross`. <br>`params.coalesce` (optional): bool, whether to coalesce a key if it is present in both DataFrames. |

#### Sample YAML (`rules/test.yaml`):
```yaml
title: 'Test Data Processing Pipeline'
description: 'A sample Quilt pipeline demonstrating various stages and operations.'
version: '0.1.1'
author: 'Qsv User <user@example.com>'
stages:
  raw_data_load:
    type: process
    steps:
      load:
        path: "../sample/simple.csv" # Relative to the YAML file location or absolute
        # separator: "," # Optional, defaults to comma

  stage_1_select_data1:
    type: process
    source: raw_data_load # Use output from 'raw_data_load' stage
    steps:
      select:
        colnames: 
          - col1
          - col2 # Assuming 'simple.csv' has col1, col2, col3

  stage_2_select_data2:
    type: process
    source: raw_data_load
    steps:
      select:
        colnames: 
          - col1 # Common key for join
          - col3

  merged_data:
    type: join
    sources: # List of two stages to join
      - stage_1_select_data1
      - stage_2_select_data2
    params:
      how: "inner" # e.g., inner, left, outer
      on: "col1"   # Column to join on (must exist in both sources)
      # coalesce: true # Optional: if true, duplicate join key columns are merged

  final_output_table:
    type: process
    source: merged_data
    steps:
      head: 5 # Takes the first 5 rows
      showtable: # Displays the result as a table
        # No arguments needed for showtable
```

#### Note: Step Duplication in YAML
Quilt supports YAML configurations where multiple steps of the same command type (e.g., `renamecol`) are needed within a single `process` stage.

```yaml
stages:
  data_cleaning_stage:
    type: process
    source: raw_data_load
  steps:
      renamecol: # First renamecol
        old_name: "old_col_name_1"
        new_name: "new_col_1"
      renamecol_: # Second renamecol (note the underscore)
        old_name: "anotherOldName"
        new_name: "clean_col_2"
      renamecol__: # Third renamecol
        old_name: "yet_another"
        new_name: "final_col_3"
      show: # Finalizer for this stage
```
To achieve this, append underscores (`_`, `__`, etc.) to the command name in the YAML to make the keys unique. Internally, `qsv` will still recognize them as the base command (e.g., all three will be treated as `renamecol` operations).

## Installation

### From Source (Recommended for latest features)
1.  Ensure you have Rust installed. If not, visit [rust-lang.org](https://www.rust-lang.org/tools/install).
2.  Clone the repository:
    ```bash
    git clone https://github.com/sumeshi/qsv-rs.git
    cd qsv-rs
    ```
3.  Build and install:
    ```bash
    cargo install --path .
    ```
    This will install `qsv` to your Cargo binary directory (e.g., `~/.cargo/bin`), which should be in your PATH.

### From GitHub Releases (Pre-compiled binaries)
Pre-compiled binary versions for various platforms (Linux, macOS, Windows) may be available on the [GitHub Releases page](https://github.com/sumeshi/qsv-rs/releases). Download the appropriate binary for your system.

#### For Linux/macOS:
```bash
# Download the binary
chmod +x ./qsv-rs-linux-x86_64
# Optionally, move it to a directory in your PATH, e.g., /usr/local/bin/qsv
sudo mv ./qsv-rs-linux-x86_64 /usr/local/bin/qsv
# Then you can run it directly
qsv --version
```

#### For Windows:
```bash
# Download the .exe file, e.g., qsv-x86_64-pc-windows-msvc.exe
# You can run it directly or add its location to your system's PATH environment variable.
./qsv-x86_64-pc-windows-msvc.exe --version
```

## License
Quilter-CSV is released under the [MIT](https://github.com/sumeshi/qsv-rs/blob/master/LICENSE) License.