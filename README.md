# Quilter-CSV
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg?style=flat)](LICENSE)
[![CI/CD Pipeline](https://github.com/sumeshi/qsv-rs/actions/workflows/ci-cd.yml/badge.svg?branch=main)](https://github.com/sumeshi/qsv-rs/actions/workflows/ci-cd.yml)

![qsv-rs](https://gist.githubusercontent.com/sumeshi/c2f430d352ae763273faadf9616a29e5/raw/8484142e88948ecc0c8887db8f3bbb5be0dbe51e/qsv-rs.svg)

A fast, flexible, and memory-efficient command-line tool written in Rust for processing large CSV files. Inspired by [xsv](https://github.com/BurntSushi/xsv) and built on [Polars](https://www.pola.rs/), it's designed for handling tens or hundreds of gigabytes of CSV data efficiently in workflows like log analysis and digital forensics.

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
Select columns by name, numeric index, or range notation.

| Parameter | Type                | Default | Description                                                                                                |
|-----------|---------------------|---------|------------------------------------------------------------------------------------------------------------|
| colnames  | str/list/range      |         | Column name(s) or indices. Supports multiple formats (see examples below). This is a required argument. |

**Column Selection Formats:**
- **Individual columns**: `col1,col3` - Select specific columns by name
- **Numeric indices**: `1,3` - Select columns by position (1-based indexing)  
- **Range notation (hyphen)**: `col1-col3` - Select range using hyphen
- **Range notation (colon)**: `col1:col3` - Select range using colon
- **Numeric range**: `1:3` - Select columns col1, col2, col3 (1-based column names)
- **Quoted colon notation**: `"col:1":"col:3"` - For column names containing colons
- **Mixed formats**: `1,col2,4:6` - Combine different selection methods

```bash
$ qsv load data.csv - select datetime
$ qsv load data.csv - select col1,col3
$ qsv load data.csv - select col1-col3
$ qsv load data.csv - select col1:col3  
$ qsv load data.csv - select 1:3        # Select col1, col2, col3
$ qsv load data.csv - select 2,4        # Select 2nd and 4th columns
$ qsv load data.csv - select "col:1":"col:3"  # For columns with colons in names
$ qsv load data.csv - select 1,datetime,3:5   # Mixed selection methods
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
| number | int  | | Number of rows to display. This is a required positional argument. |

```bash
$ qsv load data.csv - head 3
$ qsv load data.csv - head 10
```

#### `tail`
Displays the last N rows of the dataset.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| number | int  | | Number of rows to display. This is a required positional argument. |

```bash
$ qsv load data.csv - tail 3
$ qsv load data.csv - tail 10
```

#### `sort`
Sorts the dataset based on the specified column(s).

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| colnames  | str/list |         | Column name(s) to sort by. Comma-separated for multiple columns (e.g., `col1,col3`) or a single column name. Required. |
| -d, --desc    | flag | `false` | Sort in descending order. Applies to all specified columns. |

```bash
$ qsv load data.csv - sort str
$ qsv load data.csv - sort str -d
$ qsv load data.csv - sort str --desc
$ qsv load data.csv - sort col1,col2,col3 --desc
```

#### `count`
Count duplicate rows, grouping by all columns. Results are automatically sorted by count in descending order.

| Parameter | Type | Default | Description |
|---|---|---|---|
| (None)    |      |         | Takes no arguments. Automatically sorts output by count column in descending order. |

```bash
$ qsv load data.csv - count
$ qsv load data.csv - count - sort col1  # Count and then sort by col1 instead
```

#### `uniq`
Filters unique rows, removing duplicates based on all columns.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| (None)    |      |         | Takes no arguments. Removes duplicate rows based on all columns. |

```bash
$ qsv load data.csv - uniq
```

#### `changetz`
Changes the timezone of a datetime column.

| Parameter | Type | Default | Description |
|---|---|---|---|
| colname | str |         | Name of the datetime column. Required. |
| --from-tz | str |         | Source timezone (e.g., `UTC`, `America/New_York`, `local`). Required. |
| --to-tz | str |         | Target timezone (e.g., `Asia/Tokyo`). Required. |
| --input-format | str | `auto` | Input datetime format string (e.g., `%Y-%m-%d %H:%M:%S%.f`). `auto` uses intelligent parsing similar to Python's dateutil.parser, supporting fuzzy parsing and automatic format detection. |
| --output-format | str | `auto` | Output datetime format string (e.g., `%Y/%m/%d %H:%M:%S`). `auto` uses ISO8601 format `%Y-%m-%dT%H:%M:%S%.7f%:z` (100-nanosecond precision for Windows forensics). |
| --ambiguous | str | `earliest` | Strategy for ambiguous times during DST transitions: `earliest` (first occurrence) or `latest` (second occurrence). |

**Understanding `--ambiguous` option:**

During Daylight Saving Time (DST) transitions in autumn, clocks "fall back" creating duplicate hours. For example, 2:30 AM occurs twice:
- First time: 2:30 AM DST (before transition)  
- Second time: 2:30 AM Standard Time (after transition)

When encountering such ambiguous times:
- `earliest`: Uses the first occurrence (DST time)
- `latest`: Uses the second occurrence (Standard time)

Example:
```bash
$ qsv load data.csv - changetz datetime --from-tz UTC --to-tz Asia/Tokyo
# Output: 2023-01-01T09:00:00.123456+09:00 (ISO8601 with microsecond precision)

$ qsv load data.csv - changetz datetime --from-tz UTC --to-tz America/New_York --input-format "%Y/%m/%d %H:%M" --output-format "%Y-%m-%d %H:%M:%S"
# Custom output format

$ qsv load data.csv - changetz datetime --from-tz America/New_York --to-tz UTC --ambiguous latest
# Handle ambiguous DST times

# Automatic format detection (similar to Python dateutil.parser):
$ qsv load logs.csv - changetz timestamp --from-tz local --to-tz UTC
# Handles: "Jan 15, 2023 2:30 PM", "2023/01/15 14:30", "15-Jan-2023 14:30:00", etc.

# Fuzzy parsing with embedded text:
$ qsv load events.csv - changetz event_time --from-tz EST --to-tz UTC  
# Handles: "Meeting on January 15th, 2023 at 2:30 PM", "Call scheduled for Jan 15 2023"
```

**TODO:** Upgrade to 7-digit sub-second precision (100-nanosecond precision for Windows FILETIME compatibility) when chrono-tz library supports it.

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

#### `pivot`
Creates pivot tables with cross-tabulation functionality.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| --rows | str |         | Comma-separated list of columns for rows. Optional. |
| --cols | str |         | Comma-separated list of columns for columns. Optional. |
| --values | str |         | Column to aggregate values from. Required. |
| --agg | str |         | Aggregation function: `sum`, `mean`, `count`, `min`, `max`, `median`, `std`. Optional (default behavior depends on implementation). |

At least one of `--rows` or `--cols` must be specified. Creates a cross-tabulation table with specified row and column groupings, aggregating values using the chosen function.

Example:
```bash
$ qsv load sales.csv - pivot --rows region --cols product --values sales_amount --agg sum
$ qsv load data.csv - pivot --rows category --cols year --values revenue --agg mean
$ qsv load logs.csv - pivot --rows date --cols error_type --values count --agg count
$ qsv load metrics.csv - pivot --rows department --values performance --agg median
```

#### `timeround`
Rounds datetime values to specified time units, creating a new rounded column while preserving the original.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| colname | str |         | Name of the datetime column to round. Required. |
| --unit | str |         | Time unit for rounding: `y`/`year`, `M`/`month`, `d`/`day`, `h`/`hour`, `m`/`minute`, `s`/`second`. Required. |
| --output | str | (replaces original) | Name for the output column. If not specified, replaces the original column. |

**Features:**
- Rounds datetime values down to the nearest specified time unit boundary
- Useful for time-based grouping and analysis
- Supports both short (`h`, `d`) and long (`hour`, `day`) unit names
- Output format automatically adjusts to the specified unit (clean, minimal format)

**Output formats by unit:**
- **year (y)**: `2023`
- **month (M)**: `2023-01`
- **day (d)**: `2023-01-01`
- **hour (h)**: `2023-01-01 12`
- **minute (m)**: `2023-01-01 12:34`
- **second (s)**: `2023-01-01 12:34:56`

Example:
```bash
$ qsv load data.csv - timeround timestamp --unit d --output date_only
# Input:  2023-01-01 12:34:56
# Output: 2023-01-01

$ qsv load data.csv - timeround timestamp --unit h --output hour_rounded
# Input:  2023-01-01 12:34:56
# Output: 2023-01-01 12

$ qsv load logs.csv - timeround timestamp --unit m
# Rounds to minute boundary, replaces original column

$ qsv load metrics.csv - timeround created_at --unit year --output created_year
# Input:  2023-01-01 12:34:56
# Output: 2023
```

### Finalizers

Finalizers are used to output or summarize the processed data. They are typically the last command in a chain.

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

#### `headers`
Displays the column headers of the current dataset.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| -p, --plain   | flag | `false` | Display headers as plain text, one per line, instead of a formatted table. |

Example:
```bash
$ qsv load data.csv - headers
$ qsv load data.csv - headers -p
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
| output_path | str | | File path to save the CSV data. Optional positional argument. |
| --separator | char | `,` | Field separator character for the output CSV file. |

Example:
```bash
$ qsv load data.csv - head 100 - dump results.csv
$ qsv load data.csv - head 100 - dump --separator ';' results.csv
$ qsv load data.csv - head 100 - dump  # May use default filename
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
| `join`         | Joins datasets from multiple stages based on keys.         | `sources`: List of two stage names whose outputs to join. <br>`params.left_on`/`params.right_on` or `params.on`: Column(s) for joining. <br>`

## Contributing
The source code for qsv-rs is hosted at GitHub, and you may download, fork, and review it from this repository(https://github.com/sumeshi/qsv-rs). Please report issues and feature requests. :sushi: :sushi: :sushi:

## License
qsv-rs is released under the MIT License.
Inspired by [xsv](https://github.com/BurntSushi/xsv).
