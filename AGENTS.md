# Creating New Notebooks

This guide helps you create notebooks to analyze Deutsche Bahn train data using the preprocessed monthly data.

## Available Dataset

The project includes one main dataset:

**monthly_processed_data** - Processed train stops with delays and cancellations
- Location: `../monthly_processed_data/*.parquet`
- Partitioned by year and month
- Contains all train stops with arrival/departure times, delays, and cancellations

## Dataset Structure

The monthly processed dataset contains the following key columns:

### Identification
- `station_name` - Name of the station
- `eva` - EVA station number (unique identifier)
- `train_name` - Name of the train (e.g., "ICE 123", "RE 5")
- `train_type` - Type of train. There are 65 unique train types in the dataset. Most common types include:
  - `S` - S-Bahn (urban trains)
  - `RE` - Regional Express
  - `RB` - Regionalbahn (regional trains)
  - `ICE` - InterCity Express (high-speed trains)
  - `IC` - InterCity
  - `Bus` - Replacement bus services
- `final_destination_station` - Final destination of the train
- `id` - Unique identifier for the train stop

### Timing
- `arrival_planned_time` - Planned arrival time
- `arrival_change_time` - Actual/changed arrival time
- `departure_planned_time` - Planned departure time
- `departure_change_time` - Actual/changed departure time
- `time` - Actual arrival or departure time

### Status
- `delay_in_min` - Delay in minutes
- `is_canceled` - Whether the train stop was canceled

### Train Journey
- `train_line_ride_id` - Unique identifier for the train ride
- `train_line_station_num` - Station number in the train's route

## Project Setup

### Standard Notebook Initialization

```python
import duckdb
import util

util.init()
```

## Language Requirements

**All notebooks, titles, and plots must be in German.**

When creating statistics:
- Use German for chart titles (e.g., "Monatliche Durchschnittsverzögerung")
- Use German for axis labels and column names in plots
- Use German for text descriptions and markdown cells
- Keep code comments in English for consistency with the codebase

Example:
```python
util.FigureConfig(
    title="Monatliche Durchschnittsverzögerung",  # German title
    label="Durchschnittsverzögerung",              # German label
    x_col="months",
    y_col="Durchschnittsverzögerung (min)",        # German column name
    query_or_df=df,
)
```

## Core Patterns

### Pattern 1: Basic Time Series Analysis

**Example: Monthly Average Delays**

```python
df = duckdb.sql("""
SELECT
    strftime(time, '%Y-%m') as months,
    AVG(delay_in_min) as "Durchschnittsverzögerung (min)",
    COUNT(*) as "Anzahl Stopps",
    SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) as "Ausgefallene Stopps"
FROM '../monthly_processed_data/*.parquet'
WHERE delay_in_min IS NOT NULL
GROUP BY months
ORDER BY months
""").df()

util.show_figure(
    [
        util.FigureConfig(
            title="Monatliche Durchschnittsverzögerung",
            label="Durchschnittsverzögerung",
            x_col="months",
            y_col="Durchschnittsverzögerung (min)",
            query_or_df=df,
        )
    ]
)
```

## Utility Functions Reference

### Figure Configuration (`util.FigureConfig`)

```python
@dataclass
class FigureConfig:
    title: str                          # Chart title
    x_col: str                          # X-axis column name
    y_col: str                          # Y-axis column name
    z_col: str = None                   # Z-axis for maps
    query_or_df: str | pd.DataFrame     # SQL query or DataFrame
    group_col: str = None               # Column for grouping traces
    label: str = None                   # Label for button in multi-chart
    y_unit_hover_template: str = None   # Unit for hover template (e.g., "%")
    plot_type: str = "scatter"          # "scatter", "bar", or "map"
    trace_names: list[str] = None       # Custom ordering of traces
```

### Data Reshaping with Pandas

When you need to plot multiple series from columns in wide format, use `df.melt()` to convert to long format:

```python
# Before melt (wide format):
# months    | series_1 | series_2 | series_3
# 2024-07   | 100      | 200      | 300

df_melted = df.melt(
    id_vars=['months'],            # Columns to keep as identifiers
    var_name='series_name',        # Name for new column with old column names
    value_name='value'             # Name for new column with values
)

# After melt (long format):
# months    | series_name | value
# 2024-07   | series_1    | 100
# 2024-07   | series_2    | 200
# 2024-07   | series_3    | 300

# Now can use with group_col in FigureConfig
util.show_figure([
    util.FigureConfig(
        title="Title",
        x_col="months",
        y_col="value",
        group_col="series_name",
        query_or_df=df_melted,
    )
])
```

## SQL Patterns and Best Practices

### Common SQL Patterns

1. Month Formatting:
   ```sql
   strftime(time, '%Y-%m') as months
   ```

2. Date Filtering:
   ```sql
   WHERE time >= '2024-07-01' AND time < '2024-08-01'
   ```

3. Top N Selection:
   ```sql
   WITH top_n AS (
       SELECT column
       FROM table
       ORDER BY metric DESC
       LIMIT n
   )
   ```

4. Percentage Calculations:
   ```sql
   ROUND((subset_count * 100.0) / total_count, 2) as percentage
   ```

## Performance Considerations

### Query Optimization
- Use CTEs (Common Table Expressions) for complex queries
- Filter early in the query pipeline
- Use appropriate aggregations before JOINs
- Consider partitioning when accessing specific time ranges
- Use `strftime()` for date formatting instead of extracting individual components

## Advanced Features

### Multi-Chart Figures with Button Navigation

```python
util.show_figure(
    [
        util.FigureConfig(title="Chart 1", label="Button 1", ...),
        util.FigureConfig(title="Chart 2", label="Button 2", ...),
        util.FigureConfig(title="Chart 3", label="Button 3", ...),
    ]
)
```

### Custom Trace Ordering

```python
# Get trace names ordered by last value, unique values, or first value
trace_names = util.get_trace_names(df, "group_col", "x_col", "y_col", "last")
# Options: "last", "first", "unique"

util.FigureConfig(
    # ... other parameters ...
    trace_names=trace_names
)
```