{% docs macro_incremental_filter %}

# incremental_filter

## Description
This macro generates an SQL filter for incremental data processing, allowing users to filter data based on a 
specified time interval. The macro supports five types of intervals: hour, day, week, month, and lookback. It also allows 
users to set custom start and end timestamps for filtering, or use default timestamps based on the current 
timestamp.

for hourly runs if the `should_truncate_hour` flag is set to `True` it will truncate the start and end timestamps

## Parameters

- **timestamp_column_name** (default: 'receivedTimestamp'): The name of the timestamp column to filter by.
- **interval** (default: 'hour'): The interval for the filter, should be one of the following: 'hour', 'day',
  'week', 'month', or 'lookback'.
- **lookback_window_hours** (default: null): lookback window will apply a lookback of the number of hours provided (positive values will be converted to negative)

## Optional Parameters (set as env vars)

- **start_timestamp**: (env_var= `DBT_START_TIMESTAMP`) Optional custom start timestamp for the filter. If not provided, it will be calculated 
  based on the interval and current timestamp.
- **end_timestamp**: (env_var= `DBT_START_TIMESTAMP`) Optional custom end timestamp for the filter. If not provided, it will be calculated based on 
  the interval and current timestamp.

## Usage

```sql
SELECT *
FROM your_table
WHERE
    {% raw -%}{{ incremental_filter(timestamp_column_name='your_timestamp_column', interval='day') }}{%- endraw -%}
```
output: 
```sql
SELECT *
FROM your_table
WHERE
    your_timestamp_column between '2023-04-08' and '2023-04-08'
```

{% enddocs %}