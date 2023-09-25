-- noinspection SqlNoDataSourceInspectionForFile
{%- macro incremental_filter(timestamp_column_name='receivedTimestamp', interval='hour', lookback_window_hours=null) -%}
{%- if execute -%}
    {#- manage the intervals we know how to handle -#}
    {%- if interval not in ['hour', 'day', 'week','month', 'lookback'] -%}
        {{ exceptions.raise_compiler_error('Incremental filter interval parameter should be one of: hour, day, week. Got: ' ~ interval) }}
    {%- endif -%}
    {#- convert the timestamps from epoch to date time-#}
    {%- set converted_start_timestamp = convert_epoch_to_datetime(var('start_timestamp')) -%}
    {%- set converted_end_timestamp = convert_epoch_to_datetime(var('end_timestamp')) -%}
    {# check the 'should_truncate_hour' flag if set truncate the hours of the timestamps#}
    {%- if var('should_truncate_hour') == "True" or var('should_truncate_hour') == "true" -%}
        {%- if converted_start_timestamp != 'none' %}
            {%- set converted_start_timestamp = truncate_hour(converted_start_timestamp) -%}
        {%- endif -%}
        {%- if converted_end_timestamp != 'none' -%}
            {%- set converted_end_timestamp = truncate_hour(converted_end_timestamp) -%}
        {%- endif %}
    {%- endif -%}
    {#- daily weekly and monthly intervals only need a working timestamp the blocks below will manage their upper and lower bounds -#}
    {%- if interval in ['day', 'week', 'month'] -%}
        {%- set working_timestamp = generate_working_timestamp(var('start_timestamp'),var('end_timestamp')) -%}
    {%- else -%}
        {%- if converted_start_timestamp == 'none' and converted_end_timestamp == 'none' -%}
            {%- set start_timestamp = run_query("select dateadd(hour, -1, current_timestamp())")[0][0] -%}
            {%- set end_timestamp = run_query("select current_timestamp()")[0][0] -%}
        {%- elif converted_start_timestamp != 'none' and converted_end_timestamp != 'none' -%}
            {%- set start_timestamp = converted_start_timestamp -%}
            {%- set end_timestamp = converted_end_timestamp -%}
        {%- elif converted_start_timestamp != 'none' and converted_end_timestamp == 'none' -%}
            {%- set start_timestamp = converted_start_timestamp -%}
            {%- set end_timestamp = run_query("select dateadd(hour, +1, '" ~ converted_start_timestamp ~ "')")[0][0] -%}
        {%- elif converted_start_timestamp == 'none' and var('end_timestamp') != 'none' %}
            {%- set start_timestamp = run_query("select dateadd(hour, -1, '" ~ converted_end_timestamp ~ "')")[0][0] -%}
            {%- set end_timestamp = converted_end_timestamp -%}
        {%- endif -%}
    {%- endif -%}
    {%- if interval == 'day' -%}
        {% set start_date_utc = run_query("select date_add(date('" ~ working_timestamp ~ "'),-1)")[0][0] -%}
        {% set end_date_utc = start_date_utc -%}
        {#- daily models should run between midnight and midnight Eastern Time, not UTC -#}
        {{ timestamp_column_name }} between '{{ start_date_utc }}' and '{{ end_date_utc }}'
    {%- elif interval == 'week' -%}
        {% set start_week_utc = run_query("select date_add(date(date_trunc('week', '" ~ working_timestamp ~ "')),-7)")[0][0] -%}
        {% set end_week_utc = run_query("select date_add(date(date_trunc('WEEK','" ~ working_timestamp ~ "')),-1)")[0][0] -%}
        {#- daily models should run between midnight and midnight Eastern Time, not UTC -#}
        {{ timestamp_column_name }} between '{{ start_week_utc }}' and '{{ end_week_utc }}'
    {%- elif interval == 'month' -%}
        {% set start_month_utc = run_query("select date_add(add_months(date_trunc('month','" ~ working_timestamp ~ "'),-1),0)")[0][0] -%}
        {% set end_month_utc = run_query("select date_add(add_months(date_trunc('month','" ~ working_timestamp ~ "'),0),-1)")[0][0] -%}
        {#- daily models should run between midnight and midnight Eastern Time, not UTC -#}
        {{ timestamp_column_name }} between '{{ start_month_utc }}' and '{{ end_month_utc }}'
    {%- else -%}
        {%- if interval == 'lookback' -%}
            {%- if lookback_window_hours > 0 -%}
                {%- set lookback_window_hours = (-1 * lookback_window_hours) -%}
            {%- endif -%}
            {{timestamp_column_name}} between TIMESTAMPADD(HOUR, {{lookback_window_hours}}, '{{ converted_end_timestamp }}') and '{{ converted_end_timestamp }}'
        {%- else -%}
            {{ timestamp_column_name }} between '{{ start_timestamp }}' and '{{ end_timestamp }}'
        {%- endif %}
    {%- endif %}
{%- endif -%}
{%- endmacro -%}
