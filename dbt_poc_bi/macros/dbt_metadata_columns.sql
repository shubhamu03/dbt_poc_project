{%- macro dbt_metadata_columns() -%}
    current_timestamp() as dbt_ts_utc,
    date_trunc('HOUR', current_timestamp()) as dbt_hour,
    '{{ invocation_id }}' as dbt_invocation_id,
    '{{ var('db_job_id') }}' as db_job_id,
    '{{ var('db_run_id') }}' as db_run_id,
    '{{ var('db_task_id') }}' as db_task_id
{%- endmacro -%}