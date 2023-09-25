{%- macro generate_working_timestamp(start_timestamp='none', end_timestamp='none') -%}
    {%- if execute -%}
    {#- daily weekly and monthly intervals only need a working timestamp the blocks below will manage their upper and lower bounds -#}
        {%- if start_timestamp != 'none' -%}
            {%- set working_timestamp = start_timestamp -%}
        {%- elif end_timestamp != 'none' -%}
            {%- set working_timestamp = end_timestamp -%}
        {%- else -%}
            {%- set working_timestamp = run_query("select current_timestamp()")[0][0] -%}
        {%- endif-%}
        {{ return(convert_epoch_to_datetime(working_timestamp)) }}
    {%- endif -%}
{%- endmacro -%}