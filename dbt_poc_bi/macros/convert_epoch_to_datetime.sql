{%- macro convert_epoch_to_datetime(timestamp) -%}

        {%- if timestamp is not string -%}
            {%- set timestamp = timestamp | string -%}
        {%- endif -%}

        {%- set datetime = modules.datetime.datetime -%}
        {%- set is_digit = timestamp.isdigit()  -%}
    {%- if is_digit -%}
        {%- if timestamp|length == 13 -%}
            {%- set epoch_timestamp = (timestamp | int) / 1000 -%}
        {%- elif timestamp|length == 10 -%}
            {%- set epoch_timestamp = (timestamp | int)-%}
        {%- else -%}
            {{ exceptions.raise_compiler_error('Expected epoch second(10) or millisecond(13). Got: ' ~ timestamp) }}
        {%- endif -%}
            {%- set datetime_timestamp = datetime.fromtimestamp(epoch_timestamp) -%}
            {%- set timestamp = (datetime_timestamp.strftime('%Y-%m-%d %H:%M:%S')) -%}
    {%- endif -%}

    {%- do return(timestamp) -%}
{%- endmacro -%}