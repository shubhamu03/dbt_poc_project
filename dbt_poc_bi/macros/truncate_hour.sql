{%- macro truncate_hour(timestamp) -%}
        {%- set datetime = modules.datetime.datetime -%}
        {%- set datetime_str = timestamp -%}
        {%- set datetime_object = datetime.strptime(datetime_str[ 0 : 19 ], '%Y-%m-%d %H:%M:%S') -%}
        {%- set datetime_object = datetime_object.replace(minute=0, second=0) -%}
        {%- set timestamp = (datetime_object.strftime('%Y-%m-%d %H:%M:%S')) -%}
        {%- do return(timestamp) -%}
{%- endmacro -%}

