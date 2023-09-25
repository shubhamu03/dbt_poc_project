{%- macro generate_unique_key(columns) -%}
{%- if columns is none or columns|length == 0 -%}
    {{ exceptions.raise_compiler_error("Error: Columns Array is empty") }}
{%- endif -%}
xxhash64(
    concat_ws(
        '-'
        {% for col in columns -%}
        , coalesce(cast({{ col }} as string),'__was__null_value')
        {% endfor -%}
))
{%- endmacro -%}