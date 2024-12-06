{#- macro to add cost related formula to base jobs table  -#}
{% macro create_source_model_from_raw_table_airbyte(source_name, table_name, json_data, date_columns, string_columns, numeric_columns) -%}

select
    {% for date_column in date_columns %}
        cast(JSON_EXTRACT_SCALAR('{{json_data}}', '$.{{date_column}}') as date) as {{date_column}},
    {% endfor %}
    {% for sting_column in string_columns %}
        JSON_EXTRACT_SCALAR('{{json_data}}', '$.{{sting_column}}') as {{sting_column}},
    {% endfor %}
    {% for numeric_column in numeric_columns %}
        CAST(JSON_EXTRACT_SCALAR('{{json_data}}', '$.{{numeric_column}}') as numeric) as {{numeric_column}}
    {% endfor %}
from {{ source('{{source_name}}', '{{table_name}}') }}

{%- endmacro %}