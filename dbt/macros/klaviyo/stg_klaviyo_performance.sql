{#- macro to add cost related formula to base jobs table  -#}
{% macro 
    create_source_model_stg_klaviyo_data(
        table_ref,
        json_data,
        json_data_1,
        date_columns, 
        second_level_date_columns, 
        string_columns, 
        second_level_string_columns, 
        numeric_columns, 
        second_level_numeric_columns
) -%}

select
    {% for date_column in date_columns %}
        cast(JSON_EXTRACT_SCALAR('{{json_data}}', '$.{{date_column}}') as timestamp) as {{date_column}},
    {% endfor %}

    {% for second_level_date_column in second_level_date_columns %}
        cast(JSON_EXTRACT_SCALAR('{{json_data}}', '$.{{json_data_1}}.{{second_level_date_column}}') as timestamp) as {{second_level_date_column}},
    {% endfor %}

    {% for string_column in string_columns %}
        JSON_EXTRACT_SCALAR('{{json_data}}', '$.{{string_column}}') as {{string_column}},
    {% endfor %}

    {% for second_level_string_column in second_level_string_columns %}
        JSON_EXTRACT_SCALAR('{{json_data}}', '$.{{json_data_1}}.{{second_level_string_column}}') as {{second_level_string_column}},
    {% endfor %}

    {% for numeric_column in numeric_columns %}
        CAST(JSON_EXTRACT_SCALAR('{{json_data}}', '$.{{numeric_column}}') as numeric) as {{numeric_column}},
    {% endfor %}

    {% for second_level_numeric_column in second_level_numeric_columns %}
        CAST(JSON_EXTRACT_SCALAR('{{json_data}}', '$.{{json_data_1}}.{{second_level_numeric_column}}') as numeric) as {{second_level_numeric_column}},
    {% endfor %}

from {{table_ref}}

{%- endmacro %}