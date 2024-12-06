{#- macro to add cost related formula to base jobs table  -#}
{% macro create_model_bigquery_cost() -%}
with data_cost as (
    SELECT
        creation_time AS creation_time,
        query,
        user_email,
        destination_table.dataset_id,
        destination_table.table_id,
        labels.key as label_key,
        labels.value as label_value,
        case 
            when labels.key like '%dbt%' then 'DBT'
            when labels.value = 'looker_studio' then 'LOOKER STUDIO'
            when labels.value = 'scheduled_query' then 'SCHEDULED QUERY'
            else 'OTHERS'
        end as label_group,
        jobs.total_bytes_billed/1024/1024/1024/1024 * 5 AS job_cost_usd
    FROM
        `region-{{ var('bq_region') }}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT jobs
        left join unnest(labels) labels
    WHERE
        DATE(jobs.creation_time) >= current_date("Europe/Paris") - 7
)
select
    cast(creation_time as date) as date,
    user_email as user,
    label_group,
    label_key,
    sum(job_cost_usd) as cost_usd,
from data_cost
group by 1, 2, 3, 4
{%- endmacro %}