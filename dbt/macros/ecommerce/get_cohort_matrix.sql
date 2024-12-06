{% macro get_cohort_matrix(
    table_ref,
    order_id_column,
    customer_id_column,
    order_created_at_column,
    channel_column = None,
    platform_column = None
) %}

with prepare_cohorts as (
  select
    distinct {{ order_id_column }},
    {{ customer_id_column }},
    {{ order_created_at_column}}
  from {{ table_ref }}
    where {{ customer_id_column }} is not null and {{ order_id_column }} is not null),
  cohorts as (
  select
    {{ order_id_column }},
    {{ customer_id_column }},
    format_date("%Y-%m",min({{ order_created_at_column}}) over (partition by {{ customer_id_column }})) as acquisition_cohort,
    format_date("%Y-%m",{{ order_created_at_column}}) as order_cohort
  from prepare_cohorts),
  get_retention as (
  select
    acquisition_cohort,
    order_cohort,
    date_diff(parse_date('%Y-%m', order_cohort), parse_date('%Y-%m', acquisition_cohort), month) as periods,
    count(distinct {{ customer_id_column }}) as customers
  from cohorts
  group by 1, 2)
select
  acquisition_cohort,
  order_cohort,
  periods,
  customers,
  first_value(customers) over (partition by acquisition_cohort order by order_cohort) as cohort_customers,
  round(customers / first_value(customers) over (partition by acquisition_cohort order by order_cohort), 3) as retention_percentage
from get_retention
order by 1, 2, 3
{% endmacro %}
