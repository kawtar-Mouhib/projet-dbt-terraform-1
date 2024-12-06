{% macro get_lifetimes_rfmt(
    table_ref,
    replacement_column,
    customer_id_column,
    order_date_column,
    order_id_column,
    revenue_column,
    observation_periode_end,
    columns_list
) %}

with  filtered_transactions as (
  select
    {{ customer_id_column }} as customer_id,
    {{ order_date_column }} as order_date,
    {{ order_id_column }} as order_id,
    {{ revenue_column }} as revenue
  from {{ table_ref }}
  where {{ replacement_column }} = 0 )
select
  customer_id,
  safe_cast(max(order_date) as date) as last_order_date,
  count(distinct order_id) as frequency,
  date_diff(safe_cast({{ observation_periode_end }} as date), safe_cast(min(order_date) as date),day) as recency,
  date_diff(safe_cast(max(order_date) as date), safe_cast(min(order_date) as date),day) as T,
  round(sum(revenue), 2) AS monetary
from filtered_transactions
group by customer_id
{% endmacro %}