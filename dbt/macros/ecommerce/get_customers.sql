{% macro get_customers(
    table_ref,
    customer_id_column,
    revenue_column,
    order_id_column,
    quantity_column,
    date_column,
    transaction_type_column
) %}

with
  customers as (
  select
    {{ customer_id_column }} as customer_id,
    sum({{ revenue_column }}) as revenue,
    count(distinct {{ order_id_column }}) as orders,
    sum({{ quantity_column }}) as items,
    safe_cast(min({{ date_column }}) as date) as first_order_date,
    safe_cast(max({{ date_column }}) as date) as last_order_date
  from {{ table_ref }}
  where {{ transaction_type_column }} = "VENTE"
  group by 1 )
select
  *,
  round(items / orders, 2) as avg_items,
  round(revenue / orders, 2) as avg_order_value,
  date_diff(current_date(), first_order_date, day) as tenure,
  date_diff(current_date(), last_order_date, day) as recency,
  concat(cast(extract(year from first_order_date) as string), '-Q', cast(extract(quarter from first_order_date) as string)) as cohort
from customers
{% endmacro %}