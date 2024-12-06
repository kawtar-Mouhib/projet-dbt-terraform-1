{% macro get_latency(
    table_ref,
    customer_id_column,
    order_date_column,
    order_id_column,
    columns_list
) %}

with
  latency as (
  select
    {{ customer_id_column }} as customer_id, 
    {{ order_id_column }} as order_id, 
    {{ order_date_column }} as order_date,
    lag({{ order_date_column }}) over (partition by {{ customer_id_column }} order by {{ order_date_column }}) as prev_order_date,
    date_diff(date({{ order_date_column }}),date(lag({{ order_date_column }}) over (partition by {{ customer_id_column }} order by {{ order_date_column }})),day) as days_since_prev_order
  from {{ table_ref }}
  order by {{ customer_id_column }}, {{ order_date_column }}),
  get_latency as (
  select
    customer_id,
    count(distinct {{ order_id_column }}) as frequency,
    max({{ order_date_column }}) as recency_date,
    date_diff(current_date(),date(max({{ order_date_column }})),day) as recency,
    round(avg(days_since_prev_order)) as avg_latency,
    round(min(days_since_prev_order)) as min_latency,
    round(max(days_since_prev_order)) as max_latency,
    round(stddev(days_since_prev_order)) as std_latency,
    safe_divide(stddev(days_since_prev_order), avg(days_since_prev_order)) as cv
  from latency
  group by 1)
select
  *,
  avg_latency - (recency - std_latency) as days_to_next_order_upper,
  avg_latency - (recency + std_latency) as days_to_next_order_lower,
  case
    when recency < (avg_latency - (recency + std_latency)) then "Order not due"
    when recency <= avg_latency - (recency + std_latency)
  or recency <= avg_latency - (recency - std_latency) then "Order due soon"
    when recency > avg_latency - (recency - std_latency) then "Order overdue"
  else "Not sure"
end as latency_label
from get_latency
{% endmacro %}