{% macro get_rfm_segments(
    table_ref,
    customer_id_column,
    first_order_date_column,
    last_order_date_column,
    recency_column,
    orders_column,
    revenue_column,
    tenure_column,
    columns_list
) %}

with
  prepare_rfm as (
  select
    {{ customer_id_column }} as customer_id,
    {{ first_order_date_column }} as acquisition_date,
    {{ last_order_date_column }} as recency_date,
    {{ recency_column }} as recency,
    {{ orders_column }} as frequency,
    {{ revenue_column }} as monetary,
    {{ tenure_column }} as tenure
  from {{ table_ref }}),
  get_rfm_rank as (
  select
    *,
    ntile(5) over (order by recency) as recency_rank,
    ntile(5) over (order by frequency) as frequency_rank,
    ntile(5) over (order by monetary) as monetary_rank
  from prepare_rfm),
  get_rfm_label as (
  select
    *,
    recency_rank * 100 + frequency_rank * 10 + monetary_rank as rfm_label
  from get_rfm_rank )
select
  customer_id,
  acquisition_date,
  recency_date,
  recency,
  frequency,
  monetary,
  tenure,
  case
    when rfm_label between 111 and 155 then "Risky"
    when rfm_label between 211 and 255 then 'Hold and improve'
    when rfm_label between 311 and 353 then 'Potential loyal'
    when rfm_label between 354 and 454 or rfm_label between 511 and 535 or rfm_label = 541 then 'Loyal'
    when rfm_label = 455 or rfm_label between 542 and 555 then 'Star'
  else "Other"
end as rfm_segments_name
from get_rfm_label
{% endmacro %}