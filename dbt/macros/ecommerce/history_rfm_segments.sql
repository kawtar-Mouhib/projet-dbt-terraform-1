{% macro history_rfm_segments(
    table_ref,
    customer_id_column,
    transaction_type_column,
    date_column,
    revenue_column,
    order_id_column,
    columns_list
) %}

with data_agg as (
  select
    distinct cast({{ date_column }} as date) as date, 
    {{ customer_id_column }} as customer_id, 
    sum({{ revenue_column }}) over (partition by {{ customer_id_column }}) as total_revenue, 
    count(distinct {{ order_id_column }}) over (partition by {{ customer_id_column }}, {{ date_column }}) as total_order, 
    min(cast({{ date_column }} as date)) over (partition by {{ customer_id_column }}) as first_transaction_date
  from {{ table_ref }}
  where {{ transaction_type_column }} = "VENTE" 
  order by 1, 2
),
preparing_data as (
  select 
    date,
    customer_id,
    total_revenue,
    round(sum(total_revenue) over (partition by customer_id order by date asc rows between unbounded preceding and current row), 2) as total_revenue_to_date,
    total_order,
    sum(total_order) over (partition by customer_id order by date asc rows between unbounded preceding and current row) as total_order_to_date,
    first_transaction_date,
    coalesce(date_sub(lead(date) over (partition by customer_id order by date asc), interval 1 day), current_date()) as next_transaction_date
    from data_agg
), 
daily_stats as (
  select 
    date_list as date_,
    date as latest_conversion_date,
    preparing_data.* except(date),
  from unnest(
    generate_date_array(DATE('1990-01-01'), date("2100-01-01"), interval 1 day)) as date_list
  join 
  preparing_data
  on date_list between date and next_transaction_date 
),
rfm_values as (
  select 
    date_, 
    customer_id,
    total_revenue_to_date,
    total_order_to_date,
    latest_conversion_date,
    first_transaction_date,
    --if(date_diff(date_, latest_conversion_date, day) = 0, 1, date_diff(date_, latest_conversion_date, day)) as recency,
    date_diff(date_, latest_conversion_date, day) as recency,
    coalesce(safe_divide(total_order_to_date, date_diff(date_, first_transaction_date, YEAR)), 1) as frequency,
    --if(date_diff(date_, first_transaction_date, day) = 0, 1, date_diff(date_, first_transaction_date, day)) as seniority,
    date_diff(date_, first_transaction_date, day) as seniority,
    
  from daily_stats
),
rfm_scores as (
  select 
    *,
    case
        when recency > 12 then 1 when recency between 6 and 12 then 2 else 3
    end as recency_segment,
    case
        when frequency < 1
        then 1
        when frequency between 1 and 2
        then 2
        when frequency > 2
        then 3
        else 3
    end as frequency_segment,
    case
        when total_revenue_to_date <= 150
        then 1
        when total_revenue_to_date between 150 and 500
        then 2
        when total_revenue_to_date > 500
        then 3
        else 3
    end as monetary_segment
  from rfm_values
),
optimization as (
  select 
    min(date_) as first_date_state,
    max(date_) as last_date_state,
    customer_id,
    total_revenue_to_date,
    total_order_to_date,
    latest_conversion_date,
    first_transaction_date,
    recency_segment,
    frequency_segment,
    monetary_segment
  from rfm_scores
  group by 3,4,5,6,7,8,9,10
)

select 
    *, 
    recency_segment * frequency_segment * monetary_segment as score  
from optimization
order by customer_id asc, first_date_state asc
{% endmacro %}