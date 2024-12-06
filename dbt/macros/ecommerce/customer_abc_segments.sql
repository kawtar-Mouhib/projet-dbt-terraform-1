{% macro customer_abc_segments(
    table_ref,
    customer_id_column,
    revenue_column,
    recency_column,
    columns_list,
    threshold_revenue_running_percentage
) %}

with calculate_revenue as (
    select
        {{ customer_id_column }},
        sum({{ revenue_column }}) over (order by {{ revenue_column }} desc rows between unbounded preceding and current row) as revenue_cumsum,
        sum({{ revenue_column }}) over (partition by {{ customer_id_column }}) as revenue_total
    from {{ table_ref }}
    where {{recency_column}} <= 365
),
running_percentage as (
    select
        *,
        round(safe_divide(revenue_cumsum * 100,revenue_total),2) as revenue_running_percentage
    from calculate_revenue
),

rank_running_percentage as (
    select
        *,
        row_number() over (order by revenue_running_percentage) as abc_rank_name
    from running_percentage
)
    
select
    {{ customer_id_column }},
    case
        when revenue_running_percentage is null then "D"
        when revenue_running_percentage > 0 and revenue_running_percentage <= {{threshold_revenue_running_percentage}} then "A"
        when revenue_running_percentage > {{threshold_revenue_running_percentage}} and revenue_running_percentage <= 90 then "B"
        else "C" 
    end as abc_class_name,
    case
        when abc_rank_name is not null then abc_rank_name
        else (select count(*) + 1 from calculate_revenue)
    end as abc_rank_name
from {{ table_ref }}
left join rank_running_percentage
using ({{ customer_id_column }})
{% endmacro %}