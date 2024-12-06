{% macro get_next_purchase_date(
    table_ref,
    customer_id_column,
    order_date_column,
    order_id_column,
    revenue_column,
    quantity_column,
    columns_list,
    row_condition
) %}

with filtered_transactions as (
    select distinct
        {{ customer_id_column }} as customer_id,
        cast({{ order_date_column }} as date) as order_date,
        {{ order_id_column }} as order_id,
        {{ revenue_column }} as revenue,
        {{ quantity_column }} as quantity,
        min(cast({{ order_date_column }} as date)) over (partition by {{ customer_id_column }}) as first_order_date,
    from {{ table_ref }}
    where {{ row_condition }} )
select
    order_id,
    order_date,
    customer_id,
    lag(order_date) over (partition by customer_id order by order_date) as previous_order_date,
    count(order_id) over (partition by customer_id order by order_date rows between unbounded preceding and current row) as total_transactions,
    sum(revenue) over (partition by customer_id order by order_date rows between unbounded preceding and current row) as total_revenue,
    date_diff(
        order_date,
        lag(order_date) over (partition by customer_id order by order_date),
        day
    ) as recency,
    date_diff(
        order_date,
        first_order_date,
        day
    ) as customer_lifetime,
    date_diff(
        first_value(order_date) over (partition by customer_id order by order_date asc rows between 1 following and unbounded following),
        order_date,
        day
    ) as days_to_next_purchase,
    safe_divide(
        count(order_id) over (partition by customer_id order by order_date rows between unbounded preceding and 1 preceding),
        date_diff(
            order_date,
            first_order_date,
            day
        )
    ) as frequency,
    first_value(quantity) over (partition by customer_id order by order_date asc rows between 1 following and unbounded following) as next_purchase_quantity,
    first_value(revenue) over (partition by customer_id order by order_date asc rows between 1 following and unbounded following) as next_purchase_revenue,
from filtered_transactions
{% endmacro %}