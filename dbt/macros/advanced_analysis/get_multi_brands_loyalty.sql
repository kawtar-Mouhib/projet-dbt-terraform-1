{% macro get_multi_brands_loyalty(
    table_ref,
    customer_id_column,
    date_column,
    order_id_column,
    brand_column,
    revenue_column,
    quantity_column,
    days_threshold,
    row_condition
) %}

with data_raw as (
    select distinct
        {{ customer_id_column }} as customer_id,
        cast({{ date_column }} as date) as date,
        {{ order_id_column }} as order_id,
        {{ brand_column }} as brand,
        {{ revenue_column }} as revenue,
        {{ quantity_column }} as quantity,
        min(cast({{ date_column }} as date)) over (partition by {{ customer_id_column }} ) as customer_first_purchase_date,
        min(cast({{ date_column }} as date)) over (partition by {{ customer_id_column }}, {{ brand_column }} ) as customer_first_purchase_date_brand,
    from {{ table_ref }}
    where {{ row_condition }}
),

brand_purchase as (
    select
        date,
        brand,
        sum(revenue) as brand_revenue,
        sum(quantity) as brand_quantity,
        count(distinct order_id) as brand_orders
        count(distinct customer_id) as brand_customers,
        count(distinct case when date = customer_first_purchase_date_brand then customer_id end) as new_clients_to_brand,
        count(distinct case when date = customer_first_purchase_date then customer_id end) as new_clients,
    from data_raw
    group by 1, 2
),

brand_profile as (
    select distinct
        customer_id,
        brand,
        customer_first_purchase_date_brand,
    from data_raw
)

brand_loyalty as (
    select
        brand_profile.customer_first_purchase_date_brand,
        brand_profile.brand,
        count(distinct 
            case when 
                date >= brand_profile.customer_first_purchase_date_brand
                and date <= date_add(brand_profile.customer_first_purchase_date_brand, interval {{ days_threshold }} day)
                then data_raw.customer_id
            end
        ) as customer_returned,
        count(distinct 
            case when 
                date >= brand_profile.customer_first_purchase_date_brand
                and date <= date_add(brand_profile.customer_first_purchase_date_brand, interval {{ days_threshold }} day)
                and data_raw.brand = brand_profile.brand
                then data_raw.customer_id
            end
        ) as customer_returned_to_same_brand,
    from data_raw
    left join brand_profile
    on data_raw.customer_id = brand_profile.customer_id
    and data_raw.date >= brand_profile.customer_first_purchase_date_brand
)

select
    brand_purchase.date,
    brand_purchase.brand,
    brand_purchase.brand_revenue,
    brand_purchase.brand_quantity,
    brand_purchase.brand_orders,
    brand_purchase.brand_customers,
    brand_purchase.new_clients_to_brand,
    brand_purchase.new_clients,
    brand_loyalty.customer_returned,
    brand_loyalty.customer_returned_to_same_brand,
from brand_purchase
left join brand_loyalty
on brand_purchase.brand = brand_loyalty.brand
and brand_purchase.date = brand_loyalty.customer_first_purchase_date_brand

{% endmacro %}