{% macro basket_product_analytics(
    table_ref,
    product_column,
    order_id_column
) %}

with
  order_detail as (
  select
    distinct {{ product_column }} as product_item,
    {{ order_id_column }} as order_id,
  from {{ table_ref }}
  where {{ product_column }} != "Unassigned"),
  associations as (
  select
    order_detail.product_item,
    order_detail_b.product_item as product_item_b,
    count(distinct order_detail.order_id) as associated_orders
  from order_detail
  inner join order_detail as order_detail_b
  on
    order_detail.order_id=order_detail_b.order_id
    and order_detail.product_item!=order_detail_b.product_item
  group by 1, 2),
  count_orders as (
  select
    distinct product_item,
    count(distinct order_id) over (partition by product_item) as item_total_orders,
    count(distinct order_id) over () as total_orders
  from order_detail)
select
  distinct associations.product_item,
  associations.product_item_b,
  associations.associated_orders,
  count_orders.item_total_orders,
  count_orders_b.item_total_orders as item_total_orders_b,
  count_orders.total_orders,
  round(associations.associated_orders/count_orders.item_total_orders,5) as attachment_percentage,
  round(count_orders_b.item_total_orders/count_orders.total_orders,5) as expected_attachment_percentage
from associations
inner join count_orders
on
  associations.product_item=count_orders.product_item
inner join count_orders as count_orders_b
on 
  associations.product_item_b=count_orders_b.product_item
{% endmacro %}