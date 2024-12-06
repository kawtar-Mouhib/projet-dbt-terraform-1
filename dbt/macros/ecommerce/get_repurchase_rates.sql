{% macro get_repurchase_rates(
    table_ref,
    product_column,
    order_id_column,
    quantity_column,
    customer_id_column,
    line_price_column,
    unit_price_column, 
    columns_list
) %}

with
  purchased_metrics as (
  select
    {{ product_column }} as product,
    {{ order_id_column }} as order_id,
    {{ quantity_column }} as quantity,
    {{ customer_id_column }} as customer_id,
    {{ line_price_column }} as line_price,
    {{ unit_price_column }} as unit_price,
   {# {% if columns_list == ['*'] %}
        source.* except ({{ product_column }}, {{ order_id_column }}, {{ quantity_column }}, {{ customer_id_column }}, {{ line_price_column }}, {{ unit_price_column }}),
    {% elif columns_list|length > 0 %}
        {% for column in columns_list %}
            source.{{ column }},
        {% endfor %}
    {% endif %} #}
    count(distinct {{ product_column}}) over (partition by {{ order_id_column }}) as article_products_types,
    coalesce(count(distinct
        case
          when {{ quantity_column }} > 0 then {{ order_id_column }}
      end
        ) over (partition by {{ product_column}}, {{ customer_id_column }}), 0) as times_purchased,
  from {{ table_ref }}
  where {{ product_column }} != "Unassigned"),
  purchased_metrics_complete as (
  select
    *,
    coalesce(count(distinct
        case when article_products_types = 1 then order_id end
        ) over (partition by product), 0) as purchased_individually,
    coalesce(count(distinct
        case when times_purchased = 1 then order_id end
        ) over (partition by product), 0) as purchased_once
  from purchased_metrics),
  sku_metrics as (
  select
    product,
    round(sum(line_price), 2) as revenue,
    sum(quantity) as items,
    count(distinct order_id) as orders,
    count(distinct customer_id) as customers,
    round(avg(unit_price), 2) as avg_unit_price,
    round(avg(line_price), 2) as avg_line_price
  from purchased_metrics
  group by 1),
  sku_metrics_complete as (
  select
    distinct product,
    revenue,
    items,
    orders,
    customers,
    avg_unit_price,
    avg_line_price,
    round(items / orders, 2) as avg_items_per_order,
    round(items / customers, 2) as avg_items_per_customer,
    purchased_individually,
    purchased_once
  from sku_metrics
  left join purchased_metrics_complete 
  using (product)),
  repurchase as (
  select
    *,
    orders - purchased_individually as bulk_purchases,
    round((orders - purchased_individually) / orders, 2) as bulk_purchase_rate,
    orders - purchased_once as repurchases,
    round((orders - purchased_once) / orders, 2) as repurchase_rate,
  from sku_metrics_complete),
  repurchase_ntile as (
  select
    *,
    ntile(5) over (order by bulk_purchase_rate) as bulk_purchase_rate_ntile,
    ntile(5) over (order by repurchase_rate) as repurchase_rate_ntile
  from repurchase)
select
  product,
  revenue,
  items,
  orders,
  customers,
  avg_unit_price,
  avg_line_price,
  avg_items_per_order,
  avg_items_per_customer,
  purchased_individually,
  purchased_once,
  bulk_purchases,
  bulk_purchase_rate,
  repurchases,
  repurchase_rate,
  concat(case bulk_purchase_rate_ntile
      when 1 then "Very low bulk"
      when 2 then "Low bulk"
      when 3 then "Moderate bulk"
      when 4 then "High bulk"
      when 5 then "Very high bulk"
  end
    ,"_",
    case repurchase_rate_ntile
      when 1 then "Very low repurchase"
      when 2 then "Low repurchase"
      when 3 then "Moderate repurchase"
      when 4 then "High repurchase"
      when 5 then "Very high repurchase"
  end
    ) as bulk_and_repurchase_label
from repurchase_ntile
{% endmacro %}