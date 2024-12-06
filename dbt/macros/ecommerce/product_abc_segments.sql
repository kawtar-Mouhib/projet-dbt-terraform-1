{% macro product_abc_segments(
    table_ref,
    product_column,
    revenue_column,
    columns_list,
    threshold_revenue_running_percentage
) %}

with
  product_revenue_sum as (
  select
    {{ product_column }} as product,
    {# {{ product_id_column }} as product_id, #}
    {{ revenue_column }} as revenue,
    {# {% if columns_list == ['*'] %}
        source.* except ({{ product_column }}, {{ revenue_column }})
    {% elif columns_list|length > 0 %}
        {% for column in columns_list %}
            source.{{ column }}
        {% endfor %}
    {% endif %} #}
    sum({{ revenue_column }}) over (order by {{ revenue_column }} desc rows between unbounded preceding and current row) as revenue_cumsum,
    sum({{ revenue_column }}) over () as revenue_total
  from {{ table_ref }}),
  running_percentage as (
  select
    *,
    round(revenue_cumsum * 100 / revenue_total, 2) as revenue_running_percentage
  from product_revenue_sum),
  rank_running_percentage as (
  select
    *,
    row_number() over (order by revenue_running_percentage) as abc_rank_name
  from running_percentage)
select
  product,
  {# product_id, #}
  case
    when revenue_running_percentage > 0 and revenue_running_percentage <= {{threshold_revenue_running_percentage}} then "A"
    when revenue_running_percentage > {{threshold_revenue_running_percentage}} and revenue_running_percentage <= 90 then "B"
  else "C"
end as abc_class_name,
  abc_rank_name
from rank_running_percentage
{% endmacro %}