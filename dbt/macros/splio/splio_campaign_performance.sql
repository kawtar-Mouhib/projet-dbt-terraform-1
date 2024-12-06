{%
    macro splio_campaign_performance
    (
        table_ref,
        dimension_columns,
        campaign_Event_Type_column,
        Email_column
    )
%}

select
    {% for dimension_column in dimension_columns %}

        {{ dimension_column }},

    {% endfor %}
    
    count({{Email_column}}) as nb_recipients,

    count( distinct {{Email_column}}) as nbre_unique_recipients,

    count( case when {{campaign_Event_Type_column}} = 'open' then {{Email_column}} end) as nb_opening,

    count(distinct case when {{campaign_Event_Type_column}} = 'open' then {{Email_column}} end) as nb_unique_opening,

    (
        count(case when {{campaign_Event_Type_column}} = 'open' then {{Email_column}} end)
        / count({{Email_column}})
    ) as percentage_opening,

    count(case when {{campaign_Event_Type_column}} = 'click' then {{Email_column}} end)
        as nb_clicks,

   count(
        distinct case when {{campaign_Event_Type_column}} = 'click' then {{Email_column}} end
    ) as nb_clicks_unique,

    (
        count(case when {{campaign_Event_Type_column}} = 'click' then {{Email_column}} end)
        / count({{Email_column}})
    ) as percentage_of_clicks,

    count(case when {{campaign_Event_Type_column}} = 'unsub' then {{Email_column}} end)
        as nb_unsubscribe,

    count( distinct case when {{campaign_Event_Type_column}} = 'unsub' then {{Email_column}} end)
        as nb_unique_unsubscribe,

    (
        count(case when {{campaign_Event_Type_column}} = 'unsub' then {{Email_column}} end)
        / count({{Email_column}})
    ) as percentage_unsubscribe

    from {{table_ref}}

    group by
    {% for dimension_column in dimension_columns %}
        {{ loop.index }}{{ "," if not loop.last }}
    {% endfor %}

{% endmacro %}