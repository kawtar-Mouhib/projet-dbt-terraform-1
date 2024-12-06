{%
    macro klaviyo_campaign_performance
    (
        table_ref,
        dimension_columns,
        campaign_event_name_column,
        Email_column
    )
%}

select
    {% for dimension_column in dimension_columns %}

        {{ dimension_column }},

    {% endfor %}
    
    count({{Email_column}}) as nb_recipients,

    count( distinct {{Email_column}}) as nbre_unique_recipients,

    count( case when {{campaign_event_name_column}} = "Opened Email"  then {{Email_column}} end) as nb_opening,

    count(distinct case when {{campaign_event_name_column}} = "Opened Email"  then {{Email_column}} end) as nb_unique_opening,

    (
        count(case when {{campaign_event_name_column}} = "Opened Email"  then {{Email_column}} end)
        / count({{Email_column}})
    ) as percentage_opening,

    count(case when {{campaign_event_name_column}} = "Clicked Email"  then {{Email_column}} end)
        as nb_clicks,

   count(
        distinct case when {{campaign_event_name_column}} = "Clicked Email"  then {{Email_column}} end
    ) as nb_clicks_unique,

    (
        count(case when {{campaign_event_name_column}} = "Clicked Email"  then {{Email_column}} end)
        / count({{Email_column}})
    ) as percentage_of_clicks,

    count(case when {{ campaign_event_name_column }} = 'Unsubscribed' or {{ campaign_event_name_column }}  = "Unsubscribed from List" then {{Email_column}} end)
        as nb_unsubscribe,

    count( distinct case when {{ campaign_event_name_column }} = 'Unsubscribed' or {{ campaign_event_name_column }}  = "Unsubscribed from List" then {{Email_column}} end)
        as nb_unique_unsubscribe,

    (
        count(case when {{ campaign_event_name_column }} = 'Unsubscribed' or {{ campaign_event_name_column }}  = "Unsubscribed from List" then {{Email_column}} end)
        / count({{Email_column}})
    ) as percentage_unsubscribe,

     count(case when {{ campaign_event_name_column }} = "Marked Email as Spam" then {{Email_column}} end)
        as nb_spam,

    count( distinct case when {{ campaign_event_name_column }} = "Marked Email as Spam" then {{Email_column}} end) as nb_unique_spam,

    (
        count(case when {{ campaign_event_name_column }} = "Marked Email as Spam" then {{Email_column}} end)
        / count({{Email_column}})
    ) as percentage_spam,

    from {{table_ref}}

    group by
    {% for dimension_column in dimension_columns %}
        {{ loop.index }}{{ "," if not loop.last }}
    {% endfor %}

{% endmacro %}

"Marked Email as Spam"