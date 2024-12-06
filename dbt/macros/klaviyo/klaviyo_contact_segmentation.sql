{% macro 
    klaviyo_contact_segmentation
    (
        table_ref, 
        dimension_columns,
        email_column,
        campaign_event_name_column,
        event_date_column,
        number_of_days
    )
%}

SELECT

    {% for dimension_column in dimension_columns %}

        {{ dimension_column }},

    {% endfor %}

    (count(CASE WHEN {{ campaign_event_name_column }} = "Clicked Email" THEN {{ email_column }} END) / count({{ email_column }})) AS percentage_of_clicks,

    (count(CASE WHEN {{ campaign_event_name_column }} = "Opened Email"  THEN {{ email_column }} END) / count({{ email_column }})) AS percentage_of_opens,

    CASE
        WHEN
            max(CASE WHEN {{ campaign_event_name_column }} = "Opened Email" THEN {{ event_date_column }} END) IS NULL
            THEN 'Inactive'
        WHEN
            date_diff(current_date(), cast(max(CASE WHEN {{ campaign_event_name_column }} = "Opened Email" THEN {{ event_date_column }} END) AS date), DAY) > {{ number_of_days }}
            THEN 'Inactive'
        ELSE 'Active'
    END AS contact_inactive_over_{{ number_of_days }}_days,

    CASE
        WHEN
            (count(CASE WHEN {{ campaign_event_name_column }} = "Opened Email"  THEN {{ email_column }} END) / count({{ email_column }})) > 0.70
            AND
            (count(CASE WHEN {{ campaign_event_name_column }} = "Clicked Email" THEN {{ email_column }} END)/ count({{ email_column }})) > 0.20
            THEN 'Active'
        ELSE 'Inactive'
    END AS is_contact_active,

    CASE
        WHEN
            (count(CASE WHEN {{ campaign_event_name_column }} = "Clicked Email" THEN {{ email_column }} END)) = 0
            AND
            (count(CASE WHEN {{ campaign_event_name_column }} = "Opened Email"  THEN {{ email_column }} END) / count({{ email_column }})) > 0
            THEN 'Opened but never clicked'
        WHEN
            (count(CASE WHEN {{ campaign_event_name_column }} = "Opened Email"  THEN {{ email_column }} END) / count({{ email_column }})) = 0 
            THEN 'Never Opened'
        ELSE 'Opened and Clicked'
    END AS opened_but_never_clicked,

    max({{ event_date_column }}) AS last_interaction_date

FROM {{ table_ref }}

group by
    {% for dimension_column in dimension_columns %}
        {{ loop.index }}{{ "," if not loop.last }}
    {% endfor %}


{% endmacro %}
