{{ config(materialized='external',location='../neo4j/import/event_types.csv') }}

{% set attribute_columns = dbt_utils.get_column_values(ref('event_attributes'),'description') %}

select 
    'X' || event_type_id as 'event_type_id:ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    replace(event_type,' ','_') as 'event_type',
    count_events as 'events_count',
    '(' || count_events || ') ' || replace(event_type,' ','_') as label,
    {% if attribute_columns != None %}
        {% for attribute_column in attribute_columns %}
            {{'attribute_' ~ attribute_column ~ '_usage_perc'}},
        {% endfor %}
        {% for attribute_column in attribute_columns %}
            {{'attribute_' ~ attribute_column ~ '_avg_update_count'}},
        {% endfor %}
        {% for attribute_column in attribute_columns %}
            {{'attribute_' ~ attribute_column ~ '_stdv_update_count'}},
        {% endfor %}
    {% endif %}
    'EVENT_TYPE_NODE' as ':LABEL'
from {{ ref('event_type_nodes') }}
