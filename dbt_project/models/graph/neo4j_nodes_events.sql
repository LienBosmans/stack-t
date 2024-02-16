{{ config(materialized='external',location='../neo4j/import/events.csv',options={'force_quote':'description'}) }}

{% set attribute_columns = dbt_utils.get_column_values(ref('event_attributes'),'description') %}

with event_nodes as (
    select * from {{ ref('event_nodes') }}
)

select 
    'X' || event_id as 'event_id:ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    event_timestamp as 'timestamp',
    event_description as 'description', -- force_quote
    replace(event_type,' ','_') as 'event_type',
    {% if attribute_columns != None %}
        {% for attribute_column in attribute_columns %}
            {{attribute_column}} as {{'attribute_' ~ attribute_column}},
        {% endfor %}
    {% endif %}
    'EVENT' || ';' || replace(event_type,' ','_') as ':LABEL'
from event_nodes
