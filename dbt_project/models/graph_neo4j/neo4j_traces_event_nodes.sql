{{ config(materialized='external',location='../neo4j/import/traces_event_nodes.csv',options={'force_quote':'description'}) }}

{% set attribute_columns = dbt_utils.get_column_values(ref('event_attributes'),'description') %}

select
    'x' || event_id as 'event_id:ID',  -- needs prefix letter, because neo4j does not accept id's that start with number
    event_timestamp as 'timestamp',
    event_description as description, -- force_quote
    {% if attribute_columns != None %}
        {% for attribute_column in attribute_columns %}
            {{attribute_column}},
        {% endfor %}
    {% endif %}
    event_type,
    concat(event_type,' (',event_timestamp,')') as label,
    'EVENT' || ';' || replace(event_type,' ','_') as ':LABEL'
from
    {{ ref('traces_event_nodes')}}
