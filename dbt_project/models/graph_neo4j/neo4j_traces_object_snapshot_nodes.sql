{{ config(materialized='external',location='../neo4j/import/traces_object_snapshot_nodes.csv',options={'force_quote':'description'}) }}

{% set attribute_columns = dbt_utils.get_column_values(ref('object_attributes'),'description') %}

select
    'x' || snapshot_id as 'object_snapshot_id:ID',  -- needs prefix letter, because neo4j does not accept id's that start with number
    snapshot_timestamp as 'timestamp',
    object_description as description, -- force_quote
    {% if attribute_columns != None %}
        {% for attribute_column in attribute_columns %}
            {{attribute_column}},
        {% endfor %}
    {% endif %}
    object_type,
    concat(object_type,' (',snapshot_timestamp,')') as label,
    'OBJECT' || ';' || replace(object_type,' ','_') as ':LABEL'
from
    {{ ref('traces_object_snapshot_nodes')}}
