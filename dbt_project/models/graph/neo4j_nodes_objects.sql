{{ config(materialized='external',location='../neo4j/import/objects.csv',options={'force_quote':'description'}) }}

{% set attribute_columns = dbt_utils.get_column_values(ref('object_attributes'),'description') %}

select 
    'X' || object_snapshot_id as 'object_id:ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    object_description as 'description', -- force_quote
    {% if attribute_columns != None %}
        {% for attribute_column in attribute_columns %}
            {{attribute_column}},
        {% endfor %}
    {% endif %}
    replace(object_type,' ','_') as 'object_type',
    'OBJECT' || ';' || replace(object_type,' ','_') as ':LABEL'
from {{ ref('object_snapshots') }}
