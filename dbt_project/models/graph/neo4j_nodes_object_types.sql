{{ config(materialized='external',location='../neo4j/import/object_types.csv') }}

{% set attribute_columns = dbt_utils.get_column_values(ref('object_attributes'),'description') %}

select 
    'X' || object_type_id as 'object_type_id:ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    replace(object_type,' ','_') as 'object_type',
    count_objects as 'objects_count',
    replace(object_type,' ','_') || ' (' || count_objects || ')' as label,
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
    'OBJECT_TYPE_NODE' as ':LABEL'
from {{ ref('object_type_nodes') }}
