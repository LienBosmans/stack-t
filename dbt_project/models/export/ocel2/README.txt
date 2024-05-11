Run below command to generate the dbt models for event_<type> and object_<type> tables.

'''
python3 ../python_code/export_ocel2_generate_dbt_models.py 
'''

Please be aware that because of OCEL 2.0 limitations, any 'event-to-object-attribute-value' relations will be ignored and 'object-to-object' relations will assumed to be static. To generate the OCEL 2.0 logthe description of the object-to-object relation qualifier will be used instead of the qualifier value.
