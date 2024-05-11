import duckdb

# ATTACH DuckDB and SQLITE databases

duck_db = 'dev.duckdb'
sqlite_db = '../exports/ocel2/ocel2_export.db'
sql_query = '''
ATTACH ''' + "'" + sqlite_db + "'" + ''' as sqlite_db (TYPE SQLITE);
ATTACH ''' + "'" + duck_db + "'" + ''' as duck_db;
'''
duckdb.sql(sql_query)

# COPY "easy" tables to SQLITE

def generate_copy_table_sql(from_duckdb,from_table,to_sqlite,to_table,table_columns):

    sql_query = '''
CREATE TABLE sqlite_db.''' + to_table + ''' (''' + ', '.join(table_columns) + ''');
INSERT INTO sqlite_db.''' + to_table + ''' SELECT * FROM duck_db.''' + from_table + ''';
'''
    
    return sql_query

    # event
sql_query = generate_copy_table_sql(duck_db,'main_export.ocel2_event',sqlite_db,'event',['ocel_id VARCHAR','ocel_type VARCHAR'])
duckdb.sql(sql_query)

    # event_map_type
sql_query = generate_copy_table_sql(duck_db,'main_export.ocel2_event_map_type',sqlite_db,'event_map_type',['ocel_type VARCHAR','ocel_type_map VARCHAR'])
duckdb.sql(sql_query)

    # event_object
sql_query = generate_copy_table_sql(duck_db,'main_export.ocel2_event_object',sqlite_db,'event_object',['ocel_event_id VARCHAR','ocel_object_id VARCHAR','ocel_qualifier VARCHAR'])
duckdb.sql(sql_query)

    # object_object
sql_query = generate_copy_table_sql(duck_db,'main_export.ocel2_object_object',sqlite_db,'object_object',['ocel_source_id VARCHAR','ocel_target_id VARCHAR','ocel_qualifier VARCHAR'])
duckdb.sql(sql_query)

    # object
sql_query = generate_copy_table_sql(duck_db,'main_export.ocel2_object',sqlite_db,'object',['ocel_id VARCHAR','ocel_type VARCHAR'])
duckdb.sql(sql_query)

    # object_map_type
sql_query = generate_copy_table_sql(duck_db,'main_export.ocel2_object_map_type',sqlite_db,'object_map_type',['ocel_type VARCHAR','ocel_type_map VARCHAR'])
duckdb.sql(sql_query)


# COPY 'event_<type>' and 'object_<type>' to SQLITE

# get map_types
sql_query = '''
SELECT ocel_type_map FROM duck_db.main_export.ocel2_event_map_type
'''
event_map_types = duckdb.sql(sql_query).df()
event_map_types = event_map_types['ocel_type_map'].tolist()

sql_query = '''
SELECT ocel_type_map FROM duck_db.main_export.ocel2_object_map_type
'''
object_map_types = duckdb.sql(sql_query).df()
object_map_types = object_map_types['ocel_type_map'].tolist()

for ocel_type in event_map_types:
    table_name = 'event_' + ocel_type

    # GET ATTRIBUTES
    sql_query = '''
SELECT DISTINCT
    attribute_column_name,
    attribute_datatype
FROM
    duck_db.main_export.ocel2_event_type_unpivoted
WHERE
    ocel_type_map = ''' + "'" + ocel_type + "'" + '''
    and attribute_column_name is not null
    and attribute_datatype is not null
'''

    attribute_columns = duckdb.sql(sql_query).df().values.tolist()

    table_columns = ['ocel_id VARCHAR','ocel_time VARCHAR']

    sql_statement = ',\n'
    for column_name,datatype in attribute_columns:
        sql_statement = sql_statement + '\n\t\t' + column_name + '::' + datatype + ' as ' + column_name + ','
        
        if datatype.lower() == 'boolean':
            datatype = 'varchar' # SQLITE has no boolean datatype

        table_columns.append(column_name + ' ' + datatype.upper())
    sql_statement = sql_statement[:-1] # remove last comma

    sql_query = '''
CREATE TABLE sqlite_db.''' + 'event_' + ocel_type + ''' (''' + ', '.join(table_columns) + ''');
INSERT INTO sqlite_db.''' + 'event_' + ocel_type + ''' with unpivoted_table as (
    SELECT 
        ocel_id,
        ocel_time,
        attribute_column_name,
        attribute_value,
        attribute_datatype
    FROM
        duck_db.main_export.ocel2_event_type_unpivoted
    WHERE
        ocel_type_map = ''' + "'" + ocel_type + "'" + '''
),
pivoted_table as (
    PIVOT unpivoted_table
    ON attribute_column_name
    USING first(attribute_value)
    GROUP BY
        ocel_id,
        ocel_time
),
cast_attribute_columns as (
    SELECT
        ocel_id,
        ocel_time''' + sql_statement + '''
    FROM
        pivoted_table
)

SELECT * FROM pivoted_table;

'''
    # 

    duckdb.sql(sql_query)

for ocel_type in object_map_types:
    table_name = 'object_' + ocel_type

    # GET ATTRIBUTES
    sql_query = '''
SELECT DISTINCT
    attribute_column_name,
    attribute_datatype
FROM
    duck_db.main_export.ocel2_object_type_unpivoted
WHERE
    ocel_type_map = ''' + "'" + ocel_type + "'" + '''
    and attribute_column_name is not null
    and attribute_datatype is not null
'''

    attribute_columns = duckdb.sql(sql_query).df().values.tolist()
    print(attribute_columns)

    table_columns = ['ocel_id VARCHAR','ocel_time VARCHAR','ocel_changed_field VARCHAR']

    sql_statement = ',\n'
    for column_name,datatype in attribute_columns:
        sql_statement = sql_statement + '\n\t\t' + column_name + ','
        
        if datatype.lower() == 'boolean':
            datatype = 'varchar' # SQLITE has no boolean datatype
        elif datatype.lower() == 'number':
            datatype = 'numeric'

        table_columns.append(column_name + ' ' + datatype.upper())  
    
    sql_statement = sql_statement[:-1] # remove last comma

    sql_query = '''
CREATE TABLE sqlite_db.''' + 'object_' + ocel_type + ''' (''' + ', '.join(table_columns) + ''');
INSERT INTO sqlite_db.''' + 'object_' + ocel_type + ''' with unpivoted_table as (
    SELECT 
        ocel_id,
        ocel_time,
        ocel_changed_field,
        attribute_column_name,
        attribute_value,
        attribute_datatype
    FROM
        duck_db.main_export.ocel2_object_type_unpivoted
    WHERE
        ocel_type_map = ''' + "'" + ocel_type + "'" + '''
),
pivoted_table as (
    PIVOT unpivoted_table
    ON attribute_column_name
    USING first(attribute_value)
    GROUP BY
        ocel_id,
        ocel_time,
        ocel_changed_field
),
cast_attribute_columns as (
    SELECT
        ocel_id,
        ocel_time,
        ocel_changed_field''' + sql_statement + '''
    FROM
        pivoted_table
)

SELECT * FROM cast_attribute_columns;

'''
    duckdb.sql(sql_query)
