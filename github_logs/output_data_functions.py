from class_definitions import *
import polars as pl
import duckdb

def extract_dataframe(table:dict) -> pl.DataFrame:
    """Converts a dictionary of objects into a table (as a dataframe)."""

    lst_records = []
    for key,record in table.items():
        lst_records.append(record.get_dict())

    df_records = pl.DataFrame(lst_records)

    return df_records

def dataframe_to_persistent_duckdb(df_records:pl.DataFrame,table_name:str,duckdb_file_name) -> None:
    """Stores a dataframe in a duckdb table (in defined duckdb_file database).
    Overwrites a table if it already exists."""

    # connect to DuckDB database
    con = duckdb.connect(duckdb_file_name)

    # drop table if it already exists
    con.sql('''--sql
    drop table if exists ''' + table_name + '''
    ''')

    # write table to database
    if not df_records.is_empty():
        con.sql('''--sql
        create table  ''' + table_name + ''' as       
            select * from df_records
        ''')

    return None
