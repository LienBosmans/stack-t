import duckdb

duckdb.sql("INSTALL sqlite;")
duckdb.sql("LOAD sqlite;")

duckdb.sql("INSTALL spatial;")
duckdb.sql("LOAD spatial;")
