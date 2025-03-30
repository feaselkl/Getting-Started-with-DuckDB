import duckdb
import pandas as pd

# TODO: explain why we create a connection
con = duckdb.connect()
con.sql("SELECT 42 as X").show()
con.close()

with duckdb.connect("duckdbdata/file.db") as con:
    con.sql("CREATE TABLE test (i INTEGER)")
    con.sql("INSERT INTO test VALUES (42)")
    con.table("test").show()

con = duckdb.connect("duckdbdata/chicago.db")
df1 = pd.read_parquet("duckdbdata/ChicagoParkingTickets.parquet")
con.execute("CREATE TABLE ChicagoParkingTickets AS SELECT * FROM df1")

con.table("duckdbdata/ChicagoParkingTickets.parquet").show()
con.sql("DESCRIBE 'duckdbdata/ChicagoParkingTickets.parquet'")
# TODO: show loading data into duckdb table
con.close()

# TODO: Replicate SQL Server schema for Chicago Parking Tickets
# TODO: Try out a variety of queries
# Standard operations
# PIVOT / UNPIVOT

# TODO: functionality not in T-SQL
# SUMMARIZE
con.sql("SUMMARIZE ChicagoParkingTickets")
con.sql("CREATE TABLE cpt_summary AS SELECT * FROM (SUMMARIZE ChicagoParkingTickets)")
con.sql("SUMMARIZE SELECT Community_Name, Per_capita_income FROM 'duckdbdata/ChicagoParkingTickets.parquet'")


# COPY
# Friendly SQL
# Special join types

# TODO: Demo querying a Pandas DataFrame
# TODO: Demo querying a Polars DataFrame

# TODO: Demo on deleting data and vacuuming

# TODO: Demo on indexing techniques

# TODO: Come up with specific extensions to install
# TODO: Demo on installing extensions

# TODO: SET, PRAGMA, and secrets

# TODO: Demo Explain plans