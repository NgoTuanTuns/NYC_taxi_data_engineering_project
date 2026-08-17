import duckdb
import os

# initial load 
con = duckdb.connect('NYC_taxi.db')
for f in os.listdir('data'):
    con.read_parquet(f'data/{f}').to_table(f[0:f.find('_')])