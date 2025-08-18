import duckdb

# create a connection to a file called 'file.db'
con = duckdb.connect(
    "/Users/davidmeadows_1/programs/birds-eye-app/swan-lake/dbs/parsed_ebd.db"
)
# query the table
con.table("taxonomy").show()
# explicitly close the connection
con.close()
