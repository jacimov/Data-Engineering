import psycopg
import pandas as pd

conn = psycopg.connect(
    host="pinniped.postgres.database.azure.com", dbname="examples",
    user="njacimov", password="Vj2A0rxBtk"
)

d = pd.read_sql_query("SELECT persona, element FROM events WHERE id = %(id)s",
                      conn, params = {'id': 17})

try:
    cur.execute("INSERT INTO countries (country_code, country_name) "
                "VALUES ('us', 'Estados Unidos')")
except psycopg.errors.UniqueViolation as e:
    print("we got a problem")

conn.commit() # commit transaction
conn.close() # close connection
