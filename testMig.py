import psycopg2
import logging

conn = psycopg2.connect(
    dbname="odisha_db",
    user="highcourt",
    password="highcourt",
    host="localhost",
    port=5432
)

cur = conn.cursor()

cur.execute("""
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
    ORDER BY tablename;
""")
tables = [r[0] for r in cur.fetchall()]

print(f"Found {len(tables)} tables")

for table in tables:
    try:
        cur.execute(f'SELECT * FROM "{table}" LIMIT 15;')
        rows = cur.fetchall()
        if rows:
            colnames = [desc[0] for desc in cur.description]
            print(f"\nTable: {table}")
            print("Columns:", colnames)
            for row in rows:
                print(row)
        else:
            print(f"\nTable: {table} is empty")
    except Exception as e:
        print(f"\nCould not fetch from {table}: {e}")

cur.close()
conn.close()