import psycopg
import pandas as pd

# Connection configuration
DATABASE_CONFIG = {
    'dbname': 'football',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

# Step 1: Connect to PostgreSQL
def connect_to_db():
    conn = psycopg.connect(**DATABASE_CONFIG)
    conn.autocommit = True
    return conn

# Step 2: Create the database (if it doesn't exist)
def create_database():
    with psycopg.connect(
        dbname="football",
        user=DATABASE_CONFIG["user"],
        password=DATABASE_CONFIG["password"],
        host=DATABASE_CONFIG["host"],
        port=DATABASE_CONFIG["port"]
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {DATABASE_CONFIG['dbname']}")

# Step 3: Create table based on CSV schema
def create_table(conn, table_name, schema):
    with conn.cursor() as cur:
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
        cur.execute(create_table_query)
        print(f"Table {table_name} created.")

# Step 4: Load CSV data into the table
def load_csv_to_table(conn, csv_file, table_name):
    data = pd.read_csv(csv_file)
    with conn.cursor() as cur:
        for index, row in data.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join([f"%s"] * len(row))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cur.execute(insert_query, tuple(row))
        print(f"Data from {csv_file} inserted into {table_name}")

# Sample usage
try:
    # Establish the connection
    conn = connect_to_db()

    # Define table schema
    table_schema = """
        id SERIAL PRIMARY KEY,
        column1 TEXT,
        column2 INT,cl
        column3 DATE
    """

    # Create a table
    create_table(conn, "your_table_name", table_schema)

    # Load CSV data into the table
    load_csv_to_table(conn, "path/to/your_file.csv", "your_table_name")

finally:
    # Close the connection
    conn.close()
