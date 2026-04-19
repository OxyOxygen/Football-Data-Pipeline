import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def initialize_database():
    """
    Executes the DDL script (schema_setup.sql) to prepare the database structure.
    This is a boilerplate step for initializing a new data warehouse environment.
    """
    try:
        # DB Connection parameters
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        # Read the SQL file
        sql_file_path = os.path.join(os.path.dirname(__file__), 'schema_setup.sql')
        with open(sql_file_path, 'r') as f:
            sql_script = f.read()

        print("[INIT] Executing database schema setup...")
        cur.execute(sql_script)
        
        conn.commit()
        print("[SUCCESS] Database schema initialized successfully.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Error initializing database: {e}")

if __name__ == "__main__":
    initialize_database()
