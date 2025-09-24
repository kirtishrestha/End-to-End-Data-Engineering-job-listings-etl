import os
import psycopg2
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT")
        )
        print("PostgreSQL connection successful.")
        return conn
    except Exception as e:
        print(f"PostgreSQL connection failed: {e}")
        return None

from sqlalchemy import create_engine


def connect_to_snowflake():
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )
        print("Snowflake connection successful.")
    except Exception as e:
        print(f"Snowflake connection failed: {e}")
    return conn

if __name__ == "__main__":
    pg_connection = connect_to_postgres()
    if pg_connection:
        pg_connection.close()
        print("PostgreSQL connection closed.")

    print("-" * 20)

    sf_connection = connect_to_snowflake()
    if sf_connection:
        sf_connection.close()
        print("Snowflake connection closed.")