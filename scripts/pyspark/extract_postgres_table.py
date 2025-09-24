import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from airflow.models import Variable
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

TABLE_CONFIG = {
    "job_listings": {
        "countries": {
            "landing": {"timestamp_column": "ingested_at"},
        }
    }
}

def extract_postgres_table(country, table, load_date):
    print(f"Starting extraction for table: {table} in country: {country}")

    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB")

    pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    var_key = f"last_extracted_parquet_timestamp_{country}_{table}"
    try:
        last_extracted_parquet_timestamp = Variable.get(var_key)
        print(f"[INFO] Last extracted timestamp: {last_extracted_parquet_timestamp}")
    except KeyError:
        last_extracted_parquet_timestamp = "2000-01-01 00:00:00.000"
        print(f"[INFO] No variable found. Using default: {last_extracted_parquet_timestamp}")

    spark = SparkSession.builder \
        .appName(f"Extract_{country}_{table}") \
        .config(
            "spark.jars",
            "/opt/airflow/jars/postgresql-42.7.3.jar,"
            "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,"
            "/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar"
        ) \
        .getOrCreate()

    timestamp_col = TABLE_CONFIG[table]["countrys"][country]["timestamp_column"]
    query = f"(SELECT * FROM {country}.{table} WHERE {timestamp_col} > '{last_extracted_parquet_timestamp}') AS filtered_data"
    print(f"[INFO] Query: {query}")

    df = spark.read.format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", query) \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    record_count = df.count()
    if record_count == 0:
        spark.stop()
        print(f"[INFO] No new data to extract from {country}.{table}")
        return

    df = df.withColumn("_country", lit(country)) \
           .withColumn("_source", lit("postgres"))
    df.show()

    output_path = f"/opt/airflow/data/raw/country={country}/table={table}/load_date={load_date}/"
    print(f"[INFO] Writing data to {output_path}")
    df.write.mode("append").parquet(output_path)

    max_ts = df.agg({timestamp_col: "max"}).collect()[0][0]
    if max_ts:
        max_ts_str = str(max_ts)
        Variable.set(var_key, max_ts_str)
        print(f"[INFO] Updated Airflow Variable '{var_key}' to {max_ts_str}")

    spark.stop()
    print(f"[INFO] Finished extraction for {table}")
