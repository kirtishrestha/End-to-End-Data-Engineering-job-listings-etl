# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import sys
# import os
# import json
# import io
# import csv
# from airflow.models.dag import DAG


# # sys.path.append('/opt/airflow/scripts')
# # sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
# from scripts.connection import connect_to_postgres, connect_to_snowflake

# def transfer_data(**context):
#     pg_conn = connect_to_postgres()
#     pg_cursor = pg_conn.cursor('postgres_to_snowflake_cursor')
    
#     sf_conn = connect_to_snowflake()
#     sf_cursor = sf_conn.cursor()

#     try:
#         pg_cursor.execute("""
#             SELECT 
#                 job_title, employer_name, job_publisher, job_employment_type, job_description,
#                 job_is_remote, job_posted_at, job_posted_at_datetime_utc, job_location, job_city,
#                 job_state, job_country, job_highlights, ingested_at, skills
#             FROM landing.job_listings;
#         """)

#         def serialize_for_csv(row):
#             serialized = []
#             for idx, value in enumerate(row):
#                 if idx == 12 or idx == 14:
#                     if value is None:
#                         serialized.append(None)
#                     else:
#                         try:
#                             if isinstance(value, str):
#                                 json_object = json.loads(value)
#                             elif isinstance(value, (dict, list)):
#                                 json_object = value
#                             else:
#                                 json_object = value
#                             serialized.append(json.dumps(json_object))
#                         except (json.JSONDecodeError, TypeError):
#                             serialized.append(None)
#                 else:
#                     serialized.append(value)
#             return serialized

#         print("Streaming data from Postgres and preparing in-memory CSV file...")
#         string_buffer = io.StringIO()
#         csv_writer = csv.writer(string_buffer)
        
#         rows_processed = 0
#         for row in pg_cursor:
#             csv_writer.writerow(serialize_for_csv(row))
#             rows_processed += 1
        
#         if rows_processed == 0:
#             print("No rows found in Postgres. Exiting.")
#             return

#         print(f"Processed {rows_processed} rows. Uploading to Snowflake stage...")
        
#         csv_content_bytes = string_buffer.getvalue().encode('utf-8')
#         bytes_buffer = io.BytesIO(csv_content_bytes)
        
#         sf_cursor.execute("PUT file:///tmp/data.csv @~", file_stream=bytes_buffer)
#         print("Upload successful.")

#         print("Copying data from stage into the final table...")
#         copy_sql = f"""
#             COPY INTO landing.job_listings (
#                 job_title, employer_name, job_publisher, job_employment_type, job_description,
#                 job_is_remote, job_posted_at, job_posted_at_datetime_utc, job_location, job_city,
#                 job_state, job_country, job_highlights, ingested_at, skills
#             )
#             FROM (
#                 SELECT
#                     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
#                     PARSE_JSON($13),
#                     $14,
#                     PARSE_JSON($15)
#                 FROM @~/data.csv.gz
#             )
#             FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' EMPTY_FIELD_AS_NULL = true)
#             ON_ERROR = 'CONTINUE';
#         """
#         sf_cursor.execute(copy_sql)
#         print("COPY command successful.")

#     except Exception as e:
#         print(f"An error occurred: {e}")
#         sf_conn.rollback()
#         raise
#     finally:
#         try:
#             sf_cursor.execute("REMOVE @~/data.csv.gz")
#             print("Staged file removed.")
#         except Exception as e:
#             print(f"Could not remove staged file: {e}")

#         sf_cursor.close()
#         sf_conn.close()
#         pg_cursor.close()
#         pg_conn.close()
#         print("Connections closed.")

# default_args = {
#     'start_date': datetime(2024, 1, 1),
#     'catchup': False,
# }

# with DAG(
#     dag_id='postgres_to_snowflake',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# ) as dag:
#     transfer_task = PythonOperator(
#         task_id='transfer_data',
#         python_callable=transfer_data,
#         provide_context=True,
#     )