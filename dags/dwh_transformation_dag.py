# import pendulum
# from airflow.models.dag import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
# snowflake_conn = snowflake_hook.get_connection(snowflake_hook.snowflake_conn_id)

# spark_jars_str = (
#     "/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.4.jar,"
#     "/opt/airflow/jars/snowflake-jdbc-3.16.1.jar,"
#     "/opt/airflow/jars/postgresql-42.7.3.jar,"
#     "/opt/airflow/jars/checker-qual-3.42.0.jar"
# )

# with DAG(
#     dag_id='job_listings_data_warehousing_dag',
#     start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
#     schedule=None,
#     catchup=False,
#     tags=['spark', 'snowflake', 'dwh'],
# ) as dag:
    
#     submit_spark_job = SparkSubmitOperator(
#         task_id='submit_spark_etl_job',
#         conn_id='spark_default', 
#         application='/opt/airflow/dags/spark_etl_script.py',
        
#         jars=spark_jars_str,
        
#         conf={
#             "spark.driver.extraClassPath": spark_jars_str.replace(",", ":"),
#             "spark.executor.extraClassPath": spark_jars_str.replace(",", ":")
#         },
        
#         env_vars={
#             'AIRFLOW_CONN_SNOWFLAKE_DEFAULT_HOST': snowflake_conn.host,
#             'AIRFLOW_CONN_SNOWFLAKE_DEFAULT_ACCOUNT': snowflake_conn.extra_dejson.get('account'),
#             'AIRFLOW_CONN_SNOWFLAKE_DEFAULT_LOGIN': snowflake_conn.login,
#             'AIRFLOW_CONN_SNOWFLAKE_DEFAULT_PASSWORD': snowflake_conn.password,
#             'AIRFLOW_CONN_SNOWFLAKE_DEFAULT_DATABASE': snowflake_conn.extra_dejson.get('database'),
#             'AIRFLOW_CONN_SNOWFLAKE_DEFAULT_SCHEMA': snowflake_conn.schema,
#             'AIRFLOW_CONN_SNOWFLAKE_DEFAULT_WAREHOUSE': snowflake_conn.extra_dejson.get('warehouse'),
#             'AIRFLOW_CONN_SNOWFLAKE_DEFAULT_ROLE': snowflake_conn.extra_dejson.get('role'),
#         }
#     )
