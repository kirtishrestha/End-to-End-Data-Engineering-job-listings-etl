import os
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

from test_extraction import (
    _extract_job_data_task,
    _transform_job_data_task,
    _load_data_to_postgres_task,
    _setup_postgres_database_task
)

spark_jars_str = (
    "/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.4.jar,"
    "/opt/airflow/jars/snowflake-jdbc-3.16.1.jar,"
    "/opt/airflow/jars/postgresql-42.7.3.jar"
)

POSTGRES_CONNECTION_ID = "postgres_workforce_db"

with DAG(
    dag_id='master_job_analytics_pipeline',
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=['production', 'unified'],
) as dag:

    setup_postgres = PythonOperator(
        task_id='setup_postgres_database',
        python_callable=_setup_postgres_database_task,
        op_kwargs={'postgres_conn_id': POSTGRES_CONNECTION_ID}
    )

    extract_data = PythonOperator(
        task_id='extract_job_data',
        python_callable=_extract_job_data_task,
    )

    transform_data = PythonOperator(
        task_id='transform_job_data',
        python_callable=_transform_job_data_task,
    )

    load_to_postgres = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=_load_data_to_postgres_task,
        op_kwargs={'postgres_conn_id': POSTGRES_CONNECTION_ID}
    )

    build_star_schema = SparkSubmitOperator(
        task_id='build_star_schema_with_spark',
        conn_id='spark_default',
        application='/opt/airflow/dags/spark_etl_script.py',
        jars=spark_jars_str,
        conf={
            "spark.driver.extraClassPath": spark_jars_str.replace(",", ":"),
            "spark.executor.extraClassPath": spark_jars_str.replace(",", ":")
        },
        env_vars={
            'POSTGRES_USER': os.environ.get("APP_POSTGRES_USER", ''),
            'POSTGRES_PASSWORD': os.environ.get("APP_POSTGRES_PASSWORD", ''),
            'POSTGRES_DB': os.environ.get("APP_POSTGRES_DB", ''),
            'SNOWFLAKE_ACCOUNT': os.environ.get("SNOWFLAKE_ACCOUNT", ''),
            'SNOWFLAKE_USER': os.environ.get("SNOWFLAKE_USER", ''),
            'SNOWFLAKE_PASSWORD': os.environ.get("SNOWFLAKE_PASSWORD", ''),
            'SNOWFLAKE_DATABASE': os.environ.get("SNOWFLAKE_DATABASE", ''),
            'SNOWFLAKE_STAGING_SCHEMA': os.environ.get("SNOWFLAKE_STAGING_SCHEMA", ''),
            'SNOWFLAKE_ANALYTICS_SCHEMA': os.environ.get("SNOWFLAKE_ANALYTICS_SCHEMA", ''),
            'SNOWFLAKE_WAREHOUSE': os.environ.get("SNOWFLAKE_WAREHOUSE", ''),
            'SNOWFLAKE_ROLE': os.environ.get("SNOWFLAKE_ROLE", '')
        }
    )

    setup_postgres >> extract_data >> transform_data >> load_to_postgres >> build_star_schema