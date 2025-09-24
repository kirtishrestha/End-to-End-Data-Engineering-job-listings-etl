import requests
import pandas as pd
import json
from datetime import datetime
import os
from sqlalchemy import create_engine
from airflow import DAG


from scripts.connection import connect_to_postgres, connect_to_snowflake

API_URL = "https://jsearch.p.rapidapi.com/search"
API_HEADERS = {
    "x-rapidapi-key": os.environ.get("JSEARCH_API_KEY"),
    "x-rapidapi-host": "jsearch.p.rapidapi.com"
}
COUNTRIES = ["us", "np", "in"]
NUM_PAGES_TO_EXTRACT = 20

PG_DB_USER = os.environ.get("POSTGRES_USER")
PG_DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
PG_DB_HOST = os.environ.get("POSTGRES_HOST")
PG_DB_PORT = os.environ.get("POSTGRES_PORT")
PG_DB_NAME = os.environ.get("POSTGRES_DB")
POSTGRES_TABLE_NAME = "job_listings"
POSTGRES_SCHEMA = "landing"

SF_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SF_USER = os.environ.get("SNOWFLAKE_USER")
SF_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SF_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.environ.get("SNOWFLAKE_ODS_SCHEMA")
SNOWFLAKE_TABLE_NAME = "job_listings"

def extract_job_data(api_url, api_headers, countries, num_pages):

    all_jobs = []
    selected_columns = [
        "job_title", "employer_name", "job_publisher", "job_employment_type",
        "job_description", "job_is_remote", "job_posted_at",
        "job_posted_at_datetime_utc", "job_location", "job_city",
        "job_state", "job_country", "job_highlights"
    ]

    print(f"Starting job data extraction for countries: {countries} for up to {num_pages} pages each.")
    for country_code in countries:
        for page_num in range(1, num_pages + 1):
            querystring = {
                "query": "jobs",
                "page": str(page_num),
                "num_pages": str(num_pages),
                "country": country_code,
                "date_posted": "month"
            }
            try:
                response = requests.get(api_url, headers=api_headers, params=querystring)
                response.raise_for_status()
                data = response.json()

                if "data" in data and data["data"]:
                    for job in data["data"]:
                        job["job_country"] = country_code.upper()
                        filtered_job = {key: job.get(key, None) for key in selected_columns}
                        all_jobs.append(filtered_job)
                    print(f"Extracted {len(data['data'])} jobs for {country_code.upper()} on page {page_num}")
                else:
                    print(f"No job data found for {country_code.upper()} on page {page_num}. Moving on.")
                    if page_num > 1: 
                        break
            except requests.exceptions.RequestException as e:
                print(f"Error extracting data for {country_code.upper()} on page {page_num}: {e}")
                break 
            except json.JSONDecodeError:
                print(f"Error decoding JSON for {country_code.upper()} on page {page_num}. Response: {response.text[:200]}...")
                break

    print(f"Finished extraction. Total jobs collected: {len(all_jobs)}")
    return all_jobs

def transform_job_data(raw_jobs_list):

    if not raw_jobs_list:
        print("No raw job data to transform.")
        return pd.DataFrame()

    df = pd.DataFrame(raw_jobs_list)

    print("Applying transformations to DataFrame...")

    common_skills = [
        'python', 'java', 'sql', 'javascript', 'react', 'angular', 'node.js',
        'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'tensorflow', 'pytorch',
        'machine learning', 'data science', 'analytics', 'excel', 'tableau',
        'power bi', 'c++', 'c#', 'php', 'ruby', 'go', 'devops', 'agile',
        'scrum', 'git', 'api', 'rest', 'graphql', 'cloud', 'security',
        'linux', 'unix', 'windows server', 'networking', 'database', 'html', 'css',
        'mongodb', 'cassandra', 'kafka', 'spark', 'hadoop', 'big data', 'etl',
        'data warehousing', 'airflow', 'dbt', 'azure devops', 'jira', 'confluence',
        'communication', 'teamwork', 'collaboration', 'leadership', 'problem-solving',
        'critical thinking', 'adaptability', 'creativity', 'time management',
        'organization', 'attention to detail', 'customer service', 'negotiation',
        'presentation', 'mentoring', 'coaching', 'decision-making', 'strategic thinking',
        'emotional intelligence', 'interpersonal skills', 'conflict resolution',
        'client management', 'stakeholder management', 'cross-functional',
        'project management', 'documentation'
    ]

    def extract_skills_from_desc(description, skills_list):
        if pd.isna(description) or not isinstance(description, str):
            return []
        found_skills = []
        description_lower = description.lower()
        for skill in skills_list:
            if skill in description_lower:
                found_skills.append(skill)
        return list(set(found_skills))

    df['skills'] = df['job_description'].apply(lambda x: extract_skills_from_desc(x, common_skills))
    print(f"Extracted skills for {df['skills'].apply(len).astype(bool).sum()} jobs.")

    df['ingested_at'] = datetime.now()
    print("Added 'ingested_at' column.")

    target_postgres_columns = [
        'job_title',
        'employer_name',
        'job_publisher',
        'job_employment_type',
        'job_description',
        'job_is_remote',
        'job_posted_at',
        'job_posted_at_datetime_utc',
        'job_location',
        'job_city',
        'job_state',
        'job_country',
        'job_highlights',
        'ingested_at',
        'skills'
    ]

    for col in target_postgres_columns:
        if col not in df.columns:
            df[col] = None
            print(f"Added missing column '{col}' to DataFrame with None values.")

    df = df[target_postgres_columns]

    print(f"Transformation complete. Final DataFrame columns for load: {df.columns.tolist()}")
    print(f"Transformed DataFrame shape: {df.shape}")
    return df

def save_to_csv(df, filename="jobs_extracted_data.csv"):
    if df.empty:
        print("No data to save to CSV.")
        return
    try:
        df.to_csv(filename, index=False)
        print(f"CSV file '{filename}' created successfully.")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def load_data_to_postgres(df_transformed, pg_db_name, pg_table_name, pg_schema):
    if df_transformed.empty:
        print("No data to load to PostgreSQL.")
        return

    conn_string = f"postgresql+psycopg2://{PG_DB_USER}:{PG_DB_PASSWORD}@{PG_DB_HOST}:{PG_DB_PORT}/{pg_db_name}"
    pg_engine = create_engine(conn_string)

    try:
        with pg_engine.connect() as connection:
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {pg_schema};")
            connection.commit()
            print(f"Schema '{pg_schema}' ensured in PostgreSQL.")

        df_transformed.to_sql(pg_table_name, pg_engine, schema=pg_schema, if_exists='append', index=False)
        print(f"Successfully loaded {len(df_transformed)} rows to {pg_schema}.{pg_table_name} in PostgreSQL.")
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        raise
    finally:
        if pg_engine:
            pg_engine.dispose()
            print("PostgreSQL engine disposed.")

def transfer_postgres_to_snowflake(pg_db_name, pg_table_name, pg_schema, sf_table_name, sf_schema):
    print(f"Starting data transfer from PostgreSQL {pg_schema}.{pg_table_name} to Snowflake {sf_schema}.{sf_table_name}...")

    pg_conn_string = f"postgresql+psycopg2://{PG_DB_USER}:{PG_DB_PASSWORD}@{PG_DB_HOST}:{PG_DB_PORT}/{pg_db_name}"
    pg_engine = create_engine(pg_conn_string)

    sf_conn = None
    try:
        sf_conn = connect_to_snowflake()
        if not sf_conn:
            raise Exception("Failed to establish Snowflake connection via connection.py. Check environment variables and connection.py setup.")

        select_sql = f"SELECT * FROM {pg_schema}.{pg_table_name};"
        # WHERE ingested_at > (SELECT MAX(ingested_at) FROM {sf_schema}.{sf_table_name})
        
        df_from_pg = pd.read_sql(select_sql, pg_engine)
        print(f"Fetched {len(df_from_pg)} rows from PostgreSQL for transfer.")

        if df_from_pg.empty:
            print("No new data to transfer from PostgreSQL to Snowflake.")
            return

        sf_cursor = sf_conn.cursor()

        if 'id' in df_from_pg.columns:
            df_from_pg['id'] = df_from_pg['id'].astype(str)
            
        if 'job_highlights' in df_from_pg.columns:
            df_from_pg['job_highlights'] = df_from_pg['job_highlights'].apply(lambda x: json.dumps(x) if x else None)
        if 'skills' in df_from_pg.columns:
            df_from_pg['skills'] = df_from_pg['skills'].apply(lambda x: json.dumps(x) if x else None)

        sf_insert_columns = df_from_pg.columns.tolist()
        value_placeholders = ', '.join(['%s'] * len(sf_insert_columns))

        insert_sql = f"""
            INSERT INTO {sf_schema}.{sf_table_name} (
                {', '.join(sf_insert_columns)}
            ) VALUES ({value_placeholders})
        """
        
        data_to_insert = [tuple(row) for row in df_from_pg.values]
        
        sf_cursor.executemany(insert_sql, data_to_insert)
        sf_conn.commit()
        print(f"Successfully inserted {len(data_to_insert)} rows into Snowflake {sf_schema}.{sf_table_name}.")

    except Exception as e:
        print(f"Error transferring data from PostgreSQL to Snowflake: {e}")
        if sf_conn:
            sf_conn.rollback()
        raise
    finally:
        if pg_engine:
            pg_engine.dispose()
            print("PostgreSQL engine disposed for Snowflake transfer.")
        if 'sf_cursor' in locals() and sf_cursor:
            sf_cursor.close()
        if sf_conn:
            sf_conn.close()
            print("Snowflake connection closed.")

def run_full_etl_pipeline():
    print("--- Starting Full ETL Pipeline ---")

    raw_data = extract_job_data(API_URL, API_HEADERS, COUNTRIES, NUM_PAGES_TO_EXTRACT)
    transformed_df = transform_job_data(raw_data)

    if transformed_df.empty:
        print("No data to process. Exiting pipeline.")
        return

    save_to_csv(transformed_df, "jobs_extracted_data_pipeline.csv")

    load_data_to_postgres(transformed_df, PG_DB_NAME, POSTGRES_TABLE_NAME, POSTGRES_SCHEMA)

    transfer_postgres_to_snowflake(PG_DB_NAME, POSTGRES_TABLE_NAME, POSTGRES_SCHEMA, SNOWFLAKE_TABLE_NAME, SF_SCHEMA)

    print("--- Full ETL Pipeline Complete ---")

if __name__ == "__main__":

    required_env_vars = [
        "JSEARCH_API_KEY", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_DB", 
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE"
    ]
    
    missing_vars = [var for var in required_env_vars if os.environ.get(var) is None]

    if missing_vars:
        print(f"ERROR: The following environment variables are not set: {', '.join(missing_vars)}")
        print("Please set these environment variables with your credentials before running the script.")
        print("\nExample:")
        print("export JSEARCH_API_KEY=\"f2714d7bc5mshec22205b211439fp1acfd7jsn203721eb4014\" # Replace with your actual key!")
        print("export POSTGRES_USER=\"your_user\"")
        print("export POSTGRES_PASSWORD=\"your_password\"")
        print("export POSTGRES_HOST=\"localhost\"")
        print("export POSTGRES_DB=\"workforce_db\"")
        print("export SNOWFLAKE_ACCOUNT=\"your_account_identifier\"")
        print("export SNOWFLAKE_USER=\"your_sf_user\"")
        print("export SNOWFLAKE_PASSWORD=\"your_sf_password\"")
        print("export SNOWFLAKE_WAREHOUSE=\"COMPUTE_WH\"")
        print("export SNOWFLAKE_DATABASE=\"ETL_DB\"")
        print("export SNOWFLAKE_SCHEMA=\"LANDING\"")
        print("export SNOWFLAKE_ROLE=\"SYSADMIN\" # Example, if needed by connection.py")
    else:
        run_full_etl_pipeline()