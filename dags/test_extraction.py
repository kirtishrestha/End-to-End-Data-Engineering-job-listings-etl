import requests
import pandas as pd
import json
from datetime import datetime
import os
import time
import random

from airflow.providers.postgres.hooks.postgres import PostgresHook

TEMP_DATA_DIR = "/opt/airflow/dags/data"
RAW_JOBS_FILE = os.path.join(TEMP_DATA_DIR, "raw_jobs.json")
TRANSFORMED_JOBS_FILE = os.path.join(TEMP_DATA_DIR, "transformed_jobs.csv")

os.makedirs(TEMP_DATA_DIR, exist_ok=True)

API_URL = "https://jsearch.p.rapidapi.com/search"
API_HEADERS = {
    "x-rapidapi-key": os.environ.get("JSEARCH_API_KEY"),
    "x-rapidapi-host": "jsearch.p.rapidapi.com"
}
COUNTRIES = ["us", "in", "jp", "ru"]
NUM_PAGES_TO_EXTRACT = 5
POSTGRES_TABLE_NAME = "job_listings"
POSTGRES_SCHEMA = "landing"

def _extract_job_data_task():
    def fetch_with_retry(url, headers, params, retries=5):
        for i in range(retries):
            try:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 429:
                    wait = 2 ** i + random.uniform(1, 3)
                    print(f"Rate limit hit. Waiting {wait:.2f} seconds before retrying...")
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                wait = 2 ** i
                print(f"Request failed (attempt {i+1}/{retries}). Retrying in {wait} seconds... Error: {e}")
                time.sleep(wait)
        raise Exception("Maximum retry attempts reached for API call.")

    all_jobs = []
    selected_columns = [
        "job_title", "employer_name", "job_publisher", "job_employment_type",
        "job_description", "job_is_remote", "job_posted_at",
        "job_posted_at_datetime_utc", "job_location", "job_city",
        "job_state", "job_country", "job_highlights"
    ]

    print(f"Starting job data extraction for countries: {COUNTRIES} for up to {NUM_PAGES_TO_EXTRACT} pages each.")
    for country_code in COUNTRIES:
        for page_num in range(1, NUM_PAGES_TO_EXTRACT + 1):
            querystring = {
                "query": "jobs",
                "page": str(page_num),
                "num_pages": str(NUM_PAGES_TO_EXTRACT),
                "country": country_code,
                "date_posted": "all" 
            }
            try:
                response = fetch_with_retry(API_URL, headers=API_HEADERS, params=querystring)
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
            except json.JSONDecodeError:
                print(f"Error decoding JSON for {country_code.upper()} on page {page_num}. Response: {response.text[:200]}...")
                raise
            except Exception as e:
                print(f"Failed to extract data for {country_code.upper()} on page {page_num}: {e}")
                raise

    print(f"Finished extraction. Total jobs collected: {len(all_jobs)}")

    with open(RAW_JOBS_FILE, 'w') as f:
        json.dump(all_jobs, f)
    print(f"Raw job data saved to {RAW_JOBS_FILE}")

def _transform_job_data_task():
    if not os.path.exists(RAW_JOBS_FILE) or os.stat(RAW_JOBS_FILE).st_size == 0:
        print(f"Raw data file {RAW_JOBS_FILE} does not exist or is empty. No data to transform.")
        pd.DataFrame().to_csv(TRANSFORMED_JOBS_FILE, index=False)
        return

    with open(RAW_JOBS_FILE, 'r') as f:
        raw_jobs_list = json.load(f)

    if not raw_jobs_list:
        print("No raw job data to transform from JSON file.")
        pd.DataFrame().to_csv(TRANSFORMED_JOBS_FILE, index=False)
        return

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
        'job_title', 'employer_name', 'job_publisher', 'job_employment_type',
        'job_description', 'job_is_remote', 'job_posted_at',
        'job_posted_at_datetime_utc', 'job_location', 'job_city',
        'job_state', 'job_country', 'job_highlights', 'ingested_at', 'skills'
    ]

    for col in target_postgres_columns:
        if col not in df.columns:
            df[col] = None

    df = df[target_postgres_columns]

    if 'job_highlights' in df.columns:
        df['job_highlights'] = df['job_highlights'].apply(lambda x: json.dumps(x) if x is not None else None)
    if 'skills' in df.columns:
        df['skills'] = df['skills'].apply(lambda x: json.dumps(x) if x is not None else None)

    print(f"Transformation complete. Final DataFrame columns for load: {df.columns.tolist()}")
    print(f"Transformed DataFrame shape: {df.shape}")

    df.to_csv(TRANSFORMED_JOBS_FILE, index=False)
    print(f"Transformed job data saved to {TRANSFORMED_JOBS_FILE}")

def _load_data_to_postgres_task(postgres_conn_id: str):
    if not os.path.exists(TRANSFORMED_JOBS_FILE) or os.stat(TRANSFORMED_JOBS_FILE).st_size == 0:
        print(f"Transformed data file {TRANSFORMED_JOBS_FILE} does not exist or is empty. No data to load.")
        return
    
    print(f"Loading data from {TRANSFORMED_JOBS_FILE} to {POSTGRES_SCHEMA}.{POSTGRES_TABLE_NAME} using conn_id '{postgres_conn_id}'...")
    
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    target_table = f"{POSTGRES_SCHEMA}.{POSTGRES_TABLE_NAME}"

    try:
        print(f"Truncating table {target_table} before load.")
        pg_hook.run(f"TRUNCATE TABLE {target_table};")
        
        print(f"Starting COPY EXPERT from {TRANSFORMED_JOBS_FILE}...")
        sql_copy_command = f"""
            COPY {target_table} 
            FROM STDIN 
            WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        """
        pg_hook.copy_expert(
            sql=sql_copy_command,
            filename=TRANSFORMED_JOBS_FILE
        )
        print(f"Successfully loaded data into {target_table}.")
        
    except Exception as e:
        print(f"Error during data load to PostgreSQL: {e}")
        raise

def _setup_postgres_database_task(postgres_conn_id: str):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA};"
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.{POSTGRES_TABLE_NAME} (
        job_title TEXT,
        employer_name TEXT,
        job_publisher TEXT,
        job_employment_type TEXT,
        job_description TEXT,
        job_is_remote BOOLEAN,
        job_posted_at TEXT,
        job_posted_at_datetime_utc TEXT,
        job_location TEXT,
        job_city TEXT,
        job_state TEXT,
        job_country TEXT,
        job_highlights TEXT,
        ingested_at TEXT,
        skills TEXT
    );
    """
    
    print(f"Ensuring schema '{POSTGRES_SCHEMA}' exists...")
    pg_hook.run(create_schema_sql)
    print("Schema check complete.")
    
    print(f"Ensuring table '{POSTGRES_SCHEMA}.{POSTGRES_TABLE_NAME}' exists...")
    pg_hook.run(create_table_sql)
    print("Table check complete. Database is ready for loading.")