import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, upper, trim, initcap, to_date, date_format, month, quarter, year,
    monotonically_increasing_id, row_number, explode, split, lit,
    udf, from_json, translate
)
from pyspark.sql.types import TimestampType, BooleanType, ArrayType, StringType
from datetime import datetime, timedelta
import re

def parse_relative_time(time_str):
    if not isinstance(time_str, str):
        return None
    now = datetime.utcnow()
    time_str = time_str.lower()
    try:
        if 'hour' in time_str:
            hours = int(re.search(r'\d+', time_str).group())
            return now - timedelta(hours=hours)
        elif 'day' in time_str:
            days = int(re.search(r'\d+', time_str).group())
            return now - timedelta(days=days)
        elif 'yesterday' in time_str:
            return now - timedelta(days=1)
        else:
            return None
    except (AttributeError, ValueError):
        return None

def write_to_snowflake(df, table_name, sf_options):
    print(f"Writing {df.count()} rows to Snowflake table: {table_name}")
    df.write \
      .format("snowflake") \
      .options(**sf_options) \
      .option("dbtable", table_name) \
      .mode("overwrite") \
      .save()
    print(f"Successfully wrote to {table_name}")

def main():
    spark = SparkSession.builder.appName("JobPostingsStarSchemaETL-Final").getOrCreate()

    account_identifier = os.environ.get('SNOWFLAKE_ACCOUNT')
    if not account_identifier:
        raise ValueError("SNOWFLAKE_ACCOUNT not found in environment variables.")

    snowflake_url = f"{account_identifier}.snowflakecomputing.com"

    sf_options_staging = {
        "sfUrl": snowflake_url,
        "sfUser": os.environ.get('SNOWFLAKE_USER'),
        "sfPassword": os.environ.get('SNOWFLAKE_PASSWORD'),
        "sfDatabase": os.environ.get('SNOWFLAKE_DATABASE'),
        "sfSchema": os.environ.get('SNOWFLAKE_STAGING_SCHEMA'),
        "sfWarehouse": os.environ.get('SNOWFLAKE_WAREHOUSE'),
        "sfRole": os.environ.get('SNOWFLAKE_ROLE')
    }
    
    sf_options_analytics = sf_options_staging.copy()
    sf_options_analytics["sfSchema"] = os.environ.get('SNOWFLAKE_ANALYTICS_SCHEMA')

    print("--- Reading data from PostgreSQL landing table ---")
    try:
        df_raw = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://postgres:5432/{os.environ.get('POSTGRES_DB')}") \
            .option("dbtable", "landing.job_listings") \
            .option("user", os.environ.get("POSTGRES_USER")) \
            .option("password", os.environ.get("POSTGRES_PASSWORD")) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        count = df_raw.count()
        if count == 0:
            print("No data found in landing.job_listings table. Stopping job.")
            spark.stop()
            return

        print(f"Successfully read {count} records from Postgres.")
    except Exception as e:
        print(f"Failed to read from Postgres. Error: {e}")
        spark.stop()
        raise

    parse_time_udf = udf(parse_relative_time, TimestampType())
    df_raw = df_raw.withColumn("job_posted_at_utc", parse_time_udf(col("job_posted_at")))
    df_raw = df_raw.withColumn("job_natural_key", monotonically_increasing_id())

    print("Creating Dimension Tables...")

    dim_company_df = df_raw.select(
        trim(upper(col("employer_name"))).alias("employer_name")
    ).filter(col("employer_name").isNotNull()).distinct() \
     .withColumn("company_sk", row_number().over(Window.orderBy("employer_name")))
    
    dim_publisher_df = df_raw.select(
        trim(initcap(col("job_publisher"))).alias("publisher_name")
    ).filter(col("publisher_name").isNotNull()).distinct() \
     .withColumn("publisher_sk", row_number().over(Window.orderBy("publisher_name")))

    dim_employment_type_df = df_raw.select(
        trim(initcap(col("job_employment_type"))).alias("employment_type_name")
    ).filter(col("employment_type_name").isNotNull()).distinct() \
     .withColumn("employment_type_sk", row_number().over(Window.orderBy("employment_type_name")))

    dim_location_df = df_raw.select(
        "job_location", "job_city", "job_state", "job_country"
    ).filter(col("job_location").isNotNull()).distinct() \
     .withColumn("location_sk", row_number().over(Window.orderBy("job_location")))

    dim_date_df = df_raw.select(
        to_date(col("job_posted_at_utc")).alias("full_date")
    ).filter(col("full_date").isNotNull()).distinct() \
     .withColumn("date_sk", date_format(col("full_date"), "yyyyMMdd").cast("int")) \
     .withColumn("day_of_week", date_format(col("full_date"), "EEEE")) \
     .withColumn("month_name", date_format(col("full_date"), "MMMM")) \
     .withColumn("month_number", month(col("full_date"))) \
     .withColumn("quarter_number", quarter(col("full_date"))) \
     .withColumn("year_number", year(col("full_date")))

    dim_job_details_df = df_raw.select(
        col("job_natural_key"),
        trim(col("job_title")).alias("job_title"),
        col("job_description"),
        from_json(col("job_highlights"), ArrayType(StringType())).alias("job_highlights_array"),
        col("job_is_remote").cast(BooleanType()),
        col("job_posted_at"),
        col("job_posted_at_utc")
    ).withColumn("job_sk", row_number().over(Window.orderBy("job_natural_key")))

    df_with_skills_array = df_raw.withColumn(
        "skills_array",
        split(translate(col("skills"), "[]'\"", ""), ", ")
    )
    skills_exploded_df = df_with_skills_array.select(
        col("job_natural_key"),
        explode(col("skills_array")).alias("skill_name_raw")
    ).filter(col("skill_name_raw") != '')

    dim_skill_df = skills_exploded_df.select(
        trim(initcap(col("skill_name_raw"))).alias("skill_name")
    ).distinct() \
     .withColumn("skill_sk", row_number().over(Window.orderBy("skill_name")))

    dim_company_df.cache()
    dim_publisher_df.cache()
    dim_employment_type_df.cache()
    dim_location_df.cache()
    dim_date_df.cache()
    dim_job_details_df.cache()
    dim_skill_df.cache()
    skills_exploded_df.cache()

    print("Creating Fact and Bridge Tables...")
    
    fact_data_df = df_raw.alias("raw") \
        .join(dim_job_details_df.alias("djd"), ["job_natural_key"]) \
        .join(dim_company_df.alias("dc"), trim(upper(col("raw.employer_name"))) == col("dc.employer_name"), "left") \
        .join(dim_publisher_df.alias("dp"), trim(initcap(col("raw.job_publisher"))) == col("dp.publisher_name"), "left") \
        .join(dim_employment_type_df.alias("det"), trim(initcap(col("raw.job_employment_type"))) == col("det.employment_type_name"), "left") \
        .join(dim_location_df.alias("dl"), col("raw.job_location") == col("dl.job_location"), "left") \
        .join(dim_date_df.alias("dd"), to_date(col("raw.job_posted_at_utc")) == col("dd.full_date"), "left") \
        .select(
            "raw.job_natural_key", "djd.job_sk", "dc.company_sk", "dp.publisher_sk",
            "det.employment_type_sk", "dl.location_sk", "dd.date_sk"
        ) \
        .withColumn("job_posting_pk", row_number().over(Window.orderBy("job_natural_key"))) \
        .cache()

    fact_job_postings_df = fact_data_df.select(
        "job_posting_pk", "job_sk", "company_sk", "location_sk", "date_sk", "employment_type_sk", "publisher_sk"
    ).withColumn("job_count", lit(1))

    bridge_job_skill_df = skills_exploded_df.alias("se") \
        .join(dim_skill_df.alias("ds"), trim(initcap(col("se.skill_name_raw"))) == col("ds.skill_name")) \
        .join(fact_data_df.alias("fact"), ["job_natural_key"]) \
        .select("fact.job_posting_pk", "ds.skill_sk") \
        .distinct()

    write_to_snowflake(dim_company_df.select("company_sk", "employer_name"), "DIM_COMPANY", sf_options_analytics)
    write_to_snowflake(dim_publisher_df.select("publisher_sk", "publisher_name"), "DIM_PUBLISHER", sf_options_analytics)
    write_to_snowflake(dim_employment_type_df.select("employment_type_sk", "employment_type_name"), "DIM_EMPLOYMENT_TYPE", sf_options_analytics)
    write_to_snowflake(dim_location_df.select("location_sk", "job_location", "job_city", "job_state", "job_country"), "DIM_LOCATION", sf_options_analytics)
    write_to_snowflake(dim_date_df.select("date_sk", "full_date", "day_of_week", "month_name", "month_number", "quarter_number", "year_number"), "DIM_DATE", sf_options_analytics)
    write_to_snowflake(dim_job_details_df.select("job_sk", "job_title", "job_description", "job_highlights_array", "job_is_remote", "job_posted_at", "job_posted_at_utc"), "DIM_JOB_DETAILS", sf_options_analytics)
    write_to_snowflake(dim_skill_df.select("skill_sk", "skill_name"), "DIM_SKILL", sf_options_analytics)
    write_to_snowflake(fact_job_postings_df, "FACT_JOB_POSTINGS", sf_options_analytics)
    write_to_snowflake(bridge_job_skill_df, "BRIDGE_JOB_SKILL", sf_options_analytics)

    spark.stop()
    print("Spark ETL job finished successfully. All data loaded to Snowflake.")

if __name__ == "__main__":
    main()