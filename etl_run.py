#!/usr/bin/env python
"""
Script to run the ETL process for "Project: Capstone Project"
"""

from configparser import ConfigParser
from pathlib import Path

from etl.aws import (
    ensure_output_bucket_exists,
    empty_output_bucket,
    get_s3,
    get_output_bucket,
    upload_directory,
    validate_aws_config,
    validate_redshift_config,
    validate_runtime_iam_config,
    validate_runtime_redshift_config,
    validate_s3_config
)
from etl.redshift import get_connection
from etl.sources import (
    get_airports_df,
    get_cbp_codes_df,
    get_countries_df,
    get_i94_data_df,
    get_i94cntyl_df,
    get_languages_df,
    get_national_accounts_df,
    get_temperatures_df,
    validate_airports_df,
    validate_cbp_codes_df,
    validate_countries_df,
    validate_i94_data_df,
    validate_i94cntyl_df,
    validate_languages_df,
    validate_national_accounts_df,
    validate_temperatures_df
)
from etl.spark import get_spark
from etl.tables import (
    generate_dim_airport_parquet,
    generate_dim_country_parquet,
    generate_dim_date_parquet,
    generate_fact_ingress_parquet,
    load_dim_tables,
    load_fact_tables,
    truncate_dim_tables,
    validate_dim_tables,
    validate_fact_tables
)


PARQUET_FILES_DIR = "parquet_files"


def cli():
    config = ConfigParser()
    with open("etl.cfg", "r") as f:
        config.read_file(f)

    print("Validating configuration...")
        
    try:
        validate_aws_config(config)
        validate_s3_config(config)
        validate_redshift_config(config)
        validate_runtime_iam_config(config)
        validate_runtime_redshift_config(config)
    except ValueError as e:
        raise SystemExit(f"Invalid AWS configuration: {e}")

    print("Checking runtime environment...")

    s3 = get_s3(config)
    ensure_output_bucket_exists(config, s3)

    print("Running ETL process environment...")

    spark = get_spark()

    print("Generating staging PySpark dataframes...")

    airports_df = get_airports_df(spark)
    cbp_codes_df = get_cbp_codes_df(spark)
    countries_df = get_countries_df(spark)
    i94_data_df = get_i94_data_df(spark)
    i94cntyl_df = get_i94cntyl_df(spark)
    languages_df = get_languages_df(spark)
    national_accounts_df = get_national_accounts_df(spark)
    temperatures_df = get_temperatures_df(spark)

    print("Validating staging PySpark dataframes...")

    validate_airports_df(airports_df)
    validate_cbp_codes_df(cbp_codes_df)
    validate_countries_df(countries_df)
    validate_i94_data_df(i94_data_df)
    validate_i94cntyl_df(i94cntyl_df)
    validate_languages_df(languages_df)
    validate_national_accounts_df(national_accounts_df)
    validate_temperatures_df(temperatures_df)

    print("Generating dimension and fact tables parquet files...")

    generate_dim_airport_parquet(airports_df, output_dir=PARQUET_FILES_DIR)
    generate_dim_country_parquet(countries_df, languages_df, national_accounts_df, output_dir=PARQUET_FILES_DIR)
    generate_dim_date_parquet(spark)
    generate_fact_ingress_parquet(
        i94_data_df,
        airports_df,
        cbp_codes_df,
        countries_df,
        i94cntyl_df,
        temperatures_df,
        output_dir=PARQUET_FILES_DIR
    )

    bucket = get_output_bucket(config, s3)

    print("Emptying output bucket...")

    empty_output_bucket(bucket)

    print("Uploading dimension and fact tables to S3...")

    upload_directory(bucket, Path(PARQUET_FILES_DIR))

    connection = get_connection(config)

    print("Truncating dimension tables...")

    truncate_dim_tables(connection)

    print("Loading dimension tables...")
    
    load_dim_tables(config, connection)
    
    print("Loading fact tables...")

    load_fact_tables(config, connection)

    print("Validating dimansion and fact tables...")

    validate_dim_tables(connection)
    validate_fact_tables(connection)

    print("Done.")


if __name__ == '__main__':
    cli()
