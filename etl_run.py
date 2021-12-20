#!/usr/bin/env python
"""
Script to run the ETL process for "Project: Capstone Project"
"""

from configparser import ConfigParser

from etl.aws import ensure_output_bucket_exists, get_s3, validate_aws_config, validate_s3_config
from etl.spark import get_spark
from etl.sources import (
    get_airports_df,
    get_cbp_codes_df,
    get_countries_df,
    get_i94_data_df,
    get_i94cntyl_df,
    get_languages_df,
    get_national_accounts_df,
    get_temperatures_df
)
from etl.tables import generate_dim_airport_parquet, generate_dim_country_parquet, generate_fact_ingress_parquet


def cli():
    config = ConfigParser()
    with open("etl.cfg", "r") as f:
        config.read_file(f)

    print("Validating configuration...")
        
    try:
        validate_aws_config(config)
        validate_s3_config(config)
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

    print("Generating dimension and fact tables parquet files...")

    generate_dim_airport_parquet(airports_df, output_dir="parquet_files")
    generate_dim_country_parquet(countries_df, languages_df, national_accounts_df, output_dir="parquet_files")
    generate_fact_ingress_parquet(
        i94_data_df,
        airports_df,
        cbp_codes_df,
        countries_df,
        i94cntyl_df,
        temperatures_df,
        output_dir="parquet_files"
    )

    print("Done.")


if __name__ == '__main__':
    cli()
