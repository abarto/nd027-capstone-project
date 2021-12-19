#!/usr/bin/env python
"""
Script to run the ETL process for "Project: Capstone Project"
"""

from configparser import ConfigParser

from etl.spark import get_spark
from etl.aws import ensure_output_bucket_exists, get_s3, validate_aws_config, validate_s3_config


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


    print("Done.")


if __name__ == '__main__':
    cli()
