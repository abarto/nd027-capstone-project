#!/usr/bin/env python
"""
Script to validate and prepare the runtime environment for "Project: Capstone Project"
"""
from pathlib import Path
from configparser import ConfigParser

from etl.aws import (
    ensure_output_bucket_exists,
    get_ec2,
    get_iam,
    get_redshift,
    get_s3,
    update_role,
    update_cluster,
    validate_aws_config,
    validate_redshift_config,
    validate_s3_config
)
from etl.redshift import get_connection
from etl.tables import create_dim_fact_tables, truncate_dim_tables


def cli():
    config = ConfigParser()
    with open("etl.cfg", "r") as f:
        config.read_file(f)

    print("Validating configuration...")
        
    try:
        validate_aws_config(config)
        validate_s3_config(config)
        validate_redshift_config(config)
    except ValueError as e:
        raise SystemExit(f"Invalid AWS configuration: {e}")

    print("Preparing runtime environment...")

    ec2 = get_ec2(config)
    iam = get_iam(config)
    redshift = get_redshift(config)
    s3 = get_s3(config)

    ensure_output_bucket_exists(config, s3)
    update_role(config, iam)
    update_cluster(config, redshift, ec2)

    print("Creating dimenension and tables...")

    connection = get_connection(config)
    create_dim_fact_tables(connection)
    truncate_dim_tables(connection)
    
    print("Done.")


if __name__ == '__main__':
    cli()
