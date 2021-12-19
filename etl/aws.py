"""
AWS related functions for "Project: Capstone Project"
"""

from json import dumps
from time import sleep

import boto3

from botocore.exceptions import ClientError


def validate_aws_config(config):
    """Validate AWS config file contents"""
    if not config.has_section("AWS"):
        raise ValueError("Missing AWS section on config file.")
        
    if not config.has_option("AWS", "AWS_ACCESS_KEY_ID") or not config.get("AWS", "AWS_ACCESS_KEY_ID"):
        raise ValueError("Missing or invalid AWS_ACCESS_KEY_ID option of AWS section of config file.")
        
    if not config.has_option("AWS", "AWS_SECRET_ACCESS_KEY") or not config.get("AWS", "AWS_SECRET_ACCESS_KEY"):
        raise ValueError("Missing AWS_SECRET_ACCESS_KEY option of AWS section of config file.")
        
    if not config.has_option("AWS", "REGION") or not config.get("AWS", "REGION"):
        raise ValueError("Missing REGION option of AWS section of config file.")


def validate_s3_config(config):
    """Validate S3 related AWS config file contents"""
    if not config.has_section("S3"):
        raise ValueError("Missing AWS section on config file.")

    if not config.has_option("S3", "OUTPUT_BUCKET") or not config.get("S3", "OUTPUT_BUCKET"):
        raise ValueError("Missing or invalid OUTPUT_BUCKET option of S3 section of config file.")


def get_s3(config):
    """Create a boto3 S3 resource"""
    return boto3.resource(
        "s3",
        region_name=config.get("AWS", "REGION"),
        aws_access_key_id=config.get("AWS", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config.get("AWS", "AWS_SECRET_ACCESS_KEY")
    )


def ensure_output_bucket_exists(config, s3):
    """Checks if the output bucket exists and creates it otherwise"""
    output_bucket_name = config.get("S3", "OUTPUT_BUCKET")
    bucket = s3.Bucket(output_bucket_name)

    if bucket.creation_date is None:
        region = config.get("AWS", "REGION")

        try:
            print(f"Creating output bucket '{output_bucket_name}' in region '{region}'")
            create_response = bucket.create(
                ACL='public-read',
                CreateBucketConfiguration={
                    'LocationConstraint': region
                }
            )
        except ClientError as client_error:
            print(f"Exception raised while creating output S3 bucket: {client_error}")
            raise SystemExit()


def destroy_output_bucket(config, s3):
    output_bucket_name = config.get("S3", "OUTPUT_BUCKET")
    bucket = s3.Bucket(output_bucket_name)

    try:
        print(f"Deleting output bucket '{output_bucket_name}'")
        bucket.delete()
    except ClientError as client_error:
        if client_error.response['Error']['Code'] != 'NoSuchBucket':
            # Do nothing if the bucket does not exist, exit otherwise
            print(f"Exception raised while deleting output S3 bucket: {client_error}")
            raise SystemExit()


