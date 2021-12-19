
#!/usr/bin/env python
"""
Script to teardown the runtime environment for "Project: Capstone Project"
"""

from configparser import ConfigParser

from etl.aws import destroy_output_bucket, get_s3, validate_aws_config, validate_s3_config


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

    print("Tearing down runtime environment...")    

    s3 = get_s3(config)
    destroy_output_bucket(config, s3)
    
    print("Done.")


if __name__ == '__main__':
    cli()