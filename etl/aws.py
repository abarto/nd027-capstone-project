"""
AWS related functions for "Project: Capstone Project"
"""

from json import dumps
from pathlib import Path
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


def validate_redshift_config(config):
    """Validate Redshit related AWS config file contents"""
    for option in [
        "CLUSTER_TYPE", "NODE_TYPE", "IDENTIFIER", "DB_NAME", "DB_USER", "DB_PASSWORD"
    ]:
        if not config.has_option("CLUSTER", option) or not config.get("CLUSTER", option):
            raise ValueError(f"Missing or invalid {option} option of CLUSTER section of config file.")


def validate_runtime_iam_config(config):
    """Validates AWS IAN runtime configuration"""
    if not config.has_option("IAM_ROLE", "ARN") or not config.get("IAM_ROLE", "ARN"):
        raise ValueError("Missing or invalid ARN option of IAM_ROLE section of config file. Run etl_prepare.py.")


def validate_runtime_redshift_config(config):
    """Validates Redshift runtime configuration"""
    if not config.has_option("CLUSTER", "HOST") or not config.get("CLUSTER", "HOST"):
        raise ValueError("Missing or invalid HOST option of CLUSTER section of config file. Run etl_prepare.py.")
    
    if not config.has_option("CLUSTER", "DB_PORT") or not config.get("CLUSTER", "DB_PORT"):
        raise ValueError("Missing or invalid PORT option of CLUSTER section of config file. Run etl_prepare.py.")


def get_s3(config):
    """Create a boto3 S3 resource"""
    return boto3.resource(
        "s3",
        region_name=config.get("AWS", "REGION"),
        aws_access_key_id=config.get("AWS", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config.get("AWS", "AWS_SECRET_ACCESS_KEY")
    )


def get_output_bucket(config, s3):
    """Get the output Bucket object"""
    output_bucket_name = config.get("S3", "OUTPUT_BUCKET")
    bucket = s3.Bucket(output_bucket_name)

    return bucket


def ensure_output_bucket_exists(config, s3):
    """Checks if the output bucket exists and creates it otherwise"""
    bucket = get_output_bucket(config, s3)

    if bucket.creation_date is None:
        region = config.get("AWS", "REGION")

        try:
            print(f"Creating output bucket '{output_bucket_name}' in region '{region}'")
            create_response = bucket.create(
                ACL="public-read",
                CreateBucketConfiguration={
                    "LocationConstraint": region
                }
            )
        except ClientError as client_error:
            print(f"Exception raised while creating output S3 bucket: {client_error}")
            raise SystemExit()


def empty_output_bucket(bucket):
    """Empties the output bucket"""
    bucket.objects.all().delete()


def destroy_output_bucket(config, s3):
    """Deletes S3 output bucket"""
    bucket = get_output_bucket(config, s3)

    try:
        print(f"Deleting output bucket '{output_bucket_name}'")
        bucket.delete()
    except ClientError as client_error:
        if client_error.response['Error']['Code'] != 'NoSuchBucket':
            # Do nothing if the bucket does not exist, exit otherwise
            print(f"Exception raised while deleting output S3 bucket: {client_error}")
            raise SystemExit()


def upload_directory(bucket, directory):
    """Uploads a directory to an s3 Bucket"""
    root_path = directory if isinstance(directory, Path) else Path(directory)

    try:
        for file_path in filter(lambda p: p.is_file() and not p.match("**/*.crc"), root_path.glob("**/*")):
            print(f"Uploading '{file_path}' to '{bucket}'")
            with file_path.open("rb") as f:
                bucket.upload_fileobj(f, str(file_path.relative_to(root_path)))
    except ClientError as client_error:
        print(f"Exception raised while directory '{directory}' to S3 bucket '{bucket}': {client_error}")
        raise SystemExit()


def get_iam(config):
    """Create a boto3 IAM client"""
    return boto3.client(
        "iam",
        region_name=config.get("AWS", "REGION"),
        aws_access_key_id=config.get("AWS", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config.get("AWS", "AWS_SECRET_ACCESS_KEY")
    )


def get_redshift(config):
    """Create a boto3 redshift client"""
    return boto3.client(
        "redshift",
        region_name=config.get("AWS", "REGION"),
        aws_access_key_id=config.get("AWS", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config.get("AWS", "AWS_SECRET_ACCESS_KEY")
    )


def get_ec2(config):
    """Create a boto3 EC2 resource"""
    return boto3.resource(
        "ec2",
        region_name=config.get("AWS", "REGION"),
        aws_access_key_id=config.get("AWS", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config.get("AWS", "AWS_SECRET_ACCESS_KEY")
    )


def update_role(config, iam):
    """Set the Role's ARN value on the config (if needed)"""
    if config.has_option("IAM_ROLE", "ARN") and config.get("IAM_ROLE", "ARN"):
        return  # Role ARN already set
    
    iam_role = None
    
    try:
        iam_role = iam.get_role(RoleName=config.get("IAM_ROLE", "ROLE_NAME"))
    except ClientError as client_error:
        if client_error.response['Error']['Code'] != 'NoSuchEntity':
            raise  # Do nothing if the does not exist, raise otherwise
            
    if iam_role is None:
        print(f"Creating IAM role '{config.get('IAM_ROLE', 'ROLE_NAME')}'...")

        iam_role = _create_role(config, iam)
        
    print("Updating IAM configuration.")
        
    config["IAM_ROLE"]["ARN"] = iam_role['Role']['Arn']
    
    with open("etl.cfg", "w") as f:
        config.write(f)


def _create_role(config, iam):
    """Creates an IAM role to allow redshift to access S3"""
    try:
        iam.create_role(
            Path="/",
            RoleName=config.get("IAM_ROLE", "ROLE_NAME"),
            Description="Allows Redshift cluster to call AWS services on your behalf.",
            AssumeRolePolicyDocument=dumps({
                "Statement": [{
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "redshift.amazonaws.com"
                    }
                }],
                "Version": "2012-10-17"
            })
        )
        iam.attach_role_policy(
            RoleName=config.get("IAM_ROLE", "ROLE_NAME"),
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    except ClientError as client_error:
        if client_error.response['Error']['Code'] != 'EntityAlreadyExist':
            raise  # Do nothing if the role already exists, raise otherwise
        
    return iam.get_role(RoleName=config.get("IAM_ROLE", "ROLE_NAME"))


def _create_cluster(config, redshift):
    """Create redshift cluster"""
    if not config.has_option("IAM_ROLE", "ARN"):
        raise ValueError("Missing ARN option on IAM_ROLE config section.")

    create_cluster_args = {
        "ClusterType": config.get("CLUSTER", "CLUSTER_TYPE"),
        "NodeType": config.get("CLUSTER", "NODE_TYPE"),
        "ClusterIdentifier": config.get("CLUSTER", "IDENTIFIER"),
        "DBName": config.get("CLUSTER", "DB_NAME"),
        "MasterUsername": config.get("CLUSTER", "DB_USER"),
        "MasterUserPassword": config.get("CLUSTER", "DB_PASSWORD"),
        "IamRoles": [config["IAM_ROLE"]["ARN"]]
    }

    if config.get("CLUSTER", "CLUSTER_TYPE") == "multi-node":
        create_cluster_args["NumberOfNodes"] = config.getint("CLUSTER", "NUM_NODES"),
    
    try:
        response = redshift.create_cluster(**create_cluster_args)
    except ClientError as client_error:
        if client_error.response['Error']['Code'] != 'ClusterAlreadyExists':
            raise  # Do nothing if the role already exists, raise otherwise
            
    return redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER", "IDENTIFIER"))['Clusters'][0]


def update_cluster(config, redshift, ec2):
    """Set the Redshift cluster related config values (if needed)."""
    
    if config.has_option("CLUSTER", "HOST") and config.get("CLUSTER", "HOST"):
        return  # Cluster HOST already set

    cluster_identifier = config.get('CLUSTER', 'IDENTIFIER')
    cluster = None
    
    try:
        cluster = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    except ClientError as client_error:
        if client_error.response['Error']['Code'] != 'ClusterNotFound':
            raise  # Do nothing if the does not exist, raise otherwise

    if cluster is None:
        print(f"Creating cluster '{cluster_identifier}'...")
        
        cluster = _create_cluster(config, redshift)
        
        print(f"Waiting for cluster '{cluster_identifier}' to become available...")
        
        while cluster['ClusterStatus'] != 'available':
            sleep(5)
            cluster = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
            
        print(cluster)
            
        print(f"Updating security group to allow external access to cluster '{cluster_identifier}'.")
        
        # Allow access from the outside. This is only necessary for testing and demonstration
        # purposes, as the ETL happens within the same VPC.
        try:
            vpc = ec2.Vpc(id=cluster['VpcId'])
            default_security_group = next(iter(vpc.security_groups.all()))
            default_security_group.authorize_ingress(
                GroupName=default_security_group.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(cluster["Endpoint"]["Port"]),
                ToPort=int(cluster["Endpoint"]["Port"])
            )
        except ClientError as client_error:
            if client_error.response['Error']['Code'] != 'InvalidPermission.Duplicate':
                raise  # Do nothing if the permission already exists, raise otherwise
                
    print(f"Updating Redshift configuration.")
        
    config["CLUSTER"]["HOST"] = cluster["Endpoint"]["Address"]
    config["CLUSTER"]["DB_PORT"] = str(cluster["Endpoint"]["Port"])
    
    with open("etl.cfg", "w") as f:
        config.write(f)


def delete_cluster(config, redshift):
    """Tears down cluster and updates configuration to remove the references to it"""
    if not config.has_option("CLUSTER", "HOST") and not config.get("CLUSTER", "HOST"):
        return  # Cluster HOST already removed
    
    cluster_identifier = config.get('CLUSTER', 'IDENTIFIER')
    
    print(f"Deleting cluster '{cluster_identifier}'...")
    
    try:
        response = redshift.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=True
        )
    except ClientError as client_error:
        if client_error.response['Error']['Code'] != 'ClusterNotFound':
            raise  # Do nothing if the cluster doesn't exist, raise otherwise
            
    print(f"Updating Redshift configuration.")

    config["CLUSTER"]["HOST"] = ""
    
    with open("etl.cfg", "w") as f:
        config.write(f)
