import boto3
import pandas as pd
from io import StringIO
import os
import logging
from dotenv import load_dotenv

# === Setup Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# === Load AWS credentials ===
load_dotenv()

# Create S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
)

def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a CSV file from S3 and return as a pandas DataFrame."""
    try:
        logger.info(f"Reading from s3://{bucket}/{key}")
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(content))
        logger.info(f"Loaded {len(df)} rows from {key}")
        return df
    except Exception as e:
        logger.exception(f"Failed to read s3://{bucket}/{key}")
        raise

def write_csv_to_s3(df: pd.DataFrame, bucket: str, key: str, save_local: bool = True):
    """Write a pandas DataFrame to S3, optionally saving a local copy."""
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        logger.info(f"Written to s3://{bucket}/{key}")

        if save_local:
            os.makedirs("output", exist_ok=True)
            local_path = f"output/{key.replace('/', '_')}"
            df.to_csv(local_path, index=False)
            logger.info(f"💾 Also saved locally to {local_path}")
    except Exception as e:
        logger.exception(f"Failed to write DataFrame to s3://{bucket}/{key}")
        raise

def list_s3_files(bucket: str, prefix: str) -> list:
    """List all file keys in an S3 prefix."""
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    files = []
    for page in pages:
        for obj in page.get("Contents", []):
            files.append(obj["Key"])
    return files

def read_manifest(bucket: str, key: str) -> set:
    """Read processed file list from manifest."""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        return set(content.strip().splitlines())
    except s3.exceptions.NoSuchKey:
        return set()

def update_manifest(bucket: str, key: str, processed_files: list):
    """Append newly processed files to the manifest."""
    existing = read_manifest(bucket, key)
    updated = existing.union(set(processed_files))
    body = "\n".join(sorted(updated))
    s3.put_object(Bucket=bucket, Key=key, Body=body)
