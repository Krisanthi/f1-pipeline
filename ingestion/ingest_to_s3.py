#!/usr/bin/env python3
"""
CM2606 - Data Engineering Coursework
F1 Race Outcome Prediction Pipeline
Task B - Ingestion Flow: Local CSV -> AWS S3 Data Lake (Raw Zone)

HOW TO RUN (macOS / Python 3.13):
  pip3 install boto3
  python3 ingestion/ingest_to_s3.py

Author: Krisanthi Segar
RGU ID: 2425596
"""

import sys
if sys.version_info < (3, 8):
    print(f"ERROR: Python 3.8+ required. You have {sys.version}")
    print("On macOS use: python3 ingestion/ingest_to_s3.py")
    sys.exit(1)

import boto3
import os
import logging
import json
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ingestion.log")
    ]
)
logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
CONFIG = {
    "raw_bucket":       "f1-pipeline-raw",          # S3 Data Lake (raw zone)
    "processed_bucket": "f1-pipeline-processed",    # S3 sink (ML-ready features)
    "region":           "eu-west-1",
    "local_data_dir":   "./data/raw",               # Local Kaggle CSV directory
    "s3_prefix":        "f1/raw/",                  # S3 key prefix
    "required_files": [                             # All 14 Kaggle F1 CSV files
        "races.csv",
        "results.csv",
        "drivers.csv",
        "constructors.csv",
        "pit_stops.csv",
        "lap_times.csv",
        "qualifying.csv",
        "driver_standings.csv",
        "constructor_standings.csv",
        "constructor_results.csv",
        "circuits.csv",
        "seasons.csv",
        "status.csv",
        "sprint_results.csv"
    ]
}


def get_s3_client(region: str):
    """Initialise boto3 S3 client. Credentials from AWS CLI / IAM role."""
    try:
        client = boto3.client("s3", region_name=region)
        # Validate credentials
        client.list_buckets()
        logger.info("AWS S3 client initialised successfully.")
        return client
    except NoCredentialsError:
        logger.error("AWS credentials not found. Run 'aws configure' or set env vars.")
        raise
    except ClientError as e:
        logger.error(f"AWS client error: {e}")
        raise


def ensure_buckets_exist(s3_client, buckets: list, region: str):
    """Create S3 buckets if they do not already exist."""
    for bucket in buckets:
        try:
            s3_client.head_bucket(Bucket=bucket)
            logger.info(f"Bucket already exists: s3://{bucket}")
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                logger.info(f"Creating bucket: {bucket}")
                if region == "us-east-1":
                    s3_client.create_bucket(Bucket=bucket)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket,
                        CreateBucketConfiguration={"LocationConstraint": region}
                    )
                # Enable versioning for data lineage
                s3_client.put_bucket_versioning(
                    Bucket=bucket,
                    VersioningConfiguration={"Status": "Enabled"}
                )
                logger.info(f"Bucket created with versioning: s3://{bucket}")
            else:
                raise


def validate_local_files(data_dir: str, required_files: list) -> dict:
    """
    Validate that all required CSV files exist locally.
    Returns dict of {filename: filepath} for files that exist.
    Raises FileNotFoundError if any required file is missing.
    """
    found = {}
    missing = []

    for filename in required_files:
        filepath = os.path.join(data_dir, filename)
        if os.path.isfile(filepath):
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            found[filename] = {"path": filepath, "size_mb": round(size_mb, 3)}
            logger.info(f"  [OK] {filename} ({size_mb:.3f} MB)")
        else:
            missing.append(filename)
            logger.warning(f"  [MISSING] {filename}")

    if missing:
        raise FileNotFoundError(
            f"Missing required data files: {missing}\n"
            f"Please download the F1 dataset from:\n"
            f"https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020\n"
            f"and place CSVs in: {data_dir}"
        )

    return found


def upload_file_to_s3(s3_client, local_path: str, bucket: str, s3_key: str) -> bool:
    """
    Upload a single file to S3 with metadata tagging.
    Returns True on success, False on failure.
    """
    try:
        file_size = os.path.getsize(local_path)
        logger.info(f"Uploading: {os.path.basename(local_path)} -> s3://{bucket}/{s3_key}")

        s3_client.upload_file(
            Filename=local_path,
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs={
                "Metadata": {
                    "source":           "kaggle-f1-dataset",
                    "ingestion_date":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "pipeline":         "f1-race-outcome-predictor",
                    "file_size_bytes":  str(file_size)
                },
                "ContentType": "text/csv"
            }
        )
        logger.info(f"  Upload successful ({file_size / 1024:.1f} KB)")
        return True

    except ClientError as e:
        logger.error(f"  Upload failed for {local_path}: {e}")
        return False


def write_ingestion_manifest(s3_client, bucket: str, uploaded_files: dict, prefix: str):
    """
    Write a JSON manifest to S3 documenting what was ingested and when.
    Used for data lineage and pipeline auditing.
    """
    manifest = {
        "pipeline":        "f1-race-outcome-predictor",
        "ingestion_run":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":          "kaggle/rohanrao/formula-1-world-championship-1950-2020",
        "destination":     f"s3://{bucket}/{prefix}",
        "files_ingested":  uploaded_files,
        "total_files":     len(uploaded_files),
        "status":          "SUCCESS"
    }
    manifest_key = f"{prefix}manifest_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json"
    )
    logger.info(f"Ingestion manifest written: s3://{bucket}/{manifest_key}")


def run_ingestion():
    """
    Main ingestion function.
    Uploads all required F1 CSV files from local storage to S3 raw data lake.
    """
    logger.info("=" * 60)
    logger.info("F1 Pipeline - Ingestion Flow Starting")
    logger.info(f"Timestamp: {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)

    # 1. Initialise S3 client
    s3 = get_s3_client(CONFIG["region"])

    # 2. Ensure both S3 buckets exist
    ensure_buckets_exist(
        s3,
        [CONFIG["raw_bucket"], CONFIG["processed_bucket"]],
        CONFIG["region"]
    )

    # 3. Validate local CSV files
    logger.info(f"\nValidating local data files in: {CONFIG['local_data_dir']}")
    file_info = validate_local_files(CONFIG["local_data_dir"], CONFIG["required_files"])
    logger.info(f"All {len(file_info)} required files found.\n")

    # 4. Upload each file to S3 raw zone
    upload_results = {}
    success_count = 0

    for filename, info in file_info.items():
        s3_key = CONFIG["s3_prefix"] + filename
        success = upload_file_to_s3(s3, info["path"], CONFIG["raw_bucket"], s3_key)
        upload_results[filename] = {
            "s3_key":        s3_key,
            "size_mb":       info["size_mb"],
            "upload_status": "SUCCESS" if success else "FAILED"
        }
        if success:
            success_count += 1

    # 5. Write manifest
    write_ingestion_manifest(s3, CONFIG["raw_bucket"], upload_results, CONFIG["s3_prefix"])

    # 6. Summary
    logger.info("\n" + "=" * 60)
    logger.info(f"Ingestion Complete: {success_count}/{len(file_info)} files uploaded")
    logger.info(f"Raw Data Lake: s3://{CONFIG['raw_bucket']}/{CONFIG['s3_prefix']}")
    logger.info("=" * 60)

    if success_count < len(file_info):
        raise RuntimeError(f"Ingestion partially failed. Check logs.")

    return {"status": "SUCCESS", "files_uploaded": success_count}


if __name__ == "__main__":
    run_ingestion()
