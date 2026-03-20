"""
CM2606 - Data Engineering Coursework
F1 Race Outcome Prediction Pipeline
Ingestion Lambda: Wraps the S3 upload logic for Step Functions invocation.

NOTE: For local testing, run ingestion/ingest_to_s3.py directly.
This Lambda is deployed to AWS and called by Step Functions.
Author: Krisanthi Segar | RGU ID: 2425596
"""

import boto3
import os
import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

# In production, data is already in /tmp or pre-staged.
# For coursework, we assume files are pre-uploaded to S3 raw zone
# by running ingest_to_s3.py locally first, then Step Functions
# triggers this Lambda to confirm and log the ingestion.

def lambda_handler(event, context):
    """
    Confirms raw F1 CSV files are present in the S3 raw data lake.
    Returns file inventory for downstream Glue job.
    """
    raw_bucket = event.get("raw_bucket", "f1-pipeline-raw")
    raw_prefix = event.get("raw_prefix", "f1/raw/")

    EXPECTED_FILES = [
        "races.csv", "results.csv", "drivers.csv", "constructors.csv",
        "pit_stops.csv", "lap_times.csv", "qualifying.csv",
        "driver_standings.csv", "constructor_standings.csv",
        "constructor_results.csv", "circuits.csv",
        "seasons.csv", "status.csv", "sprint_results.csv"
    ]

    logger.info(f"Checking raw files in s3://{raw_bucket}/{raw_prefix}")

    found = []
    missing = []

    for filename in EXPECTED_FILES:
        key = raw_prefix + filename
        try:
            head = s3.head_object(Bucket=raw_bucket, Key=key)
            size_kb = head["ContentLength"] / 1024
            found.append({"file": filename, "size_kb": round(size_kb, 1)})
            logger.info(f"  [OK] {filename} ({size_kb:.1f} KB)")
        except s3.exceptions.ClientError:
            missing.append(filename)
            logger.error(f"  [MISSING] {filename}")

    if missing:
        raise FileNotFoundError(
            f"Raw data missing from S3: {missing}. "
            f"Run ingestion/ingest_to_s3.py locally first."
        )

    return {
        "status":      "INGESTION_CONFIRMED",
        "raw_bucket":  raw_bucket,
        "raw_prefix":  raw_prefix,
        "files_found": len(found),
        "inventory":   found,
        "timestamp":   datetime.utcnow().isoformat()
    }
