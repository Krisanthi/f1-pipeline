"""
CM2606 - Data Engineering Coursework
F1 Race Outcome Prediction Pipeline
Validation Lambda: Verifies processed Parquet outputs exist in S3 after ETL.

Triggered by AWS Step Functions as the final pipeline stage.
Author: Krisanthi Segar | RGU ID: 2425596
"""

import boto3
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


def check_s3_prefix_has_files(bucket: str, prefix: str) -> dict:
    """Check if an S3 prefix contains any Parquet files."""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]
    return {
        "prefix":    prefix,
        "has_files": len(files) > 0,
        "file_count": len(files),
        "sample":    files[:2]
    }


def lambda_handler(event, context):
    """
    Validates that all expected output tables exist in S3 processed zone.
    Called by Step Functions after the Glue ETL job completes.
    """
    processed_bucket = event.get("processed_bucket", "f1-pipeline-processed")
    processed_prefix = event.get("processed_prefix", "f1/processed/")
    expected_tables  = event.get("expected_tables",  ["ml_features"])

    logger.info(f"Validating output in s3://{processed_bucket}/{processed_prefix}")

    results = {}
    all_passed = True

    for table in expected_tables:
        prefix = f"{processed_prefix}{table}/"
        check   = check_s3_prefix_has_files(processed_bucket, prefix)
        results[table] = check
        if not check["has_files"]:
            all_passed = False
            logger.error(f"  [FAIL] {table}: no Parquet files found at {prefix}")
        else:
            logger.info(f"  [PASS] {table}: {check['file_count']} file(s) found")

    if not all_passed:
        raise ValueError(
            f"Validation failed. Missing outputs: "
            f"{[t for t, r in results.items() if not r['has_files']]}"
        )

    logger.info("All output tables validated successfully.")
    return {
        "status":  "VALIDATION_PASSED",
        "results": results,
        "message": f"All {len(expected_tables)} output tables present in S3."
    }
