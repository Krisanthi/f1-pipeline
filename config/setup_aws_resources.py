#!/usr/bin/env python3
# ============================================================
# CM2606 Data Engineering Coursework
# F1 Race Outcome Prediction Pipeline
# AWS Resource Setup Script
#
# HOW TO RUN (macOS / Python 3.13):
#   python3 config/setup_aws_resources.py
#
# NOTE: On macOS, always use "python3" not "python".
#       Run this script ONCE to provision all AWS resources.
#       Requires: AWS CLI configured (run: aws configure)
#
# Author: Krisanthi Segar | RGU ID: 2425596
# ============================================================

import sys
import boto3
import json
import time
import logging

# ── Python version guard ───────────────────────────────────
if sys.version_info < (3, 8):
    print(f"ERROR: Python 3.8+ required. You have Python {sys.version}")
    print("On macOS, run: python3 config/setup_aws_resources.py")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
# CONFIG — Edit ACCOUNT_ID before running
# ══════════════════════════════════════════════════════════
REGION                = "eu-west-1"
ACCOUNT_ID            = "387659571495"   # <-- Replace with your 12-digit AWS Account ID
RAW_BUCKET            = "f1-pipeline-raw"
PROCESSED_BUCKET      = "f1-pipeline-processed"
GLUE_JOB_NAME         = "f1-etl-transform"
GLUE_ROLE_NAME        = "f1-glue-role"
SF_ROLE_NAME          = "f1-stepfunctions-role"
SF_STATE_MACHINE_NAME = "f1-pipeline-orchestrator"
GLUE_SCRIPT_S3_KEY    = "scripts/f1_etl_transform.py"

# Validate account ID was updated
if ACCOUNT_ID == "YOUR_AWS_ACCOUNT_ID":
    print("\nERROR: You must set your AWS Account ID in this file first.")
    print("  Open config/setup_aws_resources.py")
    print("  Change:  ACCOUNT_ID = \"YOUR_AWS_ACCOUNT_ID\"")
    print("  To:      ACCOUNT_ID = \"123456789012\"  (your 12-digit AWS account ID)")
    print("  Find it: AWS Console -> top right corner -> your account name -> Account ID\n")
    sys.exit(1)

s3   = boto3.client("s3",            region_name=REGION)
iam  = boto3.client("iam",           region_name=REGION)
glue = boto3.client("glue",          region_name=REGION)
sf   = boto3.client("stepfunctions", region_name=REGION)


# ── Step 1: Create S3 Buckets ──────────────────────────────
def create_s3_buckets():
    logger.info("Creating S3 buckets...")
    for bucket in [RAW_BUCKET, PROCESSED_BUCKET]:
        try:
            s3.head_bucket(Bucket=bucket)
            logger.info(f"  Bucket already exists: s3://{bucket}")
        except Exception:
            try:
                if REGION == "us-east-1":
                    s3.create_bucket(Bucket=bucket)
                else:
                    s3.create_bucket(
                        Bucket=bucket,
                        CreateBucketConfiguration={"LocationConstraint": REGION}
                    )
                # Enable versioning for data lineage
                s3.put_bucket_versioning(
                    Bucket=bucket,
                    VersioningConfiguration={"Status": "Enabled"}
                )
                logger.info(f"  Created: s3://{bucket} (versioning enabled)")
            except Exception as e:
                logger.error(f"  Failed to create {bucket}: {e}")
                raise


# ── Step 2: Upload Glue Script to S3 ──────────────────────
def upload_glue_script():
    logger.info("Uploading Glue ETL script to S3...")
    try:
        s3.upload_file(
            "glue_jobs/f1_etl_transform.py",
            RAW_BUCKET,
            GLUE_SCRIPT_S3_KEY
        )
        logger.info(f"  Script uploaded: s3://{RAW_BUCKET}/{GLUE_SCRIPT_S3_KEY}")
    except FileNotFoundError:
        logger.error("  ERROR: glue_jobs/f1_etl_transform.py not found.")
        logger.error("  Make sure you are running this script from the f1-pipeline/ directory:")
        logger.error("    cd f1-pipeline")
        logger.error("    python3 config/setup_aws_resources.py")
        raise


# ── Step 3: Create IAM Role for Glue ──────────────────────
def create_glue_iam_role():
    logger.info("Creating IAM role for AWS Glue...")
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }
    try:
        resp = iam.create_role(
            RoleName=GLUE_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="IAM role for F1 Glue ETL job"
        )
        role_arn = resp["Role"]["Arn"]
        iam.attach_role_policy(
            RoleName=GLUE_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        )
        iam.attach_role_policy(
            RoleName=GLUE_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )
        logger.info(f"  Glue IAM role created: {role_arn}")
        logger.info("  Waiting 15s for IAM propagation...")
        time.sleep(15)
        return role_arn
    except iam.exceptions.EntityAlreadyExistsException:
        role_arn = iam.get_role(RoleName=GLUE_ROLE_NAME)["Role"]["Arn"]
        logger.info(f"  Glue IAM role already exists: {role_arn}")
        return role_arn


# ── Step 4: Create AWS Glue Job ────────────────────────────
def create_glue_job(role_arn: str):
    logger.info("Creating AWS Glue Job...")
    try:
        glue.create_job(
            Name=GLUE_JOB_NAME,
            Role=role_arn,
            Command={
                "Name":           "glueetl",
                "ScriptLocation": f"s3://{RAW_BUCKET}/{GLUE_SCRIPT_S3_KEY}",
                "PythonVersion":  "3"
            },
            DefaultArguments={
                "--raw_bucket":                          RAW_BUCKET,
                "--processed_bucket":                    PROCESSED_BUCKET,
                "--raw_prefix":                          "f1/raw/",
                "--processed_prefix":                    "f1/processed/",
                "--enable-continuous-cloudwatch-log":    "true",
                "--enable-metrics":                      "true",
                "--job-language":                        "python"
            },
            GlueVersion="4.0",
            NumberOfWorkers=5,
            WorkerType="G.1X",
            MaxRetries=1,
            Timeout=60,
            Description="F1 ETL: S3 raw CSVs -> cleansed Parquet ML feature table (CM2606 CW)"
        )
        logger.info(f"  Glue job created: {GLUE_JOB_NAME}")
    except glue.exceptions.AlreadyExistsException:
        logger.info(f"  Glue job already exists: {GLUE_JOB_NAME}")


# ── Step 5: Create IAM Role for Step Functions ─────────────
def create_stepfunctions_role():
    logger.info("Creating IAM role for Step Functions...")
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "states.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun"
                ],
                "Resource": f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:job/{GLUE_JOB_NAME}"
            },
            {
                "Effect": "Allow",
                "Action": ["lambda:InvokeFunction"],
                "Resource": [
                    f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:f1-pipeline-ingest",
                    f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:f1-pipeline-validate"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogDelivery",
                    "logs:PutLogEvents"
                ],
                "Resource": "*"
            }
        ]
    }
    try:
        resp = iam.create_role(
            RoleName=SF_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust)
        )
        role_arn = resp["Role"]["Arn"]
        iam.put_role_policy(
            RoleName=SF_ROLE_NAME,
            PolicyName="f1-sf-inline-policy",
            PolicyDocument=json.dumps(policy)
        )
        logger.info(f"  Step Functions IAM role created: {role_arn}")
        time.sleep(10)
        return role_arn
    except iam.exceptions.EntityAlreadyExistsException:
        role_arn = iam.get_role(RoleName=SF_ROLE_NAME)["Role"]["Arn"]
        logger.info(f"  Step Functions IAM role already exists: {role_arn}")
        return role_arn


# ── Step 6: Create Step Functions State Machine ────────────
def create_state_machine(sf_role_arn: str):
    logger.info("Creating Step Functions state machine...")
    import os
    # Find the definition file relative to this script's location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    definition_path = os.path.join(script_dir, "..", "orchestration", "step_functions_definition.json")
    with open(definition_path, "r") as f:
        definition = f.read()
    try:
        resp = sf.create_state_machine(
            name=SF_STATE_MACHINE_NAME,
            definition=definition,
            roleArn=sf_role_arn,
            type="STANDARD",
            loggingConfiguration={
                "level": "ERROR",
                "includeExecutionData": True,
                "destinations": [
                    {
                        "cloudWatchLogsLogGroup": {
                            # This must match the log group you created in the console
                            "logGroupArn": f"arn:aws:logs:eu-west-1:387659571495:log-group:/aws/vendedlogs/states/f1-pipeline-logs:*"
                        }
                    }
                ]
            }
        )
        logger.info(f"  State machine created: {resp['stateMachineArn']}")
        return resp["stateMachineArn"]
    except sf.exceptions.StateMachineAlreadyExists:
        logger.info(f"  State machine already exists: {SF_STATE_MACHINE_NAME}")


# ── Main ────────────────────────────────────────────────────
def setup_all():
    logger.info("=" * 60)
    logger.info("F1 Pipeline - AWS Resource Setup")
    logger.info(f"Region: {REGION} | Account: {ACCOUNT_ID}")
    logger.info("=" * 60)

    create_s3_buckets()
    upload_glue_script()
    glue_role_arn = create_glue_iam_role()
    create_glue_job(glue_role_arn)
    sf_role_arn = create_stepfunctions_role()
    create_state_machine(sf_role_arn)

    logger.info("\n" + "=" * 60)
    logger.info("All AWS resources provisioned successfully.")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Run:  python3 ingestion/ingest_to_s3.py")
    logger.info("  2. Open: AWS Console > Step Functions > f1-pipeline-orchestrator")
    logger.info("  3. Click: Start Execution > Start")
    logger.info("=" * 60)


if __name__ == "__main__":
    setup_all()
