
# F1 Race Outcome Prediction: Cloud Data Pipeline

<div align="center">
  
  ![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=for-the-badge&logo=python&logoColor=white)
  ![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
  ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
  ![Parquet](https://img.shields.io/badge/Parquet-000000?style=for-the-badge&logo=apache&logoColor=white)

</div>

This repository contains an end-to-end AWS cloud data pipeline designed for the CM2606 Data Engineering Coursework. The system ingests the Formula 1 World Championship dataset spanning 1950 to 2020 from Kaggle. It transforms the raw data using distributed processing via AWS Glue and PySpark. The final output is a machine-learning-ready feature table stored in Amazon S3, specifically engineered for a podium finish prediction model.

## Technical Highlights

* **Cloud Ingestion:** Automates the upload of 14 raw Kaggle CSV files into an Amazon S3 raw data lake using Boto3, generating a manifest JSON for data lineage and auditing.
* **Distributed ETL Processing:** Utilizes AWS Glue and PySpark to perform heavy data transformations, including duplication handling, missing value imputation, and data type conversions.
* **Advanced Feature Engineering:** Calculates complex predictive metrics such as rolling averages for driver points, total pit stops, and career win rates to prepare the target `is_podium` feature.
* **Automated Orchestration:** Ties all ingestion, transformation, and validation stages together into a single execution flow using AWS Step Functions.

## System Architecture

The pipeline is orchestrated entirely within the AWS ecosystem.

1. **Data Lake (Raw):** Local CSV files are ingested via a Python script into the `f1-pipeline-raw` S3 bucket.
2. **Transformation Layer:** An AWS Glue PySpark job reads the raw data and applies standardizations, such as converting lap time strings to milliseconds and handling sentinel null values.
3. **Data Sink (Processed):** The transformed data is written to the `f1-pipeline-processed` S3 bucket. The machine learning features are saved in Parquet format and partitioned by year for optimized downstream querying.

## Tech Stack

**Data Engineering & Cloud**
* **Cloud Provider:** AWS (S3, Glue, Step Functions, IAM, CloudWatch)
* **Data Processing:** Apache Spark (PySpark)
* **Storage Format:** Parquet
* **Scripting & API:** Python 3.13, Boto3

## Local Setup & Execution

**1. Prerequisites**
Install Python 3.13 and configure the AWS CLI with your credentials and the `eu-west-1` region. Download the 14 Formula 1 CSV files from Kaggle and place them in the `data/raw/` directory.

**2. Provision Infrastructure**
Execute the automated setup script to provision the required S3 buckets, IAM roles, Glue jobs, and Step Functions state machine:
```bash
python3 config/setup_aws_resources.py
```

**3. Run the Pipeline**
Upload the raw data to S3 using the ingestion script:
```bash
python3 ingestion/ingest_to_s3.py
```
Trigger the fully orchestrated pipeline via the AWS Console or the AWS CLI:
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:eu-west-1:YOUR_ACCOUNT_ID:stateMachine:f1-pipeline-orchestrator \
  --name "run-$(date +%Y%m%d-%H%M%S)"
```

**4. Verify Output**
Confirm the generation of the ML feature tables in Parquet format:
```bash
aws s3 ls s3://f1-pipeline-processed/f1/processed/ --recursive
```
