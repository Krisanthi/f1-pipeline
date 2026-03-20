"""
CM2606 - Data Engineering Coursework
F1 Race Outcome Prediction Pipeline
Task B - ETL Flow (AWS Glue / PySpark): S3 Raw -> Transform -> S3 Processed (ML-Ready)

This script runs as an AWS Glue Job (distributed PySpark environment).
It reads raw F1 CSVs from S3, applies transformations and cleansing,
and writes ML-ready feature tables back to S3 as Parquet.

Transformations included (satisfying ≥2 requirement):
  1. Duplication Handling        - Drop duplicate race/driver/result records
  2. Missing Value Handling      - Impute or drop nulls in key feature columns
  3. Data Type Conversions       - Ensure correct types (int, float, date)
  4. Data Aggregation            - Compute per-driver rolling stats per season
  5. Data Standardization        - Normalise lap time formats, circuit names

Author: Krisanthi Segar | RGU ID: 2425596
"""

import sys
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType
from pyspark.sql.window import Window

# ── Glue Job Parameters ────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "raw_bucket",
    "processed_bucket",
    "raw_prefix",
    "processed_prefix"
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

RAW_BASE       = f"s3://{args['raw_bucket']}/{args['raw_prefix']}"
PROCESSED_BASE = f"s3://{args['processed_bucket']}/{args['processed_prefix']}"


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def read_csv(path: str, table_name: str):
    """Read a CSV from S3 into a Spark DataFrame with header and schema inference."""
    logger.info(f"Reading {table_name} from: {path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    logger.info(f"  {table_name}: {df.count()} rows, {len(df.columns)} columns")
    return df


def write_parquet(df, path: str, table_name: str, partition_by: str = None):
    """Write DataFrame to S3 as Parquet (columnar, efficient for ML queries)."""
    logger.info(f"Writing {table_name} -> {path}")
    writer = df.write.mode("overwrite").parquet
    if partition_by:
        df.write.mode("overwrite").partitionBy(partition_by).parquet(path)
    else:
        df.write.mode("overwrite").parquet(path)
    logger.info(f"  {table_name} written successfully.")


def log_quality_check(df, table_name: str, column: str = None):
    """Log a basic data quality summary for auditing."""
    total = df.count()
    if column:
        nulls = df.filter(F.col(column).isNull()).count()
        logger.info(f"  [QC] {table_name}.{column}: {total} rows, {nulls} nulls ({100*nulls/total:.1f}%)")
    else:
        logger.info(f"  [QC] {table_name}: {total} rows")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — READ ALL RAW TABLES FROM S3
# ══════════════════════════════════════════════════════════════════════════════
logger.info("\n" + "="*60)
logger.info("STEP 1: Reading raw data from S3 Data Lake")
logger.info("="*60)

races_raw          = read_csv(f"{RAW_BASE}races.csv",                "races")
results_raw        = read_csv(f"{RAW_BASE}results.csv",              "results")
drivers_raw        = read_csv(f"{RAW_BASE}drivers.csv",              "drivers")
constructors_raw   = read_csv(f"{RAW_BASE}constructors.csv",         "constructors")
pit_stops_raw      = read_csv(f"{RAW_BASE}pit_stops.csv",            "pit_stops")
lap_times_raw      = read_csv(f"{RAW_BASE}lap_times.csv",            "lap_times")
qualifying_raw     = read_csv(f"{RAW_BASE}qualifying.csv",           "qualifying")
driver_std_raw     = read_csv(f"{RAW_BASE}driver_standings.csv",     "driver_standings")
circuits_raw       = read_csv(f"{RAW_BASE}circuits.csv",             "circuits")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — CLEANSING: DUPLICATION HANDLING  (Transformation #1)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("\n" + "="*60)
logger.info("STEP 2: Duplication Handling")
logger.info("="*60)

def drop_duplicates_log(df, subset: list, name: str):
    before = df.count()
    df_clean = df.dropDuplicates(subset)
    after = df_clean.count()
    removed = before - after
    logger.info(f"  {name}: removed {removed} duplicate rows ({before} -> {after})")
    return df_clean

races       = drop_duplicates_log(races_raw,        ["raceId"],           "races")
results     = drop_duplicates_log(results_raw,      ["resultId"],         "results")
drivers     = drop_duplicates_log(drivers_raw,      ["driverId"],         "drivers")
constructors = drop_duplicates_log(constructors_raw, ["constructorId"],   "constructors")
pit_stops   = drop_duplicates_log(pit_stops_raw,    ["raceId","driverId","stop"], "pit_stops")
qualifying  = drop_duplicates_log(qualifying_raw,   ["qualifyId"],        "qualifying")
driver_std  = drop_duplicates_log(driver_std_raw,   ["driverStandingsId"],"driver_standings")
circuits    = drop_duplicates_log(circuits_raw,     ["circuitId"],        "circuits")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — CLEANSING: MISSING VALUE HANDLING  (Transformation #2)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("\n" + "="*60)
logger.info("STEP 3: Missing Value Handling")
logger.info("="*60)

# Replace F1 dataset sentinel value "\\N" with null across all DataFrames
SENTINEL = r"\N"

def replace_sentinel(df):
    """F1 Kaggle dataset uses \\N for missing values - standardise to null."""
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).cast(StringType()) == SENTINEL, None)
             .otherwise(F.col(col_name))
        )
    return df

races       = replace_sentinel(races)
results     = replace_sentinel(results)
drivers     = replace_sentinel(drivers)
constructors = replace_sentinel(constructors)
pit_stops   = replace_sentinel(pit_stops)
qualifying  = replace_sentinel(qualifying)
driver_std  = replace_sentinel(driver_std)
circuits    = replace_sentinel(circuits)
lap_times_raw = replace_sentinel(lap_times_raw)

# Results: rows without a valid finishing position are DNFs - keep them,
# but fill positionOrder nulls with 20 (back of grid) for model stability
results = results.withColumn(
    "positionOrder",
    F.when(F.col("positionOrder").isNull(), F.lit(20))
     .otherwise(F.col("positionOrder"))
)

# Qualifying: missing Q2/Q3 times (drivers eliminated early) -> fill with null is fine,
# but we'll also create a best_quali_time for feature engineering
results = results.withColumn(
    "points",
    F.when(F.col("points").isNull(), F.lit(0.0)).otherwise(F.col("points"))
)

log_quality_check(results, "results", "positionOrder")
log_quality_check(results, "results", "points")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — DATA TYPE CONVERSIONS  (Transformation #3)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("\n" + "="*60)
logger.info("STEP 4: Data Type Conversions")
logger.info("="*60)

results = (results
    .withColumn("raceId",          F.col("raceId").cast(IntegerType()))
    .withColumn("driverId",        F.col("driverId").cast(IntegerType()))
    .withColumn("constructorId",   F.col("constructorId").cast(IntegerType()))
    .withColumn("grid",            F.col("grid").cast(IntegerType()))
    .withColumn("positionOrder",   F.col("positionOrder").cast(IntegerType()))
    .withColumn("points",          F.col("points").cast(FloatType()))
    .withColumn("laps",            F.col("laps").cast(IntegerType()))
    .withColumn("fastestLap",      F.col("fastestLap").cast(IntegerType()))
    .withColumn("fastestLapSpeed", F.col("fastestLapSpeed").cast(FloatType()))
    .withColumn("milliseconds",    F.col("milliseconds").cast(FloatType()))
)

races = (races
    .withColumn("raceId",    F.col("raceId").cast(IntegerType()))
    .withColumn("year",      F.col("year").cast(IntegerType()))
    .withColumn("round",     F.col("round").cast(IntegerType()))
    .withColumn("date",      F.to_date(F.col("date"), "yyyy-MM-dd"))
)

drivers = (drivers
    .withColumn("driverId",  F.col("driverId").cast(IntegerType()))
    .withColumn("number",    F.col("number").cast(IntegerType()))
    .withColumn("dob",       F.to_date(F.col("dob"), "yyyy-MM-dd"))
)

pit_stops = (pit_stops
    .withColumn("raceId",     F.col("raceId").cast(IntegerType()))
    .withColumn("driverId",   F.col("driverId").cast(IntegerType()))
    .withColumn("stop",       F.col("stop").cast(IntegerType()))
    .withColumn("lap",        F.col("lap").cast(IntegerType()))
    .withColumn("milliseconds", F.col("milliseconds").cast(FloatType()))
)

qualifying = (qualifying
    .withColumn("raceId",    F.col("raceId").cast(IntegerType()))
    .withColumn("driverId",  F.col("driverId").cast(IntegerType()))
    .withColumn("position",  F.col("position").cast(IntegerType()))
)

logger.info("  Type conversions applied to: results, races, drivers, pit_stops, qualifying")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — DATA STANDARDIZATION  (Transformation #4)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("\n" + "="*60)
logger.info("STEP 5: Data Standardisation")
logger.info("="*60)

# Standardise circuit/country names to Title Case
circuits = circuits.withColumn("country", F.initcap(F.trim(F.col("country"))))
circuits = circuits.withColumn("name",    F.trim(F.col("name")))

# Standardise driver names
drivers = (drivers
    .withColumn("forename",  F.initcap(F.trim(F.col("forename"))))
    .withColumn("surname",   F.initcap(F.trim(F.col("surname"))))
    .withColumn("full_name", F.concat_ws(" ", F.col("forename"), F.col("surname")))
)

# Standardise constructor names
constructors = constructors.withColumn(
    "name", F.trim(F.col("name"))
)

# Convert lap time strings (e.g. "1:23.456") to milliseconds for ML
def laptime_to_ms(col_name):
    """Convert 'M:SS.mmm' lap time string to float milliseconds."""
    return (
        F.when(F.col(col_name).isNull(), None)
         .when(F.col(col_name).rlike(r"^\d+:\d+\.\d+$"),
               (F.split(F.col(col_name), ":")[0].cast(FloatType()) * 60000) +
               (F.split(F.col(col_name), ":")[1].cast(FloatType()) * 1000))
         .otherwise(None)
    )

qualifying = (qualifying
    .withColumn("q1_ms", laptime_to_ms("q1"))
    .withColumn("q2_ms", laptime_to_ms("q2"))
    .withColumn("q3_ms", laptime_to_ms("q3"))
    .withColumn("best_quali_ms",
                F.least(F.col("q3_ms"), F.col("q2_ms"), F.col("q1_ms")))
)

logger.info("  Standardisation applied: circuits, drivers, constructors, qualifying times -> ms")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — DATA AGGREGATION: FEATURE ENGINEERING  (Transformation #5)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("\n" + "="*60)
logger.info("STEP 6: Data Aggregation - Feature Engineering for ML Model")
logger.info("="*60)

# Join results with races to get year/round context
results_with_race = results.join(
    races.select("raceId", "year", "round", "circuitId", "date"),
    on="raceId", how="inner"
)

# --- Pit stop aggregates per driver per race ---
pit_agg = (pit_stops
    .groupBy("raceId", "driverId")
    .agg(
        F.count("stop").alias("total_pit_stops"),
        F.avg("milliseconds").alias("avg_pit_duration_ms"),
        F.min("milliseconds").alias("fastest_pit_ms")
    )
)

# --- Lap count aggregates (proxy for race completion) ---
lap_agg = (lap_times_raw
    .withColumn("raceId",   F.col("raceId").cast(IntegerType()))
    .withColumn("driverId", F.col("driverId").cast(IntegerType()))
    .withColumn("milliseconds", F.col("milliseconds").cast(FloatType()))
    .groupBy("raceId", "driverId")
    .agg(
        F.count("lap").alias("laps_completed"),
        F.avg("milliseconds").alias("avg_lap_ms"),
        F.min("milliseconds").alias("fastest_lap_ms"),
        F.stddev("milliseconds").alias("lap_time_consistency")
    )
)

# --- Rolling driver form: avg points in last 3 races (season-scoped window) ---
w_driver = Window.partitionBy("driverId", "year").orderBy("round").rowsBetween(-3, -1)

driver_form = results_with_race.withColumn(
    "rolling_avg_points_3", F.avg("points").over(w_driver)
).withColumn(
    "rolling_avg_pos_3",    F.avg("positionOrder").over(w_driver)
)

# --- Constructor reliability: avg points last 3 races ---
w_constructor = Window.partitionBy("constructorId", "year").orderBy("round").rowsBetween(-3, -1)

constructor_form = results_with_race.withColumn(
    "constructor_rolling_points_3", F.avg("points").over(w_constructor)
)

# --- Career stats per driver (total wins, podiums, points, races) ---
career_stats = (results
    .groupBy("driverId")
    .agg(
        F.sum(F.when(F.col("positionOrder") == 1, 1).otherwise(0)).alias("career_wins"),
        F.sum(F.when(F.col("positionOrder") <= 3, 1).otherwise(0)).alias("career_podiums"),
        F.sum("points").alias("career_points"),
        F.count("raceId").alias("career_races")
    )
    .withColumn("win_rate",   F.col("career_wins")   / F.col("career_races"))
    .withColumn("podium_rate", F.col("career_podiums") / F.col("career_races"))
)

logger.info("  Computed: pit aggregates, lap aggregates, rolling driver form, constructor form, career stats")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — ASSEMBLE FINAL ML FEATURE TABLE
# ══════════════════════════════════════════════════════════════════════════════
logger.info("\n" + "="*60)
logger.info("STEP 7: Assembling ML Feature Table")
logger.info("="*60)

# Base: each row = one driver in one race
feature_table = (
    driver_form
    .select(
        "raceId", "driverId", "constructorId", "year", "round", "circuitId",
        "grid",             # qualifying grid position (key predictor)
        "positionOrder",    # TARGET variable: finishing position
        "points",
        "laps",
        "statusId",
        "rolling_avg_points_3",
        "rolling_avg_pos_3"
    )
    # Join constructor form
    .join(
        constructor_form.select("raceId","driverId","constructor_rolling_points_3"),
        on=["raceId","driverId"], how="left"
    )
    # Join qualifying times
    .join(
        qualifying.select("raceId","driverId","position","best_quali_ms","q1_ms"),
        on=["raceId","driverId"], how="left"
    )
    # Join pit stop aggregates
    .join(
        pit_agg, on=["raceId","driverId"], how="left"
    )
    # Join lap time aggregates
    .join(
        lap_agg.select("raceId","driverId","avg_lap_ms","fastest_lap_ms","lap_time_consistency"),
        on=["raceId","driverId"], how="left"
    )
    # Join career stats
    .join(
        career_stats, on="driverId", how="left"
    )
    # Join driver metadata
    .join(
        drivers.select("driverId","full_name","dob","nationality"),
        on="driverId", how="left"
    )
    # Join constructor name
    .join(
        constructors.select("constructorId","name").withColumnRenamed("name","constructor_name"),
        on="constructorId", how="left"
    )
    # Join circuit info
    .join(
        circuits.select("circuitId","name","country","lat","lng","alt")
                .withColumnRenamed("name","circuit_name"),
        on="circuitId", how="left"
    )
    # Derive target: podium finish (top 3) = 1, else 0
    .withColumn("is_podium",    F.when(F.col("positionOrder") <= 3, 1).otherwise(0))
    .withColumn("is_winner",    F.when(F.col("positionOrder") == 1, 1).otherwise(0))
    # Driver age at race date
    .join(races.select("raceId","date"), on="raceId", how="left")
    .withColumn("driver_age_at_race",
                F.round(F.datediff(F.col("date"), F.col("dob")) / 365.25, 1))
)

# Final null fills for ML compatibility
feature_table = (feature_table
    .withColumn("total_pit_stops",    F.coalesce(F.col("total_pit_stops"),    F.lit(0)))
    .withColumn("avg_pit_duration_ms", F.coalesce(F.col("avg_pit_duration_ms"), F.lit(0.0)))
    .withColumn("rolling_avg_points_3", F.coalesce(F.col("rolling_avg_points_3"), F.lit(0.0)))
    .withColumn("rolling_avg_pos_3",    F.coalesce(F.col("rolling_avg_pos_3"),    F.lit(10.0)))
    .withColumn("constructor_rolling_points_3",
                F.coalesce(F.col("constructor_rolling_points_3"), F.lit(0.0)))
)

row_count = feature_table.count()
col_count = len(feature_table.columns)
logger.info(f"  Feature table assembled: {row_count} rows x {col_count} columns")

# Data quality check on target variable
nulls_in_target = feature_table.filter(F.col("positionOrder").isNull()).count()
logger.info(f"  [QC] Target variable 'positionOrder' nulls: {nulls_in_target}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — WRITE OUTPUTS TO S3 PROCESSED ZONE (SINK)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("\n" + "="*60)
logger.info("STEP 8: Writing processed data to S3 sink (Parquet)")
logger.info("="*60)

# Main ML feature table - partitioned by year for efficient querying
feature_table.write.mode("overwrite").partitionBy("year").parquet(
    f"{PROCESSED_BASE}ml_features/"
)
logger.info(f"  ML features -> {PROCESSED_BASE}ml_features/ (partitioned by year)")

# Also write supporting dimension tables for BI use
races.write.mode("overwrite").parquet(f"{PROCESSED_BASE}dim_races/")
drivers.write.mode("overwrite").parquet(f"{PROCESSED_BASE}dim_drivers/")
constructors.write.mode("overwrite").parquet(f"{PROCESSED_BASE}dim_constructors/")
circuits.write.mode("overwrite").parquet(f"{PROCESSED_BASE}dim_circuits/")

logger.info("  Dimension tables written: dim_races, dim_drivers, dim_constructors, dim_circuits")

# ── Job Complete ───────────────────────────────────────────────────────────────
logger.info("\n" + "="*60)
logger.info("ETL Job Complete")
logger.info(f"Output location: {PROCESSED_BASE}")
logger.info("="*60)

job.commit()
