"""
AWS Glue ETL Job — Sales Data Transform
Author: Lohith Kumar V

Reads raw sales CSV from S3, applies transformations,
runs data quality checks, and writes Parquet to the data warehouse layer.
Schema mapping arg (--schema_mapping) is dynamically patched by the DE agent
when upstream column names change.
"""

import sys
import json
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

# ─── Init ────────────────────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_bucket",
        "target_bucket",
        "source_prefix",
        "target_prefix",
        "--schema_mapping",  # Injected/patched by DE agent
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = logging.getLogger(args["JOB_NAME"])
logger.setLevel(logging.INFO)

# Parse agent-injected schema mapping: "old_col:new_col,old2:new2"
SCHEMA_MAPPING = {}
raw_mapping = args.get("--schema_mapping", "")
if raw_mapping:
    for pair in raw_mapping.split(","):
        if ":" in pair:
            old, new = pair.strip().split(":", 1)
            SCHEMA_MAPPING[old.strip()] = new.strip()
    logger.info(f"Schema mapping applied: {SCHEMA_MAPPING}")


# ─── Extract ─────────────────────────────────────────────────────────────────

logger.info(f"Reading from s3://{args['source_bucket']}/{args['source_prefix']}")

raw_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3://{args['source_bucket']}/{args['source_prefix']}")
)

logger.info(f"Loaded {raw_df.count():,} raw records")


# ─── Apply Agent Schema Mapping ──────────────────────────────────────────────

for old_col, new_col in SCHEMA_MAPPING.items():
    if old_col in raw_df.columns:
        raw_df = raw_df.withColumnRenamed(old_col, new_col)
        logger.info(f"Renamed column: {old_col} → {new_col}")


# ─── Transform ───────────────────────────────────────────────────────────────

transformed_df = (
    raw_df
    # Standardise column names
    .toDF(*[c.lower().replace(" ", "_") for c in raw_df.columns])
    # Cast numeric columns
    .withColumn("revenue_usd", F.col("revenue_local_currency").cast(DoubleType()))
    .withColumn("quantity", F.col("quantity").cast("integer"))
    # Parse dates
    .withColumn("order_date", F.to_timestamp("order_date", "yyyy-MM-dd"))
    # Derived columns
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("revenue_bucket",
        F.when(F.col("revenue_usd") < 100, "low")
         .when(F.col("revenue_usd") < 1000, "medium")
         .otherwise("high")
    )
    # Drop duplicates on business key
    .dropDuplicates(["order_id"])
    # Drop rows with null primary keys
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
)


# ─── Data Quality Gate ───────────────────────────────────────────────────────

def run_dq_checks(df) -> dict:
    total = df.count()
    null_revenue = df.filter(F.col("revenue_usd").isNull()).count()
    negative_revenue = df.filter(F.col("revenue_usd") < 0).count()
    null_dates = df.filter(F.col("order_date").isNull()).count()

    checks = {
        "total_records": total,
        "null_revenue_pct": round(null_revenue / total * 100, 2),
        "negative_revenue_count": negative_revenue,
        "null_date_pct": round(null_dates / total * 100, 2),
        "passed": (null_revenue / total < 0.01) and (negative_revenue == 0),
    }
    return checks


dq_results = run_dq_checks(transformed_df)
logger.info(f"DQ results: {json.dumps(dq_results)}")

if not dq_results["passed"]:
    # Agent can detect this in job logs and decide to quarantine or retry
    logger.error(f"DQ GATE FAILED: {dq_results}")
    raise Exception(f"Data quality gate failed: {json.dumps(dq_results)}")


# ─── Load ────────────────────────────────────────────────────────────────────

target_path = f"s3://{args['target_bucket']}/{args['target_prefix']}"
logger.info(f"Writing {transformed_df.count():,} records to {target_path}")

(
    transformed_df.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(target_path)
)

logger.info("✅ Job completed successfully")
job.commit()
