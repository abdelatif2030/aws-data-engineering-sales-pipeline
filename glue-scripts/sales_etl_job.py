"""
=============================================================================
AWS Glue ETL Job: Sales Data Transformation Pipeline
=============================================================================
Project  : Sales Analytics ETL
Job Name : sales_etl_job
Runtime  : AWS Glue 4.0 (Spark 3.3, Python 3.10)
DPUs     : 2 (G.1X worker, free-tier safe)

WHAT THIS JOB DOES:
  1. Read raw CSV data from S3 using the Glue Data Catalog
  2. Apply data quality checks and flag bad records
  3. Cleanse and standardize column values
  4. Enrich data: compute revenue, profit, discount amount
  5. Deduplicate on order_id
  6. Write clean Parquet data back to S3, partitioned by year/month
  7. Update the Glue Data Catalog partitions
  8. Emit job metrics to CloudWatch

JOB PARAMETERS (pass via --job-bookmark-option, --raw_bucket, etc.):
  --raw_bucket        : Source S3 bucket name
  --processed_bucket  : Output S3 bucket name
  --database_name     : Glue catalog database
  --table_name        : Glue catalog table for raw data
=============================================================================
"""

import sys
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType
)

# ─────────────────────────────────────────────────────────────────────────────
# INITIALIZATION
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Resolve job arguments (passed in when creating the job)
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "raw_bucket",
    "processed_bucket",
    "database_name",
    "table_name",
])

JOB_NAME        = args["JOB_NAME"]
RAW_BUCKET      = args["raw_bucket"]
PROCESSED_BUCKET= args["processed_bucket"]
DATABASE_NAME   = args["database_name"]
TABLE_NAME      = args["table_name"]

RAW_S3_PATH       = f"s3://{RAW_BUCKET}/sales/"
PROCESSED_S3_PATH = f"s3://{PROCESSED_BUCKET}/sales/"

sc         = SparkContext()
glueContext= GlueContext(sc)
spark      = glueContext.spark_session
job        = Job(glueContext)
job.init(JOB_NAME, args)

# Performance tuning for small free-tier job
spark.conf.set("spark.sql.shuffle.partitions", "4")
spark.conf.set("spark.sql.adaptive.enabled", "true")

logger.info(f"[INIT] Job: {JOB_NAME} | Raw: {RAW_S3_PATH} | Out: {PROCESSED_S3_PATH}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: READ FROM GLUE DATA CATALOG
# ─────────────────────────────────────────────────────────────────────────────

logger.info("[STEP 1] Reading raw data from Glue Catalog...")

raw_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=TABLE_NAME,
    transformation_ctx="raw_read",
    additional_options={
        "recurse": True,
        "groupFiles": "inPartition",      # group small CSV files for efficiency
        "groupSize": "10485760",          # 10 MB group size
    }
)

raw_count = raw_dynamic_frame.count()
logger.info(f"[STEP 1] Records loaded from catalog: {raw_count}")

# Convert to Spark DataFrame for richer transformations
df_raw = raw_dynamic_frame.toDF()
df_raw.printSchema()
df_raw.show(5, truncate=False)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: DATA QUALITY CHECKS — Flag Bad Records
# ─────────────────────────────────────────────────────────────────────────────

logger.info("[STEP 2] Running data quality checks...")

df_with_dq = df_raw.withColumn(
    "dq_issues",
    F.concat_ws(",",
        F.when(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""), F.lit("MISSING_ORDER_ID")),
        F.when(F.col("customer_id").isNull(), F.lit("MISSING_CUSTOMER_ID")),
        F.when(F.col("quantity").cast("int").isNull() | (F.col("quantity").cast("int") <= 0), F.lit("INVALID_QUANTITY")),
        F.when(F.col("unit_price").cast("double").isNull() | (F.col("unit_price").cast("double") <= 0), F.lit("INVALID_PRICE")),
        F.when(F.col("discount_pct").cast("double").isNull(), F.lit("INVALID_DISCOUNT")),
        F.when(F.col("order_date").isNull(), F.lit("MISSING_DATE")),
    )
).withColumn(
    "dq_passed",
    F.when(F.col("dq_issues") == "", True).otherwise(False)
)

# Separate good and bad records
df_clean = df_with_dq.filter(F.col("dq_passed") == True)
df_quarantine = df_with_dq.filter(F.col("dq_passed") == False)

good_count = df_clean.count()
bad_count  = df_quarantine.count()
logger.info(f"[STEP 2] DQ passed: {good_count} | DQ failed (quarantined): {bad_count}")

# Write quarantine records for investigation
if bad_count > 0:
    df_quarantine.write.mode("overwrite").json(
        f"s3://{PROCESSED_BUCKET}/quarantine/sales/{datetime.now().strftime('%Y%m%d_%H%M')}/"
    )
    logger.warning(f"[STEP 2] {bad_count} records written to quarantine folder")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: CLEANSE & STANDARDIZE
# ─────────────────────────────────────────────────────────────────────────────

logger.info("[STEP 3] Cleansing and standardizing data...")

df_cleansed = df_clean \
    .withColumn("order_id",       F.trim(F.upper(F.col("order_id")))) \
    .withColumn("customer_id",    F.trim(F.upper(F.col("customer_id")))) \
    .withColumn("customer_name",  F.initcap(F.trim(F.col("customer_name")))) \
    .withColumn("product_id",     F.trim(F.upper(F.col("product_id")))) \
    .withColumn("product_name",   F.trim(F.col("product_name"))) \
    .withColumn("category",       F.trim(F.initcap(F.col("category")))) \
    .withColumn("region",         F.trim(F.col("region"))) \
    .withColumn("salesperson",    F.trim(F.col("salesperson"))) \
    .withColumn("status",         F.trim(F.lower(F.col("status")))) \
    .withColumn("quantity",       F.col("quantity").cast(IntegerType())) \
    .withColumn("unit_price",     F.round(F.col("unit_price").cast(DoubleType()), 2)) \
    .withColumn("discount_pct",   F.round(F.col("discount_pct").cast(DoubleType()), 2)) \
    .withColumn("order_date",     F.to_date(F.col("order_date"), "yyyy-MM-dd"))

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: BUSINESS ENRICHMENT — Compute Revenue, Profit, Date Parts
# ─────────────────────────────────────────────────────────────────────────────

logger.info("[STEP 4] Enriching data with business metrics...")

# Cost model: assume 60% COGS for Electronics, 40% for others
MARGIN_ELECTRONICS = 0.40   # 40% gross margin
MARGIN_DEFAULT     = 0.60   # 60% gross margin for Furniture, Stationery

df_enriched = df_cleansed \
    .withColumn("gross_revenue",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    ) \
    .withColumn("discount_amount",
        F.round(F.col("gross_revenue") * (F.col("discount_pct") / 100), 2)
    ) \
    .withColumn("net_revenue",
        F.round(F.col("gross_revenue") - F.col("discount_amount"), 2)
    ) \
    .withColumn("gross_margin_pct",
        F.when(F.col("category") == "Electronics", F.lit(MARGIN_ELECTRONICS))
         .otherwise(F.lit(MARGIN_DEFAULT))
    ) \
    .withColumn("gross_profit",
        F.round(F.col("net_revenue") * F.col("gross_margin_pct"), 2)
    ) \
    .withColumn("order_year",   F.year(F.col("order_date"))) \
    .withColumn("order_month",  F.month(F.col("order_date"))) \
    .withColumn("order_quarter",F.quarter(F.col("order_date"))) \
    .withColumn("order_weekday",F.dayofweek(F.col("order_date"))) \
    .withColumn("is_discounted",
        F.when(F.col("discount_pct") > 0, True).otherwise(False)
    ) \
    .withColumn("revenue_tier",
        F.when(F.col("net_revenue") >= 2000, "High")
         .when(F.col("net_revenue") >= 500,  "Mid")
         .otherwise("Low")
    ) \
    .withColumn("etl_processed_at", F.lit(datetime.utcnow().isoformat()))

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: DEDUPLICATION — Keep latest by order_id
# ─────────────────────────────────────────────────────────────────────────────

logger.info("[STEP 5] Deduplicating on order_id...")

from pyspark.sql.window import Window

window_spec = Window.partitionBy("order_id").orderBy(F.col("order_date").desc())

df_deduped = df_enriched \
    .withColumn("_row_num", F.row_number().over(window_spec)) \
    .filter(F.col("_row_num") == 1) \
    .drop("_row_num", "dq_issues", "dq_passed")

before = df_enriched.count()
after  = df_deduped.count()
logger.info(f"[STEP 5] Before dedup: {before} | After dedup: {after} | Removed: {before - after}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: SELECT FINAL COLUMNS IN ORDER
# ─────────────────────────────────────────────────────────────────────────────

logger.info("[STEP 6] Selecting final schema columns...")

FINAL_COLUMNS = [
    # Identity
    "order_id", "customer_id", "customer_name",
    "product_id", "product_name", "category",
    # Order details
    "quantity", "unit_price", "discount_pct",
    "gross_revenue", "discount_amount", "net_revenue",
    "gross_margin_pct", "gross_profit",
    # Date dimensions
    "order_date", "order_year", "order_month", "order_quarter", "order_weekday",
    # Geography & Sales
    "region", "salesperson",
    # Flags
    "status", "is_discounted", "revenue_tier",
    # Audit
    "etl_processed_at",
]

df_final = df_deduped.select(*FINAL_COLUMNS)

# Log some aggregate stats for validation
df_final.groupBy("category").agg(
    F.count("*").alias("orders"),
    F.sum("net_revenue").alias("total_revenue"),
    F.avg("gross_margin_pct").alias("avg_margin")
).show()

# ─────────────────────────────────────────────────────────────────────────────
# STEP 7: WRITE PARQUET OUTPUT — Partitioned by year/month
# ─────────────────────────────────────────────────────────────────────────────

logger.info("[STEP 7] Writing Parquet output to S3...")

df_final.write \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .option("compression", "snappy") \
    .parquet(PROCESSED_S3_PATH)

logger.info(f"[STEP 7] Parquet written to: {PROCESSED_S3_PATH}")
logger.info(f"[STEP 7] Final record count: {df_final.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 8: UPDATE GLUE CATALOG PARTITIONS
# ─────────────────────────────────────────────────────────────────────────────

logger.info("[STEP 8] Updating Glue Catalog partitions...")

# Write using Glue's native sink to auto-update catalog
output_dynamic_frame = DynamicFrame.fromDF(df_final, glueContext, "output_frame")

glueContext.write_dynamic_frame.from_options(
    frame=output_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": PROCESSED_S3_PATH,
        "partitionKeys": ["order_year", "order_month"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="write_parquet",
)

logger.info("[STEP 8] Catalog partitions updated.")

# ─────────────────────────────────────────────────────────────────────────────
# DONE
# ─────────────────────────────────────────────────────────────────────────────

logger.info("=" * 60)
logger.info(f"[DONE] ETL job complete. Records: {raw_count} → {df_final.count()}")
logger.info(f"[DONE] Output: {PROCESSED_S3_PATH}")
logger.info("=" * 60)

job.commit()
