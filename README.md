# AWS Glue Sales Analytics ETL Pipeline

> A complete, production-grade ETL pipeline built on AWS Glue, S3, Athena, and CloudWatch — automated end-to-end with PowerShell. Designed for AWS Free Tier practice.

---

## Table of Contents

- [Architecture](#architecture)
- [AWS Services Used](#aws-services-used)
- [Free Tier Budget](#free-tier-budget)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Run Order](#run-order)
- [What the ETL Job Does](#what-the-etl-job-does)
- [Key Concepts Practiced](#key-concepts-practiced)
- [Commands Cheat Sheet](#commands-cheat-sheet)
- [Troubleshooting](#troubleshooting)
- [Author](#author)

---

## Architecture

```
S3 Raw Bucket (CSV)
        |
        v
  Glue Crawler  ──────────────►  Glue Data Catalog (sales_db)
        |                                   |
        |                                   v
        └──────────────────►  Glue ETL Job (PySpark)
                                            |
                              ┌─────────────┴─────────────┐
                              v                           v
                    S3 Processed (Parquet)        S3 Quarantine (bad records)
                    partitioned year/month
                              |
                              v
                    Glue Crawler (output)
                              |
                              v
                    Athena SQL Analytics
                              |
                              v
                    Glue Workflow (orchestrates all steps)
                              |
                              v
                    CloudWatch Logs & Metrics
```

---

## AWS Services Used

| Service | Role in Project |
|---|---|
| **Amazon S3** | Raw CSV storage, processed Parquet output, ETL scripts, Athena results |
| **AWS Glue Crawlers** | Auto-detect CSV schema and register it in the Data Catalog |
| **AWS Glue Data Catalog** | Central metadata store — databases, tables, partitions |
| **AWS Glue ETL Jobs** | PySpark transformation: cleanse, enrich, deduplicate, write Parquet |
| **AWS Glue Workflows** | Pipeline orchestration: Crawler → Job → Crawler as one unit |
| **Amazon Athena** | Serverless SQL queries directly on Parquet files in S3 |
| **AWS IAM** | Least-privilege service role scoped to project buckets only |
| **Amazon CloudWatch** | Job run metrics, error logs, log retention policy |

---

## Free Tier Budget

| Service | Free Tier Allowance | This Project Uses |
|---|---|---|
| AWS Glue | 40 DPU-hours / month | ~3 DPU-hours per run |
| Amazon S3 | 5 GB storage | < 5 MB |
| Amazon Athena | 1 TB queries / month | < 10 MB |
| CloudWatch Logs | 5 GB / month | < 1 MB |

> **Always run `07_cleanup.ps1` when done practicing** to avoid any charges.

---

## Prerequisites

- **AWS CLI v2** — [Installation guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- **PowerShell 5.1+** on Windows (or PowerShell 7+ cross-platform)
- **AWS account** with an IAM user that has `AdministratorAccess` (or scoped Glue + S3 + IAM permissions)

Configure your credentials before running anything:

```powershell
aws configure
# Prompts for: Access Key ID, Secret Access Key, Region (us-east-1), Output format (json)
```

---

## Project Structure

```
aws-glue-sales-etl/
│
├── powershell/
│   ├── 01_setup_infrastructure.ps1     # S3 buckets, IAM role, Glue DB, CloudWatch
│   ├── 02_upload_data_and_scripts.ps1  # Upload CSV data + PySpark script to S3
│   ├── 03_create_crawler.ps1           # Create & run Glue Crawler on raw CSV
│   ├── 04_create_glue_job.ps1          # Create & run Glue ETL Job
│   ├── 05_setup_athena_and_query.ps1   # Athena workgroup + 5 analytics queries
│   ├── 06_create_workflow.ps1          # Wire all steps into a Glue Workflow
│   └── 07_cleanup.ps1                  # Full teardown — deletes all AWS resources
│
├── glue-scripts/
│   └── sales_etl_job.py                # PySpark ETL script (runs inside AWS Glue)
│
├── sample-data/
│   └── sales_2024_q1.csv               # 30 realistic sales records (Q1 2024)
│
├── athena-queries/
│   └── analytics_queries.sql           # 8 production-grade analytics queries
│
└── README.md
```

---

## Run Order

```powershell
cd .\powershell\

.\01_setup_infrastructure.ps1    # ~2 min  — S3 buckets, IAM role, Glue database
.\02_upload_data_and_scripts.ps1 # ~30 sec — push CSV data and PySpark script to S3
.\03_create_crawler.ps1          # ~2 min  — crawl raw CSV, register schema in catalog
.\04_create_glue_job.ps1         # ~8 min  — create and run the ETL job
.\05_setup_athena_and_query.ps1  # ~3 min  — run 5 analytics queries on Parquet output
.\06_create_workflow.ps1         # ~1 min  — orchestrate everything as one pipeline
.\07_cleanup.ps1                 # always  — delete all AWS resources when done
```

---

## What the ETL Job Does

The `sales_etl_job.py` PySpark script runs inside AWS Glue 4.0 (Spark 3.3) and performs 9 steps:

| Step | Action |
|---|---|
| 1 | **Read** raw CSV from S3 via Glue Data Catalog as a DynamicFrame |
| 2 | **Data Quality** — flag records with nulls, invalid prices, or bad quantities |
| 3 | **Quarantine** — write bad records to a separate S3 path for investigation |
| 4 | **Cleanse** — trim whitespace, uppercase IDs, cast types, parse date strings |
| 5 | **Enrich** — compute `gross_revenue`, `discount_amount`, `net_revenue`, `gross_profit`, date parts, `revenue_tier`, `is_discounted` |
| 6 | **Deduplicate** — keep the latest record per `order_id` using Spark Window functions |
| 7 | **Write Parquet** — Snappy-compressed, partitioned by `order_year` / `order_month` |
| 8 | **Update Catalog** — register new Parquet partitions in Glue Data Catalog |
| 9 | **Job Bookmarks** — enabled to skip already-processed files on future runs |

---

## Key Concepts Practiced

| Concept | Where |
|---|---|
| Glue Crawler schema inference | `03_create_crawler.ps1` |
| Glue Data Catalog — databases & tables | `01_setup_infrastructure.ps1` |
| PySpark DynamicFrames vs DataFrames | `sales_etl_job.py` |
| Data quality checks & quarantine pattern | `sales_etl_job.py` — Step 2 & 3 |
| Spark Window functions for deduplication | `sales_etl_job.py` — Step 6 |
| Parquet + Snappy compression | `sales_etl_job.py` — Step 7 |
| Hive-style S3 partitioning | `sales_etl_job.py` — Step 7 |
| Job Bookmarks (incremental loads) | `04_create_glue_job.ps1` |
| Glue Workflow triggers (Crawler → Job → Crawler) | `06_create_workflow.ps1` |
| Athena partition pruning (cost saving) | `analytics_queries.sql` — Q6 |
| IAM least-privilege inline policy | `01_setup_infrastructure.ps1` |
| CloudWatch metrics & log retention | `04_create_glue_job.ps1` |

---

## Commands Cheat Sheet

### AWS CLI Setup

```powershell
aws configure
```

---

### S3

```powershell
# List all buckets
aws s3 ls

# List all files in a bucket
aws s3 ls s3://BUCKET_NAME --recursive

# Upload a file
aws s3 cp sales_2024_q1.csv s3://BUCKET_NAME/

# Delete all contents and the bucket itself
aws s3 rb s3://BUCKET_NAME --force

# Check object versions (when versioning is enabled)
aws s3api list-object-versions --bucket BUCKET_NAME

# Delete a specific object version
aws s3api delete-object `
  --bucket BUCKET_NAME `
  --key "FILE_PATH" `
  --version-id VERSION_ID
```

---

### AWS Glue

```powershell
# List all Glue jobs
aws glue list-jobs

# List all crawlers
aws glue list-crawlers

# List all Glue databases
aws glue get-databases

# Get tables in a database
aws glue get-tables --database-name sales_db

# Get job run history
aws glue get-job-runs `
  --job-name sales_etl_job `
  --region us-east-1

# Delete a Glue job
aws glue delete-job --job-name sales_etl_job

# Delete a crawler
aws glue delete-crawler --name sales_raw_crawler

# Delete a Glue database
aws glue delete-database --name sales_db
```

---

### Amazon Athena

```powershell
# Run a query
aws athena start-query-execution `
  --query-string "SELECT * FROM sales LIMIT 10;" `
  --query-execution-context Database=sales_db `
  --result-configuration OutputLocation=s3://ATHENA_BUCKET/results/ `
  --region us-east-1

# Fetch query results
aws athena get-query-results `
  --query-execution-id QUERY_ID `
  --region us-east-1 `
  --query "ResultSet.Rows[*].Data[*].VarCharValue" `
  --output text

# List Athena workgroups
aws athena list-work-groups

# Delete an Athena workgroup (and all saved queries inside it)
aws athena delete-work-group `
  --work-group WORKGROUP_NAME `
  --recursive-delete-option
```

---

### CloudWatch Logs

```powershell
# List all log groups
aws logs describe-log-groups

# List only log group names
aws logs describe-log-groups `
  --query "logGroups[].logGroupName" `
  --output text

# Delete a log group
aws logs delete-log-group `
  --log-group-name "/aws-glue/jobs/error"
```

---

### IAM

```powershell
# List IAM roles
aws iam list-roles

# Detach a managed policy from a role
aws iam detach-role-policy `
  --role-name ROLE_NAME `
  --policy-arn POLICY_ARN

# Delete an IAM role (must detach all policies first)
aws iam delete-role --role-name ROLE_NAME
```

---

### Cleanup Verification

Run these after `07_cleanup.ps1` to confirm everything is removed:

```powershell
# S3 — should return empty or only unrelated buckets
aws s3 ls

# Glue — should return empty lists
aws glue list-jobs
aws glue list-crawlers
aws glue get-databases

# CloudWatch — confirm log group is gone
aws logs describe-log-groups

# Athena — confirm workgroup is gone
aws athena list-work-groups
```

---

## Troubleshooting

**Crawler finds no tables**
- Verify the S3 path in the crawler config matches where data was uploaded
- Confirm the IAM role has `s3:ListBucket` permission on the raw bucket

**Glue job fails: `EntityNotFoundException`**
- The table name passed to the job must exactly match what the crawler created
- Check the real table name: `aws glue get-tables --database-name sales_db`
- Update `$TABLE = "sales"` in step 4 if the crawler named it differently

**Glue job fails: `AccessDeniedException`**
- The IAM role is missing a required permission
- Check CloudWatch logs: `aws logs tail /aws-glue/jobs/error --region us-east-1`

**Athena: `TABLE_NOT_FOUND`**
- The processed Parquet data crawler must run before Athena can see the table
- Step 5 runs this crawler automatically — check it completed successfully

**Job stuck in `STARTING` for more than 10 minutes**
- Normal for first run — Glue spins up Spark clusters cold
- If it exceeds 20 minutes, check for IAM or S3 path misconfigurations

---

## Author

**Abdelatif Mohamed**
Cloud & DevOps Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat&logo=linkedin)](https://linkedin.com)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?style=flat&logo=github)](https://github.com)
