# AWS Glue Sales Analytics ETL — Production Project

A complete, real-world ETL pipeline using AWS Glue, S3, Athena, and CloudWatch.  
Built for **AWS Free Tier** practice with PowerShell automation.

---

## Architecture

```
S3 Raw (CSV)  →  Glue Crawler  →  Glue Data Catalog
                                         |
                                   Glue ETL Job (PySpark)
                                         |
                              S3 Processed (Parquet, partitioned)
                                         |
                               Glue Crawler (output)  →  Athena SQL
                                         |
                                   Glue Workflow (orchestrates all steps)
```

**AWS Services used:**
- Amazon S3 (raw data, processed Parquet, scripts, Athena results)
- AWS Glue Crawlers (schema discovery)
- AWS Glue Data Catalog (metadata store)
- AWS Glue ETL Jobs (PySpark transformation)
- AWS Glue Workflows (pipeline orchestration)
- Amazon Athena (serverless SQL analytics)
- AWS IAM (least-privilege service role)
- Amazon CloudWatch (job metrics, logs)

---

## Free Tier Budget

| Service      | Free Tier Allowance   | This Project Uses |
|--------------|-----------------------|-------------------|
| AWS Glue     | 40 DPU-hours/month    | ~3 DPU-hours/run  |
| Amazon S3    | 5 GB storage          | < 5 MB            |
| Amazon Athena| 1 TB queries/month    | < 10 MB           |
| CloudWatch   | 5 GB logs/month       | < 1 MB            |

> **Always run `07_cleanup.ps1` after practicing** to avoid charges.

---

## Prerequisites

1. **AWS CLI v2** — [Install guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
2. **PowerShell 5.1+** (Windows) or PowerShell 7+ (cross-platform)
3. **AWS account** with an IAM user that has AdministratorAccess (or scoped permissions)
4. Configure AWS CLI:
   ```powershell
   aws configure
   # Enter: Access Key ID, Secret Access Key, Region (us-east-1), output (json)
   ```

---

## Run Order

```powershell
cd .\powershell\

.\01_setup_infrastructure.ps1    # S3 buckets, IAM role, Glue DB
.\02_upload_data_and_scripts.ps1 # Upload CSV data + PySpark script to S3
.\03_create_crawler.ps1          # Crawl raw CSV, register schema in catalog
.\04_create_glue_job.ps1         # Create + run ETL job (takes ~5-8 min)
.\05_setup_athena_and_query.ps1  # Run analytics queries on Parquet output
.\06_create_workflow.ps1         # Wire everything into a Glue Workflow
.\07_cleanup.ps1                 # DELETE all resources (always do this!)
```

---

## What the ETL Job Does

The `glue-scripts/sales_etl_job.py` PySpark script performs:

1. **Read** — Load CSV from S3 via Glue Data Catalog (dynamic frame)
2. **Data Quality** — Flag records with nulls, invalid prices/quantities
3. **Quarantine** — Write bad records to a separate S3 location
4. **Cleanse** — Trim, uppercase IDs, cast types, parse dates
5. **Enrich** — Compute `gross_revenue`, `discount_amount`, `net_revenue`, `gross_profit`, date parts, `revenue_tier`, `is_discounted`
6. **Deduplicate** — Keep latest record per `order_id` using Spark Window
7. **Write Parquet** — Snappy-compressed, partitioned by `order_year/order_month`
8. **Update Catalog** — Register new partitions in Glue Data Catalog
9. **Job Bookmarks** — Enabled to avoid reprocessing already-seen files

---

## Project Files

```
aws-glue-sales-etl/
├── powershell/
│   ├── 01_setup_infrastructure.ps1    # S3, IAM, Glue DB, CloudWatch
│   ├── 02_upload_data_and_scripts.ps1 # Upload data + ETL script
│   ├── 03_create_crawler.ps1          # Glue Crawler (input)
│   ├── 04_create_glue_job.ps1         # Glue ETL Job + run
│   ├── 05_setup_athena_and_query.ps1  # Athena workgroup + 5 queries
│   ├── 06_create_workflow.ps1         # Glue Workflow orchestration
│   └── 07_cleanup.ps1                 # Full teardown
├── glue-scripts/
│   └── sales_etl_job.py               # PySpark ETL (runs in Glue)
├── sample-data/
│   └── sales_2024_q1.csv              # 30 realistic sales records
├── athena-queries/
│   └── analytics_queries.sql          # 8 production analytics queries
└── README.md
```

---

## Key Concepts Practiced

| Concept | Where |
|---------|-------|
| Glue Crawler schema inference | `03_create_crawler.ps1` |
| Glue Data Catalog (database/tables) | `01_setup_infrastructure.ps1` |
| PySpark DynamicFrames vs DataFrames | `sales_etl_job.py` |
| Data quality checks & quarantine | `sales_etl_job.py` step 2 |
| Spark Window functions (dedup) | `sales_etl_job.py` step 5 |
| Parquet + Snappy compression | `sales_etl_job.py` step 7 |
| Hive-style S3 partitioning | `sales_etl_job.py` step 7 |
| Job bookmarks (incremental) | `04_create_glue_job.ps1` |
| Glue Workflow triggers | `06_create_workflow.ps1` |
| Athena partition pruning | `analytics_queries.sql` Q6 |
| IAM least-privilege roles | `01_setup_infrastructure.ps1` |
| CloudWatch metrics & logs | `04_create_glue_job.ps1` |

---

## Troubleshooting

**Crawler finds no tables:**
- Check S3 path in crawler config matches actual data location
- Ensure the IAM role has `s3:ListBucket` on the raw bucket

**Glue job fails with EntityNotFoundException:**
- Table name in job args must match what the crawler created
- Re-run the crawler and check the exact table name: `aws glue get-tables --database-name sales_db`

**Athena: TABLE_NOT_FOUND:**
- Run the processed data crawler first (`03b` step in `05_setup_athena_and_query.ps1`)
- Check the database name is correct in query context


👨‍💻 Author

Abdelatif Mohamed
Cloud & DevOps Engineer

