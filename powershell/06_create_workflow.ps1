# =============================================================================
# AWS Glue Sales ETL Project - Step 6: Glue Workflow (Orchestration)
# =============================================================================
# Ties everything together: Crawler → ETL Job → Output Crawler
# Triggered manually or on a schedule. This is real production practice!
# Run after: 05_setup_athena_and_query.ps1
# =============================================================================

$config          = Get-Content ".\project-config.json" | ConvertFrom-Json
$AWS_REGION      = $config.AWS_REGION

$WORKFLOW_NAME   = "sales_etl_workflow"
$CRAWLER_IN      = "sales_raw_crawler"
$JOB_NAME        = "sales_etl_job"
$CRAWLER_OUT     = "sales_processed_crawler"

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Step 6: Create Glue Workflow (Pipeline)" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan

# --------------------------------------------------------------------------
# CREATE WORKFLOW
# --------------------------------------------------------------------------
Write-Host ""
Write-Host "[1/4] Creating Glue Workflow: $WORKFLOW_NAME ..." -ForegroundColor Yellow

$wfCheck = aws glue get-workflow --name $WORKFLOW_NAME --region $AWS_REGION 2>&1
if ($LASTEXITCODE -eq 0) {
    aws glue delete-workflow --name $WORKFLOW_NAME --region $AWS_REGION | Out-Null
    Start-Sleep -Seconds 2
}

aws glue create-workflow `
    --name $WORKFLOW_NAME `
    --description "End-to-end Sales ETL: Crawl raw → Transform → Crawl processed" `
    --default-run-properties "{`"Environment`":`"Dev`",`"Project`":`"SalesETL`"}" `
    --region $AWS_REGION | Out-Null

Write-Host "  Workflow created: $WORKFLOW_NAME" -ForegroundColor Green

# --------------------------------------------------------------------------
# ADD TRIGGERS
# --------------------------------------------------------------------------
Write-Host ""
Write-Host "[2/4] Adding workflow triggers..." -ForegroundColor Yellow

# Trigger 1: Start (ON_DEMAND) → Run input crawler
aws glue create-trigger `
    --name "t_start_input_crawler" `
    --workflow-name $WORKFLOW_NAME `
    --type ON_DEMAND `
    --actions "[{`"CrawlerName`":`"$CRAWLER_IN`"}]" `
    --description "Manually start: kicks off the input crawler" `
    --region $AWS_REGION | Out-Null
Write-Host "  Trigger 1: ON_DEMAND → $CRAWLER_IN" -ForegroundColor Green

# Trigger 2: After input crawler succeeds → Run ETL job
aws glue create-trigger `
    --name "t_after_crawler_run_job" `
    --workflow-name $WORKFLOW_NAME `
    --type CONDITIONAL `
    --start-on-creation `
    --predicate "{`"Logical`":`"ANY`",`"Conditions`":[{`"LogicalOperator`":`"EQUALS`",`"CrawlerName`":`"$CRAWLER_IN`",`"CrawlState`":`"SUCCEEDED`"}]}" `
    --actions "[{`"JobName`":`"$JOB_NAME`"}]" `
    --description "After input crawler succeeds, run the ETL job" `
    --region $AWS_REGION | Out-Null
Write-Host "  Trigger 2: $CRAWLER_IN SUCCEEDED → $JOB_NAME" -ForegroundColor Green

# Trigger 3: After ETL job succeeds → Run output crawler
aws glue create-trigger `
    --name "t_after_job_run_output_crawler" `
    --workflow-name $WORKFLOW_NAME `
    --type CONDITIONAL `
    --start-on-creation `
    --predicate "{`"Logical`":`"ANY`",`"Conditions`":[{`"LogicalOperator`":`"EQUALS`",`"JobName`":`"$JOB_NAME`",`"State`":`"SUCCEEDED`"}]}" `
    --actions "[{`"CrawlerName`":`"$CRAWLER_OUT`"}]" `
    --description "After ETL job succeeds, crawl and register Parquet output" `
    --region $AWS_REGION | Out-Null
Write-Host "  Trigger 3: $JOB_NAME SUCCEEDED → $CRAWLER_OUT" -ForegroundColor Green

# --------------------------------------------------------------------------
# OPTIONAL: Add daily schedule trigger (commented by default - uncomment for prod)
# --------------------------------------------------------------------------
<#
Write-Host ""
Write-Host "  [OPTIONAL] To add a daily schedule trigger, uncomment this block:"
aws glue create-trigger `
    --name "t_daily_schedule" `
    --workflow-name $WORKFLOW_NAME `
    --type SCHEDULED `
    --schedule "cron(0 2 * * ? *)" `
    --actions "[{`"CrawlerName`":`"$CRAWLER_IN`"}]" `
    --start-on-creation `
    --region $AWS_REGION | Out-Null
#>

# --------------------------------------------------------------------------
# VIEW WORKFLOW GRAPH
# --------------------------------------------------------------------------
Write-Host ""
Write-Host "[3/4] Workflow graph:" -ForegroundColor Yellow
Write-Host ""
Write-Host "  [ON_DEMAND START]" -ForegroundColor DarkYellow
Write-Host "       |"
Write-Host "       v"
Write-Host "  [CRAWLER: $CRAWLER_IN]" -ForegroundColor Yellow
Write-Host "       | (SUCCEEDED)"
Write-Host "       v"
Write-Host "  [GLUE JOB: $JOB_NAME]" -ForegroundColor Cyan
Write-Host "       | (SUCCEEDED)"
Write-Host "       v"
Write-Host "  [CRAWLER: $CRAWLER_OUT]" -ForegroundColor Green
Write-Host "       | (catalog updated)"
Write-Host "       v"
Write-Host "  [ATHENA ready for queries]" -ForegroundColor Blue

# --------------------------------------------------------------------------
# START THE WORKFLOW
# --------------------------------------------------------------------------
Write-Host ""
Write-Host "[4/4] Starting the workflow now..." -ForegroundColor Yellow

$wfRun = aws glue start-workflow-run `
    --name $WORKFLOW_NAME `
    --region $AWS_REGION `
    --output json | ConvertFrom-Json

$WF_RUN_ID = $wfRun.RunId
Write-Host "  Workflow run started: $WF_RUN_ID" -ForegroundColor Green
Write-Host "  Monitor in AWS Console: Glue → Workflows → $WORKFLOW_NAME"
Write-Host ""
Write-Host "  Check status with:"
Write-Host "  aws glue get-workflow-run --name $WORKFLOW_NAME --run-id $WF_RUN_ID --region $AWS_REGION"
Write-Host ""

Write-Host "================================================" -ForegroundColor Green
Write-Host "  Workflow Orchestration COMPLETE" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
Write-Host ""
Write-Host "IMPORTANT: When done practicing, run:"
Write-Host "  .\07_cleanup.ps1  (removes all AWS resources to avoid charges)"
Write-Host ""
