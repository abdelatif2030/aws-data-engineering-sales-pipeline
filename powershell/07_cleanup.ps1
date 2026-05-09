# =============================================================================
# AWS Glue Sales ETL Project - Step 7: CLEANUP (Remove All Resources)
# =============================================================================
# ALWAYS run this when done practicing to stay on free tier.
# Deletes: S3 buckets, IAM role, Glue jobs, crawlers, workflow, DB, Athena WG
# =============================================================================

$config = Get-Content ".\project-config.json" | ConvertFrom-Json

Write-Host ""
Write-Host "================================================" -ForegroundColor Red
Write-Host "  Step 7: CLEANUP - Remove All Resources" -ForegroundColor Red
Write-Host "================================================" -ForegroundColor Red
Write-Host ""
Write-Host "This will permanently delete all project resources."
$confirm = Read-Host "Type 'DELETE' to confirm"
if ($confirm -ne "DELETE") {
    Write-Host "Cleanup cancelled." -ForegroundColor Yellow
    exit 0
}

$REGION = $config.AWS_REGION

Write-Host ""
Write-Host "[1/7] Deleting Glue Workflow & Triggers..." -ForegroundColor Yellow
aws glue stop-workflow-run --name "sales_etl_workflow" --run-id $config.LAST_JOB_RUN_ID --region $REGION 2>&1 | Out-Null

@("t_start_input_crawler","t_after_crawler_run_job","t_after_job_run_output_crawler","t_daily_schedule") | ForEach-Object {
    aws glue delete-trigger --name $_ --region $REGION 2>&1 | Out-Null
    Write-Host "  Trigger deleted: $_"
}
aws glue delete-workflow --name "sales_etl_workflow" --region $REGION 2>&1 | Out-Null
Write-Host "  Workflow deleted." -ForegroundColor Green

Write-Host ""
Write-Host "[2/7] Deleting Glue Jobs..." -ForegroundColor Yellow
aws glue delete-job --job-name "sales_etl_job" --region $REGION 2>&1 | Out-Null
Write-Host "  Job deleted: sales_etl_job" -ForegroundColor Green

Write-Host ""
Write-Host "[3/7] Deleting Glue Crawlers..." -ForegroundColor Yellow
@("sales_raw_crawler","sales_processed_crawler") | ForEach-Object {
    aws glue stop-crawler --name $_ --region $REGION 2>&1 | Out-Null
    Start-Sleep -Seconds 3
    aws glue delete-crawler --name $_ --region $REGION 2>&1 | Out-Null
    Write-Host "  Crawler deleted: $_" -ForegroundColor Green
}

Write-Host ""
Write-Host "[4/7] Deleting Glue Database..." -ForegroundColor Yellow
aws glue delete-database --name $config.GLUE_DB --region $REGION 2>&1 | Out-Null
Write-Host "  Database deleted: $($config.GLUE_DB)" -ForegroundColor Green

Write-Host ""
Write-Host "[5/7] Deleting Athena Workgroup..." -ForegroundColor Yellow
aws athena delete-work-group --work-group "sales-etl-workgroup" --recursive-delete-option --region $REGION 2>&1 | Out-Null
Write-Host "  Workgroup deleted." -ForegroundColor Green

Write-Host ""
Write-Host "[6/7] Deleting S3 Buckets (all objects + bucket)..." -ForegroundColor Yellow
$buckets = @($config.BUCKET_RAW, $config.BUCKET_PROCESSED, $config.BUCKET_SCRIPTS, $config.BUCKET_ATHENA)
foreach ($bucket in $buckets) {
    Write-Host "  Emptying: $bucket ..."
    # Remove all object versions (needed if versioning is enabled)
    aws s3api list-object-versions --bucket $bucket --output json 2>&1 | ConvertFrom-Json | ForEach-Object {
        if ($_.Versions) {
            $_.Versions | ForEach-Object {
                aws s3api delete-object --bucket $bucket --key $_.Key --version-id $_.VersionId 2>&1 | Out-Null
            }
        }
        if ($_.DeleteMarkers) {
            $_.DeleteMarkers | ForEach-Object {
                aws s3api delete-object --bucket $bucket --key $_.Key --version-id $_.VersionId 2>&1 | Out-Null
            }
        }
    }
    aws s3 rm "s3://$bucket" --recursive 2>&1 | Out-Null
    aws s3api delete-bucket --bucket $bucket --region $REGION 2>&1 | Out-Null
    Write-Host "  Deleted: $bucket" -ForegroundColor Green
}

Write-Host ""
Write-Host "[7/7] Deleting IAM Role..." -ForegroundColor Yellow
$roleName = $config.GLUE_ROLE
# Detach managed policies
@("arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole","arn:aws:iam::aws:policy/CloudWatchLogsFullAccess") | ForEach-Object {
    aws iam detach-role-policy --role-name $roleName --policy-arn $_ 2>&1 | Out-Null
}
# Delete inline policy
aws iam delete-role-policy --role-name $roleName --policy-name "GlueSalesS3Access" 2>&1 | Out-Null
aws iam delete-role --role-name $roleName 2>&1 | Out-Null
Write-Host "  IAM role deleted: $roleName" -ForegroundColor Green

Write-Host ""
Write-Host "================================================" -ForegroundColor Green
Write-Host "  CLEANUP COMPLETE - All resources removed" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Your AWS account is now clean. No ongoing charges."
Write-Host ""
