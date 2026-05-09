# =============================================================================
# AWS Glue Sales ETL Project - Step 5 (CLEAN & FIXED VERSION)
# =============================================================================

$ErrorActionPreference = "Stop"

$config = Get-Content ".\project-config.json" | ConvertFrom-Json

$BUCKET_PROCESSED = $config.processed
$BUCKET_ATHENA    = $config.athena
$ROLE_ARN         = $config.role
$GLUE_DB          = $config.glue_db
$AWS_REGION       = $config.region

$WORKGROUP        = "sales-etl-workgroup"
$CRAWLER_NAME     = "sales_processed_crawler"

Write-Host "`n==============================="
Write-Host " STEP 5 - ATHENA PIPELINE (FIXED)"
Write-Host "===============================`n"


# =================================================
# 1. WORKGROUP SAFE CREATE
# =================================================
Write-Host "[1] Checking Workgroup..." -ForegroundColor Yellow

$wgList = aws athena list-work-groups --region $AWS_REGION | ConvertFrom-Json
$exists = $wgList.WorkGroups | Where-Object { $_.Name -eq $WORKGROUP }

if (-not $exists) {

    Write-Host "Creating Workgroup..." -ForegroundColor Cyan

    $configJson = @{
        ResultConfiguration = @{
            OutputLocation = "s3://$BUCKET_ATHENA/results/"
            EncryptionConfiguration = @{
                EncryptionOption = "SSE_S3"
            }
        }
        BytesScannedCutoffPerQuery = 104857600
        EnforceWorkGroupConfiguration = $true
    } | ConvertTo-Json -Depth 10 -Compress

    aws athena create-work-group `
        --name $WORKGROUP `
        --configuration "$configJson" `
        --region $AWS_REGION | Out-Null

    Write-Host "Workgroup CREATED" -ForegroundColor Green
}
else {
    Write-Host "Workgroup EXISTS" -ForegroundColor Green
}


# =================================================
# 2. CRAWLER SAFE CREATE
# =================================================
Write-Host "`n[2] Checking Crawler..." -ForegroundColor Yellow

$crawlerExists = aws glue get-crawler --name $CRAWLER_NAME --region $AWS_REGION 2>$null

if ($LASTEXITCODE -ne 0) {

    Write-Host "Creating Crawler..." -ForegroundColor Cyan

    $targetsJson = @{
        S3Targets = @(
            @{ Path = "s3://$BUCKET_PROCESSED/sales/" }
        )
    } | ConvertTo-Json -Depth 5 -Compress

    aws glue create-crawler `
        --name $CRAWLER_NAME `
        --role $ROLE_ARN `
        --database-name $GLUE_DB `
        --targets "$targetsJson" `
        --region $AWS_REGION | Out-Null

    Write-Host "Crawler CREATED" -ForegroundColor Green
}
else {
    Write-Host "Crawler EXISTS" -ForegroundColor Green
}

Write-Host "Starting crawler..."
aws glue start-crawler --name $CRAWLER_NAME --region $AWS_REGION | Out-Null

do {
    Start-Sleep 5
    $c = aws glue get-crawler --name $CRAWLER_NAME --region $AWS_REGION | ConvertFrom-Json
} while ($c.Crawler.State -eq "RUNNING")

Write-Host "Crawler DONE" -ForegroundColor Green


# =================================================
# 3. ATHENA QUERY FUNCTION (FIXED SAFE)
# =================================================
function Run-Query {
    param($sql, $name)

    Write-Host "`nRunning: $name" -ForegroundColor Cyan

    $exec = aws athena start-query-execution `
        --query-string "$sql" `
        --work-group $WORKGROUP `
        --query-execution-context "Database=$GLUE_DB" `
        --region $AWS_REGION | ConvertFrom-Json

    if (-not $exec.QueryExecutionId) {
        Write-Host "FAILED: no execution id" -ForegroundColor Red
        return
    }

    $id = $exec.QueryExecutionId

    do {
        Start-Sleep 3
        $st = aws athena get-query-execution `
            --query-execution-id $id `
            --region $AWS_REGION | ConvertFrom-Json

        $state = $st.QueryExecution.Status.State
    } while ($state -in @("RUNNING","QUEUED"))

    if ($state -ne "SUCCEEDED") {
        Write-Host "FAILED: $name => $($st.QueryExecution.Status.StateChangeReason)" -ForegroundColor Red
        return
    }

    Write-Host "SUCCESS: $name" -ForegroundColor Green
}


# =================================================
# 4. QUERIES
# =================================================
Run-Query "SELECT category, COUNT(*) FROM sales GROUP BY category" "Revenue by Category"
Run-Query "SELECT salesperson, COUNT(*) FROM sales GROUP BY salesperson LIMIT 5" "Top Salespersons"
Run-Query "SELECT order_year, order_month, COUNT(*) FROM sales GROUP BY order_year, order_month" "Monthly Trend"
Run-Query "SELECT is_discounted, COUNT(*) FROM sales GROUP BY is_discounted" "Discount Impact"
Run-Query "SELECT revenue_tier, COUNT(*) FROM sales GROUP BY revenue_tier" "Revenue Tier"

# =================================================
Write-Host "`n===============================" -ForegroundColor Green
Write-Host " PIPELINE COMPLETED SUCCESSFULLY" -ForegroundColor Green
Write-Host "===============================" -ForegroundColor Green