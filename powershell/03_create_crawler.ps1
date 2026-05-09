# =============================================================================
# AWS Glue Sales ETL Project - Step 3 (PRODUCTION FINAL FIX)
# =============================================================================

# ----------------------------
# LOAD CONFIG
# ----------------------------
$configPath = ".\project-config.json"

if (-not (Test-Path $configPath)) {
    throw "Config file not found. Run Step 1 first."
}

$config = Get-Content $configPath | ConvertFrom-Json

$BUCKET_RAW = $config.raw
$GLUE_DB    = $config.glue_db
$ROLE_ARN   = $config.role
$AWS_REGION = $config.region

if (-not $AWS_REGION) {
    throw "AWS_REGION missing in config"
}

$CRAWLER = "sales_raw_crawler"

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host " Step 3: Glue Crawler (FINAL PRODUCTION)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# ----------------------------
# BUILD SAFE TARGET FILE
# ----------------------------
$targets = @{
    S3Targets = @(
        @{
            Path = "s3://$BUCKET_RAW/sales/"
            Exclusions = @("**/.keep")
        }
    )
}

$targetsFile = "$env:TEMP\crawler-targets.json"
($targets | ConvertTo-Json -Depth 10) | Set-Content $targetsFile -Encoding UTF8

# ----------------------------
# CHECK IF CRAWLER EXISTS (ROBUST)
# ----------------------------
Write-Host "`n[1] Checking crawler..." -ForegroundColor Yellow

$exists = $false

try {
    aws glue get-crawler --name $CRAWLER --region $AWS_REGION | Out-Null
    $exists = $true
} catch {
    $exists = $false
}

if (-not $exists) {

    Write-Host "Creating crawler..." -ForegroundColor Yellow

    aws glue create-crawler `
        --name $CRAWLER `
        --role $ROLE_ARN `
        --database-name $GLUE_DB `
        --description "Sales raw data crawler" `
        --targets file://$targetsFile `
        --region $AWS_REGION

    if ($LASTEXITCODE -ne 0) {
        throw "Crawler creation failed"
    }

    Write-Host "Crawler created successfully" -ForegroundColor Green
}
else {
    Write-Host "Crawler already exists" -ForegroundColor Yellow
}

# ----------------------------
# START CRAWLER
# ----------------------------
Write-Host "`n[2] Starting crawler..." -ForegroundColor Yellow

aws glue start-crawler --name $CRAWLER --region $AWS_REGION

if ($LASTEXITCODE -ne 0) {
    throw "Failed to start crawler"
}

# ----------------------------
# WAIT FOR COMPLETION
# ----------------------------
Write-Host "`nWaiting for crawler completion..." -ForegroundColor Yellow

$state = ""
$i = 0

do {
    Start-Sleep -Seconds 10
    $i++

    $info = aws glue get-crawler --name $CRAWLER --region $AWS_REGION | ConvertFrom-Json
    $state = $info.Crawler.State

    Write-Host "[$($i*10)s] State: $state"

    if ($state -eq "FAILED") {
        throw "Crawler FAILED - check AWS Glue logs"
    }

} while ($state -in @("RUNNING","STOPPING") -and $i -lt 30)

Write-Host "`nFinal State: $state" -ForegroundColor Green

# ----------------------------
# SHOW TABLES
# ----------------------------
Write-Host "`n[3] Glue Catalog Tables..." -ForegroundColor Yellow

$tables = aws glue get-tables --database-name $GLUE_DB --region $AWS_REGION | ConvertFrom-Json

if ($tables.TableList.Count -eq 0) {
    Write-Host "No tables found" -ForegroundColor Yellow
}
else {
    foreach ($t in $tables.TableList) {
        Write-Host "`nTable: $($t.Name)" -ForegroundColor Green
        Write-Host "Location: $($t.StorageDescriptor.Location)"
        Write-Host "Columns: $($t.StorageDescriptor.Columns.Count)"
    }
}

# ----------------------------
# DONE
# ----------------------------
Write-Host "`n============================================" -ForegroundColor Green
Write-Host " CRAWLER COMPLETED SUCCESSFULLY" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green

Write-Host "`nNext: Run .\04_create_glue_job.ps1" -ForegroundColor Cyan