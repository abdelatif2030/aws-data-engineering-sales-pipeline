# =============================================================================
# AWS Glue Sales ETL Project - Step 2: Upload Data & Glue Scripts (FIXED)
# =============================================================================

# ----------------------------
# LOAD CONFIG SAFELY
# ----------------------------
$configPath = ".\project-config.json"

if (-not (Test-Path $configPath)) {
    throw "Config file not found. Run Step 1 first."
}

$config = Get-Content $configPath | ConvertFrom-Json

$BUCKET_RAW     = $config.raw
$BUCKET_SCRIPTS = $config.scripts
$AWS_REGION     = $config.region

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host " Step 2: Upload Data & Scripts to S3" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# ----------------------------
# STEP 1: UPLOAD CSV DATA
# ----------------------------
Write-Host "`n[1/3] Uploading sample sales data..." -ForegroundColor Yellow

$dataFiles = @(
    @{
        LocalPath = "..\sample-data\sales_2024_q1.csv"
        S3Key     = "sales/year=2024/month=01/sales_2024_q1.csv"
    }
)

foreach ($file in $dataFiles) {

    if (Test-Path $file.LocalPath) {

        aws s3 cp $file.LocalPath "s3://$BUCKET_RAW/$($file.S3Key)" | Out-Null

        Write-Host ("  Uploaded: " + $file.S3Key) -ForegroundColor Green
    }
    else {
        Write-Warning ("  Missing file: " + $file.LocalPath)
    }
}

# ----------------------------
# STEP 2: UPLOAD GLUE SCRIPT
# ----------------------------
Write-Host "`n[2/3] Uploading Glue ETL script..." -ForegroundColor Yellow

$etlScript = "..\glue-scripts\sales_etl_job.py"

if (Test-Path $etlScript) {

    aws s3 cp $etlScript "s3://$BUCKET_SCRIPTS/glue-scripts/sales_etl_job.py" | Out-Null

    Write-Host "  Script uploaded successfully" -ForegroundColor Green
}
else {
    Write-Warning "  ETL script not found"
}

# ----------------------------
# STEP 3: VERIFY UPLOADS
# ----------------------------
Write-Host "`n[3/3] Verifying S3 uploads..." -ForegroundColor Yellow

Write-Host "`nRaw bucket contents:" -ForegroundColor Cyan
aws s3 ls "s3://$BUCKET_RAW/" --recursive

Write-Host "`nScripts bucket contents:" -ForegroundColor Cyan
aws s3 ls "s3://$BUCKET_SCRIPTS/" --recursive

# ----------------------------
# DONE
# ----------------------------
Write-Host "`n============================================" -ForegroundColor Green
Write-Host " DATA & SCRIPTS UPLOAD COMPLETED" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green

Write-Host "`nNext step: Run .\03_create_crawler.ps1" -ForegroundColor Cyan