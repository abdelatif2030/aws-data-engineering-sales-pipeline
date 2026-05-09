# =============================================================================
# AWS Glue Sales ETL - Infrastructure Setup (PRO FIXED VERSION)
# =============================================================================

# ----------------------------
# CONFIGURATION
# ----------------------------
$AWS_REGION    = "us-east-1"
$PROJECT_NAME  = "sales-etl"
$UNIQUE_SUFFIX = Get-Random -Maximum 9999

$BUCKET_RAW       = "$PROJECT_NAME-raw-$UNIQUE_SUFFIX"
$BUCKET_PROCESSED = "$PROJECT_NAME-processed-$UNIQUE_SUFFIX"
$BUCKET_SCRIPTS   = "$PROJECT_NAME-scripts-$UNIQUE_SUFFIX"
$BUCKET_ATHENA    = "$PROJECT_NAME-athena-$UNIQUE_SUFFIX"

$GLUE_DB   = "sales_db"
$GLUE_ROLE = "GlueSalesETLRole"
$LOG_GROUP = "/aws-glue/jobs/sales-etl"

Write-Host "`n==== AWS Glue ETL Infrastructure Setup ====" -ForegroundColor Cyan

# ----------------------------
# CHECK AWS CLI
# ----------------------------
Write-Host "`n[CHECK] AWS CLI..." -ForegroundColor Yellow
aws --version

Write-Host "`n[CHECK] AWS Identity..." -ForegroundColor Yellow
$identity = aws sts get-caller-identity | ConvertFrom-Json
$ACCOUNT_ID = $identity.Account
Write-Host "Account: $ACCOUNT_ID"

# ----------------------------
# CREATE S3 BUCKETS
# ----------------------------
Write-Host "`n[1] Creating S3 Buckets..." -ForegroundColor Yellow

$buckets = @($BUCKET_RAW, $BUCKET_PROCESSED, $BUCKET_SCRIPTS, $BUCKET_ATHENA)

foreach ($bucket in $buckets) {

    Write-Host "Creating $bucket"

    aws s3api create-bucket --bucket $bucket --region $AWS_REGION

    aws s3api put-public-access-block `
        --bucket $bucket `
        --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

    if ($bucket -eq $BUCKET_RAW) {
        aws s3api put-bucket-versioning `
            --bucket $bucket `
            --versioning-configuration Status=Enabled
    }

    Write-Host "Created: s3://$bucket" -ForegroundColor Green
}

# ----------------------------
# IAM ROLE (SAFE + WAIT + VALIDATION)
# ----------------------------
Write-Host "`n[2] Creating IAM Role..." -ForegroundColor Yellow

$trustPolicy = @{
    Version = "2012-10-17"
    Statement = @(
        @{
            Effect = "Allow"
            Principal = @{ Service = "glue.amazonaws.com" }
            Action = "sts:AssumeRole"
        }
    )
} | ConvertTo-Json -Depth 10

$trustFile = "$env:TEMP\glue-trust.json"
[System.IO.File]::WriteAllText($trustFile, $trustPolicy, (New-Object System.Text.UTF8Encoding($false)))

# Check if role exists
$roleCheck = aws iam get-role --role-name $GLUE_ROLE 2>$null

if (-not $roleCheck) {
    aws iam create-role `
        --role-name $GLUE_ROLE `
        --assume-role-policy-document file://$trustFile
}

# Wait for IAM propagation (VERY IMPORTANT FIX)
Start-Sleep -Seconds 10

aws iam attach-role-policy `
    --role-name $GLUE_ROLE `
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy `
    --role-name $GLUE_ROLE `
    --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess

$ROLE_ARN = "arn:aws:iam::${ACCOUNT_ID}:role/$GLUE_ROLE"
Write-Host "Role ready: $ROLE_ARN" -ForegroundColor Green

# ----------------------------
# S3 POLICY
# ----------------------------
Write-Host "`n[2.1] S3 Policy..." -ForegroundColor Yellow

$s3Policy = @{
    Version = "2012-10-17"
    Statement = @(
        @{
            Effect = "Allow"
            Action = @("s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket")
            Resource = @(
                "arn:aws:s3:::$BUCKET_RAW",
                "arn:aws:s3:::$BUCKET_RAW/*",
                "arn:aws:s3:::$BUCKET_PROCESSED",
                "arn:aws:s3:::$BUCKET_PROCESSED/*",
                "arn:aws:s3:::$BUCKET_SCRIPTS",
                "arn:aws:s3:::$BUCKET_SCRIPTS/*"
            )
        }
    )
} | ConvertTo-Json -Depth 10

$s3File = "$env:TEMP\s3-policy.json"
[System.IO.File]::WriteAllText($s3File, $s3Policy, (New-Object System.Text.UTF8Encoding($false)))

aws iam put-role-policy `
    --role-name $GLUE_ROLE `
    --policy-name GlueS3Access `
    --policy-document file://$s3File

# ----------------------------
# GLUE DATABASE (NO BOM SAFE)
# ----------------------------
Write-Host "`n[3] Glue Database..." -ForegroundColor Yellow

$glueDB = @{
    Name = $GLUE_DB
    Description = "Sales analytics ETL database"
    LocationUri = "s3://$BUCKET_PROCESSED/"
} | ConvertTo-Json -Depth 5

$dbFile = "$env:TEMP\glue-db.json"
[System.IO.File]::WriteAllText($dbFile, $glueDB, (New-Object System.Text.UTF8Encoding($false)))

aws glue create-database `
    --database-input file://$dbFile `
    --region $AWS_REGION

Write-Host "Glue DB created: $GLUE_DB" -ForegroundColor Green

# ----------------------------
# CLOUDWATCH
# ----------------------------
Write-Host "`n[4] CloudWatch Logs..." -ForegroundColor Yellow

aws logs create-log-group --log-group-name $LOG_GROUP 2>$null
aws logs put-retention-policy `
    --log-group-name $LOG_GROUP `
    --retention-in-days 7

Write-Host "Logs ready" -ForegroundColor Green

# ----------------------------
# CONFIG SAVE
# ----------------------------
Write-Host "`n[5] Saving config..." -ForegroundColor Yellow

$config = @{
    region = $AWS_REGION
    account = $ACCOUNT_ID
    raw = $BUCKET_RAW
    processed = $BUCKET_PROCESSED
    scripts = $BUCKET_SCRIPTS
    athena = $BUCKET_ATHENA
    glue_db = $GLUE_DB
    role = $ROLE_ARN
    log_group = $LOG_GROUP
} | ConvertTo-Json -Depth 5

$config | Out-File ".\project-config.json" -Encoding UTF8

# ----------------------------
# DONE
# ----------------------------
Write-Host "`n=====================================" -ForegroundColor Green
Write-Host "SETUP COMPLETED SUCCESSFULLY" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green