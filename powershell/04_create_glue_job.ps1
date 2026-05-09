$ErrorActionPreference = "Stop"

$config = Get-Content ".\project-config.json" | ConvertFrom-Json

$RAW = $config.raw
$PROCESSED = $config.processed
$SCRIPTS = $config.scripts
$ROLE = $config.role
$DB = $config.glue_db
$REGION = $config.region

$JOB = "sales_etl_job"
$SCRIPT = "s3://$SCRIPTS/glue-scripts/sales_etl_job.py"

Write-Host "STEP 4 START"

$argsFile = "$env:TEMP\args.json"
$cmdFile  = "$env:TEMP\cmd.json"

# -------------------------
# DEFAULT ARGUMENTS
# -------------------------
@{
 "--job-bookmark-option"="job-bookmark-enable"
 "--enable-metrics"="true"
 "--raw_bucket"=$RAW
 "--processed_bucket"=$PROCESSED
 "--database_name"=$DB
 "--table_name"="sales"
} | ConvertTo-Json -Depth 5 | Set-Content $argsFile -Encoding utf8

# -------------------------
# GLUE COMMAND (IMPORTANT FIX)
# -------------------------
@{
 Name="glueetl"
 ScriptLocation=$SCRIPT
 PythonVersion="3"
} | ConvertTo-Json -Depth 5 | Set-Content $cmdFile -Encoding utf8

# -------------------------
# CREATE JOB
# -------------------------
aws glue create-job `
 --name $JOB `
 --role $ROLE `
 --glue-version "4.0" `
 --worker-type "G.1X" `
 --number-of-workers 2 `
 --command file://$cmdFile `
 --default-arguments file://$argsFile `
 --region $REGION | Out-Null

Write-Host "JOB CREATED"

# -------------------------
# START JOB
# -------------------------
$run = aws glue start-job-run `
 --job-name $JOB `
 --region $REGION | ConvertFrom-Json

$id = $run.JobRunId
Write-Host "RUN ID: $id"

# -------------------------
# MONITOR
# -------------------------
$state = "UNKNOWN"

for ($i=0; $i -lt 30; $i++) {
 Start-Sleep 20

 $info = aws glue get-job-run `
  --job-name $JOB `
  --run-id $id `
  --region $REGION | ConvertFrom-Json

 $state = $info.JobRun.JobRunState

 Write-Host $state

 if ($state -in @("SUCCEEDED","FAILED","STOPPED","TIMEOUT")) {
  break
 }
}

# -------------------------
# RESULT
# -------------------------
Write-Host "FINAL: $state"

if ($state -eq "SUCCEEDED") {
 Write-Host "ETL SUCCESS ✔"
 Write-Host "OUTPUT: s3://$PROCESSED/sales/"
}
elseif ($state -eq "FAILED") {
 Write-Host "ETL FAILED ❌"
 Write-Host $info.JobRun.ErrorMessage
}

Write-Host "DONE"