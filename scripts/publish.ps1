# Publish: refresh dashboard data (new OP Submit) to DBFS, then deploy the Databricks App.
# Run in PowerShell from your machine where `databricks auth login` already works.
#
# One-time user env (optional — avoids passing -AppName every time):
#   [System.Environment]::SetEnvironmentVariable("DATABRICKS_APP_NAME", "your-app-name", "User")
#   [System.Environment]::SetEnvironmentVariable("OP_SUBMIT_XLSX", "C:\Users\Cspru1\Desktop\Cursor\OP Submit.xlsx", "User")
#
# Examples:
#   .\scripts\publish.ps1
#   .\scripts\publish.ps1 -AppName "demand-plan" -Data Snowflake
#   .\scripts\publish.ps1 -Data None          # app code only, no data refresh
#
param(
    [string] $AppName = $env:DATABRICKS_APP_NAME,
    [string] $OpSubmit = $env:OP_SUBMIT_XLSX,
    # Snowflake = run refresh_from_snowflake.py (APO from Snowflake + OP Submit) -> upload blob. Required for "Snowflake APO" in the UI.
    # None        = deploy app code only; blob unchanged (still Excel if last push was Excel).
    [ValidateSet("Snowflake", "Excel", "None")]
    [string] $Data = "Snowflake",
    [string] $DatabricksProfile = "4428761713917856",
    [string] $OpSubmitDbfs = "dbfs:/FileStore/ebp_dashboard/OP_Submit.xlsx",
    [switch] $CopyOpToDbfs
)

$ErrorActionPreference = "Stop"
$AppRoot = Join-Path $PSScriptRoot ".."
Set-Location $AppRoot
if (-not (Test-Path "app.py")) { throw "app.py not found. Run from demand-plan-app\scripts" }

if ([string]::IsNullOrWhiteSpace($AppName)) {
    throw "Set env DATABRICKS_APP_NAME or pass -AppName (name from: databricks apps list)"
}

# Default OP Submit path (Cursor desktop folder)
if ([string]::IsNullOrWhiteSpace($OpSubmit) -and $Data -ne "None") {
    $OpSubmit = "C:\Users\Cspru1\Desktop\Cursor\OP Submit.xlsx"
}

Write-Host "==> Databricks profile check (profile: $DatabricksProfile)..." -ForegroundColor Cyan
databricks apps list -p $DatabricksProfile | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Databricks CLI is not logged in for profile '$DatabricksProfile'. Run: databricks auth login --profile $DatabricksProfile"
}

if ($Data -eq "Snowflake") {
    if (-not (Test-Path -LiteralPath $OpSubmit)) { throw "OP Submit not found: $OpSubmit" }
    Write-Host "==> refresh_from_snowflake.py (Snowflake + OP Submit)..." -ForegroundColor Cyan
    python refresh_from_snowflake.py --op-submit-xlsx $OpSubmit
    if ($LASTEXITCODE -ne 0) { throw "Data refresh failed" }
}
elseif ($Data -eq "Excel") {
    if (-not (Test-Path -LiteralPath $OpSubmit)) { throw "OP Submit not found: $OpSubmit" }
    Write-Host "==> push_demand_plan.py (Excel demand plan + OP Submit)..." -ForegroundColor Cyan
    python push_demand_plan.py --op-submit-xlsx $OpSubmit
    if ($LASTEXITCODE -ne 0) { throw "Data push failed" }
}

if ($CopyOpToDbfs -and $Data -ne "None") {
    Write-Host "==> Copy OP Submit to DBFS for job image ($OpSubmitDbfs)..." -ForegroundColor Cyan
    databricks fs cp --overwrite --profile $DatabricksProfile (Resolve-Path $OpSubmit) $OpSubmitDbfs
}

Write-Host "==> databricks apps deploy $AppName ..." -ForegroundColor Cyan
databricks apps deploy $AppName --skip-validation -p $DatabricksProfile
if ($LASTEXITCODE -ne 0) { throw "Deploy failed" }

Write-Host "Done. Open the app URL; optional cache bust: POST /api/v1/refresh on the app." -ForegroundColor Green
