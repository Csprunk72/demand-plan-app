# Deploy to an existing Databricks App. Run in PowerShell after: databricks auth login
#
#   cd  C:\path\to\demand-plan-app
#   .\scripts\deploy.ps1 -AppName "your-app-name"
#
# Find AppName: Databricks UI -> Apps, or: databricks apps list
#
param(
    [Parameter(Mandatory = $true)]
    [string] $AppName
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
if (-not (Test-Path "app.py")) { throw "app.py not found; run from demand-plan-app" }

databricks apps deploy $AppName --skip-validation
