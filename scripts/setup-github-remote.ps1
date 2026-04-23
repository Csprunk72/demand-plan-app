# After creating an empty GitHub repo (no README):
#   .\scripts\setup-github-remote.ps1 -RepoUrl "https://github.com/YOUR_USER/demand-plan-app.git"
param(
  [Parameter(Mandatory = $true)]
  [string] $RepoUrl
)
$ErrorActionPreference = "Stop"
$AppRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $AppRoot
if (-not (Test-Path "app.py")) { throw "app.py not found. Run from demand-plan-app." }

$old = $null
try { $old = git remote get-url origin 2>$null } catch { }
if ($old) {
  Write-Warning "Remote 'origin' is already $old"
  $ans = Read-Host "Remove and set to $RepoUrl ? [y/N]"
  if ($ans -ne "y" -and $ans -ne "Y") { exit 0 }
  git remote remove origin
}
git remote add origin $RepoUrl
Write-Host "Pushing branch main to origin…" -ForegroundColor Cyan
git push -u origin main
Write-Host "Done." -ForegroundColor Green
