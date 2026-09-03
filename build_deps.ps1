# Zip the common/ modules for deployment to Glue via --extra-py-files.
#
# Usage:
#   .\build_deps.ps1
#   aws s3 cp meta_common.zip s3://<your-bucket>/meta_common.zip
#
# Then set, on each of the six jobs in jobs/:
#   --extra-py-files s3://<your-bucket>/meta_common.zip
#
# The zip's root must contain the meta_*.py files directly, with NO
# wrapping "common" folder. This project starts from that structure
# deliberately: the sibling pinterest_ads_pipeline project originally used a
# `common` package (common/*.py + __init__.py, imported as `common.auth`) --
# the structure AWS's own docs describe as correct -- and it hit
# ModuleNotFoundError: No module named 'common' on a real Glue job run,
# matching a known unresolved AWS Glue issue with zipimport + --extra-py-files
# and nested packages (https://github.com/awslabs/aws-glue-libs/issues/173).
# Flat files avoid that machinery entirely -- each becomes directly
# importable once Glue adds this zip to sys.path.

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$zipPath = Join-Path $root "meta_common.zip"
$sourceDir = Join-Path $root "common"
$stageDir = Join-Path ([System.IO.Path]::GetTempPath()) ("meta_common_stage_" + [guid]::NewGuid())

if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}

# Compress-Archive has no exclude flag (unlike build_deps.sh's `zip -x`), so
# stage a filtered copy first -- otherwise any __pycache__/*.pyc left over
# from running/testing common/ locally silently ends up bundled into the
# deployed zip.
New-Item -ItemType Directory -Path $stageDir -Force | Out-Null
Copy-Item -Path (Join-Path $sourceDir "*") -Destination $stageDir -Recurse -Force
Get-ChildItem -Path $stageDir -Recurse -Directory -Filter "__pycache__" |
    Remove-Item -Recurse -Force
Get-ChildItem -Path $stageDir -Recurse -File -Include "*.pyc", "*.pyo" |
    Remove-Item -Force

# -Path with a wildcard archives each matched item at the zip's root, so the
# .py files land there directly instead of nested under a "common" folder.
Compress-Archive -Path (Join-Path $stageDir "*") -DestinationPath $zipPath
Remove-Item $stageDir -Recurse -Force

Write-Host "Built $zipPath"
