#!/usr/bin/env bash
# Zip the common/ modules for deployment to Glue via --extra-py-files.
#
# Usage:
#   ./build_deps.sh
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
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

rm -f meta_common.zip
(cd common && zip -r ../meta_common.zip . -x '*__pycache__*')

echo "Built meta_common.zip"
