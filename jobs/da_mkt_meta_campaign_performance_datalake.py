"""
AWS Glue job: pull campaign-level performance data from Meta's Marketing API
(Graph API Insights edge) and upsert it into an Iceberg table in S3.

Depends on the flat .py modules in ../common/ -- see ../README.md for how
they're packaged and attached via --extra-py-files. All auth, discovery,
retry, date-range, and Iceberg-upsert logic lives there and is shared with
the ad- and ad-set-level jobs; this file only declares what's specific to
the campaign level: which fields to request, the row schema, and the merge
key.

Glue job parameters expected (set as job arguments):

  --JOB_NAME                 (provided automatically by Glue)
  --SECRET_NAME               Secrets Manager secret name/ARN holding the
                               Meta System User access token, as JSON:
                               {"access_token": "..."}
  --AWS_REGION                e.g. us-east-1
  --ICEBERG_CATALOG           Glue Data Catalog name registered as an Iceberg catalog, e.g. "glue_catalog"
  --ICEBERG_DATABASE          target database name, e.g. "marketing"
  --ICEBERG_TABLE             target table name, e.g. "meta_campaign_performance"
  --ICEBERG_WAREHOUSE_PATH    s3://bucket/prefix for the Iceberg warehouse

Optional job parameters:

  --AD_ACCOUNT_IDS             comma-separated Meta ad account IDs (with or without
                                the "act_" prefix). If omitted (the normal case), the
                                job discovers every account the System User token has
                                been assigned in Business Manager. Pass this only to
                                restrict a run to a subset of accounts (e.g. testing).
  --START_DATE                 YYYY-MM-DD (inclusive). If omitted, computed from LOOKBACK_DAYS.
  --END_DATE                   YYYY-MM-DD (inclusive). If omitted, computed from LOOKBACK_DAYS.
  --LOOKBACK_DAYS               integer, default 14. Ignored if START_DATE/END_DATE are set.

Also pass, at the job level (not in this script):
  --datalake-formats iceberg
  --additional-python-modules requests>=2.31.0
  --extra-py-files s3://<your-bucket>/meta_common.zip

This script is written for Glue 4.0+ (Spark 3.3+, native Iceberg support).

Design notes specific to the campaign level:
- **Asynchronous reporting, not a synchronous GET** -- one async report job
  per account, submitted and polled via batched Graph API calls. See
  common/meta_async_insights.py for the flow, and
  da_mkt_meta_ad_performance_datalake.py's docstring plus ../README.md for why.
- One report per account, not one per campaign. level=campaign returns
  campaign-level rows for *every* campaign in the account -- see
  da_mkt_meta_ad_performance_datalake.py's docstring for the full explanation of
  why this differs from Pinterest's list-then-batch pattern.
- `objective` is included as dimensional context (Meta's insights rows do
  carry it), but there's no campaign status field on the Insights edge --
  status lives on the Campaign object itself, not point-in-time insights
  rows, and changes over time in a way a historical daily row can't
  meaningfully snapshot. See da_mkt_meta_dimension_datalake.py for
  current campaign status.
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from meta_accounts import resolve_ad_account_ids
from meta_async_insights import run_insights_reports
from meta_auth import get_access_token
from meta_dates import resolve_date_range
from meta_glue_args import resolve_args
from meta_iceberg import upsert
from meta_schema import build_table

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("da_mkt_meta_campaign_performance_datalake")

LEVEL = "campaign"

# Metric fields, deliberately kept identical across meta_ads/, meta_campaigns/,
# meta_ad_sets/, meta_ads_dma/, and meta_ads_demographics/ jobs -- see
# da_mkt_meta_ad_performance_datalake.py for the full rationale. Verified against
# Meta's live Ads Insights reference docs on 2026-08-19.
METRIC_FIELD_SPECS = [
    ("spend", "spend", "double"),
    ("impressions", "impressions", "bigint"),
    ("clicks", "clicks", "bigint"),
    ("reach", "reach", "bigint"),
    ("frequency", "frequency", "double"),
    ("cpc", "cpc", "double"),
    ("cpm", "cpm", "double"),
    ("ctr", "ctr", "double"),
    ("inline_link_clicks", "inline_link_clicks", "bigint"),
    ("inline_post_engagement", "inline_post_engagement", "bigint"),
    ("actions", "actions_json", "json"),
    ("cost_per_action_type", "cost_per_action_type_json", "json"),
    ("video_play_actions", "video_play_actions_json", "json"),
    ("video_p25_watched_actions", "video_p25_watched_actions_json", "json"),
    ("video_p50_watched_actions", "video_p50_watched_actions_json", "json"),
    ("video_p75_watched_actions", "video_p75_watched_actions_json", "json"),
    ("video_p100_watched_actions", "video_p100_watched_actions_json", "json"),
]

FIELD_SPECS = [
    ("campaign_id", "campaign_id", "string"),
    ("campaign_name", "campaign_name", "string"),
    ("objective", "objective", "string"),
    ("date_start", "stat_date", "date"),
] + METRIC_FIELD_SPECS

SCHEMA, ICEBERG_COLUMNS, to_row = build_table(FIELD_SPECS)
API_FIELDS = [f[0] for f in FIELD_SPECS]
KEY_COLUMNS = ["ad_account_id", "campaign_id", "stat_date"]
PARTITION_EXPR = "days(stat_date)"

REQUIRED_ARGS = [
    "JOB_NAME",
    "SECRET_NAME",
    "AWS_REGION",
    "ICEBERG_CATALOG",
    "ICEBERG_DATABASE",
    "ICEBERG_TABLE",
    "ICEBERG_WAREHOUSE_PATH",
]
OPTIONAL_ARGS = ["AD_ACCOUNT_IDS", "START_DATE", "END_DATE", "LOOKBACK_DAYS"]


def main():
    args = resolve_args(REQUIRED_ARGS, OPTIONAL_ARGS)

    catalog = args["ICEBERG_CATALOG"]
    database = args["ICEBERG_DATABASE"]
    table = args["ICEBERG_TABLE"]
    full_table_name = f"{catalog}.{database}.{table}"

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = (
        glueContext.spark_session.builder
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.warehouse", args["ICEBERG_WAREHOUSE_PATH"])
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # -- credentials --------------------------------------------------
    access_token = get_access_token(args["SECRET_NAME"], args["AWS_REGION"])

    # -- fetch -----------------------------------------------------------
    ad_account_ids = resolve_ad_account_ids(args, access_token)
    if not ad_account_ids:
        logger.warning("No ad accounts to pull (none visible to this token, or filter matched none)")
        job.commit()
        return

    start_date, end_date = resolve_date_range(args)
    logger.info("Pulling Meta campaign insights for %s..%s across %d account(s)",
                start_date, end_date, len(ad_account_ids))
    ingested_at = datetime.now(timezone.utc)

    rows_by_account = run_insights_reports(ad_account_ids, LEVEL, start_date, end_date,
                                           access_token, API_FIELDS)

    all_rows = []
    for ad_account_id, insights in rows_by_account.items():
        for record in insights:
            all_rows.append(to_row(ad_account_id, record, ingested_at))

    logger.info("Fetched %d campaign-day rows across %d ad account(s)", len(all_rows), len(ad_account_ids))

    if not all_rows:
        logger.info("No data returned for %s..%s, nothing to write", start_date, end_date)
        job.commit()
        return

    df = spark.createDataFrame(all_rows, schema=SCHEMA)
    upsert(spark, df, full_table_name, ICEBERG_COLUMNS, KEY_COLUMNS,
           PARTITION_EXPR, temp_view_name="meta_campaigns_source")

    job.commit()


if __name__ == "__main__":
    main()
