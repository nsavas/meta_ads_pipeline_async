"""
AWS Glue job: pull ad-level performance data broken down by geographic
market from Meta's Marketing API (Graph API Insights edge) and upsert it
into an Iceberg table in S3.

*** READ THIS BEFORE DEPLOYING -- Nielsen DMA vs. Comscore Market ***
Pinterest's DMA breakdown maps cleanly to Nielsen's classic 210 US media
markets (see the sibling pinterest_ads_dma_to_iceberg_glue_job.py). Meta's
`dma` breakdown was the same concept, but the evidence gathered while
building this job (2026-08-19) is genuinely mixed:
  - Meta's own live Marketing API breakdowns reference page still lists
    `dma` as a currently valid breakdown, with no deprecation notice.
  - Four independent production ETL vendors -- Fivetran, Airbyte, Rivery,
    and Supermetrics -- all separately confirm the Nielsen `dma` breakdown
    stopped returning results on 2026-06-22 (two months before this was
    written), replaced by a new `comscore_market` breakdown. Rivery's own
    changelog states it plainly: "Meta has fully retired Nielsen DMA across
    its reporting... choose Comscore Market to retain geographic-level
    reporting."
Given that convergent, independent operational evidence from multiple
production vendors (who'd have discovered this from real API calls
failing) outweighs a docs page that may simply not have been updated yet,
this job defaults to `comscore_market` -- GEO_BREAKDOWN below. If your
account turns out to still support legacy `dma` (e.g. on an older API
version, or if the retirement is more limited in scope than the vendor
reports suggest), this is a one-constant change: set GEO_BREAKDOWN = "dma"
and MARKET_COLUMN_COMMENT below no longer applies. Confirm against a real
API call before relying on either in production -- this is exactly the kind
of API-behavior nuance no OpenAPI spec exists to mechanically verify (see
../README.md's "No OpenAPI spec for Meta" section).

Depends on the flat .py modules in ../common/ -- see ../README.md for how
they're packaged and attached via --extra-py-files.

This is a companion to da_mkt_meta_ad_performance_datalake.py, not a replacement --
same /{ad_account_id}/insights endpoint and level=ad, same asynchronous
report flow (submit -> poll -> fetch, batched; see
common/meta_async_insights.py), with a `breakdowns` parameter added.
Produces a different grain of table: one row per (ad, date, market) instead
of one row per (ad, date).

Glue job parameters expected (set as job arguments):

  --JOB_NAME                 (provided automatically by Glue)
  --SECRET_NAME               Secrets Manager secret name/ARN holding the
                               Meta System User access token, as JSON:
                               {"access_token": "..."}
  --AWS_REGION                e.g. us-east-1
  --ICEBERG_CATALOG           Glue Data Catalog name registered as an Iceberg catalog, e.g. "glue_catalog"
  --ICEBERG_DATABASE          target database name, e.g. "marketing"
  --ICEBERG_TABLE             target table name, e.g. "meta_ad_market_performance"
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

Design notes:
- Meta's breakdown mechanism differs from Pinterest's: the breakdown value
  isn't nested under a separate "targeting_value" sibling field the way
  Pinterest's targeting_analytics rows work. Meta adds the breakdown value
  directly into the row under a key matching the breakdown's own name (e.g.
  requesting breakdowns=["comscore_market"] adds a "comscore_market" key to
  every row, alongside the requested `fields`). GEO_BREAKDOWN is therefore
  excluded from the `fields` request (API_FIELDS below) and passed via the
  separate `breakdowns` parameter instead -- requesting it in both would be
  redundant/invalid.
- No separate reference/lookup table is needed here (unlike Pinterest's
  pinterest_dma_reference_to_iceberg_glue_job.py for bare Nielsen codes) --
  confirm on your first real pull whether comscore_market's values come back
  as human-readable market names or as IDs needing a lookup; this wasn't
  confirmable from documentation alone. If it turns out to be a bare code,
  building a reference job mirroring the Pinterest one is the fix.
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
logger = logging.getLogger("da_mkt_meta_ad_dma_performance_datalake")

LEVEL = "ad"

# See the module docstring's Nielsen-DMA-vs-Comscore-Market section before
# changing this.
GEO_BREAKDOWN = "comscore_market"

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
    ("video_play_actions", "video_play_actions_json", "json"),
    ("video_p25_watched_actions", "video_p25_watched_actions_json", "json"),
    ("video_p50_watched_actions", "video_p50_watched_actions_json", "json"),
    ("video_p75_watched_actions", "video_p75_watched_actions_json", "json"),
    ("video_p100_watched_actions", "video_p100_watched_actions_json", "json"),
]

FIELD_SPECS = [
    ("ad_id", "ad_id", "string"),
    ("ad_name", "ad_name", "string"),
    ("adset_id", "ad_set_id", "string"),
    ("adset_name", "ad_set_name", "string"),
    ("campaign_id", "campaign_id", "string"),
    ("campaign_name", "campaign_name", "string"),
    (GEO_BREAKDOWN, "market", "string"),  # breakdown-derived -- see module docstring
    ("date_start", "stat_date", "date"),
] + METRIC_FIELD_SPECS

SCHEMA, ICEBERG_COLUMNS, to_row = build_table(FIELD_SPECS)
# GEO_BREAKDOWN is supplied via `breakdowns`, not `fields` -- requesting it
# in both would be redundant/invalid, so it's excluded here.
API_FIELDS = [f[0] for f in FIELD_SPECS if f[0] != GEO_BREAKDOWN]
BREAKDOWNS = [GEO_BREAKDOWN]
KEY_COLUMNS = ["ad_account_id", "ad_id", "market", "stat_date"]
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
    logger.info("Pulling Meta ad market (%s) insights for %s..%s across %d account(s)",
                GEO_BREAKDOWN, start_date, end_date, len(ad_account_ids))
    ingested_at = datetime.now(timezone.utc)

    rows_by_account = run_insights_reports(ad_account_ids, LEVEL, start_date, end_date,
                                           access_token, API_FIELDS, breakdowns=BREAKDOWNS)

    all_rows = []
    for ad_account_id, insights in rows_by_account.items():
        for record in insights:
            all_rows.append(to_row(ad_account_id, record, ingested_at))

    logger.info("Fetched %d ad-market-day rows across %d ad account(s)", len(all_rows), len(ad_account_ids))

    if not all_rows:
        logger.info("No data returned for %s..%s, nothing to write", start_date, end_date)
        job.commit()
        return

    df = spark.createDataFrame(all_rows, schema=SCHEMA)
    upsert(spark, df, full_table_name, ICEBERG_COLUMNS, KEY_COLUMNS,
           PARTITION_EXPR, temp_view_name="meta_ads_market_source")

    job.commit()


if __name__ == "__main__":
    main()
