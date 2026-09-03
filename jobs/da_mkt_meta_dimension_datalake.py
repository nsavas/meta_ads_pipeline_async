"""
AWS Glue job: pull the full Campaign, Ad Set, and Ad dimension/metadata
objects from Meta's Marketing API (Graph API) -- name, status, budget,
targeting, creative, etc. -- and write them to three Iceberg tables in S3.
No performance metrics here; see da_mkt_meta_ad_performance_datalake.py,
da_mkt_meta_campaign_performance_datalake.py, and da_mkt_meta_ad_set_performance_datalake.py
for those.

Depends on the flat .py modules in ../common/ -- see ../README.md for how
they're packaged and attached via --extra-py-files.

Glue job parameters expected (set as job arguments):

  --JOB_NAME                     (provided automatically by Glue)
  --SECRET_NAME                   Secrets Manager secret name/ARN holding the
                                   Meta System User access token, as JSON:
                                   {"access_token": "..."}
  --AWS_REGION                    e.g. us-east-1
  --ICEBERG_CATALOG               Glue Data Catalog name registered as an Iceberg catalog, e.g. "glue_catalog"
  --ICEBERG_DATABASE              target database name, e.g. "marketing"
  --ICEBERG_WAREHOUSE_PATH        s3://bucket/prefix for the Iceberg warehouse
  --ICEBERG_TABLE_CAMPAIGNS       target table for campaign dimensions, e.g. "meta_campaign_dim"
  --ICEBERG_TABLE_AD_SETS         target table for ad set dimensions, e.g. "meta_ad_set_dim"
  --ICEBERG_TABLE_ADS             target table for ad dimensions, e.g. "meta_ad_dim"

Optional job parameters:

  --AD_ACCOUNT_IDS                comma-separated Meta ad account IDs (with or without
                                   the "act_" prefix). If omitted (the normal case), the
                                   job discovers every account the System User token has
                                   been assigned in Business Manager. Pass this only to
                                   restrict a run to a subset of accounts (e.g. testing).
  --START_DATE                    YYYY-MM-DD (inclusive). If omitted, computed from LOOKBACK_DAYS.
  --END_DATE                      YYYY-MM-DD (inclusive). If omitted, computed from LOOKBACK_DAYS.
  --LOOKBACK_DAYS                 integer, default 14. Ignored if START_DATE/END_DATE are set.

Unlike the earlier version of this job, entities ARE now filtered by
created_time -- see "created_time filtering + upsert, not a full snapshot"
below for why, and for an important note on backfilling existing entities
before relying on the default rolling window.

Also pass, at the job level (not in this script):
  --datalake-formats iceberg
  --additional-python-modules requests>=2.31.0
  --extra-py-files s3://<your-bucket>/meta_common.zip

This script is written for Glue 4.0+ (Spark 3.3+, native Iceberg support).

Design notes:
- Same account-discovery pattern as every other job in this project: list
  every ad account first, then pull per account. Unlike Pinterest, Meta's
  /{ad_account_id}/campaigns (and /adsets, /ads) list edges require an
  explicit `fields` parameter -- there's no "return everything" default --
  so the full field list per entity is spelled out below in FIELD_SPECS,
  built from Meta's live Graph API reference docs
  (https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group/,
  .../ad-campaign/, .../adgroup/) fetched on 2026-08-19: Campaign (39
  fields), AdSet (62 fields -- see note below on the one field dropped), Ad
  (34 fields -- see notes below on the permission-gated field dropped and
  the four heavy nested-object fields trimmed for request cost).
- AdSet's `contextual_bundling_spec` field is deliberately excluded, even
  though it's documented on the AdSet object reference. Confirmed in
  production (2026-08-30) that requesting it returns Meta error (#3)
  "AdAccount must pass GK: contextual_bundle_test_api_accounts" -- it's
  gated behind a Gatekeeper flag only accounts enrolled in that specific
  beta program have, and most ad accounts (including the one this was
  diagnosed against) aren't enrolled. This is a hard permission failure,
  not a retryable error, so there's no reasonable way to request it
  unconditionally for every account. If you know your accounts *are*
  enrolled in that program, add `("contextual_bundling_spec",
  "contextual_bundling_spec_json", "json")` back into AD_SET_FIELD_SPECS.
- Ad's `special_ad_categories` field is likewise deliberately excluded.
  Confirmed in production (2026-08-30), isolated via field-list bisection
  in Postman: requesting it returns Meta error (#3) "App must be on the
  whitelist" -- a different gate than AdSet's (this one's tied to Meta's
  Special Ad Category program for housing/employment/credit/social-issue
  ads, which requires separate app review), but the same practical result:
  a hard permission failure for any app not enrolled, unconditionally
  across every account. `creative_asset_groups_spec` was also suspected
  during this investigation (it's the newest/most beta-flavored field on
  the Ad object) but confirmed clean -- it's requested normally. If your
  app is confirmed enrolled in the Special Ad Category program, add
  `("special_ad_categories", "special_ad_categories_json", "json")` back
  into AD_FIELD_SPECS.
- Ad's `targeting`, `tracking_and_conversion_with_defaults`, `tracking_specs`,
  and `issues_info` fields are deliberately trimmed too, but for a different
  reason than the two above -- these aren't permission-gated, they're just
  large/deeply-nested objects, and on accounts with 5,500+ ads (confirmed in
  production, 2026-08-30) even meta_config.DETAIL_PAGE_SIZE (25) wasn't
  enough to avoid recurring cost-based throttle 500s (the same "Please
  reduce the amount of data you're asking for" throttle documented below).
  Trimming these four cuts the actual data weight of every page, unlike
  lowering DETAIL_PAGE_SIZE further, which only redistributes the *same*
  total cost across more requests -- for an account this large, the
  throttle behaves like a time-windowed cost budget on total data pulled
  (see the original 500 diagnosis: an identical request succeeded once,
  then failed instantly on an immediate retry), so cutting payload weight
  is the lever that actually reduces total cost rather than just spreading
  it out. If you need any of these four for downstream analysis, consider a
  narrower follow-up call to the specific ad's own edge instead of carrying
  them on every row of a 5,500+-row full-account pull.
- Every scalar field (string/int/bool/number) becomes its own typed column.
  Every nested object, list, or map-typed field (`targeting`, `promoted_object`,
  `adlabels`, `bid_info`, `creative`, etc.) is serialized to a `..._json`
  string column instead of a native Spark struct/array column -- same
  reasoning as the sibling Pinterest project's dimensions job: these shapes
  vary by campaign objective/ad type, and a rigid Spark schema would break
  or silently null fields the moment Meta returns a shape it wasn't built
  from. Query those columns with `from_json`/`get_json_object` in Spark or
  `json_extract` in Athena.
- Datetime fields (`created_time`, `updated_time`, `start_time`, `end_time`,
  etc.) come back from Meta as ISO-8601 strings (e.g. "2023-11-14T22:13:20+0000"),
  *not* Unix-seconds integers the way Pinterest's did -- confirmed from the
  fetched reference pages' "datetime" type annotation. They're passed through
  as strings in the DataFrame and cast via `CAST(col AS timestamp)` at merge
  time (same technique already used for stat_date elsewhere in this
  project), since Spark parses ISO-8601-with-offset natively.
- `account_id` is deliberately excluded from each entity's own field list --
  it's redundant with the `ad_account_id` column already populated from the
  loop variable (the account we're actually querying), which is the more
  trustworthy source of truth than trusting the field to always round-trip
  correctly.
- **created_time filtering + upsert, not a full snapshot (changed 2026-09-01).**
  Every list_entities() call now passes a `filtering` param -- Meta's
  documented mechanism for filtering *which entities an edge returns*
  (unlike `date_preset`/`time_range`, which only scope computed stats
  fields and don't affect which objects come back -- confirmed against
  Meta's docs before using either). Each entity is filtered on its own
  `created_time` (field name convention "<entity>.created_time", e.g.
  "campaign.created_time" -- based on community/SDK examples, not the
  official per-edge reference page, so confirm the filter is actually
  narrowing results on your first real run rather than being silently
  ignored) using GREATER_THAN_OR_EQUAL/LESS_THAN_OR_EQUAL against the same
  START_DATE/END_DATE/LOOKBACK_DAYS window the performance jobs use (see
  meta_dates.resolve_date_range()), as Unix-epoch-seconds values in UTC.
  This was added to bound the request volume on very large accounts,
  parallel to (and compounding with) the field-trimming fix above.

  This forced a second change: since each run now only sees entities
  *created* within the window, the previous full CREATE OR REPLACE TABLE
  (see meta_iceberg.py's replace_table()) would wipe out every
  older-than-window entity's row on every run, even ones still active --
  the opposite failure mode replace_table() was originally chosen to avoid.
  All three tables now use upsert() (MERGE INTO, keyed on
  ad_account_id + the entity's own id) instead. **This reintroduces the
  problem replace_table() solved**: a campaign/ad set/ad deleted on Meta's
  side no longer disappears from the table, it lingers with whatever
  status/fields it had as of its last successful pull. Filter on
  `effective_status`/`configured_status` downstream (e.g. exclude
  "DELETED"/"ARCHIVED") if stale entities need to be excluded from queries.

  **Backfill note:** the default LOOKBACK_DAYS (14) window means an entity
  created before that window will *never* be pulled unless a run's
  START_DATE/END_DATE happens to cover its creation date. Before relying on
  the incremental default, run this job at least once with a START_DATE far
  enough back to cover every entity you care about (or an equivalently
  large LOOKBACK_DAYS) to seed the tables -- otherwise older, still-active
  entities created outside every window this job has ever run with will
  simply never appear.

  Tables are still written once each, at the very end, after every account
  has been fetched -- so a mid-run failure leaves the existing tables
  untouched rather than partially merged.

SCHEMA / ICEBERG_COLUMNS / the row-builder for each entity are derived from
one field-spec list via common/meta_schema.py's build_table(), rather than
three hand-written, independently-maintained lists (as the Pinterest
project's dimensions job does) -- deliberate, given these entities have
34-62 fields each, larger than anything in the sibling project. Keeping one
list per entity as the single source of truth makes a transposition bug (a
column landing under the wrong name) structurally impossible rather than
something to catch by testing afterwards.

Requests every entity at meta_config.DETAIL_PAGE_SIZE (25) per page, not the
default 100 -- confirmed in production that requesting the full 34-62-field
object for 100 entities per page, across every page of a 100+-campaign
account, trips Meta's cost-based throttle ("Please reduce the amount of data
you're asking for"), returned as an HTTP 500 that a same-request retry
doesn't fix. This job is the one place in the project pulling that much data
per object; the ID-only calls the performance jobs make stay at the default
page size. See meta_config.py's DETAIL_PAGE_SIZE docstring for the full story.
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from meta_accounts import list_entities, resolve_ad_account_ids
from meta_config import DETAIL_PAGE_SIZE
from meta_auth import get_access_token
from meta_dates import resolve_date_range
from meta_glue_args import resolve_args
from meta_iceberg import upsert
from meta_schema import build_table

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("da_mkt_meta_dimension_datalake")


# --------------------------------------------------------------------------
# Campaign fields (39, from Meta's Campaign object reference)
# --------------------------------------------------------------------------
CAMPAIGN_FIELD_SPECS = [
    ("id", "campaign_id", "string"),
    ("adlabels", "adlabels_json", "json"),
    ("bid_strategy", "bid_strategy", "string"),
    ("boosted_object_id", "boosted_object_id", "string"),
    ("brand_lift_studies", "brand_lift_studies_json", "json"),
    ("budget_rebalance_flag", "budget_rebalance_flag", "boolean"),
    ("budget_remaining", "budget_remaining", "double"),
    ("buying_type", "buying_type", "string"),
    ("campaign_group_active_time", "campaign_group_active_time", "string"),
    ("can_create_brand_lift_study", "can_create_brand_lift_study", "boolean"),
    ("can_use_spend_cap", "can_use_spend_cap", "boolean"),
    ("configured_status", "configured_status", "string"),
    ("created_time", "created_time", "timestamp"),
    ("daily_budget", "daily_budget", "double"),
    ("effective_status", "effective_status", "string"),
    ("has_secondary_skadnetwork_reporting", "has_secondary_skadnetwork_reporting", "boolean"),
    ("is_adset_budget_sharing_enabled", "is_adset_budget_sharing_enabled", "boolean"),
    ("is_budget_schedule_enabled", "is_budget_schedule_enabled", "boolean"),
    ("is_reels_trending_ads_enabled", "is_reels_trending_ads_enabled", "boolean"),
    ("is_skadnetwork_attribution", "is_skadnetwork_attribution", "boolean"),
    ("issues_info", "issues_info_json", "json"),
    ("last_budget_toggling_time", "last_budget_toggling_time", "timestamp"),
    ("lifetime_budget", "lifetime_budget", "double"),
    ("name", "name", "string"),
    ("objective", "objective", "string"),
    ("pacing_type", "pacing_type_json", "json"),
    ("primary_attribution", "primary_attribution", "string"),
    ("promoted_object", "promoted_object_json", "json"),
    ("smart_promotion_type", "smart_promotion_type", "string"),
    ("source_campaign", "source_campaign_json", "json"),
    ("source_campaign_id", "source_campaign_id", "string"),
    ("special_ad_categories", "special_ad_categories_json", "json"),
    ("special_ad_category", "special_ad_category", "string"),
    ("special_ad_category_country", "special_ad_category_country_json", "json"),
    ("spend_cap", "spend_cap", "double"),
    ("start_time", "start_time", "timestamp"),
    ("status", "status", "string"),
    ("stop_time", "stop_time", "timestamp"),
    ("topline_id", "topline_id", "string"),
    ("updated_time", "updated_time", "timestamp"),
]

# --------------------------------------------------------------------------
# AdSet fields (62, from Meta's AdSet object reference -- excludes
# contextual_bundling_spec, see module docstring's note on that field)
# --------------------------------------------------------------------------
AD_SET_FIELD_SPECS = [
    ("id", "ad_set_id", "string"),
    ("adlabels", "adlabels_json", "json"),
    ("adset_schedule", "adset_schedule_json", "json"),
    ("asset_feed_id", "asset_feed_id", "string"),
    ("attribution_spec", "attribution_spec_json", "json"),
    ("bid_adjustments", "bid_adjustments_json", "json"),
    ("bid_amount", "bid_amount", "bigint"),
    ("bid_constraints", "bid_constraints_json", "json"),
    ("bid_info", "bid_info_json", "json"),
    ("bid_strategy", "bid_strategy", "string"),
    ("billing_event", "billing_event", "string"),
    ("brand_safety_config", "brand_safety_config_json", "json"),
    ("budget_remaining", "budget_remaining", "double"),
    ("campaign", "campaign_json", "json"),
    ("campaign_active_time", "campaign_active_time", "string"),
    ("campaign_attribution", "campaign_attribution", "string"),
    ("campaign_id", "campaign_id", "string"),
    ("configured_status", "configured_status", "string"),
    ("created_time", "created_time", "timestamp"),
    ("creative_sequence", "creative_sequence_json", "json"),
    ("daily_budget", "daily_budget", "double"),
    ("daily_min_spend_target", "daily_min_spend_target", "double"),
    ("daily_spend_cap", "daily_spend_cap", "double"),
    ("destination_type", "destination_type", "string"),
    ("dsa_beneficiary", "dsa_beneficiary", "string"),
    ("dsa_payor", "dsa_payor", "string"),
    ("effective_status", "effective_status", "string"),
    ("end_time", "end_time", "timestamp"),
    ("frequency_control_specs", "frequency_control_specs_json", "json"),
    ("instagram_user_id", "instagram_user_id", "string"),
    ("is_dynamic_creative", "is_dynamic_creative", "boolean"),
    ("is_incremental_attribution_enabled", "is_incremental_attribution_enabled", "boolean"),
    ("issues_info", "issues_info_json", "json"),
    ("learning_stage_info", "learning_stage_info_json", "json"),
    ("lifetime_budget", "lifetime_budget", "double"),
    ("lifetime_imps", "lifetime_imps", "bigint"),
    ("lifetime_min_spend_target", "lifetime_min_spend_target", "double"),
    ("lifetime_spend_cap", "lifetime_spend_cap", "double"),
    ("min_budget_spend_percentage", "min_budget_spend_percentage", "double"),
    ("multi_optimization_goal_weight", "multi_optimization_goal_weight", "string"),
    ("name", "name", "string"),
    ("optimization_goal", "optimization_goal", "string"),
    ("optimization_sub_event", "optimization_sub_event", "string"),
    ("pacing_type", "pacing_type_json", "json"),
    ("promoted_object", "promoted_object_json", "json"),
    ("recommendations", "recommendations_json", "json"),
    ("recurring_budget_semantics", "recurring_budget_semantics", "boolean"),
    ("regional_regulated_categories", "regional_regulated_categories_json", "json"),
    ("regional_regulation_identities", "regional_regulation_identities_json", "json"),
    ("review_feedback", "review_feedback", "string"),
    ("rf_prediction_id", "rf_prediction_id", "string"),
    ("source_adset", "source_adset_json", "json"),
    ("source_adset_id", "source_adset_id", "string"),
    ("start_time", "start_time", "timestamp"),
    ("status", "status", "string"),
    ("targeting", "targeting_json", "json"),
    ("targeting_optimization_types", "targeting_optimization_types_json", "json"),
    ("time_based_ad_rotation_id_blocks", "time_based_ad_rotation_id_blocks_json", "json"),
    ("time_based_ad_rotation_intervals", "time_based_ad_rotation_intervals_json", "json"),
    ("updated_time", "updated_time", "timestamp"),
    ("use_new_app_click", "use_new_app_click", "boolean"),
    ("value_rule_set_id", "value_rule_set_id", "string"),
]

# --------------------------------------------------------------------------
# Ad fields (34, from Meta's Ad object reference -- excludes
# special_ad_categories (permission-gated) and targeting/
# tracking_and_conversion_with_defaults/tracking_specs/issues_info
# (trimmed for request cost on large accounts), see module docstring's
# notes on those fields)
# --------------------------------------------------------------------------
AD_FIELD_SPECS = [
    ("id", "ad_id", "string"),
    ("ad_active_time", "ad_active_time", "string"),
    ("ad_review_feedback", "ad_review_feedback_json", "json"),
    ("ad_schedule_end_time", "ad_schedule_end_time", "timestamp"),
    ("ad_schedule_start_time", "ad_schedule_start_time", "timestamp"),
    ("adlabels", "adlabels_json", "json"),
    ("adset", "adset_json", "json"),
    ("adset_id", "ad_set_id", "string"),
    ("bid_amount", "bid_amount", "bigint"),
    ("bid_info", "bid_info_json", "json"),
    ("bid_type", "bid_type", "string"),
    ("campaign", "campaign_json", "json"),
    ("campaign_id", "campaign_id", "string"),
    ("configured_status", "configured_status", "string"),
    ("conversion_domain", "conversion_domain", "string"),
    ("conversion_specs", "conversion_specs_json", "json"),
    ("created_time", "created_time", "timestamp"),
    ("creative", "creative_json", "json"),
    ("creative_asset_groups_spec", "creative_asset_groups_spec_json", "json"),
    ("demolink_hash", "demolink_hash", "string"),
    ("display_sequence", "display_sequence", "bigint"),
    ("effective_status", "effective_status", "string"),
    ("engagement_audience", "engagement_audience", "boolean"),
    ("failed_delivery_checks", "failed_delivery_checks_json", "json"),
    ("is_autobid", "is_autobid", "boolean"),
    ("last_updated_by_app_id", "last_updated_by_app_id", "string"),
    ("name", "name", "string"),
    ("preview_shareable_link", "preview_shareable_link", "string"),
    ("priority", "priority", "bigint"),
    ("recommendations", "recommendations_json", "json"),
    ("source_ad", "source_ad_json", "json"),
    ("source_ad_id", "source_ad_id", "string"),
    ("status", "status", "string"),
    ("updated_time", "updated_time", "timestamp"),
]

CAMPAIGN_SCHEMA, CAMPAIGN_ICEBERG_COLUMNS, campaign_to_row = build_table(CAMPAIGN_FIELD_SPECS)
AD_SET_SCHEMA, AD_SET_ICEBERG_COLUMNS, ad_set_to_row = build_table(AD_SET_FIELD_SPECS)
AD_SCHEMA, AD_ICEBERG_COLUMNS, ad_to_row = build_table(AD_FIELD_SPECS)

CAMPAIGN_API_FIELDS = [f[0] for f in CAMPAIGN_FIELD_SPECS]
AD_SET_API_FIELDS = [f[0] for f in AD_SET_FIELD_SPECS]
AD_API_FIELDS = [f[0] for f in AD_FIELD_SPECS]

# Merge keys for upsert() -- each entity's own id is already globally unique
# in Meta's system, but ad_account_id is included too for consistency with
# every other job's KEY_COLUMNS convention in this project.
CAMPAIGN_KEY_COLUMNS = ["ad_account_id", "campaign_id"]
AD_SET_KEY_COLUMNS = ["ad_account_id", "ad_set_id"]
AD_KEY_COLUMNS = ["ad_account_id", "ad_id"]
# All three entities carry created_time, so partition on it uniformly --
# also the column the created_time filtering below is scoped to, so rows
# from a given run land in a small number of partitions.
PARTITION_EXPR = "days(created_time)"


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

REQUIRED_ARGS = [
    "JOB_NAME",
    "SECRET_NAME",
    "AWS_REGION",
    "ICEBERG_CATALOG",
    "ICEBERG_DATABASE",
    "ICEBERG_WAREHOUSE_PATH",
    "ICEBERG_TABLE_CAMPAIGNS",
    "ICEBERG_TABLE_AD_SETS",
    "ICEBERG_TABLE_ADS",
]
OPTIONAL_ARGS = ["AD_ACCOUNT_IDS", "START_DATE", "END_DATE", "LOOKBACK_DAYS"]


def _created_time_filter(entity_type: str, start_ts: int, end_ts: int) -> list:
    """Build a Marketing API `filtering` list scoping `entity_type`'s
    created_time to [start_ts, end_ts] inclusive, as Unix-epoch-seconds.

    entity_type: "campaign", "adset", or "ad" -- matches the field-name
    convention Meta's filtering param is documented (via community/SDK
    examples) to expect: "<entity_type>.created_time".

    Uses strict GREATER_THAN/LESS_THAN, not the _OR_EQUAL variants -- confirmed
    in production (2026-09-01) that GREATER_THAN_OR_EQUAL is rejected as an
    unsupported operator on this field/edge, matching real-world examples
    seen elsewhere (e.g. Node.js SDK usage) that only ever show plain
    GREATER_THAN/LESS_THAN, never the _OR_EQUAL forms. The +/-1 second
    adjustment below keeps the boundary values themselves inside the window
    despite the operators being strict.
    """
    return [
        {"field": f"{entity_type}.created_time", "operator": "GREATER_THAN", "value": start_ts - 1},
        {"field": f"{entity_type}.created_time", "operator": "LESS_THAN", "value": end_ts + 1},
    ]


def main():
    args = resolve_args(REQUIRED_ARGS, OPTIONAL_ARGS)

    catalog = args["ICEBERG_CATALOG"]
    database = args["ICEBERG_DATABASE"]

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
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 86399
    logger.info("Pulling Meta campaign/ad set/ad dimensions created %s..%s across %d account(s)",
                start_date, end_date, len(ad_account_ids))
    ingested_at = datetime.now(timezone.utc)

    campaign_rows, ad_set_rows, ad_rows = [], [], []
    for ad_account_id in ad_account_ids:
        for c in list_entities(ad_account_id, access_token, "campaigns", CAMPAIGN_API_FIELDS,
                                page_size=DETAIL_PAGE_SIZE,
                                filtering=_created_time_filter("campaign", start_ts, end_ts)):
            campaign_rows.append(campaign_to_row(ad_account_id, c, ingested_at))

        for a in list_entities(ad_account_id, access_token, "adsets", AD_SET_API_FIELDS,
                                page_size=DETAIL_PAGE_SIZE,
                                filtering=_created_time_filter("adset", start_ts, end_ts)):
            ad_set_rows.append(ad_set_to_row(ad_account_id, a, ingested_at))

        for a in list_entities(ad_account_id, access_token, "ads", AD_API_FIELDS,
                                page_size=DETAIL_PAGE_SIZE,
                                filtering=_created_time_filter("ad", start_ts, end_ts)):
            ad_rows.append(ad_to_row(ad_account_id, a, ingested_at))

    logger.info("Fetched %d campaign(s), %d ad set(s), %d ad(s) across %d account(s)",
                len(campaign_rows), len(ad_set_rows), len(ad_rows), len(ad_account_ids))

    # -- write -------------------------------------------------------------
    # Each table is written once, at the end, after every account has been
    # fetched -- so a mid-run failure leaves existing tables untouched
    # rather than partially merged. MERGE/upsert, not full replace: see
    # module docstring's "created_time filtering + upsert" note for why.
    if campaign_rows:
        df = spark.createDataFrame(campaign_rows, schema=CAMPAIGN_SCHEMA)
        upsert(spark, df, f"{catalog}.{database}.{args['ICEBERG_TABLE_CAMPAIGNS']}",
               CAMPAIGN_ICEBERG_COLUMNS, CAMPAIGN_KEY_COLUMNS, PARTITION_EXPR,
               temp_view_name="meta_campaigns_dim_source")
    else:
        logger.warning("No campaigns created in %s..%s, leaving %s untouched",
                        start_date, end_date, args["ICEBERG_TABLE_CAMPAIGNS"])

    if ad_set_rows:
        df = spark.createDataFrame(ad_set_rows, schema=AD_SET_SCHEMA)
        upsert(spark, df, f"{catalog}.{database}.{args['ICEBERG_TABLE_AD_SETS']}",
               AD_SET_ICEBERG_COLUMNS, AD_SET_KEY_COLUMNS, PARTITION_EXPR,
               temp_view_name="meta_ad_sets_dim_source")
    else:
        logger.warning("No ad sets created in %s..%s, leaving %s untouched",
                        start_date, end_date, args["ICEBERG_TABLE_AD_SETS"])

    if ad_rows:
        df = spark.createDataFrame(ad_rows, schema=AD_SCHEMA)
        upsert(spark, df, f"{catalog}.{database}.{args['ICEBERG_TABLE_ADS']}",
               AD_ICEBERG_COLUMNS, AD_KEY_COLUMNS, PARTITION_EXPR,
               temp_view_name="meta_ads_dim_source")
    else:
        logger.warning("No ads created in %s..%s, leaving %s untouched",
                        start_date, end_date, args["ICEBERG_TABLE_ADS"])

    job.commit()


if __name__ == "__main__":
    main()
