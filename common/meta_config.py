"""Shared constants for the Meta Marketing API (Graph API) clients."""

# Pin the API version explicitly rather than using the unversioned endpoint --
# Meta retires old versions on a schedule, and an unversioned call silently
# rides whatever the current default is. Bump this deliberately, not by
# accident. Verified against Meta's live docs as the current version on
# 2026-08-19; confirm you're not about to hit a deprecation window before
# deploying (https://developers.facebook.com/docs/graph-api/changelog).
GRAPH_API_VERSION = "v25.0"
GRAPH_API_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

# Page size for ID-only / narrow-field paginated calls (account discovery,
# common/meta_accounts.py's list_entity_ids(), and paging through a finished
# async report's results).
DEFAULT_PAGE_SIZE = 100

# Page size for *full-object* listings -- da_mkt_meta_dimension_datalake.py's
# three list_entities() calls, which request every field on the Campaign
# (39 fields), AdSet (62), and Ad (34) objects, several of which are
# non-trivial nested objects (promoted_object, issues_info, source_campaign,
# targeting, creative, ...). Confirmed in production (2026-08-20) that
# requesting DEFAULT_PAGE_SIZE (100) full objects per page, across every
# page of a 100+ campaign account, triggers Meta's Marketing API cost-based
# throttle: "Please reduce the amount of data you're asking for, then retry
# your request" -- returned as an HTTP 500, not a 429, and NOT resolved by
# simply retrying the same request (the retry logic in meta_http.py backs
# off and retries, but Meta is asking for a smaller request, not just a
# later one). A single request for this account succeeded once, then
# failed identically on an immediate retry -- confirming this is a
# request-cost throttle, not a data/permissions issue with a specific
# campaign. Keep this well below DEFAULT_PAGE_SIZE. On very large accounts
# (5,500+ ads), this alone wasn't enough -- see
# da_mkt_meta_dimension_datalake.py's note on the heavy nested fields
# trimmed from AD_FIELD_SPECS, which is what actually resolved it there:
# the throttle behaves like a budget on total data pulled, so a smaller
# page size only spreads the same total cost across more requests.
DETAIL_PAGE_SIZE = 25

# Default width of the rolling incremental pull when a job isn't given
# explicit START_DATE/END_DATE. See meta_dates.py for why 14 days.
DEFAULT_LOOKBACK_DAYS = 14

# -- Async insights + batch request tuning ---------------------------------
# The five performance jobs submit their insights queries as *asynchronous*
# report jobs (POST /{ad_account_id}/insights -> report_run_id, poll for
# completion, then fetch results), not synchronous paginated GETs. See
# meta_async_insights.py for the flow and README.md for why.

# Max sub-requests per batch call. Meta's documented hard limit is 50
# ("Batch requests are limited to 50 requests per batch"), so treat this as
# a ceiling, not a knob to raise. Note each sub-request still counts
# individually toward rate limits -- batching saves HTTP round trips and
# wall-clock time, not quota.
BATCH_SIZE = 50

# How long to wait between polling rounds while async report jobs run.
# Meta's best-practices doc is explicit that firing many /insights queries
# at once is what triggers rate limiting ("Sending several queries at once
# are more likely to trigger our rate limiting. Try to spread your
# /insights queries by pacing them with wait time in your job"), so polling
# here is deliberately unhurried -- the report generation itself is the slow
# part, and polling faster doesn't make Meta finish sooner.
POLL_INTERVAL_SECONDS = 15

# Give up on a run of async jobs after this long. A large multi-account
# report legitimately takes minutes; this is a backstop against a job that
# never advances past "Job Running", so the Glue job fails loudly instead of
# polling forever and burning DPU-hours.
MAX_POLL_SECONDS = 3600

# HTTP retry/backoff defaults for meta_http.py's request_with_backoff().
# Meta also exposes proactive rate-limit signals (X-Business-Use-Case-Usage /
# X-Ad-Account-Usage response headers) that a high-volume production job
# should read and throttle against pre-emptively; this reactive retry-on-429
# baseline mirrors the Pinterest pipeline's approach and is not a substitute
# for that if you're running at scale.
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2
