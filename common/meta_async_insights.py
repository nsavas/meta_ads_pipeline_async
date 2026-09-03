"""Asynchronous Ads Insights reporting, with batched submission/polling.

This replaces the synchronous `fetch_insights()` the sibling
meta_ads_pipeline project uses. Meta's own docs recommend async reporting
for large report volumes, and synchronous paginated pulls were tripping the
cost-based throttle ("Please reduce the amount of data you're asking for")
on this org's larger accounts. See ../README.md for the full rationale.

The flow, confirmed against Meta's Ads Insights best-practices doc
(https://developers.facebook.com/docs/marketing-api/insights/best-practices,
fetched 2026-09-01):

  1. POST /{ad_object}/insights with the query (level, fields, time_range,
     time_increment, breakdowns) -> {"report_run_id": 6023920149050}.
     Meta queues the report; nothing is returned inline.
  2. GET /{report_run_id}?fields=async_status,async_percent_completion until
     async_status reaches a terminal value. Documented values:
       Job Not Started / Job Started / Job Running  -- keep waiting
       Job Completed                                -- success
       Job Failed                                   -- bad query, don't retry blindly
       Job Skipped                                  -- expired, must resubmit
  3. GET /{report_run_id}/insights to read the rows, following normal
     cursor pagination.

Both step 1 (one submission per account) and step 2 (one status check per
run) are issued as Graph API *batch* calls -- up to meta_config.BATCH_SIZE
(50) sub-requests per HTTP round trip -- rather than one HTTP call per
account. With dozens of accounts that's the difference between dozens of
sequential round trips per phase and one or two.

Important: batching reduces HTTP round trips, NOT rate-limit consumption --
Meta counts each sub-request individually ("Each call within the batch is
counted separately for the purposes of calculating API call limits"). The
throttle relief here comes from *async* (Meta generates the report
server-side instead of us paginating a synchronous query), not from batching.

report_run_ids expire after 30 days per Meta's docs, so they're deliberately
not persisted anywhere -- each run submits fresh jobs.
"""

import json
import logging
import time
from urllib.parse import urlencode

from meta_config import (
    BATCH_SIZE,
    DEFAULT_PAGE_SIZE,
    GRAPH_API_BASE,
    MAX_POLL_SECONDS,
    POLL_INTERVAL_SECONDS,
)
from meta_http import batch_request, batched, get_all_pages

logger = logging.getLogger(__name__)

# async_status values that mean "stop waiting" -- everything else
# (Job Not Started / Job Started / Job Running) means keep polling.
STATUS_COMPLETED = "Job Completed"
STATUS_FAILED = "Job Failed"
STATUS_SKIPPED = "Job Skipped"
TERMINAL_STATUSES = {STATUS_COMPLETED, STATUS_FAILED, STATUS_SKIPPED}


def build_insights_params(level: str, start_date: str, end_date: str,
                           fields: list, breakdowns: list = None) -> dict:
    """The insights query itself -- identical in shape to what the
    synchronous pipeline sent, so both produce the same rows.

    level: "ad", "adset", or "campaign".
    time_increment=1 requests daily rows (one row per entity per day) rather
    than a single aggregate across the whole range.
    breakdowns: optional, e.g. ["age", "gender"] or ["comscore_market"] --
    each becomes part of the grain of the returned rows.
    """
    params = {
        "level": level,
        "fields": ",".join(fields),
        "time_range": json.dumps({"since": start_date, "until": end_date}),
        "time_increment": 1,
    }
    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)
    return params


def start_report_jobs(ad_account_ids: list, level: str, start_date: str, end_date: str,
                       access_token: str, fields: list, breakdowns: list = None) -> dict:
    """Submit one async insights report job per ad account, batched.

    Returns {ad_account_id: report_run_id}. Raises RuntimeError if any
    account's job fails to start -- a submission failure means that
    account's data would be silently missing from the table, which is worse
    than failing the run loudly (same fail-fast stance as the rest of this
    project).
    """
    body = urlencode(build_insights_params(level, start_date, end_date, fields, breakdowns))

    started = {}
    failures = []
    for chunk in batched(ad_account_ids, BATCH_SIZE):
        sub_requests = [
            {"method": "POST", "relative_url": f"{ad_account_id}/insights", "body": body}
            for ad_account_id in chunk
        ]
        for ad_account_id, result in zip(chunk, batch_request(sub_requests, access_token)):
            report_run_id = result["body"].get("report_run_id")
            if result["error"] or not report_run_id:
                failures.append((ad_account_id, result["error"] or "no report_run_id in response"))
                continue
            started[str(ad_account_id)] = str(report_run_id)

    if failures:
        detail = "; ".join(f"{acct}: {err}" for acct, err in failures)
        raise RuntimeError(f"Failed to start async insights job(s) for {len(failures)} account(s) -- {detail}")

    logger.info("Started %d async insights report job(s) at level=%s for %s..%s",
                len(started), level, start_date, end_date)
    return started


def poll_report_jobs(report_run_ids: list, access_token: str) -> dict:
    """Poll every report run (batched) until each reaches a terminal status.

    Returns {report_run_id: final async_status}. Raises TimeoutError if
    MAX_POLL_SECONDS elapses with runs still unfinished.

    A sub-request that errors during polling is treated as still-pending
    rather than fatal -- a transient failure reading status shouldn't kill a
    report that Meta is still happily generating. A genuinely stuck run is
    caught by the MAX_POLL_SECONDS backstop instead.
    """
    pending = [str(r) for r in report_run_ids]
    final = {}
    deadline = time.monotonic() + MAX_POLL_SECONDS
    round_number = 0

    while pending:
        round_number += 1
        still_pending = []
        for chunk in batched(pending, BATCH_SIZE):
            sub_requests = [
                {"method": "GET",
                 "relative_url": f"{report_run_id}?fields=async_status,async_percent_completion"}
                for report_run_id in chunk
            ]
            for report_run_id, result in zip(chunk, batch_request(sub_requests, access_token)):
                if result["error"]:
                    logger.warning("Status check failed for report run %s, will retry: %s",
                                    report_run_id, result["error"])
                    still_pending.append(report_run_id)
                    continue

                status = result["body"].get("async_status")
                if status in TERMINAL_STATUSES:
                    final[report_run_id] = status
                else:
                    still_pending.append(report_run_id)

        pending = still_pending
        logger.info("Poll round %d: %d job(s) finished, %d still running",
                    round_number, len(final), len(pending))

        if pending:
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"{len(pending)} async insights job(s) still unfinished after "
                    f"{MAX_POLL_SECONDS}s: {pending}"
                )
            time.sleep(POLL_INTERVAL_SECONDS)

    return final


def fetch_report_results(report_run_id: str, access_token: str) -> list:
    """Read a completed report's rows: GET /{report_run_id}/insights,
    following cursor pagination the same way every other list call does.
    """
    return get_all_pages(
        f"{GRAPH_API_BASE}/{report_run_id}/insights",
        {"limit": DEFAULT_PAGE_SIZE},
        access_token,
    )


def run_insights_reports(ad_account_ids: list, level: str, start_date: str, end_date: str,
                          access_token: str, fields: list, breakdowns: list = None) -> dict:
    """Full async cycle for every account: submit -> poll -> fetch.

    Returns {ad_account_id: [insight rows]}.

    Raises RuntimeError if any account's report ends in Job Failed or
    Job Skipped. Both are actionable rather than transient:
      - Job Failed means Meta rejected the query itself (bad field,
        unsupported breakdown combination, ...) -- retrying the identical
        request would just fail again.
      - Job Skipped means the run expired before being read, which means
        resubmitting, not waiting longer.
    Failing loudly here matches the rest of this project: a silently missing
    account is worse than a failed run.
    """
    run_ids_by_account = start_report_jobs(
        ad_account_ids, level, start_date, end_date, access_token, fields, breakdowns
    )
    statuses = poll_report_jobs(list(run_ids_by_account.values()), access_token)

    bad = [
        (ad_account_id, run_id, statuses.get(run_id))
        for ad_account_id, run_id in run_ids_by_account.items()
        if statuses.get(run_id) != STATUS_COMPLETED
    ]
    if bad:
        detail = "; ".join(f"{acct} (run {run_id}): {status}" for acct, run_id, status in bad)
        raise RuntimeError(f"{len(bad)} async insights job(s) did not complete -- {detail}")

    rows_by_account = {}
    for ad_account_id, report_run_id in run_ids_by_account.items():
        rows = fetch_report_results(report_run_id, access_token)
        rows_by_account[ad_account_id] = rows
        logger.info("Ad account %s: fetched %d insight row(s) from report run %s",
                    ad_account_id, len(rows), report_run_id)

    return rows_by_account
