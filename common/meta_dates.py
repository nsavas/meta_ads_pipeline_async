"""Incremental date-range resolution shared by every job.

Run a job on a daily schedule with no START_DATE/END_DATE set, and it pulls
a rolling window of the past LOOKBACK_DAYS days (default 14), anchored on
yesterday -- today's stats are still accumulating in Meta's system, so we
don't pull a partial day. Re-pulling a 14-day window every run (instead of
just "yesterday") is deliberate: ad platforms commonly revise attributed
conversions for a date after the fact as click/view attribution windows
close, and each job's Iceberg MERGE INTO makes re-pulling overlapping days
safe -- it overwrites existing rows rather than duplicating them. Pass
explicit START_DATE/END_DATE for one-off backfills.
"""

from datetime import date, timedelta

from meta_config import DEFAULT_LOOKBACK_DAYS


def resolve_date_range(args: dict) -> tuple:
    """Return (start_date, end_date) as YYYY-MM-DD strings.

    If both START_DATE and END_DATE were passed as job parameters, use them
    verbatim (backfill mode). Otherwise compute the rolling LOOKBACK_DAYS
    window described above (incremental mode).
    """
    if args.get("START_DATE") and args.get("END_DATE"):
        return args["START_DATE"], args["END_DATE"]

    lookback_days = int(args.get("LOOKBACK_DAYS") or DEFAULT_LOOKBACK_DAYS)
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=lookback_days - 1)
    return start.isoformat(), end.isoformat()


def chunked(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]
