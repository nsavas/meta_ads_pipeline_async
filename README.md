# Meta Ads → Iceberg (AWS Glue) — async reporting

Six Glue jobs that pull data from Meta's Marketing API (Graph API) and write
it into Iceberg tables in S3.

**This is the async variant of `meta_ads_pipeline`.** The five performance
jobs submit their insights queries as *asynchronous report jobs* (submit →
poll → fetch) instead of synchronous paginated GETs, and batch their
submissions and status polls into Graph API batch calls. The dimensions job
is unchanged and still synchronous. See "Why async, and what batching does
(and doesn't) buy" below — that section is the reason this project exists.
Everything else (schemas, field lists, gated-field workarounds, DMA caveat,
Iceberg write patterns) carries over from the sibling project unchanged.

Also a sibling to `pinterest_ads_pipeline` — see "Differences from the
Pinterest pipeline" below before assuming anything carries over from there.

- **Three performance jobs** at the ad, ad set, and campaign level, all
  async. ("Ad set" is Meta's name for what Pinterest calls an "ad group.")
- **One geographic breakdown job**, at the ad level only, async, giving
  ad-level performance broken down by market (see "DMA is retired -- read
  this before deploying" below; this is the single most important caveat in
  this whole project).
- **One demographic breakdown job**, at the ad level only, async, giving
  ad-level performance broken down by age x gender as a combined cross-tab
  in one table (see "Age/gender is one combined table" below).
- **One dimensions job**, **synchronous**, that pulls the full Campaign/Ad
  Set/Ad metadata objects (name, status, budget, targeting, creative -- no
  metrics) into three tables in a single run. Async reporting is an
  Insights-API-only mechanism; the object-listing edges this job uses have
  no async equivalent, so it keeps the synchronous pull plus the
  created_time filtering and cost-trimming already worked out for it.

All six share one `common/` library for everything that isn't job-specific:
access-token handling, ad-account/entity discovery, HTTP retry/backoff +
cursor pagination + batch requests, async report orchestration, incremental
date-range resolution, the Iceberg write helpers, and the schema-building
helper described below.

## Layout

```
meta_ads_pipeline_async/
├── common/                        # shared modules, zipped flat for --extra-py-files (see "Deploying")
│   ├── meta_config.py              constants (API version/base URL, page sizes, retry/backoff, lookback, async+batch tuning)
│   ├── meta_auth.py                Secrets Manager access-token read (no refresh flow -- see "Auth" below)
│   ├── meta_http.py                retry/backoff wrapper + cursor-pagination follower (get_all_pages) + batch_request()
│   ├── meta_accounts.py            ad account discovery (/me -> assigned_ad_accounts) + generic entity pager
│   ├── meta_async_insights.py      async report orchestration: submit -> poll -> fetch, batched (replaces meta_analytics.py)
│   ├── meta_schema.py              build_table(): derives Spark SCHEMA + Iceberg DDL + row-builder from one field-spec list
│   ├── meta_dates.py               rolling-window date-range resolution + chunking
│   └── meta_glue_args.py           getResolvedOptions wrapper that supports optional args
├── jobs/
│   ├── meta_ads_to_iceberg_glue_job.py                 async
│   ├── meta_campaigns_to_iceberg_glue_job.py           async
│   ├── meta_ad_sets_to_iceberg_glue_job.py             async
│   ├── meta_ads_dma_to_iceberg_glue_job.py             async -- ad-level performance broken down by geographic market
│   ├── meta_ads_demographics_to_iceberg_glue_job.py    async -- ad-level performance broken down by age x gender
│   └── meta_dimensions_to_iceberg_glue_job.py          SYNC  -- campaign/ad set/ad metadata (no metrics)
├── build_deps.ps1 / build_deps.sh   zip common/'s contents (flat) for --extra-py-files
├── requirements.txt
└── README.md
```

Each file in `jobs/` only declares what's specific to it: which fields to
request, the breakdown (if any), and the merge key. Everything else is
imported from the `meta_*` modules in `common/`.

**Same flat-module structure as the Pinterest project, for the same reason.**
`common/` is zipped with the `.py` files directly at the root -- no wrapping
`common` folder, no `__init__.py`. That's not a stylistic choice; it's a
requirement discovered the hard way in the sibling project: the more
"obvious" `common` package structure (which AWS's own docs describe as
correct) hit `ModuleNotFoundError: No module named 'common'` on a real Glue
job run, matching a
[known unresolved AWS Glue issue](https://github.com/awslabs/aws-glue-libs/issues/173)
with zipimport + `--extra-py-files`. This project starts from the working
structure rather than repeating that discovery.

## Deploying

```bash
./build_deps.sh          # or build_deps.ps1 on Windows
aws s3 cp meta_common.zip s3://<your-bucket>/meta_common.zip
```

For **each** of the six Glue jobs, upload the corresponding script from
`jobs/` as the job's script, and set these job parameters:

```
--datalake-formats iceberg
--additional-python-modules requests>=2.31.0
--extra-py-files s3://<your-bucket>/meta_common.zip
```

Then the job-specific arguments (see the docstring at the top of each script
in `jobs/` for the full list):

| Parameter | Required | Notes |
|---|---|---|
| `--SECRET_NAME` | yes | Secrets Manager secret: `{"access_token": "..."}` (a Meta System User token -- see "Auth" below) |
| `--AWS_REGION` | yes | e.g. `us-east-1` |
| `--ICEBERG_CATALOG` | yes | Glue Data Catalog name registered as an Iceberg catalog |
| `--ICEBERG_DATABASE` | yes | target database |
| `--ICEBERG_TABLE` | yes\*\* | target table (different per job -- see below) |
| `--ICEBERG_WAREHOUSE_PATH` | yes | `s3://bucket/prefix` |
| `--AD_ACCOUNT_IDS` | no* | comma-separated allowlist (with or without "act_" prefix); omit to auto-discover every account the token can see |
| `--START_DATE` / `--END_DATE` | no* | explicit backfill range; omit for the rolling incremental window |
| `--LOOKBACK_DAYS` | no* | default 14; width of the rolling window when dates are omitted |

\* All six jobs take all three optional date args. `meta_dimensions_to_iceberg_glue_job.py`
started out with none of them (it's not time-series data), but as of
2026-09-01 it also filters by created_time using this same rolling-window/
backfill mechanism -- see "created_time filtering + upsert" below for why,
and for an important backfill note before relying on the default window.

\*\* `meta_dimensions_to_iceberg_glue_job.py` writes three tables in one
run, so instead of a single `--ICEBERG_TABLE` it takes three:
`--ICEBERG_TABLE_CAMPAIGNS`, `--ICEBERG_TABLE_AD_SETS`, `--ICEBERG_TABLE_ADS`.

Suggested table names, one per job:

| Job | Table(s) |
|---|---|
| `meta_ads_to_iceberg_glue_job.py` | `meta_ad_performance` |
| `meta_campaigns_to_iceberg_glue_job.py` | `meta_campaign_performance` |
| `meta_ad_sets_to_iceberg_glue_job.py` | `meta_ad_set_performance` |
| `meta_ads_dma_to_iceberg_glue_job.py` | `meta_ad_market_performance` |
| `meta_ads_demographics_to_iceberg_glue_job.py` | `meta_ad_demographics_performance` |
| `meta_dimensions_to_iceberg_glue_job.py` | `meta_campaign_dim`, `meta_ad_set_dim`, `meta_ad_dim` |

Whenever `common/` changes, re-run `build_deps.sh`/`.ps1` and re-upload the
zip -- Glue doesn't pick up changes to an S3 object automatically on its
own, you're re-deploying the same object key.

## Auth

Built against a Meta **System User access token** (generated once in
Business Manager), not a standard OAuth User/Page token. That choice
matters structurally: System User tokens don't expire, so `meta_auth.py` is
just "read a static token from Secrets Manager" -- there's no refresh flow
the way `pinterest_auth.py` has for Pinterest's OAuth token. If you end up
using a standard expiring token instead, `meta_auth.py` is the one place
that needs to grow a refresh step; see `pinterest_auth.py` for the shape
that takes.

Account discovery follows from the same choice: `GET /me` identifies the
System User itself (its `id` *is* the System User's ID for this token
type), then `GET /{system_user_id}/assigned_ad_accounts` lists every ad
account it's been assigned. No `AD_ACCOUNT_IDS`-equivalent required
parameter, same auto-discovery-by-default pattern as the Pinterest project.

**The token is sent as an `access_token` query parameter, not an
`Authorization: Bearer` header.** An earlier version of `meta_http.py` used
the header, which produced HTTP 400 errors on every call. Every example in
Meta's own docs (the general Graph API guide, the Insights guide, and the
breakdowns reference, all checked on 2026-08-20) uses the query-parameter
form exclusively -- none show a Bearer header. Meta's Graph API also
surfaces access-token problems as HTTP 400 (`OAuthException`), not 401,
which is why a bad-auth failure here looks like a malformed request rather
than an auth error. If you're debugging a 400 on any job, this is the first
thing to check -- confirm `access_token` actually appears in the outgoing
query string (`meta_http.get_all_pages()` and `meta_accounts.get_self_id()`
are the only two places that attach it).

## Why async, and what batching does (and doesn't) buy

This is the difference between this project and `meta_ads_pipeline`.

**The problem.** The synchronous pipeline fetched insights with paginated
`GET /{ad_account_id}/insights` calls. On this org's larger accounts that
kept tripping Meta's cost-based throttle -- HTTP 500 with *"Please reduce
the amount of data you're asking for, then retry your request"* -- which
retrying doesn't fix, because Meta is asking for a smaller request, not a
later one (see "The dimensions job's 500 error" below for the full
diagnostic history; that investigation is what motivated this rewrite).
The available synchronous levers (smaller pages, pacing between pages)
only redistribute the same total cost across more requests, or add
wall-clock time proportional to page count, which gets expensive fast when
one account has 5,500+ ads.

**The fix: let Meta do the work server-side.** Meta's Ads Insights
best-practices doc recommends async reporting exactly for this situation.
Instead of us paginating a live query, we ask Meta to *generate a report*
and tell us when it's ready:

1. `POST /{ad_account_id}/insights` with the query (level, fields,
   time_range, time_increment, breakdowns) → `{"report_run_id": ...}`.
   Nothing is returned inline; Meta queues the work.
2. `GET /{report_run_id}?fields=async_status,async_percent_completion`
   until `async_status` reaches a terminal value. Documented values:
   `Job Not Started` / `Job Started` / `Job Running` (keep waiting),
   `Job Completed` (success), `Job Failed` (bad query -- don't retry
   blindly), `Job Skipped` (expired -- resubmit).
3. `GET /{report_run_id}/insights` to read the rows, with normal cursor
   pagination.

All of this lives in `common/meta_async_insights.py`; the job scripts just
call `run_insights_reports(...)` and get back `{ad_account_id: [rows]}`.
The query shape it sends is byte-for-byte what the synchronous version
sent, so the resulting tables are identical.

**Batching, and an honest note on what it does.** Steps 1 and 2 are one
call per account, which is a lot of sequential HTTP round trips across
dozens of accounts. Both are issued as Graph API batch calls instead --
up to 50 sub-requests per HTTP request (`meta_config.BATCH_SIZE`; 50 is
Meta's documented hard limit, not a tuning knob). 120 accounts submit in 3
HTTP calls rather than 120.

But be clear about what that buys: **batching saves HTTP round trips and
wall-clock time, not rate-limit quota.** Meta's own batch docs state that
*"Each call within the batch is counted separately for the purposes of
calculating API call limits."* If you're reading this because you're being
throttled, batching alone is not the fix -- async is. Batching is a
latency/efficiency win layered on top.

Batch responses need care, and `meta_http.batch_request()` handles it:
- The overall batch returns HTTP 200 even when individual sub-requests
  fail; each entry carries its own `code`, so every caller must check
  per-entry status rather than assuming success.
- Each entry's `body` arrives as a JSON *string*, needing a second parse.
- A `null` entry means that sub-request timed out inside the batch.

**Polling pace.** `POLL_INTERVAL_SECONDS` (15) between rounds, with a
`MAX_POLL_SECONDS` (3600) backstop so a permanently-stuck report fails the
Glue job loudly instead of polling forever on billable DPUs. Meta's docs
are explicit that firing many `/insights` queries at once is itself a
rate-limiting trigger, and polling faster doesn't make a report finish
sooner.

**Failure handling is fail-fast**, consistent with the rest of the project:
a submission that doesn't return a `report_run_id`, or a report ending in
`Job Failed`/`Job Skipped`, raises with the offending account IDs and
Meta's own error detail. Both are actionable rather than transient (a bad
query won't fix itself on retry; an expired run needs resubmitting), and a
silently missing account is worse than a failed run.

## No OpenAPI spec for Meta

Pinterest publishes a real OpenAPI spec on GitHub
(https://github.com/pinterest/api-description) that every field/column name
in the sibling project was mechanically verified against. **Meta publishes
no equivalent.** Every field name, type, and endpoint behavior in this
project was instead verified by fetching Meta's live HTML reference docs
(`developers.facebook.com/docs/marketing-api/reference/...`) on 2026-08-19 --
cited with a fetch date in each file's docstring, the same discipline as
the Pinterest spec citations, just a different kind of source. If you add
fields later, re-fetch the relevant reference page rather than trust
memory or a third-party blog post -- see the DMA situation below for
exactly why that matters.

## DMA is retired -- read this before deploying

This is the most important caveat in the whole project, because you told me
DMA is the most important breakdown.

Pinterest's ad-level DMA breakdown (`pinterest_ads_dma_to_iceberg_glue_job.py`)
maps directly onto Nielsen's classic media markets. Meta had the same
concept -- a `dma` breakdown value on the Insights API -- but the evidence
gathered while building `meta_ads_dma_to_iceberg_glue_job.py` was genuinely
mixed:

- Meta's own live breakdowns reference page (fetched 2026-08-19) still
  lists `dma` as a valid breakdown, with no deprecation notice.
- Four independent production ETL vendors -- **Fivetran, Airbyte, Rivery,
  and Supermetrics** -- all separately confirm the Nielsen `dma` breakdown
  stopped returning results on **2026-06-22**, two months before this
  project was built, replaced by a new `comscore_market` breakdown.
  Rivery's changelog states it plainly: "Meta has fully retired Nielsen DMA
  across its reporting... choose Comscore Market to retain geographic-level
  reporting."

Convergent, independent operational evidence from four production vendors
(who'd have discovered this from real API calls actually failing) outweighs
a docs page that may simply not have been updated yet. **`meta_ads_dma_to_iceberg_glue_job.py`
defaults to `comscore_market`** (the `GEO_BREAKDOWN` constant at the top of
that file), not legacy `dma`.

**Before you rely on this table**, run the job once against a real account
and confirm two things neither source above could settle from documentation
alone:
1. Whether `comscore_market` values come back as human-readable market
   names or as bare IDs needing a lookup table (Pinterest's DMA breakdown
   needed one; Meta's may not -- unconfirmed).
2. That `comscore_market` is actually enabled and populated for your ad
   accounts -- Comscore Markets rolled out for automotive-vertical ads
   first per some of the source material, and account-level availability
   for other verticals (insurance, this project's actual use case) wasn't
   independently confirmed.

If your account still returns data for legacy `dma`, switching back is a
one-constant change (`GEO_BREAKDOWN = "dma"` in that file) -- see the
job's docstring for the full writeup.

## Age/gender is one combined table

Unlike Pinterest, where `GENDER` and `AGE_BUCKET` are independent
breakdowns of the same total (summing across both silently doubles every
metric -- see the sibling project's `pinterest_ads_gender_to_iceberg_glue_job.py`/
`pinterest_ads_age_to_iceberg_glue_job.py` for that whole story), Meta's
breakdowns reference docs explicitly list `age+gender` as a **permitted
combination** -- a true cross-tab. `meta_ads_demographics_to_iceberg_glue_job.py`
requests both in one call and writes one table with grain
`(ad_id, date, age, gender)`. Every row is already scoped to one
`(age, gender)` pair, so `SUM(spend)` for an `(ad_id, stat_date)` just
works -- no discriminator column, no double-counting footgun to document.

## Meta's error detail is preserved in raised errors

Every job loops over every discovered ad account and makes at least one API
call per account; a single account's request error (a 500, a timeout, a
permission problem -- anything `requests` raises) propagates straight out
of `main()` and aborts the run, the same way an unhandled exception would
anywhere else in the pipeline.

What *is* handled is the quality of that error's message. `requests`'
default `HTTPError` message (`"500 Server Error: Internal Server Error for
url: ..."`) never includes the response body, but Meta's Graph API almost
always returns a structured `{"error": {"message", "code", "error_subcode",
"fbtrace_id", ...}}` JSON payload even on a 500. `meta_http.py`'s
`describe_meta_error()` extracts that and appends it to whatever gets
raised, so a job's logs/traceback show Meta's actual message and
`fbtrace_id` instead of just the generic HTTP reason phrase -- the
difference between "500 Server Error" and knowing what Meta is actually
complaining about.

`describe_meta_error()` runs on *every* response, successes included, so it
has to tolerate every body shape the API returns -- notably batch
responses, whose body is a JSON **array** of per-sub-request results rather
than an object. (The synchronous project's version assumed a dict and
would raise `AttributeError: 'list' object has no attribute 'get'` on the
first batch call; caught here by the stub tests before it ever ran.)
Per-sub-request errors inside a batch are surfaced separately by
`batch_request()`, which formats the same fields into each entry's `error`
string.

## The dimensions job's 500 error, and why it's page-size, not data or auth

Root cause, found live in production on 2026-08-20: requesting the full
34-62-field object for `DEFAULT_PAGE_SIZE` (100) entities per page, across
every page of an account with 100+ campaigns, trips Meta's Marketing API
cost-based throttle. Meta's own message: **"Please reduce the amount of data
you're asking for, then retry your request."** Confirmed by reproducing it
directly: the identical request succeeded once in Postman, then failed with
that exact message on an immediate re-run of the same request. That rules out
a data problem with a specific campaign, a permissions problem, and a
query-format problem -- three hypotheses chased and eliminated in that order
before this one:
1. **Not auth** -- same System User token in Postman and the job.
2. **Not the field list** -- Postman succeeded requesting all 39 fields,
   `budget_rebalance_flag` (a field flagged deprecated since Marketing API
   v7.0) included.
3. **Not a specific bad record on a later page** -- the failure reproduced
   on the *same* request run twice, not on a *different* page/cursor.
4. **Is a request-cost throttle** -- confirmed by Meta's own error message,
   which explicitly asks for less data per request, not a retry of the same
   request. This is also why `request_with_backoff()`'s retry-with-backoff
   didn't help on its own: it retries the identical request, which is
   exactly what Meta's message says not to do.

Fix: `meta_config.DETAIL_PAGE_SIZE` (25, vs. the default 100) for
`meta_dimensions_to_iceberg_glue_job.py`'s three full-object listings only --
the performance jobs' ID-only/insights calls stay at the default, since
they're far cheaper per object and weren't implicated.

If a much larger account still trips this after that change (confirmed at
5,500+ ads -- see "Ad's heavy nested fields are trimmed" below), don't reach
for `DETAIL_PAGE_SIZE` first: the throttle behaves like a time-windowed cost
budget on *total* data pulled, not a flat per-request cap, so shrinking the
page size further just redistributes the same total cost across more
requests rather than reducing it -- and at large enough page counts, more
requests means more wall-clock time for no real benefit (5,500+ ads at
`DETAIL_PAGE_SIZE=25` is already 220+ pages). Trimming which fields are
actually requested is the lever that reduces total cost. (An earlier version
of this fix also added a fixed pause between pages, `PAGE_PACING_SECONDS`;
it was removed once the field-trimming fix below addressed the underlying
cost directly -- a fixed per-page sleep doesn't scale well against accounts
with hundreds of pages, and running the day-to-day retry/backoff loop
already absorbs the occasional throttle without a constant tax on every run.)

## AdSet's `contextual_bundling_spec` field requires a Gatekeeper flag

A second, unrelated 500 turned up on the ad set dimensions pull after the
above fix, on accounts with 100+ ad sets. It looked like the same
cost-based throttle at first, but reproducing the exact `AD_SET_FIELD_SPECS`
field list directly in Postman (2026-08-30) returned a completely different
error: **`(#3) AdAccount must pass GK: contextual_bundle_test_api_accounts`**
-- a permission/feature-gate exception, not a throttle. `contextual_bundling_spec`
is documented on Meta's AdSet object reference, but it's gated behind a
Gatekeeper flag that only accounts enrolled in that specific beta program
have; requesting it for any other account fails outright, no matter the
page size or pacing.

Fix: `contextual_bundling_spec` was dropped from
`meta_dimensions_to_iceberg_glue_job.py`'s `AD_SET_FIELD_SPECS` (62 fields
now, not 63) -- there's no way to request it unconditionally for every
account. If your accounts are confirmed enrolled in that program, it can be
added back; see the field-spec comment in that job for the one-line change.

## Ad's `special_ad_categories` field requires separate app review

A third gated field, same shape as the two above: the ad dimensions pull
also started failing on accounts with 100+ ads, this time with
**`(#3) App must be on the whitelist`**. Meta's error gives no field name,
so this one was isolated by bisecting `AD_FIELD_SPECS` in Postman --
repeatedly halving the field list and testing each half until a single
field remained: `special_ad_categories`. It's tied to Meta's Special Ad
Category program (required for housing/employment/credit/social-issue ads),
which gates API access behind separate app review, independent of the
Gatekeeper mechanism behind AdSet's `contextual_bundling_spec`. Same
practical effect either way: a hard permission failure for any
non-enrolled app, unconditionally across every account.

`creative_asset_groups_spec` was also suspected during this investigation
(it's the newest/most beta-flavored field on the Ad object, and the closest
parallel to `contextual_bundling_spec`) but was confirmed clean by testing
it removed-alone (still failed) and then included-alongside every other
field with only `special_ad_categories` also removed (succeeded) -- it's
requested normally.

Fix: `special_ad_categories` was dropped from
`meta_dimensions_to_iceberg_glue_job.py`'s `AD_FIELD_SPECS` (38 fields now,
not 39). If your app is confirmed enrolled in the Special Ad Category
program, it can be added back; see the field-spec comment in that job for
the one-line change.

## Ad's heavy nested fields are trimmed to cut request cost

Once the two gated fields above were removed, the dimensions job still
recurringly hit the cost-based throttle (the same 500, "Please reduce the
amount of data you're asking for") on the largest accounts -- 5,500+ ads,
confirmed in production on 2026-08-30 -- even at `DETAIL_PAGE_SIZE` (25).
Unlike the campaign/ad-set case, the fix here isn't a smaller page size or
more pacing: at that scale (220+ pages just for the ads pull on one such
account), either change trades reliability for a lot of extra wall-clock
time without addressing the actual problem -- the throttle looks like a
time-windowed budget on *total* data pulled, and total data pulled doesn't
change just because it's split into more/slower requests.

The lever that actually helps is cutting the payload itself.
`targeting`, `tracking_and_conversion_with_defaults`, `tracking_specs`, and
`issues_info` were dropped from `AD_FIELD_SPECS` -- they're not
permission-gated like `special_ad_categories`, just large/deeply-nested
objects that add real weight to every row of a full-account pull. `targeting`
in particular can be a substantial object (geo, interest, behavior, custom
audience targeting all nested together). None of the four are needed for
the current use case; if you need one later, consider a narrower per-ad
follow-up call instead of carrying it on every row of a 5,500+-row pull.

Fix: those four fields were dropped from
`meta_dimensions_to_iceberg_glue_job.py`'s `AD_FIELD_SPECS` (34 fields now,
not 38). See the field-spec comment in that job to add any back if a future
use case needs one -- just be aware of the cost tradeoff on large accounts.

## created_time filtering + upsert, not a full snapshot (2026-09-01)

To bound request volume further on very large accounts, the dimensions job
now filters each entity type by its own `created_time`, using the same
rolling `LOOKBACK_DAYS` (default 14) / explicit `START_DATE`/`END_DATE`
mechanism the five performance jobs already use (`meta_dates.resolve_date_range()`).
This is a genuinely different mechanism from `date_preset`/`time_range`
(see "The dimensions job's 500 error" above) -- confirmed against Meta's
docs before implementing that those two only scope computed stats fields
on this edge, not which objects come back. The Marketing API's `filtering`
parameter is the one that actually filters returned entities: each call now
sends `filtering=[{"field": "<entity>.created_time", "operator":
"GREATER_THAN", "value": <epoch - 1>}, ...]` (and the LESS_THAN counterpart,
`+1`, for the window's end), built by `_created_time_filter()` in the job.
Uses strict `GREATER_THAN`/`LESS_THAN`, not `_OR_EQUAL` -- confirmed in
production (2026-09-01) that `GREATER_THAN_OR_EQUAL` is rejected as an
unsupported operator on this field/edge; the +/-1 second adjustment on the
boundary values keeps the window inclusive despite the strict operators.
**The exact field-name convention (`"campaign.created_time"` /
`"adset.created_time"` / `"ad.created_time"`) is still based on community/SDK
examples, not Meta's official per-edge reference page** -- confirm on your
first real run that the filter is actually narrowing results (check the
fetched counts against what you'd expect) rather than being silently
ignored by the API.

This forced a second, more consequential change: `meta_dimensions_to_iceberg_glue_job.py`
used to do a full `CREATE OR REPLACE TABLE` every run specifically so a
campaign/ad set/ad deleted on Meta's side would disappear from the table
(see "Ad's heavy nested fields are trimmed" above and the job's own
docstring history). With created_time filtering in place, a full replace
every run would instead wipe out every entity *older* than the current
window -- the opposite failure mode, and a worse one, since it would delete
rows for entities that are still active. All three tables now use
`upsert()` (MERGE INTO, same mechanism the performance jobs use) instead.

**This reintroduces the exact problem replace_table() was chosen to solve
in the first place**: an entity deleted on Meta's side no longer disappears
from the table -- it lingers with whatever data was captured on its last
successful pull. Filter on `effective_status`/`configured_status`
downstream (e.g. exclude `"DELETED"`/`"ARCHIVED"`) if stale entities need
to be excluded from queries against these tables.

**Backfill warning, read before relying on the default window:** an entity
created before the current `LOOKBACK_DAYS` window will never be pulled by
an incremental run, full stop -- unlike the performance jobs' daily metrics
(which are meaningless before an entity exists but never disappear once
captured), a campaign/ad set/ad's *existence* is itself time-gated by this
filter. Run this job at least once with a `START_DATE` far enough back to
cover every entity you care about (or an equivalently large `LOOKBACK_DAYS`)
to seed the tables before switching to the incremental default -- otherwise
an older, still-active entity that no run's window has ever covered will
simply never appear in these tables.

## Differences from the Pinterest pipeline

- **Auth**: static System User token, no refresh flow (see "Auth" above).
- **No entity-ID batching needed for performance data.** Pinterest's
  analytics endpoints require listing every entity's ID first, then batching
  IDs into groups for the analytics call. Meta's `/{ad_account_id}/insights`
  edge accepts a `level` parameter (`ad`/`adset`/`campaign`) and covers
  *every* entity at that level for the whole account in one report --
  confirmed against Meta's docs on 2026-08-19. Simpler architecture; see
  `meta_async_insights.py`'s docstring.
- **Async reporting.** Pinterest's analytics calls are synchronous. Meta
  offers (and, for large volumes, recommends) an async report flow, which
  this project uses for all five performance jobs -- see "Why async, and
  what batching does (and doesn't) buy" above. Note this is an
  Insights-API-only mechanism: the object-listing edges the dimensions job
  uses have no async equivalent, so that job stays synchronous.
- **Conversions/leads aren't flat named columns.** Pinterest has `LEADS`/
  `TOTAL_CONVERSIONS` as dedicated fields. Meta bundles every conversion
  type into one `actions` field -- a list of `{action_type, value}` pairs,
  where the meaningful `action_type` (native Lead Ad form fill vs. a Meta
  Pixel "Lead" event vs. a purchase, etc.) depends on how each campaign is
  configured. Rather than guess which `action_type` is "the" lead metric
  for every campaign, `actions` and `cost_per_action_type` (and every
  `video_p*_watched_actions` field, which are the *same* list-of-pairs
  shape) are stored as JSON, same treatment the dimension tables give any
  field whose shape isn't a stable flat scalar. Query the specific
  `action_type` that matters for a given campaign with `from_json`/
  `json_extract`.
- **`build_table()` is shared infrastructure, not duplicated per job.**
  The Pinterest dimensions job hand-writes its `SCHEMA`/`ICEBERG_COLUMNS`/
  `to_row()` per entity. Meta's dimension entities are larger (up to 64
  fields for AdSet, vs. Pinterest's largest at 40), so `meta_schema.py`'s
  `build_table()` derives all three from one field-spec list instead,
  making a transposition bug structurally impossible rather than something
  to catch by testing afterward. Every performance job uses the same
  helper for its (smaller) row shape too, for consistency.
- **Datetime fields are ISO-8601 strings**, not Unix-seconds integers the
  way Pinterest's `created_time`/`updated_time` are. Confirmed from the
  fetched reference pages' `datetime` type annotation. Stored as the raw
  string and cast via SQL at merge time, same technique used for
  `stat_date` in every performance job.
- **`time_range` is interpreted in the ad account's own timezone, not UTC.**
  Pinterest's `start_date`/`end_date` are explicitly UTC. Meta's Insights API
  interprets `since`/`until` in whatever timezone the ad account itself is
  set to -- confirmed while researching the 400-error auth fix above, not
  something assumed going in. `resolve_date_range()` computes dates off
  `date.today()` (server/UTC time) the same way the Pinterest jobs do, so an
  ad account running in a non-UTC timezone will see its "yesterday" boundary
  shift by the timezone offset relative to what this job actually requests.
  Not a correctness bug for a 14-day rolling window -- a day or so of skew
  at the edges of the window self-corrects on the next run via the `MERGE
  INTO` -- but worth knowing if you need exact per-account-timezone-day
  alignment.
